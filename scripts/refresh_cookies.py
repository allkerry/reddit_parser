#!/usr/bin/env python3
"""
scripts/refresh_cookies.py

Независимый (от event loop/процессов scraper) Playwright-джоб. Раз в
запуск (расписание — снаружи, см. README.md: systemd timer/cron), для
каждого enabled: true аккаунта из accounts.yaml:

  1. открывает браузерный контекст через ТОТ ЖЕ proxy_port, что и сам
     скрапер для этого аккаунта (mihomo, тот же выходной IP — логин и
     последующие запросы с разных IP резко повышают риск на аккаунт);
  2. переиспользует сохранённую с прошлого раза Playwright-сессию
     (storage_state/account_N.json), либо конвертирует на лету текущий
     cookies/account_N.json, если сохранённой сессии ещё нет;
  3. проверяет, залогинены ли — по ПОЗИТИВНОМУ признаку (ответ
     авторизованного API-эндпоинта Reddit), а не по отсутствию формы
     логина в DOM (та может ложно отсутствовать на промежуточных
     состояниях загрузки страницы);
  4. если НЕ залогинены — cookies/account_N.json НЕ трогает, логирует
     ERROR и переходит к следующему аккаунту. Логин по паролю этот
     скрипт сознательно не автоматизирует (высокий риск капчи/детекта
     на датацентровом IP) — это ручная операция;
  5. если залогинены — сохраняет storage_state/account_N.json (для
     переиспользования в следующий раз) и АТОМАРНО (os.replace())
     перезаписывает cookies/account_N.json в формате, который уже
     понимает scraper.config.load_cookies() (Cookie Editor export);
  6. сверяет фактический navigator.userAgent браузера с
     accounts.yaml:user_agent этого аккаунта — при расхождении WARNING
     в лог (accounts.yaml НЕ трогается автоматически).

Полностью независим от scraper/ (main.py, account_worker) — читает те
же accounts.yaml/cookies/*.json, что и они, но не импортирует и не
трогает их event loop. Компоненты делят только сам файл
cookies/account_N.json: запись сюда атомарна (os.replace()), поэтому
account_worker, параллельно читающий тот же файл через свой hot-reload
(scraper/config.py:CookieFileWatcher), никогда не увидит частично
записанный/битый JSON.

Установка (отдельно от requirements.txt основного скрапера — падение/
зависание этого джоба не должно требовать пересборки образа scraper'а):

    python3 -m venv .venv-refresh
    .venv-refresh/bin/pip install -r scripts/requirements-refresh.txt
    .venv-refresh/bin/playwright install chromium

Запуск:

    python3 scripts/refresh_cookies.py                    # все enabled-аккаунты
    python3 scripts/refresh_cookies.py --account account_1
    python3 scripts/refresh_cookies.py --no-headless       # окно браузера, для дебага
    python3 scripts/refresh_cookies.py --dry-run           # без реальной записи файлов

Возвращает ненулевой exit code, если хотя бы один аккаунт требует
ручного вмешательства (разлогинен/ошибка) — удобно вешать на
cron/systemd-мониторинг (см. scripts/systemd/).

Расписание НЕ встроено в сам скрипт: один запуск = один полный прогон
по всем enabled-аккаунтам, планировщик (systemd timer/cron) — снаружи,
см. README.md.
"""

import argparse
import copy
import json
import os
import sys
import time
from dataclasses import dataclass
from pathlib import Path

import yaml

try:
    from playwright.sync_api import TimeoutError as PlaywrightTimeoutError
    from playwright.sync_api import sync_playwright
except ImportError:
    print(
        "Playwright не установлен. Установи зависимости джоба отдельно от основного "
        "requirements.txt:\n"
        "  python3 -m venv .venv-refresh\n"
        "  .venv-refresh/bin/pip install -r scripts/requirements-refresh.txt\n"
        "  .venv-refresh/bin/playwright install chromium",
        file=sys.stderr,
    )
    raise

try:
    from playwright_stealth import stealth_sync
except ImportError:
    stealth_sync = None

# Скрипт лежит в scripts/, а не в корне — добавляем корень репозитория в
# sys.path, чтобы переиспользовать scraper.config/scraper.constants
# (тот же формат accounts.yaml, тот же логгер/PROXY_HOST/BASE_DIR — не
# заводим второй несовместимый набор этих вещей в проекте).
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from scraper.config import load_accounts  # noqa: E402
from scraper.constants import BASE_DIR, PROXY_HOST, log  # noqa: E402


# ---------------------------------------------------------------- #
#  Конфиг джоба (refresh_cookies.yaml, опционально)
# ---------------------------------------------------------------- #

DEFAULT_CONFIG = {
    "headless": True,
    "login_check_timeout_seconds": 20,
    "stagger_seconds": 45,
    "storage_state_dir": "storage_state",
    "reddit_base_url": "https://www.reddit.com",
    "login_check_path": "/api/me.json",
    "geo_defaults": {
        "locale": "en-US",
        "timezone_id": "America/New_York",
        "viewport": {"width": 1920, "height": 1080},
    },
    "account_geo": {
        "account_1": {"locale": "en-CA", "timezone_id": "America/Toronto"},
        "account_2": {"locale": "ru-RU", "timezone_id": "Europe/Moscow"},
        "account_3": {"locale": "en-US", "timezone_id": "America/New_York"},
        "account_4": {"locale": "en-GB", "timezone_id": "Europe/London"},
        "account_5": {"locale": "de-DE", "timezone_id": "Europe/Berlin"},
        "account_6": {"locale": "nl-NL", "timezone_id": "Europe/Amsterdam"},
    },
}


def _deep_merge(base: dict, override: dict) -> None:
    for k, v in override.items():
        if isinstance(v, dict) and isinstance(base.get(k), dict):
            _deep_merge(base[k], v)
        else:
            base[k] = v


def load_refresh_config(path: Path) -> dict:
    cfg = copy.deepcopy(DEFAULT_CONFIG)
    if not path.exists():
        log.info("%s не найден — использую дефолты джоба", path)
        return cfg
    with open(path, "r", encoding="utf-8") as f:
        user_cfg = yaml.safe_load(f) or {}
    # Поддержка альтернативного варианта из спеки: секция `refresh_cookies:`
    # внутри общего config.yaml вместо отдельного файла.
    top_level_keys = set(DEFAULT_CONFIG) - {"refresh_cookies"}
    if "refresh_cookies" in user_cfg and not (top_level_keys & user_cfg.keys()):
        user_cfg = user_cfg["refresh_cookies"]
    _deep_merge(cfg, user_cfg)
    return cfg


# ---------------------------------------------------------------- #
#  Конвертация форматов cookies: Cookie Editor <-> Playwright
# ---------------------------------------------------------------- #

def cookie_editor_to_playwright(raw_cookies: list[dict], default_domain: str = ".reddit.com") -> list[dict]:
    """Cookie Editor export (name/value [+ прочие поля, необязательные])
    -> формат, который принимает Playwright storage_state (domain и path
    обязательны — используем default_domain как fallback, если в экспорте
    их не было)."""
    out = []
    for c in raw_cookies:
        name = c.get("name")
        value = c.get("value")
        if name is None or value is None:
            continue
        same_site = c.get("sameSite") or "Lax"
        if same_site not in ("Strict", "Lax", "None"):
            same_site = "Lax"
        expires = c.get("expirationDate", c.get("expires"))
        try:
            expires_f = float(expires) if expires not in (None, "", -1) else -1
        except (TypeError, ValueError):
            expires_f = -1
        out.append(
            {
                "name": name,
                "value": value,
                "domain": c.get("domain") or default_domain,
                "path": c.get("path") or "/",
                "expires": expires_f,
                "httpOnly": bool(c.get("httpOnly", False)),
                "secure": bool(c.get("secure", True)),
                "sameSite": same_site,
            }
        )
    return out


def playwright_to_cookie_editor(pw_cookies: list[dict]) -> list[dict]:
    """Обратная конвертация — формат, который понимает
    scraper.config.load_cookies() (нужны только name/value, остальные
    поля он игнорирует, но сохраняем их для наглядности при ручном
    дебаге файла)."""
    out = []
    for c in pw_cookies:
        out.append(
            {
                "name": c.get("name"),
                "value": c.get("value"),
                "domain": c.get("domain", ""),
                "path": c.get("path", "/"),
                "expirationDate": c.get("expires", -1),
                "httpOnly": c.get("httpOnly", False),
                "secure": c.get("secure", True),
                "sameSite": c.get("sameSite", "Lax"),
            }
        )
    return out


# ---------------------------------------------------------------- #
#  Атомарная запись (см. README/спеку — account_worker читает тот же
#  cookies/account_N.json параллельно через свой hot-reload и не должен
#  увидеть частично записанный файл)
# ---------------------------------------------------------------- #

def atomic_write_json(path: Path, data) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp_path = path.parent / f".{path.name}.tmp"
    with open(tmp_path, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)
        f.flush()
        os.fsync(f.fileno())
    os.replace(tmp_path, path)


# ---------------------------------------------------------------- #
#  Stealth
# ---------------------------------------------------------------- #

_MANUAL_STEALTH_INIT_SCRIPT = """
Object.defineProperty(navigator, 'webdriver', {get: () => undefined});
Object.defineProperty(navigator, 'plugins', {get: () => [1, 2, 3, 4, 5]});
Object.defineProperty(navigator, 'languages', {get: () => ['en-US', 'en']});
window.chrome = window.chrome || { runtime: {} };
const originalQuery = window.navigator.permissions.query;
window.navigator.permissions.query = (parameters) => (
    parameters.name === 'notifications'
        ? Promise.resolve({ state: Notification.permission })
        : originalQuery(parameters)
);
"""


def _apply_stealth(page) -> None:
    if stealth_sync is not None:
        try:
            stealth_sync(page)
            return
        except Exception as e:
            log.warning("playwright-stealth упал (%s) — продолжаю с ручными патчами", e)
    page.add_init_script(_MANUAL_STEALTH_INIT_SCRIPT)


# ---------------------------------------------------------------- #
#  Проверка залогиненности — позитивный признак, не отсутствие формы
#  логина. Вынесена в константу пути, чтобы легко поправить при
#  редизайне Reddit.
# ---------------------------------------------------------------- #

def _is_logged_in(page, base_url: str, login_check_path: str, timeout_ms: int) -> tuple[bool, str]:
    url = base_url.rstrip("/") + login_check_path
    try:
        resp = page.request.get(url, timeout=timeout_ms)
    except Exception as e:
        return False, f"запрос к {login_check_path} упал: {e}"
    if resp.status != 200:
        return False, f"{login_check_path} вернул статус {resp.status}"
    try:
        data = resp.json()
    except Exception as e:
        return False, f"{login_check_path} вернул не-JSON: {e}"
    user_data = data.get("data") if isinstance(data, dict) else None
    if isinstance(user_data, dict) and user_data.get("name"):
        return True, f"залогинен как {user_data.get('name')}"
    return False, f"{login_check_path} без data.name — сессия не авторизована"


# ---------------------------------------------------------------- #
#  Обработка одного аккаунта
# ---------------------------------------------------------------- #

@dataclass
class AccountResult:
    name: str
    ok: bool
    detail: str


def process_account(pw, account: dict, cfg: dict, dry_run: bool) -> AccountResult:
    name = account["name"]
    cookie_file = BASE_DIR / account["cookie_file"]
    proxy_port = account.get("proxy_port")
    configured_ua = account.get("user_agent")

    if not proxy_port:
        log.error("[%s] нет proxy_port в accounts.yaml — пропускаю (логин с чужого IP — риск для аккаунта)", name)
        return AccountResult(name, False, "нет proxy_port в accounts.yaml")
    proxy_url = f"http://{PROXY_HOST}:{proxy_port}"

    storage_state_dir = BASE_DIR / cfg["storage_state_dir"]
    storage_state_dir.mkdir(parents=True, exist_ok=True)
    storage_state_path = storage_state_dir / f"{name}.json"

    geo = {**cfg["geo_defaults"], **cfg.get("account_geo", {}).get(name, {})}
    viewport = geo.get("viewport") or cfg["geo_defaults"]["viewport"]

    # storage_state: сохранённый с прошлого прогона (предпочтительно —
    # содержит полный контекст сессии, не только cookies), либо
    # сконвертированный на лету текущий cookies/account_N.json (первый
    # запуск джоба для этого аккаунта), либо пустая сессия, если нет ни
    # того, ни другого.
    if storage_state_path.exists():
        storage_state = str(storage_state_path)
        log.info("[%s] использую сохранённый storage_state: %s", name, storage_state_path)
    elif cookie_file.exists():
        try:
            raw = json.loads(cookie_file.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError) as e:
            log.warning("[%s] не удалось прочитать %s для инициализации storage_state: %s", name, cookie_file, e)
            raw = []
        storage_state = {"cookies": cookie_editor_to_playwright(raw), "origins": []}
        log.info("[%s] storage_state ещё нет — инициализирую из %s", name, cookie_file)
    else:
        log.warning(
            "[%s] нет ни %s, ни сохранённого storage_state — стартую с пустой сессией "
            "(скорее всего, потребуется ручной логин)",
            name, cookie_file,
        )
        storage_state = {"cookies": [], "origins": []}

    browser = pw.chromium.launch(headless=cfg["headless"])
    try:
        context = browser.new_context(
            storage_state=storage_state,
            proxy={"server": proxy_url},
            user_agent=configured_ua or None,
            locale=geo.get("locale"),
            timezone_id=geo.get("timezone_id"),
            viewport=viewport,
        )
        try:
            page = context.new_page()
            _apply_stealth(page)

            base_url = cfg["reddit_base_url"]
            login_check_path = cfg["login_check_path"]
            timeout_ms = int(cfg["login_check_timeout_seconds"] * 1000)

            try:
                page.goto(base_url, timeout=timeout_ms, wait_until="domcontentloaded")
            except PlaywrightTimeoutError as e:
                log.error("[%s] не удалось открыть %s через прокси %s за %sмс: %s", name, base_url, proxy_url, timeout_ms, e)
                return AccountResult(name, False, f"навигация не уложилась в таймаут: {e}")
            except Exception as e:
                log.error("[%s] ошибка навигации/сети через прокси %s (проверь mihomo/порт %s): %s", name, proxy_url, proxy_port, e)
                return AccountResult(name, False, f"сеть/навигация: {e}")

            logged_in, detail = _is_logged_in(page, base_url, login_check_path, timeout_ms)
            if not logged_in:
                log.error(
                    "[%s] сессия разлогинена (%s) — cookies/%s.json НЕ трогаю, нужен ручной логин",
                    name, detail, name,
                )
                return AccountResult(name, False, detail)

            log.info("[%s] сессия активна (%s)", name, detail)

            try:
                actual_ua = page.evaluate("navigator.userAgent")
            except Exception:
                actual_ua = None
            if configured_ua and actual_ua and actual_ua != configured_ua:
                log.warning(
                    "[%s] User-Agent браузера разошёлся с accounts.yaml — accounts.yaml НЕ трогаю "
                    "автоматически (impersonate/user_agent правятся руками). "
                    "accounts.yaml=%r, фактический=%r",
                    name, configured_ua, actual_ua,
                )

            new_storage_state = context.storage_state()
            new_cookies = new_storage_state.get("cookies", [])

            if dry_run:
                log.info("[%s] dry-run: cookies/storage_state НЕ записаны (нашёл %d cookies)", name, len(new_cookies))
            else:
                atomic_write_json(storage_state_path, new_storage_state)
                atomic_write_json(cookie_file, playwright_to_cookie_editor(new_cookies))
                log.info(
                    "[%s] cookies обновлены: %s (%d шт.), storage_state: %s",
                    name, cookie_file, len(new_cookies), storage_state_path,
                )

            return AccountResult(name, True, detail)
        finally:
            context.close()
    finally:
        browser.close()


# ---------------------------------------------------------------- #
#  main
# ---------------------------------------------------------------- #

def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument(
        "--account", action="append",
        help="Обработать только указанный(е) аккаунт(ы) (можно повторять флаг). По умолчанию — все enabled: true.",
    )
    parser.add_argument("--headless", dest="headless", action="store_true", default=None)
    parser.add_argument("--no-headless", dest="headless", action="store_false")
    parser.add_argument("--dry-run", action="store_true", help="Ничего не записывать в cookies/storage_state (для дебага).")
    parser.add_argument(
        "--config", default=None,
        help="Путь к refresh_cookies.yaml (по умолчанию: BASE_DIR/refresh_cookies.yaml, либо env REFRESH_COOKIES_CONFIG)",
    )
    args = parser.parse_args()

    config_path = Path(args.config) if args.config else Path(
        os.environ.get("REFRESH_COOKIES_CONFIG", BASE_DIR / "refresh_cookies.yaml")
    )
    cfg = load_refresh_config(config_path)
    if args.headless is not None:
        cfg["headless"] = args.headless

    accounts = [a for a in load_accounts() if a.get("enabled")]
    if args.account:
        wanted = set(args.account)
        accounts = [a for a in accounts if a["name"] in wanted]
        missing = wanted - {a["name"] for a in accounts}
        if missing:
            log.warning("Аккаунты не найдены среди enabled: true в accounts.yaml: %s", ", ".join(sorted(missing)))

    if not accounts:
        log.error("Нет ни одного аккаунта для обработки — проверь accounts.yaml / --account")
        return 1

    log.info(
        "refresh_cookies: старт, %d аккаунт(ов), headless=%s, stagger=%.0fs, storage_state_dir=%s%s",
        len(accounts), cfg["headless"], cfg["stagger_seconds"], cfg["storage_state_dir"],
        " [DRY-RUN]" if args.dry_run else "",
    )

    results: list[AccountResult] = []
    with sync_playwright() as pw:
        for i, account in enumerate(accounts):
            if i > 0 and cfg["stagger_seconds"] > 0:
                time.sleep(cfg["stagger_seconds"])
            try:
                result = process_account(pw, account, cfg, args.dry_run)
            except Exception as e:
                log.exception("[%s] неожиданная ошибка обработки аккаунта", account["name"])
                result = AccountResult(account["name"], False, f"необработанное исключение: {e}")
            results.append(result)

    ok = [r for r in results if r.ok]
    failed = [r for r in results if not r.ok]
    log.info(
        "refresh_cookies: завершено — успешно=%d, требуют_вмешательства=%d%s",
        len(ok), len(failed), " [DRY-RUN, ничего не записано]" if args.dry_run else "",
    )
    for r in failed:
        log.error("  [%s] требует ручного вмешательства: %s", r.name, r.detail)

    return 1 if failed else 0


if __name__ == "__main__":
    sys.exit(main())
