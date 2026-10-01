#!/usr/bin/env python3
"""
scripts/refresh_guest_cookies.py

Независимый Playwright-джоб — харвестит АНОНИМНЫЕ (гостевые) cookies
для "гостевых" слотов из accounts.yaml (те, что перечислены в секции
guest_accounts конфига этого джоба, см. refresh_guest_cookies.yaml).

В отличие от scripts/refresh_cookies.py (логин в реальный аккаунт),
здесь никакого логина нет: с мая 2026 Reddit блокирует .json без ЛЮБЫХ
cookies (403), но не требует именно авторизованной сессии — достаточно
один раз зайти на reddit.com настоящим браузером (тот сам проходит
анти-бот JS-проверку) и переиспользовать анонимные cookies, которые
Reddit выставляет через Set-Cookie. Самая короткоживущая из них —
session_tracker (~2 часа) — поэтому этот джоб рассчитан на частый
повторный запуск (см. scripts/systemd/refresh-guest-cookies.timer).

Для каждого гостевого слота:

  1. поднимает Playwright browser context — движок/channel/UA/прокси
     ЗАФИКСИРОВАНЫ на слот в guest_accounts (refresh_guest_cookies.yaml),
     а не выбираются на лету — TLS-отпечаток curl_cffi (impersonate),
     которым потом реально идут запросы comments.json, должен совпадать
     с "браузером", который эти cookies получил;
  2. заходит на reddit_base_url (тем же proxy_port, что и сам скрапер
     для этого аккаунта — та же выходная VLESS-нода, что и для
     последующих comments.json-запросов);
  3. ждёт network idle + небольшую паузу (антибот может доставлять
     часть cookies через JS уже после первой отрисовки);
  4. забирает cookies контекста, оставляет только домен reddit.com;
  5. ВЕРИФИЦИРУЕТ результат реальным запросом к comments.json через
     curl_cffi с тем же impersonate/прокси, что использует сам
     скрапер для этого аккаунта — только если ответ 200, считает
     харвест успешным;
  6. если верификация прошла — АТОМАРНО (os.replace()) перезаписывает
     cookies/account_N.json (тот же формат Cookie Editor export, что
     понимает scraper.config.load_cookies() и подхватывает hot-reload
     CookieFileWatcher). Если НЕ прошла — существующий файл не трогает,
     логирует ERROR.

Гостевая сессия НЕ переиспользуется между запусками (в отличие от
storage_state в refresh_cookies.py) — каждый прогон начинает с чистого
контекста и полностью пересобирает набор cookies; это проще и
устойчивее для короткоживущей анонимной сессии, чем инкрементальное
обновление.

Ровно один браузерный процесс работает в любой момент времени:
аккаунты обрабатываются последовательным for-циклом, browser.close()
предыдущего аккаунта отрабатывает раньше, чем launch() следующего (см.
process_guest_account() и main()) — принципиально при ограниченных
ресурсах хоста (webkit/chromium/firefox параллельно легко кладут
малую VPS в OOM).

Установка (тот же venv, что и у refresh_cookies.py, но плюс curl_cffi
для шага верификации — см. scripts/requirements-refresh.txt):

    python3 -m venv .venv-refresh
    .venv-refresh/bin/pip install -r scripts/requirements-refresh.txt
    .venv-refresh/bin/playwright install --with-deps chromium firefox
    # для chrome1xx/edge1xx-слотов из guest_accounts — настоящие каналы:
    .venv-refresh/bin/playwright install chrome msedge

Запуск:

    python3 scripts/refresh_guest_cookies.py                     # все слоты из guest_accounts
    python3 scripts/refresh_guest_cookies.py --account account_7
    python3 scripts/refresh_guest_cookies.py --no-headless        # окно браузера, для дебага
    python3 scripts/refresh_guest_cookies.py --dry-run            # без записи файлов

Возвращает ненулевой exit code, если хотя бы один слот не прошёл
верификацию — вешать на cron/systemd-мониторинг (см. scripts/systemd/).

Расписание НЕ встроено в сам скрипт — планировщик снаружи (systemd
timer, интервал МЕНЬШЕ TTL session_tracker с запасом — см. README.md).
"""

import argparse
import copy
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
        "  .venv-refresh/bin/playwright install --with-deps chromium firefox\n"
        "  .venv-refresh/bin/playwright install chrome msedge",
        file=sys.stderr,
    )
    raise

try:
    from curl_cffi import requests as cffi_requests
except ImportError:
    print(
        "curl_cffi не установлен (нужен для шага верификации — см. "
        "scripts/requirements-refresh.txt).",
        file=sys.stderr,
    )
    raise

# Скрипт лежит в scripts/ — добавляем и сам scripts/ (переиспользуем
# atomic_write_json/playwright_to_cookie_editor/_apply_stealth из
# refresh_cookies.py, не дублируем), и корень репозитория (scraper.*),
# по тому же принципу, что и в refresh_cookies.py.
_SCRIPTS_DIR = Path(__file__).resolve().parent
sys.path.insert(0, str(_SCRIPTS_DIR))
sys.path.insert(0, str(_SCRIPTS_DIR.parent))

from refresh_cookies import atomic_write_json, playwright_to_cookie_editor, _apply_stealth  # noqa: E402
from scraper.config import load_accounts  # noqa: E402
from scraper.constants import BASE_DIR, PROXY_HOST, log  # noqa: E402


# ---------------------------------------------------------------- #
#  Конфиг джоба (refresh_guest_cookies.yaml)
# ---------------------------------------------------------------- #

DEFAULT_CONFIG = {
    "headless": True,
    "reddit_base_url": "https://www.reddit.com",
    "warmup_path": "/",
    "warmup_page_load_timeout_seconds": 25,
    "warmup_retries": 2,
    "post_load_settle_seconds": 4,
    "verify_subreddit": "AskReddit",
    "verify_timeout_seconds": 20,
    "stagger_seconds": 30,
    "geo_defaults": {
        "locale": "en-US",
        "timezone_id": "America/New_York",
        "viewport": {"width": 1920, "height": 1080},
    },
    "account_geo": {},
    # slot -> {engine: chromium|firefox|webkit, channel: chrome|msedge|None,
    #          user_agent: str, impersonate: str}. Пусто по умолчанию —
    # без явного перечисления в refresh_guest_cookies.yaml джобу нечего
    # обрабатывать (см. DEFAULT_CONFIG-комментарий в README).
    "guest_accounts": {},
}

# Cookies с доменом, не относящимся к reddit.com (например, g_state —
# артефакт Google Identity Services), в файл не попадают — они не
# нужны scraper'у и только раздувают cookies/account_N.json.
_REDDIT_DOMAIN_SUBSTR = "reddit.com"


def _deep_merge(base: dict, override: dict) -> None:
    for k, v in override.items():
        if isinstance(v, dict) and isinstance(base.get(k), dict):
            _deep_merge(base[k], v)
        else:
            base[k] = v


def load_guest_config(path: Path) -> dict:
    cfg = copy.deepcopy(DEFAULT_CONFIG)
    if not path.exists():
        log.info("%s не найден — использую дефолты джоба (guest_accounts пуст!)", path)
        return cfg
    with open(path, "r", encoding="utf-8") as f:
        user_cfg = yaml.safe_load(f) or {}
    _deep_merge(cfg, user_cfg)
    return cfg


# ---------------------------------------------------------------- #
#  Обработка одного гостевого слота
# ---------------------------------------------------------------- #

@dataclass
class AccountResult:
    name: str
    ok: bool
    detail: str


def process_guest_account(pw, account: dict, cfg: dict, dry_run: bool) -> AccountResult:
    name = account["name"]
    cookie_file = BASE_DIR / account["cookie_file"]
    proxy_port = account.get("proxy_port")

    guest_spec = cfg["guest_accounts"].get(name)
    if not guest_spec:
        return AccountResult(name, False, "нет записи в guest_accounts конфига джоба — нечего харвестить")

    if not proxy_port:
        log.error("[%s] нет proxy_port в accounts.yaml — пропускаю", name)
        return AccountResult(name, False, "нет proxy_port в accounts.yaml")
    proxy_url = f"http://{PROXY_HOST}:{proxy_port}"

    engine = guest_spec.get("engine", "chromium")
    channel = guest_spec.get("channel")
    user_agent = guest_spec.get("user_agent")
    impersonate = guest_spec.get("impersonate")
    device_name = guest_spec.get("device")  # опционально: имя пресета Playwright, напр. "iPhone 13 Pro"
    if not impersonate:
        return AccountResult(name, False, "guest_accounts.%s: нужен impersonate" % name)
    if not user_agent and not device_name:
        return AccountResult(name, False, "guest_accounts.%s: нужен user_agent, либо device" % name)

    engine_launcher = getattr(pw, engine, None)
    if engine_launcher is None:
        return AccountResult(name, False, f"неизвестный engine={engine!r} (chromium/firefox/webkit)")

    device_preset = {}
    if device_name:
        try:
            device_preset = dict(pw.devices[device_name])
        except KeyError:
            return AccountResult(
                name, False,
                f"guest_accounts.{name}: нет пресета device={device_name!r} в этой версии Playwright "
                f"(python3 -c \"from playwright.sync_api import sync_playwright as s; "
                f"print(list(s().start().devices))\" — посмотреть доступные имена)",
            )
        expected_engine = device_preset.pop("default_browser_type", None)
        if expected_engine and expected_engine != engine:
            log.warning(
                "[%s] device=%r рассчитан на engine=%r, а в конфиге engine=%r — оставляю как задано в конфиге",
                name, device_name, expected_engine, engine,
            )

    geo = {**cfg["geo_defaults"], **cfg.get("account_geo", {}).get(name, {})}
    # device-пресет (viewport/UA/is_mobile/has_touch/device_scale_factor) —
    # база; explicit user_agent/viewport из guest_accounts, если заданы,
    # её переопределяют (нужно, если хочешь другую версию UA поверх
    # того же device-пресета).
    context_kwargs = dict(device_preset)
    context_kwargs["viewport"] = geo.get("viewport") or context_kwargs.get("viewport") or cfg["geo_defaults"]["viewport"]
    if user_agent:
        context_kwargs["user_agent"] = user_agent
    elif "user_agent" not in context_kwargs:
        return AccountResult(name, False, f"guest_accounts.{name}: ни user_agent, ни device не дали UA")
    user_agent = context_kwargs["user_agent"]  # для лога/единообразия дальше по функции

    launch_kwargs = {"headless": cfg["headless"]}
    if channel:
        launch_kwargs["channel"] = channel

    try:
        browser = engine_launcher.launch(**launch_kwargs)
    except Exception as e:
        return AccountResult(
            name, False,
            f"не смог запустить {engine}(channel={channel!r}): {e} "
            f"(поставлен ли канал? playwright install {channel or engine})",
        )

    try:
        context = browser.new_context(
            proxy={"server": proxy_url},
            locale=geo.get("locale"),
            timezone_id=geo.get("timezone_id"),
            **context_kwargs,
        )
        try:
            page = context.new_page()
            _apply_stealth(page)

            base_url = cfg["reddit_base_url"]
            url = base_url.rstrip("/") + cfg["warmup_path"]
            timeout_ms = int(cfg["warmup_page_load_timeout_seconds"] * 1000)

            last_err = None
            loaded = False
            for attempt in range(1, cfg["warmup_retries"] + 1):
                try:
                    page.goto(url, timeout=timeout_ms, wait_until="networkidle")
                    loaded = True
                    break
                except PlaywrightTimeoutError as e:
                    last_err = e
                    log.warning("[%s] попытка %d/%d: таймаут загрузки %s через %s: %s",
                                name, attempt, cfg["warmup_retries"], url, proxy_url, e)
                except Exception as e:
                    last_err = e
                    log.warning("[%s] попытка %d/%d: ошибка навигации/сети через %s (mihomo/порт %s живы?): %s",
                                name, attempt, cfg["warmup_retries"], proxy_url, proxy_port, e)
            if not loaded:
                return AccountResult(name, False, f"не удалось загрузить {url}: {last_err}")

            time.sleep(cfg["post_load_settle_seconds"])

            raw_cookies = context.cookies()
            reddit_cookies = [c for c in raw_cookies if _REDDIT_DOMAIN_SUBSTR in (c.get("domain") or "")]
            if not reddit_cookies:
                return AccountResult(name, False, "после загрузки reddit.com не нашёл ни одной cookie домена reddit.com")

            cookie_editor_rows = playwright_to_cookie_editor(reddit_cookies)
            cookie_dict = {c["name"]: c["value"] for c in cookie_editor_rows if c.get("name")}

            verify_url = f"{base_url.rstrip('/')}/r/{cfg['verify_subreddit']}/comments.json"
            try:
                resp = cffi_requests.get(
                    verify_url,
                    params={"limit": 5},
                    cookies=cookie_dict,
                    impersonate=impersonate,
                    proxies={"http": proxy_url, "https": proxy_url},
                    timeout=cfg["verify_timeout_seconds"],
                )
            except Exception as e:
                return AccountResult(name, False, f"верификация ({verify_url}) упала: {e}")

            if resp.status_code != 200:
                return AccountResult(
                    name, False,
                    f"верификация: {verify_url} вернул {resp.status_code} — cookies не приняты Reddit'ом",
                )

            if dry_run:
                log.info("[%s] dry-run: %d cookies домена reddit.com, верификация OK (%s)",
                          name, len(cookie_editor_rows), resp.status_code)
            else:
                atomic_write_json(cookie_file, cookie_editor_rows)
                log.info("[%s] cookies обновлены: %s (%d шт.), верификация OK (%s)",
                          name, cookie_file, len(cookie_editor_rows), resp.status_code)

            return AccountResult(name, True, f"{len(cookie_editor_rows)} cookies, verify={resp.status_code}")
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
        help="Обработать только указанный(е) гостевой(ые) слот(ы) (можно повторять флаг). "
             "По умолчанию — все слоты из guest_accounts конфига.",
    )
    parser.add_argument("--headless", dest="headless", action="store_true", default=None)
    parser.add_argument("--no-headless", dest="headless", action="store_false")
    parser.add_argument("--dry-run", action="store_true", help="Ничего не записывать в cookies/ (для дебага).")
    parser.add_argument(
        "--config", default=None,
        help="Путь к refresh_guest_cookies.yaml (по умолчанию: BASE_DIR/refresh_guest_cookies.yaml, "
             "либо env REFRESH_GUEST_COOKIES_CONFIG)",
    )
    args = parser.parse_args()

    # NB: файл лежит в scripts/ (не в корне проекта) — в отличие от
    # refresh_cookies.py, у которого дефолтный путь исторически указывает
    # на BASE_DIR (корень), хотя сам scripts/refresh_cookies.yaml лежит
    # в scripts/ (несостыковка в существующем скрипте, здесь не повторяем).
    config_path = Path(args.config) if args.config else Path(
        os.environ.get("REFRESH_GUEST_COOKIES_CONFIG", _SCRIPTS_DIR / "refresh_guest_cookies.yaml")
    )
    cfg = load_guest_config(config_path)
    if args.headless is not None:
        cfg["headless"] = args.headless

    guest_slot_names = set(cfg["guest_accounts"])
    if not guest_slot_names:
        log.error("guest_accounts пуст в %s — нечего харвестить", config_path)
        return 1

    all_accounts = {a["name"]: a for a in load_accounts()}
    wanted_names = set(args.account) if args.account else guest_slot_names
    missing_in_accounts_yaml = wanted_names - set(all_accounts)
    if missing_in_accounts_yaml:
        log.warning("Слотов нет в accounts.yaml: %s", ", ".join(sorted(missing_in_accounts_yaml)))
    missing_in_guest_cfg = wanted_names - guest_slot_names
    if missing_in_guest_cfg:
        log.warning("Слотов нет в guest_accounts конфига (%s): %s",
                    config_path, ", ".join(sorted(missing_in_guest_cfg)))

    accounts = [all_accounts[n] for n in sorted(wanted_names & set(all_accounts) & guest_slot_names)]
    if not accounts:
        log.error("Нет ни одного гостевого слота для обработки — проверь accounts.yaml / %s / --account", config_path)
        return 1

    log.info(
        "refresh_guest_cookies: старт, %d слот(ов), headless=%s, stagger=%.0fs%s",
        len(accounts), cfg["headless"], cfg["stagger_seconds"],
        " [DRY-RUN]" if args.dry_run else "",
    )

    results: list[AccountResult] = []
    # Один sync_playwright() на весь прогон, но браузер каждого слота
    # запускается и закрывается ПОСЛЕДОВАТЕЛЬНО внутри цикла (см.
    # process_guest_account) — в любой момент времени жив максимум один
    # браузерный процесс, независимо от количества слотов.
    with sync_playwright() as pw:
        for i, account in enumerate(accounts):
            if i > 0 and cfg["stagger_seconds"] > 0:
                time.sleep(cfg["stagger_seconds"])
            try:
                result = process_guest_account(pw, account, cfg, args.dry_run)
            except Exception as e:
                log.exception("[%s] неожиданная ошибка обработки гостевого слота", account["name"])
                result = AccountResult(account["name"], False, f"необработанное исключение: {e}")
            results.append(result)

    ok = [r for r in results if r.ok]
    failed = [r for r in results if not r.ok]
    log.info(
        "refresh_guest_cookies: завершено — успешно=%d, ошибок=%d%s",
        len(ok), len(failed), " [DRY-RUN, ничего не записано]" if args.dry_run else "",
    )
    for r in failed:
        log.error("  [%s] не удалось: %s", r.name, r.detail)

    return 1 if failed else 0


if __name__ == "__main__":
    sys.exit(main())