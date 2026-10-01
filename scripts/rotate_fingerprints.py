#!/usr/bin/env python3
"""
scripts/rotate_fingerprints.py

Автоматически подбирает новые пары (curl_cffi impersonate + User-Agent)
для аккаунтов и синхронно правит:
  - accounts.yaml                       (impersonate, user_agent)
  - scripts/refresh_guest_cookies.yaml  (guest_accounts: engine/channel/
                                         device/user_agent/impersonate)

Список допустимых impersonate-таргетов берётся из УСТАНОВЛЕННОГО
curl_cffi (ничего не хардкодится), UA собирается под каждый таргет
автоматически. Разные аккаунты получают разные таргеты (пока хватает
уникальных). Устаревшие версии (chrome99 и т.п.) по умолчанию
отфильтровываются — см. --allow-old.

Запускать тем же python, где стоит curl_cffi (.venv-refresh/venv):

    python3 scripts/rotate_fingerprints.py --dry-run          # только показать
    python3 scripts/rotate_fingerprints.py                    # все гостевые слоты
    python3 scripts/rotate_fingerprints.py --account account_5 --account account_6
    python3 scripts/rotate_fingerprints.py --seed 42          # воспроизводимо
    python3 scripts/rotate_fingerprints.py --refresh          # + сразу перегенерить cookies

Логин-аккаунты (те, что НЕ в guest_accounts) по умолчанию не трогаются:
их сессия привязана к отпечатку. Включить: --include-login (тогда
после этого им нужен ручной перелогин).

Перед записью делается бэкап *.bak-<время>. Комментарии в секции
guest_accounts при перегенерации теряются (остаются в бэкапе).
"""

import argparse
import random
import re
import shutil
import subprocess
import sys
import time
from dataclasses import dataclass
from pathlib import Path

import yaml

ROOT = Path(__file__).resolve().parent.parent
ACCOUNTS_PATH = ROOT / "accounts.yaml"
GUEST_PATH = ROOT / "scripts" / "refresh_guest_cookies.yaml"
GUEST_SCRIPT = ROOT / "scripts" / "refresh_guest_cookies.py"

# Минимальные мажорные версии, ниже которых таргет считается "старым и
# подозрительным" в 2026 (отключается --allow-old).
MIN_VERSION = {"chrome": 116, "firefox": 115, "edge": 120, "safari": 17}

# (имя пресета Playwright, кусок UA "Linux; Android ...; модель")
ANDROID_DEVICES = [
    ("Pixel 7", "Android 14; Pixel 7"),
    ("Pixel 5", "Android 13; Pixel 5"),
    ("Galaxy S9+", "Android 12; SM-G965F"),
]
IOS_DEVICES = ["iPhone 15", "iPhone 15 Pro", "iPhone 14"]


@dataclass(frozen=True)
class Target:
    name: str
    family: str  # chrome / edge / firefox / safari
    major: int
    minor: int = 0
    ios: bool = False
    android: bool = False


@dataclass
class Profile:
    impersonate: str
    user_agent: str
    engine: str
    channel: str | None
    device: str | None
    label: str


# ---------------------------------------------------------------- #
#  Таргеты curl_cffi
# ---------------------------------------------------------------- #

def list_curl_targets() -> list[str]:
    try:
        from curl_cffi.requests import BrowserType
        return [b.value for b in BrowserType]
    except Exception:
        pass
    try:
        import typing
        from curl_cffi.requests import BrowserTypeLiteral
        return list(typing.get_args(BrowserTypeLiteral))
    except Exception as e:
        sys.exit(f"ОШИБКА: не смог получить список impersonate из curl_cffi ({e}). "
                 "Запускай тем же python, где установлен curl_cffi.")


def parse_target(name: str) -> Target | None:
    m = re.fullmatch(r"chrome(\d+)[a-z]?(_android)?", name)
    if m:
        return Target(name, "chrome", int(m.group(1)), android=bool(m.group(2)))
    m = re.fullmatch(r"edge(\d+)", name)
    if m:
        return Target(name, "edge", int(m.group(1)))
    m = re.fullmatch(r"firefox(\d+)", name)
    if m:
        return Target(name, "firefox", int(m.group(1)))
    m = re.fullmatch(r"safari(\d+)(?:_(\d+))?(_ios)?", name)
    if m:
        if m.group(2) is not None:
            major, minor = int(m.group(1)), int(m.group(2))
        else:
            d = m.group(1)
            if len(d) < 2:
                return None
            major, minor = int(d[:-1]), int(d[-1])
        return Target(name, "safari", major, minor, ios=bool(m.group(3)))
    return None


def build_pool(args) -> list[Target]:
    seen: set[tuple] = set()
    pool: list[Target] = []
    for name in list_curl_targets():
        t = parse_target(name)
        if t is None:
            continue
        if not args.allow_old and t.major < MIN_VERSION[t.family]:
            continue
        if args.no_edge and t.family == "edge":
            continue
        if args.no_mobile and (t.ios or t.android):
            continue
        key = (t.family, t.major, t.minor, t.ios, t.android)
        if key in seen:  # safari180 и safari18_0 — один и тот же таргет
            continue
        seen.add(key)
        pool.append(t)
    return pool


# ---------------------------------------------------------------- #
#  Сборка UA + параметров Playwright под таргет
# ---------------------------------------------------------------- #

def build_profile(t: Target, rng: random.Random) -> Profile:
    if t.family in ("chrome", "edge") and t.android:
        dev, model = rng.choice(ANDROID_DEVICES)
        ua = (f"Mozilla/5.0 (Linux; {model}) AppleWebKit/537.36 (KHTML, like Gecko) "
              f"Chrome/{t.major}.0.0.0 Mobile Safari/537.36")
        return Profile(t.name, ua, "chromium", None, dev, f"Chrome {t.major} Android")

    if t.family in ("chrome", "edge"):
        plat = rng.choice([
            "Windows NT 10.0; Win64; x64",
            "Macintosh; Intel Mac OS X 10_15_7",
            "X11; Linux x86_64",
        ]) if t.family == "chrome" else rng.choice([
            "Windows NT 10.0; Win64; x64",
            "Macintosh; Intel Mac OS X 10_15_7",
        ])
        ua = (f"Mozilla/5.0 ({plat}) AppleWebKit/537.36 (KHTML, like Gecko) "
              f"Chrome/{t.major}.0.0.0 Safari/537.36")
        if t.family == "edge":
            ua += f" Edg/{t.major}.0.0.0"
            return Profile(t.name, ua, "chromium", "msedge", None, f"Edge {t.major}")
        return Profile(t.name, ua, "chromium", None, None, f"Chrome {t.major}")

    if t.family == "firefox":
        plat = rng.choice([
            f"Windows NT 10.0; Win64; x64; rv:{t.major}.0",
            f"Macintosh; Intel Mac OS X 10.15; rv:{t.major}.0",
            f"X11; Linux x86_64; rv:{t.major}.0",
        ])
        ua = f"Mozilla/5.0 ({plat}) Gecko/20100101 Firefox/{t.major}.0"
        return Profile(t.name, ua, "firefox", None, None, f"Firefox {t.major}")

    # safari
    ver = f"{t.major}.{t.minor}"
    if t.ios:
        # с iOS 26 в UA замороженно "18_6"
        osv = "18_6" if t.major >= 26 else f"{t.major}_{t.minor}"
        ua = (f"Mozilla/5.0 (iPhone; CPU iPhone OS {osv} like Mac OS X) AppleWebKit/605.1.15 "
              f"(KHTML, like Gecko) Version/{ver} Mobile/15E148 Safari/604.1")
        return Profile(t.name, ua, "webkit", None, rng.choice(IOS_DEVICES), f"Safari {ver} iOS")
    ua = (f"Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 "
          f"(KHTML, like Gecko) Version/{ver} Safari/605.1.15")
    return Profile(t.name, ua, "webkit", None, None, f"Safari {ver}")


# ---------------------------------------------------------------- #
#  Правка файлов
# ---------------------------------------------------------------- #

def acc_num(name: str) -> int:
    m = re.search(r"(\d+)$", name)
    return int(m.group(1)) if m else 0


def patch_accounts_text(text: str, profiles: dict[str, Profile]) -> str:
    """Построчная правка — комментарии и порядок в accounts.yaml сохраняются."""
    out, cur = [], None
    for line in text.splitlines(keepends=True):
        m = re.match(r"^\s*-\s*name:\s*(\S+)", line)
        if m:
            cur = m.group(1)
        p = profiles.get(cur) if cur else None
        if p:
            m1 = re.match(r"^(\s*)impersonate:\s*\S+", line)
            if m1:
                line = f"{m1.group(1)}impersonate: {p.impersonate}\n"
            else:
                m2 = re.match(r"^(\s*)user_agent:\s*.*$", line)
                if m2:
                    line = f'{m2.group(1)}user_agent: "{p.user_agent}"\n'
        out.append(line)
    return "".join(out)


def patch_guest_text(text: str, guest: dict, profiles: dict[str, Profile]) -> str:
    m = re.search(r"(?m)^guest_accounts:[ \t]*\n", text)
    if not m:
        sys.exit("ОШИБКА: в refresh_guest_cookies.yaml не нашёл строку 'guest_accounts:'")
    head = text[:m.start()]
    new = dict(guest)
    for name, p in profiles.items():
        if name not in guest:
            continue
        entry = {"engine": p.engine, "channel": p.channel}
        if p.device:
            entry["device"] = p.device
        entry["user_agent"] = p.user_agent
        entry["impersonate"] = p.impersonate
        new[name] = entry
    ordered = {k: new[k] for k in sorted(new, key=acc_num)}
    tail = yaml.safe_dump({"guest_accounts": ordered}, allow_unicode=True,
                          sort_keys=False, width=200)
    return head + tail


def backup(path: Path, stamp: str) -> None:
    shutil.copy2(path, path.with_name(f"{path.name}.bak-{stamp}"))


# ---------------------------------------------------------------- #
#  main
# ---------------------------------------------------------------- #

def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--account", action="append", help="только эти аккаунты (можно повторять)")
    ap.add_argument("--include-login", action="store_true", help="трогать и логин-аккаунты (не из guest_accounts)")
    ap.add_argument("--seed", type=int, default=None)
    ap.add_argument("--allow-old", action="store_true", help="не отфильтровывать старые версии браузеров")
    ap.add_argument("--no-edge", action="store_true", help="не использовать Edge (нужен установленный msedge)")
    ap.add_argument("--no-mobile", action="store_true", help="без ios/android профилей")
    ap.add_argument("--dry-run", action="store_true")
    ap.add_argument("--refresh", action="store_true",
                    help="после записи запустить refresh_guest_cookies.py для изменённых гостевых слотов")
    args = ap.parse_args()

    accounts_text = ACCOUNTS_PATH.read_text(encoding="utf-8")
    guest_text = GUEST_PATH.read_text(encoding="utf-8")
    all_names = [a["name"] for a in yaml.safe_load(accounts_text)["accounts"]]
    guest = (yaml.safe_load(guest_text) or {}).get("guest_accounts") or {}

    if args.account:
        unknown = set(args.account) - set(all_names)
        if unknown:
            sys.exit(f"ОШИБКА: нет в accounts.yaml: {', '.join(sorted(unknown))}")
        selected = list(args.account)
    else:
        selected = [n for n in all_names if n in guest or args.include_login]
    selected = sorted(set(selected), key=acc_num)
    if not selected:
        sys.exit("Нечего менять (гостевых слотов не найдено).")

    pool = build_pool(args)
    if not pool:
        sys.exit("ОШИБКА: после фильтров не осталось ни одного impersonate-таргета (попробуй --allow-old).")

    rng = random.Random(args.seed)
    rng.shuffle(pool)
    if len(selected) > len(pool):
        print(f"ВНИМАНИЕ: аккаунтов ({len(selected)}) больше, чем уникальных таргетов ({len(pool)}) — "
              "часть impersonate повторится (UA/ОС будут разные).", file=sys.stderr)

    profiles = {name: build_profile(pool[i % len(pool)], rng) for i, name in enumerate(selected)}

    print(f"{'аккаунт':<11} {'тип':<7} {'impersonate':<18} {'engine':<9} профиль")
    for name in selected:
        p = profiles[name]
        kind = "guest" if name in guest else "login"
        print(f"{name:<11} {kind:<7} {p.impersonate:<18} {p.engine + ('/' + p.channel if p.channel else ''):<9} "
              f"{p.label}{' [' + p.device + ']' if p.device else ''}")

    login_touched = [n for n in selected if n not in guest]
    if login_touched:
        print(f"\nВНИМАНИЕ: {', '.join(login_touched)} — логин-аккаунты: в accounts.yaml отпечаток "
              "сменится, но их cookies/сессию нужно обновить вручную (refresh_cookies.py / перелогин).")

    if args.dry_run:
        print("\n[dry-run] ничего не записано")
        return 0

    stamp = time.strftime("%Y%m%d-%H%M%S")
    backup(ACCOUNTS_PATH, stamp)
    backup(GUEST_PATH, stamp)
    ACCOUNTS_PATH.write_text(patch_accounts_text(accounts_text, profiles), encoding="utf-8")
    GUEST_PATH.write_text(patch_guest_text(guest_text, guest, profiles), encoding="utf-8")
    print(f"\nOK: записано (бэкапы *.bak-{stamp})")

    guest_changed = [n for n in selected if n in guest]
    if args.refresh and guest_changed:
        cmd = [sys.executable, str(GUEST_SCRIPT)]
        for n in guest_changed:
            cmd += ["--account", n]
        print("Запускаю:", " ".join(cmd))
        rc = subprocess.call(cmd, cwd=str(ROOT))
        print(f"refresh_guest_cookies.py завершился с кодом {rc}")
    else:
        print("\nДальше:")
        print("  1) python3 scripts/refresh_guest_cookies.py   # перегенерить cookies под новые отпечатки")
        print("  2) docker compose restart scraper             # accounts.yaml читается только при старте")
    return 0


if __name__ == "__main__":
    sys.exit(main())
