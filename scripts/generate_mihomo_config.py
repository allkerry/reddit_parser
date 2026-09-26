#!/usr/bin/env python3
"""
Генерирует mihomo_config.yaml из:
  - vless_accounts.env   — простой файл вида account_1 = vless://...
  - accounts.yaml        — берём оттуда proxy_port для каждого account_N

Запуск (из корня проекта reddit_scraper/):

    python3 scripts/generate_mihomo_config.py

После этого просто (пере)запусти контейнер mihomo:

    docker compose up -d --build mihomo
    docker compose restart mihomo

Формат vless_accounts.env — по одной ссылке на аккаунт:

    account_1 = vless://uuid@host:port?...#name
    account_2 = vless://uuid@host:port?...#name

Строки с "#" в начале и пустые строки игнорируются. Аккаунты, для
которых в vless_accounts.env нет строки (или она закомментирована),
просто не получат слушателя в mihomo — воркер такого аккаунта не сможет
подключиться, пока для него не появится ссылка.
"""

from __future__ import annotations

import re
import sys
import urllib.parse
from pathlib import Path

import yaml

ROOT = Path(__file__).resolve().parent.parent
ACCOUNTS_PATH = ROOT / "accounts.yaml"
VLESS_ENV_PATH = ROOT / "vless_accounts.env"
OUTPUT_PATH = ROOT / "mihomo_config.yaml"

ACCOUNT_LINE_RE = re.compile(r"^\s*(account_\d+)\s*=\s*(\S+)\s*$")


def load_account_ports() -> dict[str, int]:
    """account_1 -> 7891, ... — берём proxy_port из accounts.yaml."""
    with open(ACCOUNTS_PATH, "r", encoding="utf-8") as f:
        data = yaml.safe_load(f) or {}
    ports = {}
    for acc in data.get("accounts", []):
        name = acc.get("name")
        port = acc.get("proxy_port")
        if name and port:
            ports[name] = int(port)
    return ports


def load_vless_links() -> dict[str, str]:
    """account_1 -> 'vless://...' из vless_accounts.env (некомментированные строки)."""
    if not VLESS_ENV_PATH.exists():
        print(f"ОШИБКА: не найден {VLESS_ENV_PATH}", file=sys.stderr)
        sys.exit(1)

    links: dict[str, str] = {}
    for raw_line in VLESS_ENV_PATH.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#"):
            continue
        m = ACCOUNT_LINE_RE.match(line)
        if not m:
            print(f"ПРЕДУПРЕЖДЕНИЕ: не смог разобрать строку в vless_accounts.env: {raw_line!r}", file=sys.stderr)
            continue
        account, link = m.group(1), m.group(2)
        if not link.startswith("vless://"):
            print(f"ПРЕДУПРЕЖДЕНИЕ: {account} — ссылка не начинается с vless://, пропускаю", file=sys.stderr)
            continue
        links[account] = link
    return links


def parse_vless(uri: str, fallback_name: str) -> dict:
    """Разбирает одну vless:// ссылку в словарь для mihomo proxies[]."""
    parsed = urllib.parse.urlsplit(uri)
    if parsed.scheme != "vless":
        raise ValueError(f"Не vless-ссылка: {uri}")

    uuid = parsed.username
    if not uuid:
        raise ValueError(f"В ссылке нет UUID: {uri}")

    server = parsed.hostname
    port = parsed.port
    if not server or not port:
        raise ValueError(f"В ссылке нет server/port: {uri}")

    q = urllib.parse.parse_qs(parsed.query)

    def qget(key: str, default: str = "") -> str:
        v = q.get(key)
        return v[0] if v else default

    name = urllib.parse.unquote(parsed.fragment) if parsed.fragment else fallback_name
    # mihomo не любит слишком экзотичные символы в имени прокси
    name = name.strip() or fallback_name

    security = qget("security", "none").lower()
    net_type = qget("type", "tcp").lower()
    fp = qget("fp")  # client-fingerprint
    sni = qget("sni") or qget("host")
    host_header = qget("host")
    path = urllib.parse.unquote(qget("path", "/")) or "/"
    flow = qget("flow")
    alpn_raw = qget("alpn")
    allow_insecure = qget("allowInsecure") in ("1", "true", "True")

    proxy: dict = {
        "name": name,
        "type": "vless",
        "server": server,
        "port": int(port),
        "uuid": uuid,
        "udp": True,
    }

    if flow:
        proxy["flow"] = flow

    if security in ("tls", "reality"):
        proxy["tls"] = True
        if sni:
            proxy["servername"] = sni
        if fp:
            proxy["client-fingerprint"] = fp
        if alpn_raw:
            proxy["alpn"] = [a.strip() for a in alpn_raw.split(",") if a.strip()]
        if allow_insecure:
            proxy["skip-cert-verify"] = True
        if security == "reality":
            pbk = qget("pbk")
            sid = qget("sid")
            reality_opts = {}
            if pbk:
                reality_opts["public-key"] = pbk
            if sid:
                reality_opts["short-id"] = sid
            if reality_opts:
                proxy["reality-opts"] = reality_opts
    else:
        proxy["tls"] = False

    # --- транспорт ---
    if net_type in ("ws", "httpupgrade"):
        proxy["network"] = "ws"
        ws_opts: dict = {"path": path}
        if host_header:
            ws_opts["headers"] = {"Host": host_header}
        if net_type == "httpupgrade":
            ws_opts["v2ray-http-upgrade"] = True
        proxy["ws-opts"] = ws_opts
    elif net_type == "grpc":
        proxy["network"] = "grpc"
        service_name = qget("serviceName") or path.strip("/")
        proxy["grpc-opts"] = {"grpc-service-name": service_name}
    elif net_type == "h2":
        proxy["network"] = "h2"
        proxy["h2-opts"] = {"host": [host_header] if host_header else [], "path": path}
    elif net_type in ("tcp", "", "raw", "none"):
        proxy["network"] = "tcp"
    else:
        # xhttp и прочие редкие варианты — прокидываем как есть на всякий случай
        proxy["network"] = net_type
        xhttp_opts = {"path": path}
        if host_header:
            xhttp_opts["host"] = host_header
        proxy["xhttp-opts"] = xhttp_opts

    return proxy


def build_config(account_ports: dict[str, int], account_links: dict[str, str]) -> dict:
    proxies = []
    proxy_groups = []
    listeners = []

    missing = [a for a in account_ports if a not in account_links]
    used = [a for a in account_links if a in account_ports]

    for account in sorted(used, key=lambda a: int(a.split("_")[1])):
        link = account_links[account]
        port = account_ports[account]
        try:
            proxy = parse_vless(link, fallback_name=account)
        except ValueError as e:
            print(f"ОШИБКА в ссылке для {account}: {e}", file=sys.stderr)
            continue

        proxies.append(proxy)
        group_name = f"pg-{account}"
        proxy_groups.append({"name": group_name, "type": "select", "proxies": [proxy["name"]]})
        listeners.append({
            "name": f"mixed-{account}",
            "type": "mixed",
            "port": port,
            "proxy": group_name,
        })

    if missing:
        print(
            "ПРЕДУПРЕЖДЕНИЕ: для этих аккаунтов из accounts.yaml нет ссылки в vless_accounts.env "
            f"(они останутся без прокси): {', '.join(sorted(missing))}",
            file=sys.stderr,
        )

    unknown = [a for a in account_links if a not in account_ports]
    if unknown:
        print(
            "ПРЕДУПРЕЖДЕНИЕ: в vless_accounts.env есть ссылки для аккаунтов, "
            f"которых нет в accounts.yaml (игнорирую): {', '.join(sorted(unknown))}",
            file=sys.stderr,
        )

    return {
        "allow-lan": False,
        "mode": "rule",
        "log-level": "info",
        "external-controller": "127.0.0.1:9090",
        "proxies": proxies,
        "proxy-groups": proxy_groups,
        "listeners": listeners,
        "rules": ["MATCH,DIRECT"],
    }


HEADER = """\
# ============================================================
#  Автоматически сгенерировано скриптом scripts/generate_mihomo_config.py
#  НЕ РЕДАКТИРУЙ ЭТОТ ФАЙЛ РУКАМИ — правь vless_accounts.env и запускай
#  скрипт заново, иначе изменения потеряются при следующей генерации.
#
#  Запускать mihomo: mihomo -f mihomo_config.yaml
#  (mihomo = Clash.Meta core, https://github.com/MetaCubeX/mihomo)
#  Требует mihomo с поддержкой "listeners" — актуальные релизы её имеют.
# ============================================================

"""


def main() -> None:
    account_ports = load_account_ports()
    account_links = load_vless_links()
    config = build_config(account_ports, account_links)

    if not config["proxies"]:
        print("ОШИБКА: не удалось собрать ни одного прокси — проверь vless_accounts.env", file=sys.stderr)
        sys.exit(1)

    yaml_text = yaml.safe_dump(config, allow_unicode=True, sort_keys=False, width=120)
    OUTPUT_PATH.write_text(HEADER + yaml_text, encoding="utf-8")

    print(f"OK: записал {OUTPUT_PATH} — {len(config['proxies'])} нод/аккаунтов:")
    for pg in config["proxy-groups"]:
        acc = pg["name"].removeprefix("pg-")
        port = next(l["port"] for l in config["listeners"] if l["proxy"] == pg["name"])
        print(f"  {acc}: локальный порт {port} -> {pg['proxies'][0]}")


if __name__ == "__main__":
    main()
