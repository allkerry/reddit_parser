#!/usr/bin/env python3
"""
Reddit fresh-comments scraper -> POST /store_item

См. README.md за полным описанием архитектуры. Ключевое в этой версии
(v3) — минимизация риска рейт-лимита/бана:

- Экспоненциальный backoff при 429 и 5xx, с учётом заголовка Retry-After,
  если Reddit его присылает. Пока идёт backoff — воркер аккаунта просто
  спит и ничего не запрашивает (не "долбит" повторно, это только продлевает
  блокировку).
- Джиттер (+-poll_jitter_ratio) на обычный интервал опроса — паттерн
  запросов не идеально ровный, как у типичного бота.
- Реалистичный User-Agent на каждый аккаунт (задаётся в accounts.yaml),
  вместо самопального "python:...", который прямо выдаёт скрипт.
- При повторяющихся 401/403 (протухшие/забаненные cookies) — воркер
  аккаунта останавливается после max_consecutive_auth_errors подряд
  неудач (с бэкоффом между попытками), а не долбит забаненный аккаунт
  бесконечно каждые несколько секунд.
- Общий (на все аккаунты) rate limiter для отправки в store_item и общий
  in-memory дедуп — как и раньше.
- config.yaml перечитывается на лету каждые CONFIG_RELOAD_SECONDS секунд.
"""

import asyncio
import json
import logging
import os
import random
import time
from collections import deque
from datetime import datetime, timezone
from pathlib import Path

import aiohttp
import yaml

BASE_DIR = Path(__file__).parent
CONFIG_PATH = Path(os.environ.get("CONFIG_PATH", BASE_DIR / "config.yaml"))
ACCOUNTS_PATH = Path(os.environ.get("ACCOUNTS_PATH", BASE_DIR / "accounts.yaml"))

PROXY_HOST = os.environ.get("PROXY_HOST", "127.0.0.1")
STORE_ENDPOINT_OVERRIDE = os.environ.get("STORE_ENDPOINT")
CONFIG_RELOAD_SECONDS = float(os.environ.get("CONFIG_RELOAD_SECONDS", "5"))

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] [%(name)s] %(message)s",
)
log = logging.getLogger("scraper")


# ---------------------------------------------------------------- #
#  Живой конфиг (hot-reload)
# ---------------------------------------------------------------- #

class ConfigStore:
    def __init__(self, path: Path, reload_interval: float):
        self.path = path
        self.reload_interval = reload_interval
        self._data: dict = {}

    def load_once(self):
        with open(self.path, "r", encoding="utf-8") as f:
            self._data = yaml.safe_load(f)

    async def reload_loop(self):
        while True:
            await asyncio.sleep(self.reload_interval)
            try:
                old = self._data
                self.load_once()
                if old != self._data:
                    log.info(
                        "config.yaml обновлён: target_rate=%s max_age=%ss subs=%d",
                        self.get("target_rate_per_second"),
                        self.get("max_age_seconds"),
                        len(self.get("subreddits", [])),
                    )
            except Exception as e:
                log.warning("Не удалось перечитать config.yaml (оставляю старые значения): %s", e)

    def get(self, key, default=None):
        return self._data.get(key, default)


def load_accounts() -> list[dict]:
    with open(ACCOUNTS_PATH, "r", encoding="utf-8") as f:
        data = yaml.safe_load(f)
    return data.get("accounts", [])


def load_cookies(cookie_file: Path) -> dict:
    with open(cookie_file, "r", encoding="utf-8") as f:
        raw = json.load(f)
    return {c["name"]: c["value"] for c in raw if "name" in c and "value" in c}


# ---------------------------------------------------------------- #
#  Общий rate limiter (token bucket) на отправку в store_item
# ---------------------------------------------------------------- #

class TokenBucket:
    def __init__(self, rate_per_second: float):
        self.rate = rate_per_second
        self.capacity = max(rate_per_second, 1.0)
        self.tokens = self.capacity
        self.last_refill = time.monotonic()
        self.lock = asyncio.Lock()

    def update_rate(self, rate_per_second: float):
        if rate_per_second != self.rate:
            self.rate = rate_per_second
            self.capacity = max(rate_per_second, 1.0)

    async def try_acquire(self) -> bool:
        async with self.lock:
            now = time.monotonic()
            elapsed = now - self.last_refill
            self.last_refill = now
            self.tokens = min(self.capacity, self.tokens + elapsed * self.rate)
            if self.tokens >= 1.0:
                self.tokens -= 1.0
                return True
            return False


# ---------------------------------------------------------------- #
#  Общий дедуп-кэш
# ---------------------------------------------------------------- #

class SeenCache:
    def __init__(self, max_size: int):
        self._deque = deque(maxlen=max_size)
        self._set = set()
        self._lock = asyncio.Lock()

    async def seen_or_mark(self, item_id: str) -> bool:
        async with self._lock:
            if item_id in self._set:
                return True
            if len(self._deque) == self._deque.maxlen:
                old = self._deque.popleft()
                self._set.discard(old)
            self._deque.append(item_id)
            self._set.add(item_id)
            return False


# ---------------------------------------------------------------- #
#  Backoff-состояние на аккаунт
# ---------------------------------------------------------------- #

class BackoffState:
    """Отдельно считает подряд идущие rate-limit/5xx ошибки и отдельно —
    подряд идущие auth-ошибки (401/403), т.к. у них разная семантика:
    первое — "притормози", второе — "возможно, аккаунт мёртв"."""

    def __init__(self, base_seconds: float, max_seconds: float, max_auth_errors: int):
        self.base_seconds = base_seconds
        self.max_seconds = max_seconds
        self.max_auth_errors = max_auth_errors
        self.consecutive_errors = 0
        self.consecutive_auth_errors = 0

    def register_success(self):
        self.consecutive_errors = 0
        self.consecutive_auth_errors = 0

    def register_rate_or_server_error(self) -> float:
        self.consecutive_errors += 1
        delay = min(self.max_seconds, self.base_seconds * (2 ** (self.consecutive_errors - 1)))
        jitter = delay * random.uniform(0.15, 0.35)
        return min(self.max_seconds, delay + jitter)

    def register_auth_error(self) -> tuple[float, bool]:
        """Возвращает (задержка перед следующей попыткой, should_stop)."""
        self.consecutive_auth_errors += 1
        should_stop = self.consecutive_auth_errors >= self.max_auth_errors
        delay = min(self.max_seconds, self.base_seconds * (2 ** (self.consecutive_auth_errors - 1)))
        jitter = delay * random.uniform(0.15, 0.35)
        return min(self.max_seconds, delay + jitter), should_stop


# ---------------------------------------------------------------- #
#  Вспомогательные функции
# ---------------------------------------------------------------- #

def iso_utc(ts: float) -> str:
    dt = datetime.fromtimestamp(ts, tz=timezone.utc)
    return dt.strftime("%Y-%m-%dT%H:%M:%S.") + f"{dt.microsecond:06d}Z"


def build_payload(comment_data: dict) -> dict | None:
    comment_id = comment_data.get("id")
    if not comment_id:
        return None

    fullname = comment_data.get("name") or f"t1_{comment_id}"
    created_utc = comment_data.get("created_utc")
    if created_utc is None:
        return None

    body = comment_data.get("body", "") or ""
    author = comment_data.get("author", "") or ""
    permalink = comment_data.get("permalink", "")
    url = f"https://www.reddit.com{permalink}" if permalink else ""

    parent_id = comment_data.get("parent_id", "") or ""
    external_parent_id = parent_id if parent_id.startswith("t1_") else ""

    return {
        "content": body,
        "external_id": fullname,
        "created_at": iso_utc(created_utc),
        "domain": "reddit.com",
        "url": url,
        "title": "",
        "author": author,
        "username": author,
        "external_parent_id": external_parent_id,
        "_age_seconds": time.time() - created_utc,
    }


def parse_retry_after(resp: aiohttp.ClientResponse) -> float | None:
    """Reddit может прислать Retry-After в секундах (числом)."""
    value = resp.headers.get("Retry-After")
    if not value:
        return None
    try:
        return float(value)
    except ValueError:
        return None


# ---------------------------------------------------------------- #
#  Один аккаунт = один воркер
# ---------------------------------------------------------------- #

class FetchResult:
    __slots__ = ("comments", "status", "retry_after", "error_kind")

    def __init__(self, comments=None, status=None, retry_after=None, error_kind=None):
        self.comments = comments or []
        self.status = status
        self.retry_after = retry_after
        # error_kind: None | "rate_or_server" | "auth" | "network"
        self.error_kind = error_kind


async def fetch_comments(
    session: aiohttp.ClientSession,
    base_url: str,
    subs_joined: str,
    fetch_limit: int,
    proxy_url: str,
    timeout: int,
    account_name: str,
) -> FetchResult:
    url = f"{base_url}/r/{subs_joined}/comments.json"
    params = {"limit": fetch_limit}
    try:
        async with session.get(
            url, params=params, proxy=proxy_url, timeout=aiohttp.ClientTimeout(total=timeout)
        ) as resp:
            if resp.status == 429:
                retry_after = parse_retry_after(resp)
                return FetchResult(status=429, retry_after=retry_after, error_kind="rate_or_server")
            if resp.status in (401, 403):
                return FetchResult(status=resp.status, error_kind="auth")
            if resp.status >= 500:
                return FetchResult(status=resp.status, error_kind="rate_or_server")
            if resp.status != 200:
                log.warning("[%s] Reddit вернул неожиданный статус %s", account_name, resp.status)
                return FetchResult(status=resp.status, error_kind="rate_or_server")

            data = await resp.json(content_type=None)
    except asyncio.TimeoutError:
        log.warning("[%s] Таймаут запроса к Reddit", account_name)
        return FetchResult(error_kind="network")
    except aiohttp.ClientError as e:
        log.warning("[%s] Ошибка запроса к Reddit (проверь mihomo/порт): %s", account_name, e)
        return FetchResult(error_kind="network")

    children = data.get("data", {}).get("children", [])
    comments = [c.get("data", {}) for c in children if c.get("kind") == "t1"]
    return FetchResult(comments=comments, status=200)


async def send_to_store(
    session: aiohttp.ClientSession, endpoint: str, payload: dict, account_name: str
) -> bool:
    payload = {k: v for k, v in payload.items() if not k.startswith("_")}
    try:
        async with session.post(endpoint, json=payload, timeout=aiohttp.ClientTimeout(total=5)) as resp:
            if resp.status >= 300:
                text = await resp.text()
                log.warning(
                    "[%s] store_item ответил %s для %s: %s",
                    account_name, resp.status, payload["external_id"], text[:200],
                )
                return False
            return True
    except aiohttp.ClientError as e:
        log.warning("[%s] Ошибка отправки в store_item (%s): %s", account_name, endpoint, e)
        return False


async def account_worker(
    account: dict,
    config: ConfigStore,
    bucket: TokenBucket,
    seen: SeenCache,
    phase_offset: float,
    store_session: aiohttp.ClientSession,
):
    name = account["name"]
    cookie_file = BASE_DIR / account["cookie_file"]
    proxy_port = account["proxy_port"]
    proxy_url = f"http://{PROXY_HOST}:{proxy_port}"

    if not cookie_file.exists():
        log.error("[%s] Файл с cookies не найден: %s — воркер не запущен", name, cookie_file)
        return

    cookies = load_cookies(cookie_file)

    # User-Agent: приоритет — свой в accounts.yaml, иначе дефолтный из config.yaml.
    user_agent = account.get("user_agent") or config.get("user_agent", "Mozilla/5.0")
    headers = {
        "User-Agent": user_agent,
        "Accept": "application/json, text/plain, */*",
        "Accept-Language": "en-US,en;q=0.9",
    }

    backoff = BackoffState(
        base_seconds=config.get("base_backoff_seconds", 5),
        max_seconds=config.get("max_backoff_seconds", 300),
        max_auth_errors=config.get("max_consecutive_auth_errors", 5),
    )

    await asyncio.sleep(phase_offset)
    log.info("[%s] Старт (прокси %s, фаза +%.1fs, UA=%.40s...)", name, proxy_url, phase_offset, user_agent)

    async with aiohttp.ClientSession(cookies=cookies, headers=headers) as session:
        while True:
            cycle_start = time.monotonic()

            subs_joined = "+".join(config.get("subreddits", []))
            fetch_limit = config.get("fetch_limit", 50)
            max_age = config.get("max_age_seconds", 15)
            poll_interval = config.get("poll_interval_seconds", 3)
            jitter_ratio = config.get("poll_jitter_ratio", 0.25)
            timeout = config.get("request_timeout_seconds", 10)
            token_wait_timeout = config.get("token_wait_timeout_seconds", 0.5)
            store_endpoint = STORE_ENDPOINT_OVERRIDE or config.get("store_endpoint")
            base_url = config.get("reddit_base_url", "https://www.reddit.com")

            bucket.update_rate(config.get("target_rate_per_second", 25))

            if not subs_joined:
                log.warning("[%s] Список subreddits пуст в config.yaml", name)
                await asyncio.sleep(poll_interval)
                continue

            result = await fetch_comments(session, base_url, subs_joined, fetch_limit, proxy_url, timeout, name)

            # ---- обработка ошибок / backoff ----
            if result.error_kind == "rate_or_server":
                delay = backoff.register_rate_or_server_error()
                if result.retry_after:
                    delay = max(delay, result.retry_after)
                log.warning(
                    "[%s] %s — backoff %.1fs (подряд ошибок: %d)",
                    name, result.status, delay, backoff.consecutive_errors,
                )
                await asyncio.sleep(delay)
                continue

            if result.error_kind == "auth":
                delay, should_stop = backoff.register_auth_error()
                if should_stop:
                    log.error(
                        "[%s] %s подряд %d раз — похоже, cookies протухли или аккаунт забанен. "
                        "Воркер остановлен, обнови cookies и перезапусти.",
                        name, result.status, backoff.consecutive_auth_errors,
                    )
                    return
                log.warning(
                    "[%s] %s — backoff %.1fs (подряд auth-ошибок: %d/%d)",
                    name, result.status, delay, backoff.consecutive_auth_errors, backoff.max_auth_errors,
                )
                await asyncio.sleep(delay)
                continue

            if result.error_kind == "network":
                delay = backoff.register_rate_or_server_error()
                await asyncio.sleep(min(delay, poll_interval * 3))
                continue

            # ---- успешный ответ ----
            backoff.register_success()
            comments = result.comments

            payloads = []
            for c in comments:
                p = build_payload(c)
                if p is None:
                    continue
                if p["_age_seconds"] > max_age:
                    continue
                payloads.append(p)

            payloads.sort(key=lambda p: p["_age_seconds"])

            sent = dropped_dup = dropped_rate = 0
            for p in payloads:
                if await seen.seen_or_mark(p["external_id"]):
                    dropped_dup += 1
                    continue

                got_token = await _acquire_with_retry(bucket, token_wait_timeout)
                if not got_token:
                    dropped_rate += 1
                    continue

                ok = await send_to_store(store_session, store_endpoint, p, name)
                if ok:
                    sent += 1
                    log.info(
                        "[%s] OK  %-12s age=%.1fs  %s",
                        name, p["external_id"], p["_age_seconds"],
                        p["content"][:60].replace("\n", " "),
                    )

            if sent or dropped_rate:
                log.info(
                    "[%s] цикл: получено=%d свежих=%d отправлено=%d дублей=%d срезано_лимитом=%d",
                    name, len(comments), len(payloads), sent, dropped_dup, dropped_rate,
                )

            elapsed = time.monotonic() - cycle_start
            base_sleep = max(0.0, poll_interval - elapsed)
            jittered_sleep = base_sleep * random.uniform(1 - jitter_ratio, 1 + jitter_ratio)
            await asyncio.sleep(max(0.0, jittered_sleep))


async def _acquire_with_retry(bucket: TokenBucket, deadline: float) -> bool:
    start = time.monotonic()
    while True:
        if await bucket.try_acquire():
            return True
        if time.monotonic() - start >= deadline:
            return False
        await asyncio.sleep(0.02)


# ---------------------------------------------------------------- #
#  main
# ---------------------------------------------------------------- #

async def main():
    config = ConfigStore(CONFIG_PATH, CONFIG_RELOAD_SECONDS)
    config.load_once()

    accounts = [a for a in load_accounts() if a.get("enabled")]
    if not accounts:
        log.error("Нет ни одного enabled:true аккаунта в accounts.yaml — нечего запускать")
        return

    log.info(
        "Запуск: %d аккаунт(ов), %d сабреддитов, target_rate=%s/сек, max_age=%ss, proxy_host=%s",
        len(accounts), len(config.get("subreddits", [])),
        config.get("target_rate_per_second"), config.get("max_age_seconds"), PROXY_HOST,
    )

    bucket = TokenBucket(config.get("target_rate_per_second", 25))
    seen = SeenCache(config.get("seen_cache_size", 1000))

    async with aiohttp.ClientSession() as store_session:
        tasks = [asyncio.create_task(config.reload_loop())]
        for i, account in enumerate(accounts):
            phase_offset = i * (config.get("poll_interval_seconds", 3) / len(accounts))
            tasks.append(
                asyncio.create_task(
                    account_worker(account, config, bucket, seen, phase_offset, store_session)
                )
            )
        await asyncio.gather(*tasks)


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        log.info("Остановлено пользователем")
