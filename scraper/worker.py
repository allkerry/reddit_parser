import asyncio
import time

import aiohttp

from .config import ConfigStore, CookieFileWatcher
from .constants import BASE_DIR, DEFAULT_CONNECT_TIMEOUT_SECONDS, PROXY_HOST, STORE_ENDPOINT_OVERRIDE, log
from .health import ExecutorHandle, ExecutorHealth
from .http_client import CurlSessionHandle, fetch_comments
from .pipeline import build_payload, send_batch_to_store
from .scheduler import PollOutcome, PollScheduler
from .state import BackoffState, SeenCache, TokenBucket


def _fullname(c: dict) -> str:
    return c.get("name") or f"t1_{c.get('id')}"


def rl_cooldown(ratelimit: dict | None, margin: float, max_interval: float) -> float:
    """Собственный бюджет аккаунта: через сколько секунд он снова имеет
    право делать запрос, чтобы равномерно растянуть remaining на reset."""
    if not ratelimit:
        return 0.0
    remaining, reset = ratelimit["remaining"], ratelimit["reset"]
    if remaining <= 1:
        v = reset + 1.0
    elif reset > 0:
        v = reset / (remaining * margin)
    else:
        v = 0.0
    return min(v, max_interval)


async def account_worker(
    account: dict,
    config: ConfigStore,
    bucket: TokenBucket,
    seen: SeenCache,
    scheduler: PollScheduler,
    store_session: aiohttp.ClientSession,
    http_executor: ExecutorHandle,
    executor_health: ExecutorHealth | None = None,
):
    name = account["name"]
    cookie_file = BASE_DIR / account["cookie_file"]
    proxy_url = f"http://{PROXY_HOST}:{account['proxy_port']}"

    if not cookie_file.exists():
        log.error("[%s] Файл с cookies не найден: %s — воркер не запущен", name, cookie_file)
        return

    cookie_watcher = CookieFileWatcher(
        cookie_file, min_check_interval=config.get("cookie_reload_check_interval_seconds", 30)
    )
    cookies = cookie_watcher.load_if_changed(force=True)
    if cookies is None:
        log.error("[%s] Не удалось прочитать cookies из %s — воркер не запущен", name, cookie_file)
        return

    user_agent = account.get("user_agent") or config.get("user_agent", "Mozilla/5.0")
    impersonate = account.get("impersonate") or config.get("impersonate", "chrome")
    headers = {
        "User-Agent": user_agent,
        "Accept": "application/json, text/plain, */*",
        "Accept-Language": "en-US,en;q=0.9",
    }
    curl_session = CurlSessionHandle(
        impersonate=impersonate, proxies={"http": proxy_url, "https": proxy_url}
    )
    backoff = BackoffState(
        base_seconds=config.get("base_backoff_seconds", 5),
        max_seconds=config.get("max_backoff_seconds", 300),
        max_auth_errors=config.get("max_consecutive_auth_errors", 5),
    )

    slot = scheduler.register(name)
    log.info("[%s] Старт (прокси %s, impersonate=%s) — жду слоты планировщика", name, proxy_url, impersonate)

    try:
        async with aiohttp.ClientSession(cookies=cookies, headers=headers) as session:
            while True:
                await slot.queue.get()  # слот выдан планировщиком

                outcome: PollOutcome | None = None
                cooldown = 0.0
                sent = 0
                try:
                    max_age = config.get("max_age_seconds", 120)
                    max_pages = config.get("pagination_max_pages", 5)
                    pg_min = config.get("pagination_delay_min_seconds", 0.1)
                    pg_max = config.get("pagination_delay_max_seconds", 0.4)
                    margin = config.get("ratelimit_safety_margin", 0.85)
                    rl_max = config.get("ratelimit_max_interval_seconds", 900)
                    timeout = config.get("request_timeout_seconds", 10)
                    connect_timeout = config.get("connect_timeout_seconds", DEFAULT_CONNECT_TIMEOUT_SECONDS)
                    token_wait_timeout = config.get("token_wait_timeout_seconds", 0.5)
                    store_endpoint = STORE_ENDPOINT_OVERRIDE or config.get("store_endpoint")
                    batch_max_items = config.get("batch_max_items", 500)
                    base_url = config.get("reddit_base_url", "https://www.reddit.com")
                    fetch_limit = min(100, config.get("fetch_limit", 100))
                    subs_joined = "+".join(config.get("stream_subs", ["all"]))

                    cookie_watcher.min_check_interval = config.get("cookie_reload_check_interval_seconds", 30)
                    new_cookies = cookie_watcher.load_if_changed()
                    if new_cookies is not None:
                        session.cookie_jar.clear()
                        session.cookie_jar.update_cookies(new_cookies)
                        log.info("[%s] cookies обновлены из %s (%d шт.)", name, cookie_file, len(new_cookies))

                    # Потолок отправки (bucket.rate) теперь ведёт QueueGovernor
                    # (scraper/governor.py) — воркер его больше не трогает.

                    # Остановка пагинации: как только на странице встретился
                    # уже виденный комментарий — догнали, дальше листать не надо.
                    stats = {"pages": 0, "first_len": 0, "first_dupes": 0, "overlap": False}

                    def stop_when(page: list[dict]) -> bool:
                        stats["pages"] += 1
                        dupes = sum(1 for c in page if seen.contains(_fullname(c)))
                        if stats["pages"] == 1:
                            stats["first_len"] = len(page)
                            stats["first_dupes"] = dupes
                        if dupes:
                            stats["overlap"] = True
                            return True
                        return False

                    result = await fetch_comments(
                        session, base_url, subs_joined, fetch_limit, curl_session, timeout, name,
                        http_executor, max_age, max_pages, connect_timeout,
                        pg_min, pg_max, executor_health, stop_when=stop_when,
                    )

                    # ---- ошибки: бэкофф теперь только у этого аккаунта
                    #      (cooldown), остальные продолжают получать слоты ----
                    if result.error_kind == "rate_or_server":
                        cooldown = backoff.register_rate_or_server_error()
                        if result.retry_after:
                            cooldown = max(cooldown, result.retry_after)
                        log.warning("[%s] %s — аккаунт отдыхает %.1fs (подряд: %d)",
                                    name, result.status, cooldown, backoff.consecutive_errors)
                        continue

                    if result.error_kind == "auth":
                        cooldown, should_stop = backoff.register_auth_error()
                        if should_stop:
                            log.error("[%s] %s подряд %d раз — cookies протухли/бан. Воркер остановлен.",
                                      name, result.status, backoff.consecutive_auth_errors)
                            return
                        log.warning("[%s] %s — отдых %.1fs (auth-ошибок %d/%d)", name, result.status,
                                    cooldown, backoff.consecutive_auth_errors, backoff.max_auth_errors)
                        continue

                    if result.error_kind == "network":
                        cooldown = min(backoff.register_rate_or_server_error(), 30.0)
                        continue

                    # ---- успех ----
                    backoff.register_success()
                    comments = result.comments

                    payloads = []
                    for c in comments:
                        p = build_payload(c)
                        if p is None or p["_age_seconds"] > max_age:
                            continue
                        payloads.append(p)
                    payloads.sort(key=lambda p: p["_age_seconds"])

                    to_send: list[dict] = []
                    dropped_dup = dropped_rate = 0
                    for p in payloads:
                        if not await seen.try_claim(p["external_id"]):
                            dropped_dup += 1
                            continue
                        if not await _acquire_with_retry(bucket, token_wait_timeout):
                            await seen.release(p["external_id"])
                            dropped_rate += 1
                            continue
                        to_send.append(p)

                    if to_send:
                        done_ids: set[str] = set()
                        try:
                            ok_flags = await send_batch_to_store(
                                store_session, store_endpoint, to_send, name, batch_max_items
                            )
                            for p, ok in zip(to_send, ok_flags):
                                if ok:
                                    await seen.confirm(p["external_id"])
                                    sent += 1
                                else:
                                    await seen.release(p["external_id"])
                                done_ids.add(p["external_id"])
                        finally:
                            for p in to_send:
                                if p["external_id"] not in done_ids:
                                    await seen.release(p["external_id"])

                    outcome = PollOutcome(
                        page_len=stats["first_len"], dupes=stats["first_dupes"],
                        pages=result.pages_fetched, overlap_found=stats["overlap"],
                    )
                    cooldown = rl_cooldown(result.ratelimit, margin, rl_max)

                    log.info(
                        "[%s] r/%s стр=%d получено=%d дубли_стр1=%d/%d отправлено=%d "
                        "не_подтв=%d срезано_лимитом=%d догнали=%s cooldown=%.1fs",
                        name, subs_joined, result.pages_fetched, len(comments),
                        stats["first_dupes"], stats["first_len"], sent,
                        len(to_send) - sent, dropped_rate, stats["overlap"], cooldown,
                    )
                finally:
                    scheduler.release(slot, outcome, cooldown, sent)
    finally:
        scheduler.unregister(slot)


async def _acquire_with_retry(bucket: TokenBucket, deadline: float) -> bool:
    start = time.monotonic()
    while True:
        if await bucket.try_acquire():
            return True
        if time.monotonic() - start >= deadline:
            return False
        await asyncio.sleep(0.02)


async def supervised(
    coro_fn, *args, name: str = "worker",
    base_backoff: float = 2.0, max_backoff: float = 60.0, **kwargs,
):
    attempt = 0
    while True:
        try:
            await coro_fn(*args, **kwargs)
            return
        except asyncio.CancelledError:
            raise
        except Exception:
            attempt += 1
            delay = min(max_backoff, base_backoff * (2 ** (attempt - 1)))
            log.exception("[%s] воркер упал (попытка %d), рестарт через %.1fs", name, attempt, delay)
            await asyncio.sleep(delay)