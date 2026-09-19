import asyncio
import random
import time
from concurrent.futures import ThreadPoolExecutor

import aiohttp

from .config import ConfigStore, load_cookies
from .constants import BASE_DIR, DEFAULT_CONNECT_TIMEOUT_SECONDS, PROXY_HOST, STORE_ENDPOINT_OVERRIDE, log
from .health import ExecutorHealth
from .http_client import fetch_comments
from .pipeline import build_payload, send_batch_to_store
from .state import BackoffState, GroupState, SeenCache, TokenBucket

# Дефолтные базовые интервалы опроса по тиру, если не заданы в config.yaml
# (tier_base_interval_seconds). Это ТОЛЬКО стартовая точка — дальше
# GroupState.record_yield() сам подстраивает interval по факту выхода.
_DEFAULT_TIER_INTERVALS = {"hot": 3, "medium": 15, "cold": 60}


async def account_worker(
    account: dict,
    config: ConfigStore,
    bucket: TokenBucket,
    seen: SeenCache,
    phase_offset: float,
    store_session: aiohttp.ClientSession,
    http_executor: ThreadPoolExecutor,
    groups: list[dict],
    executor_health: ExecutorHealth | None = None,
):
    """Один аккаунт теперь опрашивает НЕСКОЛЬКО групп сабреддитов (см.
    scraper/grouping.py), а не один статичный список. У каждой группы —
    своё расписание (GroupState.next_poll_at), которое адаптируется по
    фактическому выходу этой группы. Цикл воркера каждую итерацию берёт
    группу с самым близким next_poll_at, ждёт до этого момента (если
    нужно) и опрашивает именно её.

    Реалистичные интервалы опроса разных сабов не совпадают друг с
    другом с самого начала (см. staggering ниже) и продолжают
    расходиться по мере того, как каждая группа подстраивает свой
    interval — это ещё один уровень анти-паттерна поверх джиттера,
    описанного в ARCHITECTURE.md.

    `executor_health` (см. scraper/health.py) — общий на процесс счётчик
    занятости http_executor, пробрасывается дальше в fetch_comments() /
    _fetch_comments_page() для диагностики зависших потоков curl_cffi."""
    name = account["name"]
    cookie_file = BASE_DIR / account["cookie_file"]
    proxy_port = account["proxy_port"]
    proxy_url = f"http://{PROXY_HOST}:{proxy_port}"

    if not cookie_file.exists():
        log.error("[%s] Файл с cookies не найден: %s — воркер не запущен", name, cookie_file)
        return

    if not groups:
        log.warning("[%s] нет назначенных сабреддитов (groups пуст) — воркер не запущен", name)
        return

    cookies = load_cookies(cookie_file)

    user_agent = account.get("user_agent") or config.get("user_agent", "Mozilla/5.0")
    impersonate = account.get("impersonate") or config.get("impersonate", "chrome")
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

    tier_intervals = {**_DEFAULT_TIER_INTERVALS, **config.get("tier_base_interval_seconds", {})}
    group_states = [
        GroupState(g["subs"], g["tier"], tier_intervals.get(g["tier"], 10))
        for g in groups
    ]

    # Стартовое расписание: не сразу все группы разом (это было бы видно
    # как залп N запросов в первую же секунду), а размазано в пределах
    # собственного интервала каждой группы + сдвиг фазы аккаунта.
    start_at = time.monotonic() + phase_offset
    for gs in group_states:
        gs.next_poll_at = start_at + random.uniform(0, gs.interval)

    log.info(
        "[%s] Старт (прокси %s, фаза +%.1fs, impersonate=%s, %d групп: %s)",
        name, proxy_url, phase_offset, impersonate, len(group_states),
        ", ".join(f"{g.tier}x{len(g.subs)}" for g in group_states),
    )

    async with aiohttp.ClientSession(cookies=cookies, headers=headers) as session:
        while True:
            now = time.monotonic()
            gs = min(group_states, key=lambda g: g.next_poll_at)
            wait = gs.next_poll_at - now
            if wait > 0:
                await asyncio.sleep(wait)

            cycle_start = time.monotonic()

            fetch_limit = config.get("fetch_limit", 50)
            max_age = config.get("max_age_seconds", 15)
            pagination_max_pages = config.get("pagination_max_pages", 3)
            pagination_delay_min = config.get("pagination_delay_min_seconds", 0.4)
            pagination_delay_max = config.get("pagination_delay_max_seconds", 1.5)
            jitter_ratio = config.get("poll_jitter_ratio", 0.25)
            respect_ratelimit_headers = config.get("respect_ratelimit_headers", True)
            ratelimit_safety_margin = config.get("ratelimit_safety_margin", 0.85)
            ratelimit_jitter_ratio = config.get("ratelimit_jitter_ratio", 0.15)
            timeout = config.get("request_timeout_seconds", 10)
            connect_timeout = config.get("connect_timeout_seconds", DEFAULT_CONNECT_TIMEOUT_SECONDS)
            token_wait_timeout = config.get("token_wait_timeout_seconds", 0.5)
            store_endpoint = STORE_ENDPOINT_OVERRIDE or config.get("store_endpoint")
            batch_max_items = config.get("batch_max_items", 500)
            base_url = config.get("reddit_base_url", "https://www.reddit.com")
            adaptive_cfg = config.get("adaptive_scheduling", {})

            bucket.update_rate(config.get("target_rate_per_second", 25))

            # ---- запрос: порядок сабов и fetch_limit пересобираются
            #      каждый раз, чтобы URL/тело запроса не было буквально
            #      одним и тем же строковым значением из цикла в цикл ----
            subs_shuffled = gs.subs[:]
            random.shuffle(subs_shuffled)
            subs_joined = "+".join(subs_shuffled)
            fetch_limit_jittered = max(10, int(fetch_limit * random.uniform(0.85, 1.0)))

            # Для медленно опрашиваемых (холодных) групп используем
            # эффективный max_age не ниже собственного интервала опроса
            # группы (с запасом x1.2) — иначе комментарий, появившийся
            # сразу после предыдущего опроса, успеет "протухнуть" по
            # статичному max_age из config.yaml раньше, чем группа будет
            # опрошена снова, и будет молча потерян. Конфиговый max_age
            # остаётся нижней границей — для горячих групп ничего не
            # меняется, если их текущий interval меньше max_age.
            effective_max_age = max(max_age, gs.interval * 1.2)

            if not subs_joined:
                log.warning("[%s] у группы (%s) пустой список сабов", name, gs.tier)
                gs.next_poll_at = cycle_start + gs.interval
                continue

            result = await fetch_comments(
                session, base_url, subs_joined, fetch_limit_jittered, proxy_url, timeout, name,
                http_executor, effective_max_age, pagination_max_pages, impersonate, connect_timeout,
                pagination_delay_min, pagination_delay_max,
                executor_health=executor_health,
            )

            # ---- обработка ошибок / backoff (общий на аккаунт, не на
            #      группу — 429/бан не привязаны к конкретному сабу) ----
            if result.error_kind == "rate_or_server":
                delay = backoff.register_rate_or_server_error()
                if result.retry_after:
                    delay = max(delay, result.retry_after)
                log.warning(
                    "[%s] группа(%s) %s — backoff %.1fs (подряд ошибок: %d)",
                    name, gs.tier, result.status, delay, backoff.consecutive_errors,
                )
                gs.next_poll_at = time.monotonic() + gs.interval
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
                gs.next_poll_at = time.monotonic() + gs.interval
                await asyncio.sleep(delay)
                continue

            if result.error_kind == "network":
                delay = backoff.register_rate_or_server_error()
                gs.next_poll_at = time.monotonic() + gs.interval
                await asyncio.sleep(min(delay, gs.interval * 3))
                continue

            # ---- успешный ответ ----
            backoff.register_success()
            comments = result.comments

            payloads = []
            for c in comments:
                p = build_payload(c)
                if p is None:
                    continue
                if p["_age_seconds"] > effective_max_age:
                    continue
                payloads.append(p)

            payloads.sort(key=lambda p: p["_age_seconds"])

            to_send: list[dict] = []
            dropped_dup = dropped_rate = 0
            for p in payloads:
                if not await seen.try_claim(p["external_id"]):
                    dropped_dup += 1
                    continue

                got_token = await _acquire_with_retry(bucket, token_wait_timeout)
                if not got_token:
                    await seen.release(p["external_id"])
                    dropped_rate += 1
                    continue

                to_send.append(p)

            sent = 0
            if to_send:
                released_ids: set[str] = set()
                try:
                    ok_flags = await send_batch_to_store(
                        store_session, store_endpoint, to_send, name, batch_max_items
                    )
                    for p, ok in zip(to_send, ok_flags):
                        if ok:
                            await seen.confirm(p["external_id"])
                            released_ids.add(p["external_id"])
                            sent += 1
                            log.info(
                                "[%s] OK  %-12s age=%.1fs  %s",
                                name, p["external_id"], p["_age_seconds"],
                                p["content"][:60].replace("\n", " "),
                            )
                        else:
                            await seen.release(p["external_id"])
                            released_ids.add(p["external_id"])
                finally:
                    for p in to_send:
                        if p["external_id"] not in released_ids:
                            await seen.release(p["external_id"])

            if sent or dropped_rate or to_send:
                not_confirmed = len(to_send) - sent
                log.info(
                    "[%s] группа(%s, %d сабов) цикл: получено=%d свежих=%d к_отправке=%d "
                    "отправлено=%d не_подтверждено=%d дублей=%d срезано_лимитом=%d интервал=%.1fs",
                    name, gs.tier, len(gs.subs), len(comments), len(payloads), len(to_send), sent,
                    not_confirmed, dropped_dup, dropped_rate, gs.interval,
                )

            # ---- адаптация интервала ЭТОЙ группы по фактическому
            #      выходу (число свежих айтемов за цикл) ----
            if adaptive_cfg.get("enabled", True):
                gs.record_yield(len(payloads), adaptive_cfg)

            elapsed = time.monotonic() - cycle_start
            pages_count = getattr(result, "pages_fetched", 1)
            adjusted_interval = gs.interval * max(1, pages_count)

            # ---- адаптация по X-Ratelimit-* — общая для аккаунта, не
            #      только для текущей группы: заголовки отражают лимит
            #      целиком на cookies/IP этого аккаунта, а не на
            #      конкретный саб. Поэтому при близком исчерпании лимита
            #      сдвигаем ВСЕ группы аккаунта, а не только текущую —
            #      иначе следующая по расписанию группа сразу же уйдёт в
            #      те же 429. ----
            ratelimit = getattr(result, "ratelimit", None)
            if respect_ratelimit_headers and ratelimit:
                remaining = ratelimit["remaining"]
                reset = ratelimit["reset"]
                if remaining <= 1:
                    rl_interval = reset + 1.0
                    log.warning(
                        "[%s] X-Ratelimit почти исчерпан (remaining=%.0f) — жду reset=%.1fs "
                        "(сдвигаю все группы аккаунта)",
                        name, remaining, reset,
                    )
                    critical_at = time.monotonic() + rl_interval
                    for other in group_states:
                        if other.next_poll_at < critical_at:
                            other.next_poll_at = critical_at
                elif reset > 0:
                    rl_interval = reset / (remaining * ratelimit_safety_margin)
                else:
                    rl_interval = 0.0

                if rl_interval > 0 and ratelimit_jitter_ratio > 0:
                    rl_interval *= random.uniform(
                        1 - ratelimit_jitter_ratio, 1 + ratelimit_jitter_ratio
                    )

                if rl_interval > adjusted_interval:
                    log.info(
                        "[%s] X-Ratelimit: remaining=%.0f reset=%.1fs -> интервал %.1fs "
                        "(вместо %.1fs)",
                        name, remaining, reset, rl_interval, adjusted_interval,
                    )
                    adjusted_interval = rl_interval

            base_sleep = max(1.0, adjusted_interval - elapsed)
            jittered_sleep = base_sleep * random.uniform(1 - jitter_ratio, 1 + jitter_ratio)
            candidate_next = cycle_start + jittered_sleep
            # Не откатываем назад то, что уже могло быть выставлено выше
            # (критический ratelimit-сдвиг) более поздним временем.
            gs.next_poll_at = max(gs.next_poll_at, candidate_next)


async def _acquire_with_retry(bucket: TokenBucket, deadline: float) -> bool:
    start = time.monotonic()
    while True:
        if await bucket.try_acquire():
            return True
        if time.monotonic() - start >= deadline:
            return False
        await asyncio.sleep(0.02)


async def supervised(
    coro_fn,
    *args,
    name: str = "worker",
    base_backoff: float = 2.0,
    max_backoff: float = 60.0,
    **kwargs,
):
    """Перезапускает coro_fn(*args, **kwargs), если она упала с
    необработанным исключением, вместо того чтобы уронить весь
    asyncio.gather(). Нормальный return (воркер сам решил завершиться)
    не перезапускается — перезапуск только при Exception."""
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
            log.exception(
                "[%s] воркер упал (попытка %d), рестарт через %.1fs",
                name, attempt, delay,
            )
            await asyncio.sleep(delay)
