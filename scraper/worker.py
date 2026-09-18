import asyncio
import random
import time
from concurrent.futures import ThreadPoolExecutor

import aiohttp

from .config import ConfigStore, load_cookies
from .constants import BASE_DIR, DEFAULT_CONNECT_TIMEOUT_SECONDS, PROXY_HOST, STORE_ENDPOINT_OVERRIDE, log
from .http_client import fetch_comments
from .pipeline import build_payload, send_batch_to_store
from .state import BackoffState, SeenCache, TokenBucket


async def account_worker(
    account: dict,
    config: ConfigStore,
    bucket: TokenBucket,
    seen: SeenCache,
    phase_offset: float,
    store_session: aiohttp.ClientSession,
    http_executor: ThreadPoolExecutor,
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
    # TLS/JA3-отпечаток curl_cffi (impersonate) — тоже свой на аккаунт,
    # и ФИКСИРОВАННЫЙ на всё время жизни аккаунта. Обязательно должен
    # соответствовать семейству браузера из user_agent (см. README) —
    # рассинхрон UA и impersonate палится сверкой TLS-отпечатка с
    # заявленным в заголовках браузером.
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

    await asyncio.sleep(phase_offset)
    log.info(
        "[%s] Старт (прокси %s, фаза +%.1fs, impersonate=%s, UA=%.40s...)",
        name, proxy_url, phase_offset, impersonate, user_agent,
    )

    async with aiohttp.ClientSession(cookies=cookies, headers=headers) as session:
        while True:
            cycle_start = time.monotonic()

            subs_joined = "+".join(config.get("subreddits", []))
            fetch_limit = config.get("fetch_limit", 50)
            max_age = config.get("max_age_seconds", 15)
            pagination_max_pages = config.get("pagination_max_pages", 3)
            # Случайная пауза МЕЖДУ запросами страниц пагинации внутри
            # одного цикла опроса (см. описание в config.yaml и в
            # http_client.fetch_comments) — не влияет на самый первый
            # запрос цикла, только на 2-ю и последующие страницы.
            pagination_delay_min = config.get("pagination_delay_min_seconds", 0.4)
            pagination_delay_max = config.get("pagination_delay_max_seconds", 1.5)
            poll_interval = config.get("poll_interval_seconds", 3)
            jitter_ratio = config.get("poll_jitter_ratio", 0.25)
            respect_ratelimit_headers = config.get("respect_ratelimit_headers", True)
            # Запас: используем не 100% оставшегося окна, а safety_margin
            # от него — иначе счёт "впритык" ломается от первой же
            # рассинхронизации часов/сети и мы всё равно ловим 429.
            ratelimit_safety_margin = config.get("ratelimit_safety_margin", 0.85)
            # Доп. джиттер прямо на расчётный интервал из X-Ratelimit-*
            # (reset/remaining), ДО общего poll_jitter_ratio ниже — иначе
            # сам расчётный интервал (напр. ровно 4.0с) остаётся "круглым"
            # и предсказуемым до того, как к нему применится финальный
            # джиттер сна. См. описание в config.yaml.
            ratelimit_jitter_ratio = config.get("ratelimit_jitter_ratio", 0.15)
            # timeout — read-таймаут (после установленного соединения);
            # connect_timeout — отдельный, обычно меньший таймаут на
            # DNS/TCP/TLS-хендшейк. Оба уходят в curl_cffi парой, а не
            # одним общим числом (см. "Блокирующие вызовы" в шапке файла).
            timeout = config.get("request_timeout_seconds", 10)
            connect_timeout = config.get("connect_timeout_seconds", DEFAULT_CONNECT_TIMEOUT_SECONDS)
            token_wait_timeout = config.get("token_wait_timeout_seconds", 0.5)
            store_endpoint = STORE_ENDPOINT_OVERRIDE or config.get("store_endpoint")
            batch_max_items = config.get("batch_max_items", 500)
            base_url = config.get("reddit_base_url", "https://www.reddit.com")

            bucket.update_rate(config.get("target_rate_per_second", 25))

            if not subs_joined:
                log.warning("[%s] Список subreddits пуст в config.yaml", name)
                await asyncio.sleep(poll_interval)
                continue

            result = await fetch_comments(
                session, base_url, subs_joined, fetch_limit, proxy_url, timeout, name, http_executor,
                max_age, pagination_max_pages, impersonate, connect_timeout,
                pagination_delay_min, pagination_delay_max,
            )

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

            # ---- дедуп (двухфазный claim) + rate-limit токены, затем батч-POST ----
            to_send: list[dict] = []
            dropped_dup = dropped_rate = 0
            for p in payloads:
                if not await seen.try_claim(p["external_id"]):
                    dropped_dup += 1
                    continue

                got_token = await _acquire_with_retry(bucket, token_wait_timeout)
                if not got_token:
                    # Токена не дождались — снимаем claim, иначе элемент
                    # навсегда "потеряется" как псевдо-дубль, хотя мы его
                    # так и не отправили.
                    await seen.release(p["external_id"])
                    dropped_rate += 1
                    continue

                to_send.append(p)

            sent = 0
            if to_send:
                # try/finally: гарантируем, что каждый item_id, заявленный
                # через try_claim() выше, будет либо confirm(), либо
                # release() — независимо от того, чем закончится отправка.
                # Без этого необработанное исключение в send_batch_to_store
                # (не-ClientError/TimeoutError, например неожиданный баг
                # парсинга) или CancelledError (таска воркера отменена
                # снаружи, например при shutdown) прервали бы выполнение
                # ДО for-цикла ниже (или посреди него) — часть to_send так
                # и осталась бы висеть в seen._pending навсегда: try_claim()
                # для этих id всегда возвращал бы False, и они бы медленно,
                # но неограниченно копились там месяцами.
                released_ids: set[str] = set()
                try:
                    ok_flags = await send_batch_to_store(
                        store_session, store_endpoint, to_send, name, batch_max_items
                    )
                    for p, ok in zip(to_send, ok_flags):
                        if ok:
                            # Подтверждено сервером (или как минимум 2xx на
                            # весь батч) -> окончательно помечаем seen.
                            await seen.confirm(p["external_id"])
                            released_ids.add(p["external_id"])
                            sent += 1
                            log.info(
                                "[%s] OK  %-12s age=%.1fs  %s",
                                name, p["external_id"], p["_age_seconds"],
                                p["content"][:60].replace("\n", " "),
                            )
                        else:
                            # Батч (или конкретный айтем) не подтверждён —
                            # освобождаем claim, чтобы он не считался дублем
                            # в следующем цикле опроса и мог уйти повторно.
                            await seen.release(p["external_id"])
                            released_ids.add(p["external_id"])
                finally:
                    # Всё, что не дошло до confirm()/release() выше (батч
                    # упал с необработанным исключением ДО получения
                    # ok_flags, или итерация по zip(...) была прервана
                    # CancelledError на середине) — освобождаем здесь.
                    for p in to_send:
                        if p["external_id"] not in released_ids:
                            await seen.release(p["external_id"])

            if sent or dropped_rate or to_send:
                not_confirmed = len(to_send) - sent
                log.info(
                    "[%s] цикл: получено=%d свежих=%d к_отправке=%d отправлено=%d "
                    "не_подтверждено=%d дублей=%d срезано_лимитом=%d",
                    name, len(comments), len(payloads), len(to_send), sent,
                    not_confirmed, dropped_dup, dropped_rate,
                )

            elapsed = time.monotonic() - cycle_start

            # Считаем интервал с учетом количества выкачанных страниц
            pages_count = getattr(result, "pages_fetched", 1)
            adjusted_interval = poll_interval * max(1, pages_count)

            # ---- адаптация по X-Ratelimit-* (если сервер их присылает) ----
            # Идея: равномерно размазать оставшиеся remaining запросов по
            # оставшемуся окну reset секунд, вместо того чтобы жить только
            # по статичному poll_interval из config.yaml и узнавать о
            # приближении лимита лишь по факту 429. Это НЕ подмена
            # poll_interval — только увеличение эффективного интервала
            # сверх сконфигурированного, если заголовки говорят, что
            # текущий темп не продержится до конца окна; поднять частоту
            # выше config.yaml эта логика никогда не может.
            ratelimit = getattr(result, "ratelimit", None)
            if respect_ratelimit_headers and ratelimit:
                remaining = ratelimit["remaining"]
                reset = ratelimit["reset"]
                if remaining <= 1:
                    # Лимит окна фактически исчерпан — ждём до reset
                    # (плюс небольшой запас), а не долбим ещё раз впустую.
                    rl_interval = reset + 1.0
                    log.warning(
                        "[%s] X-Ratelimit почти исчерпан (remaining=%.0f) — жду reset=%.1fs",
                        name, remaining, reset,
                    )
                elif reset > 0:
                    # safety_margin < 1.0 — намеренно не тратим впритык
                    # весь remaining, оставляем запас на джиттер/рассинхрон.
                    rl_interval = reset / (remaining * ratelimit_safety_margin)
                else:
                    rl_interval = 0.0

                if rl_interval > 0 and ratelimit_jitter_ratio > 0:
                    # Джиттер прямо на расчётный интервал (см. комментарий
                    # у ratelimit_jitter_ratio выше) — иначе reset/remaining
                    # часто даёт "круглые" числа вроде ровно 4.0с, что само
                    # по себе легко угадываемый паттерн, ещё до того, как
                    # к итоговому сну применится общий poll_jitter_ratio.
                    rl_interval *= random.uniform(
                        1 - ratelimit_jitter_ratio, 1 + ratelimit_jitter_ratio
                    )

                if rl_interval > adjusted_interval:
                    log.info(
                        "[%s] X-Ratelimit: remaining=%.0f reset=%.1fs -> интервал %.1fs "
                        "(вместо %.1fs из конфига)",
                        name, remaining, reset, rl_interval, adjusted_interval,
                    )
                    adjusted_interval = rl_interval

            # 1.0s — гарантированный минимальный отдых при любых задержках
            base_sleep = max(1.0, adjusted_interval - elapsed)
            jittered_sleep = base_sleep * random.uniform(1 - jitter_ratio, 1 + jitter_ratio)
            await asyncio.sleep(jittered_sleep)


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
            return  # воркер штатно завершился — выходим, не рестартуем
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
