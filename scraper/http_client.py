import asyncio
import functools
import json
import random
import time
from concurrent.futures import ThreadPoolExecutor
from curl_cffi import requests as cffi_requests

import aiohttp

from .constants import DEFAULT_CONNECT_TIMEOUT_SECONDS, HTTP_EXECUTOR_SLACK_SECONDS, log
from .health import ExecutorHealth
from .models import FetchResult


def parse_retry_after(resp: aiohttp.ClientResponse) -> float | None:
    """Reddit может прислать Retry-After в секундах (числом)."""
    value = resp.headers.get("Retry-After")
    if not value:
        return None
    try:
        return float(value)
    except ValueError:
        return None


def parse_ratelimit_headers(resp) -> dict | None:
    """Разбирает X-Ratelimit-Remaining / X-Ratelimit-Reset (и опционально
    X-Ratelimit-Used) из ответа Reddit. Обычно приходят все вместе; если
    сервер их не прислал (эндпоинт/CDN не отдаёт телеметрию) — возвращаем
    None, и адаптация по заголовкам просто не включается для этого
    ответа (используется statичный poll_interval из config.yaml)."""
    try:
        remaining = resp.headers.get("X-Ratelimit-Remaining") or resp.headers.get("x-ratelimit-remaining")
        reset = resp.headers.get("X-Ratelimit-Reset") or resp.headers.get("x-ratelimit-reset")
        used = resp.headers.get("X-Ratelimit-Used") or resp.headers.get("x-ratelimit-used")
        if remaining is None or reset is None:
            return None
        return {
            "remaining": float(remaining),
            "reset": float(reset),
            "used": float(used) if used is not None else None,
        }
    except (TypeError, ValueError):
        return None


async def _fetch_comments_page(
    session: aiohttp.ClientSession,
    base_url: str,
    subs_joined: str,
    fetch_limit: int,
    proxy_url: str,
    timeout: int,
    account_name: str,
    http_executor: ThreadPoolExecutor,
    after: str | None = None,
    impersonate: str = "chrome",
    connect_timeout: float = DEFAULT_CONNECT_TIMEOUT_SECONDS,
    executor_health: ExecutorHealth | None = None,
) -> FetchResult:
    """Один HTTP-запрос к .../comments.json (одна страница листинга).
    Пагинацию по нескольким страницам делает fetch_comments() ниже.

    `timeout` здесь используется как read-таймаут (время на получение
    ответа после установленного соединения), `connect_timeout` — отдельный,
    как правило меньший таймаут на TCP/TLS-хендшейк и DNS-резолвинг.
    Оба передаются в curl_cffi явно, парой, а не одним общим числом —
    см. "Блокирующие вызовы" в шапке файла.

    `executor_health`, если передан, отслеживает реальную занятость
    http_executor (см. scraper/health.py) — счётчик инкрементируется
    перед постановкой задачи в пул и декрементируется через
    add_done_callback на самом Future, а не после await wait_for(...):
    таймаут wait_for не останавливает физический поток curl_cffi (Python
    не умеет прерывать потоки снаружи), поэтому "занято" должно отражать
    реальное состояние потока, а не то, ждём мы его ещё или уже нет."""
    url = f"{base_url}/r/{subs_joined}/comments.json"
    params = {"limit": fetch_limit}
    if after:
        params["after"] = after

    # curl_cffi не умеет работать с aiohttp.ClientSession, поэтому cookies
    # вытаскиваем из неё вручную и передаём явно.
    cookies = {c.key: c.value for c in session.cookie_jar}

    call = functools.partial(
        cffi_requests.get,
        url,
        # TLS/JA3- и HTTP2-отпечаток задаётся на аккаунт (accounts.yaml,
        # ключ impersonate) и остаётся неизменным для этого аккаунта на
        # всём протяжении его жизни — фиксированная пара с user_agent
        # ниже, а не случайное значение на каждый запрос (см. README).
        impersonate=impersonate,
        params=params,
        proxies={"http": proxy_url, "https": proxy_url} if proxy_url else None,
        cookies=cookies,
        # Явная пара (connect_timeout, read_timeout) вместо одного общего
        # timeout=: зависший DNS/TCP/TLS-хендшейк отваливается по
        # connect_timeout, не дожидаясь полного read_timeout — curl_cffi
        # прерывает сам себя раньше, чем это придётся делать снаружи через
        # wait_for (см. "Блокирующие вызовы" в шапке файла).
        timeout=(connect_timeout, timeout),
        allow_redirects=True,
    )

    # Запас, с которым обёрнут run_in_executor ниже: полное время на
    # connect + read, плюс HTTP_EXECUTOR_SLACK_SECONDS на случай, если
    # сами сокет-таймауты curl_cffi почему-то не сработали вовремя.
    total_wait = connect_timeout + timeout + HTTP_EXECUTOR_SLACK_SECONDS

    try:
        loop = asyncio.get_running_loop()
        # Выделенный пул (см. шапку файла) вместо дефолтного shared-executor'а
        # asyncio.to_thread: несколько гарантированных потоков про запас,
        # зависшая прокси одного аккаунта не блокирует пул целиком.
        future = loop.run_in_executor(http_executor, call)

        if executor_health is not None:
            executor_health.on_submit()
            # Срабатывает, когда поток РЕАЛЬНО закончил работу (успешно,
            # с исключением или отменой) — не когда мы перестали его
            # ждать снаружи. Именно это делает счётчик пригодным для
            # диагностики зависших потоков: даже если ниже сработает
            # asyncio.TimeoutError, эта callback НЕ вызовется, пока поток
            # действительно не освободится (или не зависнет навсегда).
            future.add_done_callback(executor_health.on_future_done)

        # wait_for — подстраховка сверх connect/read-таймаутов curl_cffi на
        # случай, если сам curl не среагировал на них вовремя (напр. DNS).
        resp = await asyncio.wait_for(future, timeout=total_wait)
        # Разбираем X-Ratelimit-* сразу, независимо от статуса ответа —
        # даже 429/5xx может нести актуальные remaining/reset, полезные
        # для адаптации интервала опроса в account_worker.
        ratelimit = parse_ratelimit_headers(resp)

        if resp.status_code == 429:
            retry_after = parse_retry_after(resp)
            return FetchResult(status=429, retry_after=retry_after, error_kind="rate_or_server",
                                ratelimit=ratelimit)
        if resp.status_code in (401, 403):
            return FetchResult(status=resp.status_code, error_kind="auth", ratelimit=ratelimit)
        if resp.status_code >= 500:
            return FetchResult(status=resp.status_code, error_kind="rate_or_server", ratelimit=ratelimit)
        if resp.status_code != 200:
            log.warning("[%s] Reddit вернул неожиданный статус %s", account_name, resp.status_code)
            return FetchResult(status=resp.status_code, error_kind="rate_or_server", ratelimit=ratelimit)

        data = resp.json()

    except asyncio.TimeoutError:
        # Поток мог не успеть за timeout + slack — цикл воркера всё равно
        # разблокируется и уходит в обычный network-backoff; сам поток
        # curl_cffi доработает и освободится в фоне самостоятельно (или
        # зависнет навсегда — именно это и покажет executor_health, см.
        # scraper/health.py: on_future_done для него просто не наступит).
        if executor_health is not None:
            executor_health.on_wait_timeout()
        log.warning(
            "[%s] Запрос к Reddit не уложился в %.0fs (connect=%.0fs+read=%.0fs+запас) — проверь mihomo/порт",
            account_name, total_wait, connect_timeout, timeout,
        )
        return FetchResult(error_kind="network")
    except cffi_requests.RequestsError as e:
        log.warning("[%s] Ошибка запроса к Reddit (проверь mihomo/порт): %s", account_name, e)
        return FetchResult(error_kind="network")
    except (json.JSONDecodeError, ValueError) as e:
        # HTTP 200, но тело — не валидный JSON: капча/интерстишл от Cloudflare,
        # HTML-страница ошибки, залипший 502 в теле формально успешного
        # ответа и т.п. Reddit-специфичной ошибки тут нет (status_code уже
        # прошёл проверку выше), поэтому это не "auth" и не "rate_or_server" —
        # трактуем как сетевую аномалию и уходим в тот же backoff-путь, что
        # и обычные network-ошибки, вместо падения воркера с необработанным
        # исключением (ValueError — родитель json.JSONDecodeError, ловим оба
        # на случай нестандартного JSON-парсера внутри curl_cffi).
        log.warning(
            "[%s] Reddit вернул 200, но тело не распарсилось как JSON "
            "(похоже на капчу/интерстишл CDN): %s", account_name, e,
        )
        return FetchResult(error_kind="network")

    listing_data = data.get("data", {})
    children = listing_data.get("children", [])
    comments = [c.get("data", {}) for c in children if c.get("kind") == "t1"]

    # "after" из тела ответа — штатный курсор пагинации Reddit. Если его
    # почему-то нет (некоторые конфигурации/старые версии эндпоинта), но
    # комментарии есть — берём fullname последнего как fallback.
    listing_after = listing_data.get("after")
    if not listing_after and comments:
        listing_after = comments[-1].get("name")

    return FetchResult(comments=comments, status=200, after=listing_after, ratelimit=ratelimit)

async def fetch_comments(
    session: aiohttp.ClientSession,
    base_url: str,
    subs_joined: str,
    fetch_limit: int,
    proxy_url: str,
    timeout: int,
    account_name: str,
    http_executor: ThreadPoolExecutor,
    max_age: float,
    max_pages: int,
    impersonate: str = "chrome",
    connect_timeout: float = DEFAULT_CONNECT_TIMEOUT_SECONDS,
    pagination_delay_min: float = 0.0,
    pagination_delay_max: float = 0.0,
    executor_health: ExecutorHealth | None = None,
) -> FetchResult:
    """Тянет одну или несколько страниц .../comments.json подряд (см.
    "Пагинация fetch-запроса" в шапке файла) и возвращает объединённый
    результат. Ошибка на любой странице обрывает пагинацию и возвращает
    именно эту ошибку — уже накопленные комментарии этого цикла отбрасываются
    (это безопасно, см. пояснение в шапке файла про SeenCache).

    `pagination_delay_min`/`pagination_delay_max` (сек) — если задан
    ненулевой диапазон, перед КАЖДОЙ страницей, следующей за первой,
    делается случайная пауза (равномерно из этого диапазона). Это не
    влияет на первую страницу цикла — она уходит сразу, как и раньше;
    пауза только между уже последовавшими друг за другом запросами
    страниц внутри одного цикла опроса, чтобы они не шли слитно, без
    задержки вообще (это само по себе предсказуемый, "ботовый" паттерн).

    `executor_health` пробрасывается в каждую отдельную страницу — см.
    _fetch_comments_page()."""
    all_comments: list[dict] = []
    after: str | None = None
    pages_fetched = 0

    while True:
        page = await _fetch_comments_page(
            session, base_url, subs_joined, fetch_limit, proxy_url, timeout,
            account_name, http_executor, after, impersonate, connect_timeout,
            executor_health,
        )

        if page.error_kind is not None:
            return page

        pages_fetched += 1
        all_comments.extend(page.comments)

        if not page.comments:
            break

        last_created = page.comments[-1].get("created_utc")
        last_age = (time.time() - last_created) if last_created is not None else None

        # Последний (самый старый) комментарий страницы уже старше лимита
        # свежести — дальше в листинге только ещё более старые, следующая
        # страница ничего полезного не даст.
        if last_age is None or last_age > max_age:
            break
        if not page.after:
            break
        if pages_fetched >= max_pages:
            log.info(
                "[%s] достигнут pagination_max_pages=%d, последний коммент страницы "
                "всё ещё свежий (age=%.1fs <= max_age=%ss) — останавливаю пагинацию цикла",
                account_name, max_pages, last_age, max_age,
            )
            break

        log.info(
            "[%s] стр.%d: последний коммент ещё свежий (age=%.1fs <= max_age=%ss) — тяну следующую страницу",
            account_name, pages_fetched, last_age, max_age,
        )

        # Случайная пауза перед следующей страницей пагинации (см. описание
        # параметров выше) — только если диапазон реально задан и не нулевой,
        # чтобы не ломать вызовы, где параметры не передали (дефолт 0.0/0.0
        # эквивалентен старому поведению без пауз).
        if pagination_delay_max > 0:
            lo = min(pagination_delay_min, pagination_delay_max)
            hi = max(pagination_delay_min, pagination_delay_max)
            delay = random.uniform(lo, hi)
            log.debug(
                "[%s] пауза %.2fs перед стр.%d пагинации",
                account_name, delay, pages_fetched + 1,
            )
            await asyncio.sleep(delay)

        after = page.after

    # ratelimit берём с последней (самой свежей) полученной страницы —
    # именно её remaining/reset актуальны на момент завершения цикла.
    return FetchResult(comments=all_comments, status=200, pages_fetched=pages_fetched,
                        ratelimit=page.ratelimit)
