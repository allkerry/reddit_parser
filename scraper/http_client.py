import asyncio
import functools
import json
import random
import time
from curl_cffi import requests as cffi_requests

import aiohttp

from .constants import DEFAULT_CONNECT_TIMEOUT_SECONDS, HTTP_EXECUTOR_SLACK_SECONDS, log
from .health import ExecutorHandle, ExecutorHealth
from .models import FetchResult


def parse_retry_after(resp) -> float | None:
    """Reddit может прислать Retry-After в секундах (числом)."""
    value = resp.headers.get("Retry-After")
    if not value:
        return None
    try:
        return float(value)
    except ValueError:
        return None


def parse_ratelimit_headers(resp) -> dict | None:
    """Разбирает X-Ratelimit-Remaining / Reset (и опционально Used).
    Отбраковывает аномальные значения (reset вне [0, 3600], remaining < 0)."""
    try:
        remaining = resp.headers.get("X-Ratelimit-Remaining") or resp.headers.get("x-ratelimit-remaining")
        reset = resp.headers.get("X-Ratelimit-Reset") or resp.headers.get("x-ratelimit-reset")
        used = resp.headers.get("X-Ratelimit-Used") or resp.headers.get("x-ratelimit-used")
        if remaining is None or reset is None:
            return None
        remaining_f = float(remaining)
        reset_f = float(reset)
        if remaining_f < 0 or not (0 <= reset_f <= 3600):
            log.warning(
                "Reddit прислал аномальные X-Ratelimit-заголовки "
                "(remaining=%s reset=%s) — игнорирую, как будто их не было",
                remaining, reset,
            )
            return None
        return {
            "remaining": remaining_f,
            "reset": reset_f,
            "used": float(used) if used is not None else None,
        }
    except (TypeError, ValueError):
        return None


# ---------------------------------------------------------------- #
#  Переиспользуемая curl_cffi.Session на аккаунт (keep-alive)
# ---------------------------------------------------------------- #

class CurlSessionHandle:
    """Session на аккаунт. При таймауте не закрывается (зависший поток
    может ещё держать её), а заменяется новой через swap()."""

    def __init__(self, impersonate: str, proxies: dict | None):
        self._impersonate = impersonate
        self._proxies = proxies
        self.current = self._build()
        self.swaps = 0

    def _build(self) -> cffi_requests.Session:
        return cffi_requests.Session(impersonate=self._impersonate, proxies=self._proxies)

    def swap(self):
        self.current = self._build()
        self.swaps += 1


async def _fetch_comments_page(
    session: aiohttp.ClientSession,
    base_url: str,
    subs_joined: str,
    fetch_limit: int,
    curl_session: CurlSessionHandle,
    timeout: int,
    account_name: str,
    http_executor: ExecutorHandle,
    after: str | None = None,
    connect_timeout: float = DEFAULT_CONNECT_TIMEOUT_SECONDS,
    executor_health: ExecutorHealth | None = None,
) -> FetchResult:
    """Один HTTP-запрос к .../comments.json (одна страница листинга).
    http_executor.current и curl_session.current резолвятся здесь, на
    каждый запрос (см. health.py / CurlSessionHandle)."""
    url = f"{base_url}/r/{subs_joined}/comments.json"
    params = {"limit": fetch_limit}
    if after:
        params["after"] = after

    cookies = {c.key: c.value for c in session.cookie_jar}

    curl = curl_session.current
    call = functools.partial(
        curl.get,
        url,
        params=params,
        cookies=cookies,
        timeout=(connect_timeout, timeout),
        allow_redirects=True,
    )

    total_wait = connect_timeout + timeout + HTTP_EXECUTOR_SLACK_SECONDS

    try:
        loop = asyncio.get_running_loop()
        future = loop.run_in_executor(http_executor.current, call)

        if executor_health is not None:
            executor_health.on_submit()
            future.add_done_callback(executor_health.on_future_done)

        resp = await asyncio.wait_for(future, timeout=total_wait)
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
        if executor_health is not None:
            executor_health.on_wait_timeout()
        log.warning(
            "[%s] Запрос к Reddit не уложился в %.0fs (connect=%.0fs+read=%.0fs+запас) — "
            "проверь mihomo/порт; пересоздаю curl-сессию аккаунта",
            account_name, total_wait, connect_timeout, timeout,
        )
        curl_session.swap()
        return FetchResult(error_kind="network")
    except cffi_requests.RequestsError as e:
        log.warning("[%s] Ошибка запроса к Reddit (проверь mihomo/порт): %s", account_name, e)
        return FetchResult(error_kind="network")
    except (json.JSONDecodeError, ValueError) as e:
        log.warning(
            "[%s] Reddit вернул 200, но тело не распарсилось как JSON "
            "(похоже на капчу/интерстишл CDN): %s", account_name, e,
        )
        return FetchResult(error_kind="network")

    listing_data = data.get("data", {})
    children = listing_data.get("children", [])
    comments = [c.get("data", {}) for c in children if c.get("kind") == "t1"]

    listing_after = listing_data.get("after")
    if not listing_after and comments:
        listing_after = comments[-1].get("name")

    return FetchResult(comments=comments, status=200, after=listing_after, ratelimit=ratelimit)


async def fetch_comments(
    session: aiohttp.ClientSession,
    base_url: str,
    subs_joined: str,
    fetch_limit: int,
    curl_session: CurlSessionHandle,
    timeout: int,
    account_name: str,
    http_executor: ExecutorHandle,
    max_age: float,
    max_pages: int,
    connect_timeout: float = DEFAULT_CONNECT_TIMEOUT_SECONDS,
    pagination_delay_min: float = 0.0,
    pagination_delay_max: float = 0.0,
    executor_health: ExecutorHealth | None = None,
    stop_when=None,
) -> FetchResult:
    """Тянет одну или несколько страниц .../comments.json подряд.

    `stop_when(page_comments) -> bool` (опционально) вызывается после
    каждой успешной непустой страницы; если вернул True — пагинация
    останавливается (используется воркером: "дошли до уже виденных
    комментариев, дальше листать не нужно").

    Ошибка на любой странице обрывает пагинацию и возвращается целиком —
    накопленные комментарии отбрасываются (безопасно: они ещё не попали
    в SeenCache и будут подхвачены на следующем опросе)."""
    all_comments: list[dict] = []
    after: str | None = None
    pages_fetched = 0

    while True:
        page = await _fetch_comments_page(
            session, base_url, subs_joined, fetch_limit, curl_session, timeout,
            account_name, http_executor, after, connect_timeout,
            executor_health,
        )

        if page.error_kind is not None:
            return page

        pages_fetched += 1
        all_comments.extend(page.comments)

        if not page.comments:
            break

        if stop_when is not None and stop_when(page.comments):
            break

        last_created = page.comments[-1].get("created_utc")
        last_age = (time.time() - last_created) if last_created is not None else None

        if last_age is None or last_age > max_age:
            break
        if not page.after:
            break
        if pages_fetched >= max_pages:
            log.info(
                "[%s] достигнут pagination_max_pages=%d, пересечения с виденными так и нет "
                "(age последнего=%.1fs <= max_age=%ss) — вероятно, пропуск комментариев",
                account_name, max_pages, last_age, max_age,
            )
            break

        if pagination_delay_max > 0:
            lo = min(pagination_delay_min, pagination_delay_max)
            hi = max(pagination_delay_min, pagination_delay_max)
            await asyncio.sleep(random.uniform(lo, hi))

        after = page.after

    return FetchResult(comments=all_comments, status=200, pages_fetched=pages_fetched,
                        ratelimit=page.ratelimit)
