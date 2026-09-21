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
    ответа (используется statичный poll_interval из config.yaml).

    Дополнительно отбраковывает явно бракованные значения (баг на
    стороне Reddit/CDN, кривой прокси, подменённый заголовок и т.п.) —
    без этой проверки один такой ответ мог бы задать account_not_before
    (см. worker.py) на часы/дни вперёд, застопорив весь аккаунт, т.к.
    account_not_before растёт монотонно и сам по себе ничем не
    ограничен. X-Ratelimit-Reset у Reddit — это окно в пределах
    нескольких минут, поэтому значение вне [0, 3600] однозначно мусор,
    а не реальная телеметрия."""
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
    """Переиспользуемый curl_cffi.requests.Session на аккаунт — TLS/TCP-
    соединение к Reddit больше не поднимается заново на каждый HTTP-запрос
    (лишний хендшейк через прокси на каждый цикл опроса, и это же более
    "ботовый" паттерн — обычный браузер держит соединение открытым).
    impersonate и proxies фиксированы на аккаунт и задаются один раз при
    создании Session, а не передаются в каждый отдельный вызов .get().

    Та же опасность, что и с http_executor (см. ExecutorHandle в
    scraper/health.py, ARCHITECTURE.md §6): asyncio.wait_for() при
    таймауте не убивает физический поток curl_cffi — если сокет/DNS
    внутри curl_cffi завис, поток может продолжать держать этот Session
    сколь угодно долго уже ПОСЛЕ того, как account_worker перестал его
    ждать снаружи. Поэтому Session нельзя ни мутировать, ни .close()
    снаружи по таймауту — зависший поток в этот момент может всё ещё
    читать/писать в тот же объект, и закрытие/переиспользование того же
    curl-хендла из другого потока — undefined behavior на стороне
    libcurl (риск порчи состояния соединения или падения потока).

    Вместо этого при таймауте вызывается swap(): .current просто
    заменяется новым Session, старый явно не закрывается и не трогается —
    он остаётся жить (вместе со своим возможно ещё занятым сокетом), пока
    не завершится (или не зависнет навсегда) тот самый поток, после чего
    будет корректно собран GC как обычный питоновский объект, на который
    больше никто не держит ссылку.

    Резолвится так же, как ExecutorHandle.current — на каждый отдельный
    HTTP-запрос в _fetch_comments_page, а не один раз при старте
    воркера, — благодаря этому даже долгоживущий воркер сразу подхватывает
    свежий Session на следующем же запросе после swap()."""

    def __init__(self, impersonate: str, proxies: dict | None):
        self._impersonate = impersonate
        self._proxies = proxies
        self.current = self._build()
        self.swaps = 0

    def _build(self) -> cffi_requests.Session:
        return cffi_requests.Session(impersonate=self._impersonate, proxies=self._proxies)

    def swap(self):
        """Заменяет .current на свежий Session. Старый НЕ закрывается
        явно (см. docstring класса) — просто перестаёт быть чьей-либо
        заботой, ровно как и зависший поток ThreadPoolExecutor в
        ExecutorHandle.swap()."""
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
    Пагинацию по нескольким страницам делает fetch_comments() ниже.

    `timeout` здесь используется как read-таймаут (время на получение
    ответа после установленного соединения), `connect_timeout` — отдельный,
    как правило меньший таймаут на TCP/TLS-хендшейк и DNS-резолвинг.
    Оба передаются в curl_cffi явно, парой, а не одним общим числом —
    см. "Блокирующие вызовы" в шапке файла.

    `http_executor` — ExecutorHandle (см. scraper/health.py), а НЕ голый
    ThreadPoolExecutor: пул может быть пересоздан "на лету" из
    health_report_loop, если старый деградировал (зависшие потоки на
    мёртвых прокси/DNS, которые Python не может прервать снаружи).
    Резолвим `http_executor.current` именно здесь, в момент запроса, а
    не принимаем сам executor заранее — иначе долгоживущий account_worker
    (который может не перезапускаться неделями) держал бы ссылку на уже
    списанный пул до своего следующего собственного рестарта.

    `curl_session` — CurlSessionHandle (см. выше), а НЕ голая
    `cffi_requests.Session`/модульная функция `cffi_requests.get`: сессия
    переиспользуется между вызовами ради keep-alive TCP/TLS-соединения к
    Reddit через прокси, но может быть пересоздана "на лету" (swap()) при
    таймауте — резолвим `curl_session.current` здесь же, на каждый
    отдельный запрос, по той же причине, что и с `http_executor.current`.

    `executor_health`, если передан, отслеживает реальную занятость
    ТЕКУЩЕГО пула (см. scraper/health.py) — счётчик инкрементируется
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

    # .current резолвится именно сейчас (не принимаем сам Session заранее
    # аргументом функции) — если curl_session.swap() успел произойти
    # между циклами этого аккаунта (например, из-за таймаута на прошлом
    # запросе), следующий же запрос уходит сразу в новый, чистый Session,
    # а не в потенциально всё ещё занятый зависшим потоком старый.
    curl = curl_session.current
    call = functools.partial(
        curl.get,
        url,
        # impersonate (TLS/JA3- и HTTP2-отпечаток) и proxies больше не
        # передаются на каждый вызов — они фиксированы на аккаунт внутри
        # самого Session при его создании в CurlSessionHandle._build().
        params=params,
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
        # .current резолвится именно сейчас (см. docstring выше) — если
        # health_report_loop успел пересоздать пул между циклами этого
        # воркера, следующий же запрос уйдёт уже в новый, чистый пул.
        future = loop.run_in_executor(http_executor.current, call)

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
        # scraper/health.py: on_future_done для него просто не наступит,
        # active будет расти, и рано или поздно health_report_loop
        # пересоздаст пул целиком).
        #
        # Дополнительно: раз поток мог зависнуть именно ВНУТРИ вызова
        # curl-сессии, дальше этот же Session небезопасно переиспользовать
        # как ни в чём не бывало — зависший поток потенциально всё ещё
        # держит его состояние (см. docstring CurlSessionHandle). Поэтому
        # свопаем сессию аккаунта целиком, а не только логируем таймаут:
        # старый Session просто перестаёт быть чьей-либо заботой, новый
        # запрос (эта же или другая группа этого аккаунта) на следующем
        # цикле уйдёт уже в свежий, точно не занятый Session.
        if executor_health is not None:
            executor_health.on_wait_timeout()
        log.warning(
            "[%s] Запрос к Reddit не уложился в %.0fs (connect=%.0fs+read=%.0fs+запас) — "
            "проверь mihomo/порт; пересоздаю curl-сессию аккаунта (старая могла зависнуть "
            "в фоновом потоке и небезопасна для повторного использования)",
            account_name, total_wait, connect_timeout, timeout,
        )
        curl_session.swap()
        return FetchResult(error_kind="network")
    except cffi_requests.RequestsError as e:
        # Поток здесь гарантированно уже завершился (с исключением), а не
        # завис — Session можно спокойно переиспользовать дальше, swap()
        # не нужен.
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
        # на случай нестандартного JSON-парсера внутри curl_cffi). Поток и
        # тут уже завершился штатно (пусть и с "плохим" ответом) — swap()
        # не требуется.
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
) -> FetchResult:
    """Тянет одну или несколько страниц .../comments.json подряд (см.
    "Пагинация fetch-запроса" в шапке файла) и возвращает объединённый
    результат. Ошибка на любой странице обрывает пагинацию и возвращает
    именно эту ошибку — уже накопленные комментарии этого цикла отбрасываются
    (это безопасно, см. пояснение в шапке файла про SeenCache).

    `http_executor` — ExecutorHandle, а не голый ThreadPoolExecutor (см.
    docstring _fetch_comments_page выше) — пробрасывается в каждую
    отдельную страницу как есть, .current резолвится уже там, на каждый
    отдельный HTTP-запрос.

    `curl_session` — CurlSessionHandle (см. выше) — так же пробрасывается
    в каждую отдельную страницу как есть; .current (и возможный swap()
    при таймауте) резолвится внутри _fetch_comments_page на каждый
    отдельный запрос, а не один раз здесь.

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
