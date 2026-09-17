#!/usr/bin/env python3
"""
Reddit fresh-comments scraper -> POST /store_items (batch)

См. README.md за полным описанием архитектуры. Ключевое в этой версии
(v3) — минимизация риска рейт-лимита/бана:

- Экспоненциальный backoff при 429 и 5xx, с учётом заголовка Retry-After,
  если Reddit его присылает. Пока идёт backoff — воркер аккаунта просто
  спит и ничего не запрашивает (не "долбит" повторно, это только продлевает
  блокировку).
- Джиттер (+-poll_jitter_ratio) на обычный интервал опроса — паттерн
  запросов не идеально ровный, как у типичного бота.
- Реалистичные User-Agent на каждый аккаунт (задаётся в accounts.yaml),
  вместо самопального "python:...", который прямо выдаёт скрипт.
- При повторяющихся 401/403 (протухшие/забаненные cookies) — воркер
  аккаунта останавливается после max_consecutive_auth_errors подряд
  неудач (с бэкоффом между попытками), а не долбит забаненный аккаунт
  бесконечно каждые несколько секунд.
- Общий (на все аккаунты) rate limiter для отправки в store_items и общий
  in-memory дедуп — как и раньше.
- Отправка в пайплайн — БАТЧАМИ: один POST /store_items с JSON-массивом
  вместо N отдельных POST /store_item. Токены из общего TokenBucket
  по-прежнему берутся по одному на айтем (это и есть общий rate-limit),
  но фактическая отправка по сети идёт одним запросом на цикл на аккаунт.
- config.yaml перечитывается на лету каждые CONFIG_RELOAD_SECONDS секунд.
- curl_cffi (синхронный, блокирующий) выполняется в ВЫДЕЛЕННОМ
  ThreadPoolExecutor размером ровно под число аккаунтов (см. "Блокирующие
  вызовы" ниже), а не в дефолтном shared-пуле asyncio.to_thread.

Пагинация fetch-запроса
------------------------
Один запрос к .../comments.json отдаёт максимум `fetch_limit` комментариев,
отсортированных от новых к старым. Если ПОСЛЕДНИЙ (самый старый) комментарий
в полученной странице всё ещё не старше `max_age_seconds` — значит за это
окно свежих комментариев могло быть больше, чем `fetch_limit`, и часть
осталась не забрана. В этом случае `fetch_comments()` сама тянет следующую
страницу через параметр `after` (значение поля "after" из ответа Reddit,
либо fullname последнего элемента страницы как fallback) и объединяет
результаты. Пагинация одного цикла опроса аккаунта останавливается, как
только:
  - последний комментарий очередной страницы старше max_age_seconds
    (дальше в листинге только ещё более старые — они и так не пройдут
    фильтр по возрасту ниже, в account_worker), либо
  - страница пустая или Reddit не вернул `after` (дальше данных нет), либо
  - достигнут потолок `pagination_max_pages` из config.yaml — защита от
    неограниченного числа запросов за один цикл опроса при аномально
    высоком трафике саба.
Ошибка (429/5xx/401/403/сеть) на любой странице прерывает пагинацию и
уходит по тому же backoff-пути, что и раньше; уже накопленные в рамках
этого цикла страницы отбрасываются — потери нет, они не были подтверждены
в SeenCache и будут заново подхвачены в следующем цикле опроса.

Дедуп (SeenCache) — двухфазный (claim/confirm/release), НЕ атомарный
seen_or_mark: id помечается окончательно "виденным" только после
подтверждённой успешной отправки в store_items. Если батч целиком
упал (5xx/413/timeout/ClientError), claim снимается через release() —
элемент снова видим и может уйти в следующем цикле опроса, вместо
того чтобы быть молча и навсегда потерянным.

Блокирующие вызовы (curl_cffi) и пул потоков
---------------------------------------------
curl_cffi — синхронная библиотека, поэтому её вызов из asyncio-кода
обязан уходить в отдельный поток. По умолчанию `asyncio.to_thread()`
берёт поток из процесс-wide дефолтного executor'а (`min(32, cpu_count+4)`
потоков), который никак не связан с числом наших аккаунтов и может
шариться с чем угодно ещё в процессе.

Проблема: если прокси-нода подвисает (или зависает DNS-резолвинг —
таймаут `timeout=` у curl не всегда успевает его перехватить), поток
блокируется дольше ожидаемого. При достаточном числе таких зависаний
дефолтный пул может исчерпаться, и новые запросы (в т.ч. от аккаунтов,
у которых прокси в порядке) начнут молча ждать в очереди executor'а —
без единой ошибки в логах, просто зависшие циклы.

Проблема (уточнение): `asyncio.wait_for(...)` при срабатывании таймаута
отменяет только asyncio-задачу, обёрнутую вокруг `run_in_executor` —
сам поток в `ThreadPoolExecutor` при этом НЕ прерывается принудительно
(Python не умеет убивать потоки снаружи). Если сокет или DNS-резолвинг
внутри `curl_cffi` завис, поток остаётся заблокированным на неопределённое
время. При `max_workers = len(accounts)` это означает, что зависшая
прокси одного аккаунта перманентно отнимает у пула ровно один поток —
свободных потоков в запасе нет. На следующем цикле опроса ЭТОГО ЖЕ
воркера (или в худшем случае — при накоплении нескольких таких
зависаний от разных аккаунтов) новый `run_in_executor` встаёт в очередь
executor'а и ждёт освобождения потока бесконечно, т.к. освободиться
ему неоткуда.

Решение здесь:
1. Пул потоков создаётся с запасом, а не впритык по числу аккаунтов:
   `max_workers = max(32, len(accounts) * 2)`. Даже если у части
   аккаунтов потоки зависли навсегда, у остальных остаются свободные
   потоки про запас, и пул не вырождается в блокировку по цепочке.
   Это не устраняет утечку потока при зависании (см. выше — снаружи
   поток всё равно не убить), а даёт пулу достаточный запас, чтобы
   утечка отдельных потоков не останавливала весь сервис.
2. `curl_cffi` вызывается с явными сокет-таймаутами, а не только с
   общим `timeout=`: `timeout=(connect_timeout, read_timeout)` —
   отдельно `connect_timeout_seconds` (обычно должен успевать
   перехватить зависший DNS/handshake) и `read_timeout_seconds` (общий
   `request_timeout_seconds` из config.yaml). Это снижает вероятность
   самого зависания на уровне curl, а не только компенсирует её постфактум.
3. Вызов дополнительно обёрнут в `asyncio.wait_for(..., connect+read+запас)`
   — подстраховка на случай, если сокет-таймауты curl_cffi всё равно не
   сработают как ожидается: цикл воркера в любом случае разблокируется и
   уйдёт в обычный backoff, вместо того чтобы виснуть бесконечно. Сам
   поток при этом может доработать в фоне и корректно освободиться сам —
   это не "убивает" запрос, а лишь не даёт ему держать asyncio-цикл воркера.
"""

import asyncio
import functools
import json
import logging
import os
import random
import time
from collections import deque
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone
from pathlib import Path
from curl_cffi import requests as cffi_requests

import aiohttp
import yaml

BASE_DIR = Path(__file__).parent
CONFIG_PATH = Path(os.environ.get("CONFIG_PATH", BASE_DIR / "config.yaml"))
ACCOUNTS_PATH = Path(os.environ.get("ACCOUNTS_PATH", BASE_DIR / "accounts.yaml"))

PROXY_HOST = os.environ.get("PROXY_HOST", "127.0.0.1")
STORE_ENDPOINT_OVERRIDE = os.environ.get("STORE_ENDPOINT")
CONFIG_RELOAD_SECONDS = float(os.environ.get("CONFIG_RELOAD_SECONDS", "5"))

# Жёсткий потолок сервера (см. документацию коллектора: /store_items
# отклоняет пачки больше BATCH_MAX_ITEMS с 413). Наш batch_max_items из
# config.yaml обрезается этим значением на всякий случай.
SERVER_HARD_BATCH_LIMIT = 1000

# Сколько секунд сверх HTTP-таймаута (connect + read) ждать поток
# curl_cffi, прежде чем считать вызов зависшим и разблокировать цикл
# воркера принудительно (см. "Блокирующие вызовы" в шапке файла).
HTTP_EXECUTOR_SLACK_SECONDS = 5

# Дефолтный connect-таймаут curl_cffi, если connect_timeout_seconds не
# задан в config.yaml. Держим его меньше read-таймаута: зависший
# DNS/TCP/TLS-хендшейк обычно должен отваливаться быстрее, чем ожидание
# тела ответа на уже установленном соединении.
DEFAULT_CONNECT_TIMEOUT_SECONDS = 5

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
                        "config.yaml обновлён: target_rate=%s max_age=%ss subs=%d batch_max_items=%s",
                        self.get("target_rate_per_second"),
                        self.get("max_age_seconds"),
                        len(self.get("subreddits", [])),
                        self.get("batch_max_items"),
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
#  Общий rate limiter (token bucket) на отправку в store_items
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
#  Общий дедуп-кэш (двухфазный: claim -> confirm/release)
# ---------------------------------------------------------------- #

class SeenCache:
    """id считается окончательно "виденным" (и попадает в LRU-множество
    `_set`, которое реально защищает от повторной отправки в будущих
    циклах) только после confirm(). До этого он лежит в `_pending` —
    это чисто внутрипроцессная защита от одновременной попытки отправить
    один и тот же id дважды параллельно, а не постоянный дедуп-статус.

    Если отправка не удалась (или не дали токен) — вызывается release(),
    id снимается из `_pending` и становится снова "невиденным": в
    следующем цикле опроса (пока комментарий не вышел за max_age_seconds)
    он будет заново подхвачен и отправлен."""

    def __init__(self, max_size: int):
        self._deque = deque(maxlen=max_size)
        self._set = set()
        self._pending = set()
        self._lock = asyncio.Lock()

    async def try_claim(self, item_id: str) -> bool:
        """True — элемент свободен, можно готовить к отправке (заявлен).
        False — уже подтверждённый дубль или прямо сейчас отправляется
        другим воркером — пропускаем."""
        async with self._lock:
            if item_id in self._set or item_id in self._pending:
                return False
            self._pending.add(item_id)
            return True

    async def confirm(self, item_id: str):
        """Отправка подтверждённо успешна: pending -> постоянно seen."""
        async with self._lock:
            self._pending.discard(item_id)
            if item_id not in self._set:
                if len(self._deque) == self._deque.maxlen:
                    old = self._deque.popleft()
                    self._set.discard(old)
                self._deque.append(item_id)
                self._set.add(item_id)

    async def release(self, item_id: str):
        """Отправка не удалась (или токен не выдан вовремя): снимаем
        claim, элемент снова доступен для следующей попытки."""
        async with self._lock:
            self._pending.discard(item_id)


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


# ---------------------------------------------------------------- #
#  Один аккаунт = один воркер
# ---------------------------------------------------------------- #

class FetchResult:
    __slots__ = ("comments", "status", "retry_after", "error_kind", "after",
                 "pages_fetched", "ratelimit")

    def __init__(self, comments=None, status=None, retry_after=None, error_kind=None,
                 after=None, pages_fetched=1, ratelimit=None):
        self.comments = comments or []
        self.status = status
        self.retry_after = retry_after
        # error_kind: None | "rate_or_server" | "auth" | "network"
        self.error_kind = error_kind
        # Курсор пагинации Reddit ("after" из ответа листинга, либо
        # fullname последнего элемента как fallback) — None, если больше
        # страниц нет / страница пустая.
        self.after = after
        # Сколько страниц реально было выкачано за этот fetch_comments()
        # (используется в account_worker для adjusted_interval). По
        # умолчанию 1 — для одиночного вызова _fetch_comments_page().
        self.pages_fetched = pages_fetched
        # dict {"remaining": float, "reset": float, "used": float|None} —
        # разобранные X-Ratelimit-* заголовки последнего запроса, либо
        # None, если сервер их не прислал. См. parse_ratelimit_headers().
        self.ratelimit = ratelimit


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
) -> FetchResult:
    """Один HTTP-запрос к .../comments.json (одна страница листинга).
    Пагинацию по нескольким страницам делает fetch_comments() ниже.

    `timeout` здесь используется как read-таймаут (время на получение
    ответа после установленного соединения), `connect_timeout` — отдельный,
    как правило меньший таймаут на TCP/TLS-хендшейк и DNS-резолвинг.
    Оба передаются в curl_cffi явно, парой, а не одним общим числом —
    см. "Блокирующие вызовы" в шапке файла."""
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
        # wait_for — подстраховка сверх connect/read-таймаутов curl_cffi на
        # случай, если сам curl не среагировал на них вовремя (напр. DNS).
        resp = await asyncio.wait_for(
            loop.run_in_executor(http_executor, call),
            timeout=total_wait,
        )
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
        # curl_cffi доработает и освободится в фоне самостоятельно.
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
) -> FetchResult:
    """Тянет одну или несколько страниц .../comments.json подряд (см.
    "Пагинация fetch-запроса" в шапке файла) и возвращает объединённый
    результат. Ошибка на любой странице обрывает пагинацию и возвращает
    именно эту ошибку — уже накопленные комментарии этого цикла отбрасываются
    (это безопасно, см. пояснение в шапке файла про SeenCache)."""
    all_comments: list[dict] = []
    after: str | None = None
    pages_fetched = 0

    while True:
        page = await _fetch_comments_page(
            session, base_url, subs_joined, fetch_limit, proxy_url, timeout,
            account_name, http_executor, after, impersonate, connect_timeout,
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
        after = page.after

    # ratelimit берём с последней (самой свежей) полученной страницы —
    # именно её remaining/reset актуальны на момент завершения цикла.
    return FetchResult(comments=all_comments, status=200, pages_fetched=pages_fetched,
                        ratelimit=page.ratelimit)

def _item_ok(entry) -> bool:
    """Разбирает один элемент results[] из ответа /store_items.
    Формат отдельного результата коллектором явно не специфицирован,
    поэтому распознаём несколько разумных вариантов и по умолчанию
    считаем успехом, если явного признака ошибки нет."""
    if isinstance(entry, bool):
        return entry
    if isinstance(entry, dict):
        for key in ("ok", "success", "stored", "saved"):
            if key in entry:
                return bool(entry[key])
        if "error" in entry and entry["error"]:
            return False
        return True
    # неизвестный тип — не считаем это ошибкой, чтобы не заспамить логи
    return True


async def send_batch_to_store(
    session: aiohttp.ClientSession,
    endpoint: str,
    items: list[dict],
    account_name: str,
    batch_max_items: int,
) -> list[bool]:
    """Отправляет items одним (или несколькими, если батч больше лимита)
    POST-запросом(и) на /store_items. Возвращает список bool — по одному
    на исходный items[i], в том же порядке. False для элемента означает
    "не подтверждён как сохранённый" — вызывающий код обязан вызвать
    seen.release() для таких элементов, а не считать их отправленными."""
    if not items:
        return []

    chunk_size = max(1, min(batch_max_items, SERVER_HARD_BATCH_LIMIT))
    results: list[bool] = []

    for start in range(0, len(items), chunk_size):
        chunk = items[start:start + chunk_size]
        body = [{k: v for k, v in p.items() if not k.startswith("_")} for p in chunk]

        try:
            async with session.post(
                endpoint, json=body, timeout=aiohttp.ClientTimeout(total=15)
            ) as resp:
                if resp.status == 413:
                    text = await resp.text()
                    log.error(
                        "[%s] store_items ответил 413 (батч %d элементов слишком большой) — "
                        "уменьши batch_max_items в config.yaml: %s",
                        account_name, len(chunk), text[:200],
                    )
                    results.extend([False] * len(chunk))
                    continue

                if resp.status >= 300:
                    text = await resp.text()
                    log.warning(
                        "[%s] store_items ответил %s для батча из %d: %s",
                        account_name, resp.status, len(chunk), text[:200],
                    )
                    results.extend([False] * len(chunk))
                    continue

                try:
                    data = await resp.json(content_type=None)
                except (json.JSONDecodeError, ValueError) as e:
                    # 2xx, но тело не распарсилось как JSON (например,
                    # прокси/балансировщик перед store_endpoint подменил
                    # тело ответа). Раз сервер не подтвердил сохранение
                    # поэлементно — считаем весь чанк неподтверждённым,
                    # а не роняем воркер необработанным исключением: он
                    # уйдёт по обычному пути seen.release() и повторной
                    # отправки в следующем цикле опроса.
                    log.warning(
                        "[%s] store_items вернул %s, но тело не распарсилось как JSON: %s",
                        account_name, resp.status, e,
                    )
                    results.extend([False] * len(chunk))
                    continue
                item_results = data.get("results") if isinstance(data, dict) else None

                if isinstance(item_results, list) and len(item_results) == len(chunk):
                    results.extend(_item_ok(r) for r in item_results)
                else:
                    # Сервер принял запрос, но не вернул results поэлементно —
                    # считаем весь чанк успешным по факту получения 2xx.
                    received = data.get("received") if isinstance(data, dict) else None
                    if received is not None and received != len(chunk):
                        log.warning(
                            "[%s] store_items: received=%s, но отправлено %d — часть могла не сохраниться",
                            account_name, received, len(chunk),
                        )
                    results.extend([True] * len(chunk))

        except aiohttp.ClientError as e:
            log.warning("[%s] Ошибка отправки батча в store_items (%s): %s", account_name, endpoint, e)
            results.extend([False] * len(chunk))
        except asyncio.TimeoutError:
            log.warning("[%s] Таймаут отправки батча в store_items (%s)", account_name, endpoint)
            results.extend([False] * len(chunk))

    return results


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
            poll_interval = config.get("poll_interval_seconds", 3)
            jitter_ratio = config.get("poll_jitter_ratio", 0.25)
            respect_ratelimit_headers = config.get("respect_ratelimit_headers", True)
            # Запас: используем не 100% оставшегося окна, а safety_margin
            # от него — иначе счёт "впритык" ломается от первой же
            # рассинхронизации часов/сети и мы всё равно ловим 429.
            ratelimit_safety_margin = config.get("ratelimit_safety_margin", 0.85)
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
        "Запуск: %d аккаунт(ов), %d сабреддитов, target_rate=%s/сек, max_age=%ss, "
        "batch_max_items=%s, proxy_host=%s",
        len(accounts), len(config.get("subreddits", [])),
        config.get("target_rate_per_second"), config.get("max_age_seconds"),
        config.get("batch_max_items", 500), PROXY_HOST,
    )

    bucket = TokenBucket(config.get("target_rate_per_second", 25))
    seen = SeenCache(config.get("seen_cache_size", 1000))

    # Выделенный пул потоков под блокирующие curl_cffi-вызовы, отдельно
    # от дефолтного shared-executor'а asyncio.to_thread (см. шапку файла).
    # Важно: НЕ ровно len(accounts) — asyncio.wait_for не может убить
    # реальный поток при таймауте, только отменить asyncio-обёртку вокруг
    # него; если у аккаунта завис сокет/DNS, его поток в executor'е может
    # остаться занятым навсегда. При max_workers == len(accounts) это
    # значит, что зависший поток одного аккаунта перманентно отнимает
    # единственный запасной слот, и следующий run_in_executor того же (или
    # любого другого) воркера встаёт в очередь без шанса на освобождение.
    # max(32, len(accounts) * 2) даёт запас потоков, которого хватает
    # пережить несколько таких зависаний одновременно, не останавливая
    # остальных воркеров.
    http_executor = ThreadPoolExecutor(
        max_workers=max(32, len(accounts) * 2),
        thread_name_prefix="reddit-fetch",
    )

    try:
        # Явный коннектор вместо дефолтного: store_session живёт весь аптайм
        # процесса (main() не пересоздаёт её), поэтому важно не полагаться
        # молча на дефолтный ttl_dns_cache aiohttp, а зафиксировать TTL явно
        # прямо в коде — если у store_endpoint сменится IP (например,
        # при переподъёме принимающего сервера), запись обновится не позже
        # чем через ttl_dns_cache секунд, а не будет висеть в кэше
        # неограниченно долго. keepalive_timeout ограничивает время жизни
        # уже установленных TCP-соединений к старому IP — иначе даже после
        # обновления DNS-кэша, активное keep-alive-соединение к устаревшему
        # адресу могло бы продолжать использоваться сколь угодно долго.
        store_connector = aiohttp.TCPConnector(ttl_dns_cache=300, keepalive_timeout=60)
        async with aiohttp.ClientSession(connector=store_connector) as store_session:
            tasks = [asyncio.create_task(config.reload_loop())]
            for i, account in enumerate(accounts):
                phase_offset = i * (config.get("poll_interval_seconds", 3) / len(accounts))
                tasks.append(
                    asyncio.create_task(
                        supervised(
                            account_worker,
                            account, config, bucket, seen, phase_offset, store_session, http_executor,
                            name=account["name"]
                        )
                    )
                )
            await asyncio.gather(*tasks)
    finally:
        # wait=False: не блокируем shutdown процесса зависшими потоками —
        # они либо доработают в фоне интерпретатора, либо умрут вместе с
        # процессом при завершении.
        http_executor.shutdown(wait=False, cancel_futures=True)


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        log.info("Остановлено пользователем")
