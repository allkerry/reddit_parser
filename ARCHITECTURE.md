# Архитектура

Этот документ описывает, как устроен скрапер **по коду, который сейчас в
репозитории** (`main.py` + пакет `scraper/`). Если раньше в README
упоминалась схема с отдельной общей очередью-приоритетом (`PriorityStore`)
и выделенной корутиной-отправителем (`Sender`) — в текущей версии кода
этого нет: она заменена на более простую схему, где каждый аккаунт-воркер
сам доходит от фетча до отправки, деля с остальными только дедуп и
рейт-лимит. Ниже — как это устроено на самом деле.

---

## 1. Общая картина

```
                         ┌───────────────────────────┐
                         │          main.py           │
                         │  ConfigStore (hot-reload)  │
                         │  TokenBucket (общий)       │
                         │  SeenCache   (общий)       │
                         │  ThreadPoolExecutor (http) │
                         │  aiohttp.ClientSession     │
                         │      (для store_items)     │
                         └─────────────┬───────────────┘
                                       │ запускает N задач (supervised)
              ┌────────────────────────┼────────────────────────┐
              ▼                        ▼                        ▼
     account_worker(acc_1)    account_worker(acc_2)     account_worker(acc_N)
     свой proxy_port,          свой proxy_port,          свой proxy_port,
     свои cookies, UA,         свои cookies, UA,         свои cookies, UA,
     свой BackoffState         свой BackoffState         свой BackoffState
              │                        │                        │
              └──────── общий TokenBucket (лимит POST/сек) ──────┘
              └──────── общий SeenCache (дедуп по всем акк-ам) ──┘
              │                        │                        │
              ▼                        ▼                        ▼
        POST /store_items       POST /store_items         POST /store_items
        (батч, через store_session, общий на процесс)
```

Каждый `account_worker` — независимый бесконечный цикл: своя VPN-нода
(`proxy_port` из `accounts.yaml`), свои cookies, свой TLS/UA-отпечаток,
свой backoff-счётчик ошибок. Между воркерами общие только два объекта,
переданные им в `main.py`:

- `TokenBucket` — единый на весь процесс лимитер скорости отправки в
  пайплайн (`target_rate_per_second` — это ручка на **сумму** всех
  аккаунтов, а не на каждый в отдельности);
- `SeenCache` — единый на весь процесс дедуп-кэш, чтобы один и тот же
  комментарий, случайно попавшийся двум аккаунтам одновременно (сабы могут
  пересекаться по спискам), не улетел в пайплайн дважды.

Никакой централизованной очереди между фетчем и отправкой нет: то, что
`account_worker` вытащил и профильтровал за один цикл опроса, он сам же
и отправляет в конце того же цикла.

---

## 2. Модули

### `main.py`
Точка входа и композиция зависимостей:
- поднимает `ConfigStore` и делает первый синхронный `load_once()`;
- читает `accounts.yaml`, оставляет только `enabled: true`;
- создаёт общий `TokenBucket(target_rate_per_second)` и
  `SeenCache(seen_cache_size)`;
- создаёт `ThreadPoolExecutor` под блокирующие вызовы `curl_cffi` —
  специально **не** ровно `len(accounts)`, а `max(32, len(accounts) * 2)`
  (см. раздел 6 — зависший поток одного аккаунта не должен съедать
  единственный запасной слот у остальных);
- открывает один `aiohttp.ClientSession` на весь процесс для отправки в
  `store_items` — с явным `TCPConnector(ttl_dns_cache=300,
  keepalive_timeout=60)`, чтобы смена IP у `store_endpoint` подхватывалась
  не позже чем через 5 минут, а не висела в DNS-кэше неограниченно;
- запускает `config.reload_loop()` и по одной задаче `account_worker` на
  каждый включённый аккаунт — каждую через `supervised()` (см. ниже) и со
  сдвигом фазы `phase_offset`, чтобы аккаунты не били Reddit синхронно;
- по завершении гасит executor через `shutdown(wait=False,
  cancel_futures=True)` — не блокирует остановку процесса зависшими
  потоками curl_cffi.

### `scraper/config.py`
- `ConfigStore` — держит распарсенный `config.yaml` в памяти,
  `load_once()` читает файл синхронно, `reload_loop()` раз в
  `CONFIG_RELOAD_SECONDS` (дефолт 5с) перечитывает и логирует факт
  изменения. `get(key, default)` — единая точка доступа; если файл
  временно не читается (например, во время правки) — старые значения
  остаются в силе, ошибка только логируется.
- `load_accounts()` — читает `accounts.yaml` один раз при старте
  (`main.py` вызывает это до цикла) — новые/изменённые аккаунты требуют
  рестарта процесса.
- `load_cookies(cookie_file)` — превращает экспорт Cookie Editor
  (список объектов с `name`/`value`) в простой `dict`.

### `scraper/constants.py`
Пути (`CONFIG_PATH`, `ACCOUNTS_PATH`, с возможностью переопределить через
переменные окружения `CONFIG_PATH`/`ACCOUNTS_PATH` — этим пользуется
Docker-compose), `PROXY_HOST`, `STORE_ENDPOINT_OVERRIDE` (env
`STORE_ENDPOINT`, приоритетнее значения из `config.yaml`),
`SERVER_HARD_BATCH_LIMIT = 1000` (жёсткий потолок сервера для батча),
таймауты по умолчанию и общий логгер `log`.

### `scraper/models.py`
`FetchResult` — единственная модель данных, результат одного или
нескольких фетчей у Reddit:
- `comments: list[dict]` — сырые данные комментариев (`data` из `t1`-детей);
- `status`, `retry_after`, `error_kind` (`None | "rate_or_server" | "auth" |
  "network"`);
- `after` — курсор пагинации Reddit;
- `pages_fetched` — сколько страниц реально забрано за вызов (используется
  для расчёта эффективного интервала опроса);
- `ratelimit` — разобранные `X-Ratelimit-*` заголовки (или `None`, если
  сервер их не прислал).

### `scraper/http_client.py`
Всё общение с Reddit.

- `_fetch_comments_page(...)` — один HTTP-запрос к
  `.../r/{subs}/comments.json`. Так как `curl_cffi` синхронный, вызов
  уходит в `http_executor` через `loop.run_in_executor`, обёрнутый в
  `asyncio.wait_for(..., connect_timeout + read_timeout +
  HTTP_EXECUTOR_SLACK_SECONDS)`. Cookies берутся напрямую из
  `aiohttp.ClientSession.cookie_jar` (curl_cffi не умеет работать с
  `aiohttp.ClientSession` сам). TLS/JA3-отпечаток (`impersonate`)
  фиксирован на аккаунт. Разбирает статусы: `429` → `error_kind =
  "rate_or_server"` (+ `Retry-After`, если есть), `401/403` →
  `error_kind = "auth"`, `5xx`/неожиданный статус → `"rate_or_server"`,
  таймаут/сетевая ошибка/невалидный JSON в теле 200 (капча/интерстишл) →
  `"network"`. Курсор пагинации — `after` из тела ответа, либо fullname
  последнего комментария страницы как fallback.
- `fetch_comments(...)` — обёртка с пагинацией: тянет страницы подряд,
  пока последний (самый старый) комментарий очередной страницы всё ещё не
  старше `max_age`, есть курсор `after` и не превышен `pagination_max_pages`.
  Ошибка на любой странице обрывает всю пагинацию цикла и возвращает эту
  ошибку целиком — уже накопленные страницы этого цикла отбрасываются
  (безопасно: они ещё не попали ни в `SeenCache`, ни в отправку, будут
  заново подхвачены на следующем цикле опроса).
- `parse_retry_after`, `parse_ratelimit_headers` — разбор заголовков
  Reddit (`Retry-After`, `X-Ratelimit-Remaining/Reset/Used`).

### `scraper/pipeline.py`
Превращение сырых данных Reddit в payload пайплайна и отправка.

- `build_payload(comment_data)` — маппинг полей Reddit → схема пайплайна
  (`content`, `external_id` = fullname `t1_...`, `created_at` (ISO UTC),
  `domain`, `url`, `author`/`username`, `external_parent_id` — только если
  родитель тоже комментарий (`t1_`), плюс служебное `_age_seconds`,
  которое в тело запроса не уходит). Возвращает `None`, если нет `id` или
  `created_utc` — такие комментарии молча пропускаются выше по стеку.
- `send_batch_to_store(...)` — шлёт список payload'ов одним (или
  несколькими, если `len(items) > chunk_size`) POST на `/store_items`.
  `chunk_size = min(batch_max_items, SERVER_HARD_BATCH_LIMIT)`. Для
  каждого чанка:
  - `413` → весь чанк помечается `False`, в лог — совет уменьшить
    `batch_max_items`;
  - `>= 300` (кроме 413) → весь чанк `False`, warning с телом ответа;
  - `2xx`, но тело не парсится как JSON → весь чанк `False` (не роняем
    воркер необработанным исключением);
  - `2xx` с `results: [...]` длины, совпадающей с чанком → построчный
    разбор через `_item_ok` (понимает несколько разумных форматов
    результата — `bool`, `{"ok"/"success"/"stored"/"saved": ...}`,
    `{"error": ...}`, иначе по умолчанию считает успехом);
  - `2xx` без поэлементных `results` → весь чанк считается успешным по
    факту 2xx (с warning, если `received` в ответе не совпадает с
    размером чанка);
  - `ClientError`/`TimeoutError` → весь чанк `False`.
  Возвращает список `bool` в том же порядке, что и входные `items` —
  вызывающий код (`worker.py`) обязан для каждого `False` вызвать
  `seen.release()`, а не считать элемент отправленным.

### `scraper/state.py`
Общие для процесса примитивы и backoff-состояние на аккаунт.

- **`TokenBucket`** — простой rate limiter (пополнение токенов
  пропорционально прошедшему времени, `capacity = max(rate, 1.0)`).
  `update_rate()` вызывается каждый цикл `account_worker`, чтобы
  изменения `target_rate_per_second` в `config.yaml` подхватывались без
  рестарта. Общий на все аккаунты — то есть это лимит на **суммарную**
  скорость отправки в пайплайн, а не на каждый аккаунт по отдельности.
- **`SeenCache`** — дедуп с двухфазным протоколом `claim → confirm/release`
  (не атомарный `seen_or_mark`):
  - `try_claim(id)` — `True`, если id свободен (ещё не подтверждён и не
    заявлен параллельно другим воркером прямо сейчас) — переводит его в
    `_pending`;
  - `confirm(id)` — подтверждённо отправлен → переезжает из `_pending` в
    постоянный LRU-набор (`deque` + `set`, ограничен `seen_cache_size`,
    самый старый вытесняется при переполнении);
  - `release(id)` — отправка не удалась (или не дали токен вовремя) →
    снимает `_pending`, id снова "не виден" и может быть заново
    заявлен на следующем цикле опроса (пока не протух по `max_age_seconds`).
  `_pending` — это защита только от одновременной попытки отправить один
  и тот же id дважды параллельно (гонка между аккаунтами, если сабы
  пересекаются), а не долгосрочный статус.
- **`BackoffState`** — отдельно считает подряд идущие `rate_or_server`-
  ошибки (429/5xx/network) и подряд идущие `auth`-ошибки (401/403), у них
  разная семантика и разное действие:
  - `register_rate_or_server_error()` → экспоненциальная задержка
    `base_seconds * 2^(N-1)` (потолок `max_seconds`) + случайный джиттер
    15–35%;
  - `register_auth_error()` → аналогичная задержка, но дополнительно
    возвращает `should_stop = consecutive_auth_errors >=
    max_auth_errors` — сигнал воркеру остановиться совсем;
  - `register_success()` сбрасывает оба счётчика.

### `scraper/worker.py`
Ядро — `account_worker(...)`, бесконечный цикл на один аккаунт:

1. Инициализация: проверка файла cookies, загрузка cookies, выбор
   `user_agent`/`impersonate` (сначала из `accounts.yaml`, иначе дефолт
   из `config.yaml`), создание `BackoffState`, сон на `phase_offset`.
2. На каждой итерации цикла — читает актуальные значения из
   `ConfigStore` (hot-reload, поэтому это делается заново каждый раз, а
   не один раз при старте): список сабов, `fetch_limit`, `max_age`,
   `pagination_max_pages`, `poll_interval`/`jitter_ratio`,
   `respect_ratelimit_headers`, `ratelimit_safety_margin`, таймауты,
   `token_wait_timeout`, `store_endpoint` (с приоритетом
   `STORE_ENDPOINT_OVERRIDE` из env), `batch_max_items`, `reddit_base_url`;
   обновляет `bucket.update_rate(...)`.
3. `fetch_comments(...)` → при `error_kind`:
   - `"rate_or_server"` → backoff, `continue` (без похода к остальным
     шагам цикла);
   - `"auth"` → backoff, при исчерпании лимита — `return` (воркер
     полностью останавливается, но `supervised()` его не перезапускает,
     т.к. это штатный `return`, а не исключение);
   - `"network"` → более мягкий backoff, ограниченный `poll_interval * 3`
     сверху (сетевые сбои не должны так же сильно тормозить, как явный
     рейт-лимит).
4. Успех → `backoff.register_success()`, затем для каждого комментария:
   `build_payload` → отбросить, если `_age_seconds > max_age` → собрать
   `payloads`, отсортировать по `_age_seconds` (сначала самые свежие).
5. Для каждого payload по порядку свежести: `seen.try_claim` (иначе —
   дубль, пропуск) → `_acquire_with_retry(bucket, token_wait_timeout)`
   (короткий поллинг токена; не дождались — `seen.release()` и пропуск) →
   добавить в `to_send`.
6. Если `to_send` не пуст — `send_batch_to_store(...)` внутри `try/finally`:
   по каждому результату `True/False` — `confirm()`/`release()`
   соответственно; `finally` дополнительно освобождает (`release`) любой
   id из `to_send`, который почему-то не попал ни в один из путей выше
   (необработанное исключение внутри `send_batch_to_store` или
   `CancelledError` посреди `zip`-цикла) — иначе такой id завис бы в
   `_pending` навсегда и перестал бы когда-либо считаться дублем/свободным.
7. Логирование сводки цикла (получено/свежих/к_отправке/отправлено/
   не_подтверждено/дублей/срезано_лимитом).
8. Расчёт сна до следующего цикла:
   - база — `poll_interval * max(1, pages_fetched)` (если пагинация
     забрала несколько страниц, промежуток перед следующим циклом растёт
     пропорционально, чтобы не увеличивать эффективную частоту запросов);
   - если `respect_ratelimit_headers` и в ответе были `X-Ratelimit-*` —
     интервал может быть **увеличен** (никогда не уменьшен ниже
     конфига) на основе `remaining`/`reset`, с запасом
     `ratelimit_safety_margin` (дефолт 0.85 — не тратим remaining впритык);
   - минимум 1.0с, плюс джиттер `± poll_jitter_ratio`.

`supervised(coro_fn, ...)` — обёртка вокруг `account_worker`: ловит
любое `Exception` (кроме `CancelledError`, который пробрасывается дальше)
и перезапускает воркер с экспоненциальной задержкой (`base_backoff=2s`,
`max_backoff=60s`), не давая одному упавшему аккаунту уронить весь
`asyncio.gather()` в `main.py`. Штатный `return` (например, после
исчерпания auth-ошибок) не перезапускается.

### `scraper/utils.py`
`iso_utc(ts)` — Unix-time → ISO 8601 UTC строка с микросекундами и
суффиксом `Z`.

---

## 3. Поток одного цикла опроса (сводно)

```
fetch_comments (пагинация, до pagination_max_pages)
        │
        ▼
  ошибка? ──да──► backoff (по типу: rate_or_server / auth / network) ──► continue/return
        │ нет
        ▼
build_payload + фильтр max_age_seconds
        │
        ▼
сортировка по свежести (freshest first)
        │
        ▼
для каждого payload:
  seen.try_claim ──false──► дубль, пропуск
        │ true
        ▼
  токен из TokenBucket (с retry до token_wait_timeout)
        │
   не дали ──► seen.release, пропуск (срезано лимитом)
        │ дали
        ▼
  добавить в to_send
        │
        ▼
send_batch_to_store(to_send) — чанками по batch_max_items
        │
        ▼
по каждому: ok ──► seen.confirm   |   not ok ──► seen.release
        │
        ▼
лог сводки цикла + сон (adaptive: pages_fetched, X-Ratelimit, jitter)
```

---

## 4. Что общее между аккаунтами, а что нет

| Общее на процесс                          | Своё на аккаунт                        |
|--------------------------------------------|------------------------------------------|
| `TokenBucket` (суммарный лимит отправки)   | proxy (`proxy_port` → своя VPN-нода)     |
| `SeenCache` (дедуп по всем аккаунтам)      | cookies                                   |
| `ConfigStore` (один источник конфига)      | `user_agent` + `impersonate` (TLS-отпечаток) |
| `ThreadPoolExecutor` (пул под curl_cffi)   | `BackoffState` (свои счётчики ошибок)    |
| `aiohttp.ClientSession` в store_endpoint   | фаза опроса (`phase_offset`)             |

Благодаря общему `TokenBucket`/`SeenCache` можно смело добавлять сабы,
которые пересекаются между аккаунтами (или включать больше аккаунтов на
один и тот же список сабов, чтобы поднять частоту эффективного опроса) —
дубли не пролезут, а суммарная нагрузка на пайплайн ограничена одной
общей ручкой `target_rate_per_second`, а не суммой независимых лимитов.

---

## 5. Батчинг

Отправка в пайплайн — батчами по умолчанию: `account_worker` копит
`to_send` за весь цикл опроса и шлёт его одним (или несколькими, если
`len(to_send) > batch_max_items`) POST-запросом на `/store_items`, а не
по одному запросу на комментарий. `batch_max_items` в `config.yaml`
обрезается по `SERVER_HARD_BATCH_LIMIT = 1000` на стороне клиента
(защита от 413), но управляющая ручка — именно `batch_max_items`,
подбирается под реальный лимит конкретного развёртывания коллектора.

`TokenBucket` при этом продолжает тратить токены **по одному на айтем**,
а не по одному на батч — то есть `target_rate_per_second` остаётся честным
лимитом на число элементов в секунду, просто фактическая доставка по сети
идёт пачками.

---

## 6. Блокирующие вызовы (`curl_cffi`) и пул потоков

`curl_cffi` — синхронная библиотека, поэтому каждый запрос к Reddit
выполняется через `loop.run_in_executor(http_executor, ...)`, а не
напрямую в event loop.

Проблема: `asyncio.wait_for(...)` при срабатывании таймаута отменяет
только asyncio-обёртку вокруг `run_in_executor` — сам поток в
`ThreadPoolExecutor` при этом **не** прерывается принудительно (Python не
умеет убивать потоки снаружи). Если сокет или DNS-резолвинг внутри
`curl_cffi` завис, поток остаётся заблокированным неопределённо долго.
При `max_workers == len(accounts)` зависшая прокси одного аккаунта
навсегда отнимает у пула ровно один поток — свободных не остаётся, и
следующий `run_in_executor` (этого же или любого другого воркера) встаёт
в очередь без шанса на освобождение.

Решение, реализованное в `main.py` и `http_client.py`:

1. Пул создаётся с запасом: `max_workers = max(32, len(accounts) * 2)`, а
   не впритык по числу аккаунтов — так пул переживает несколько таких
   зависаний одновременно, не блокируя остальных воркеров.
2. `curl_cffi` вызывается с явной парой таймаутов `(connect_timeout,
   read_timeout)`, а не одним общим `timeout=` — зависший
   DNS/TCP/TLS-хендшейк должен отваливаться быстрее (`connect_timeout_seconds`,
   дефолт 5с), чем ожидание тела уже установленного соединения
   (`request_timeout_seconds`, дефолт 10с).
3. Вызов дополнительно обёрнут в `asyncio.wait_for(...,
   connect_timeout + read_timeout + HTTP_EXECUTOR_SLACK_SECONDS)` —
   подстраховка на случай, если сокет-таймауты `curl_cffi` всё равно не
   сработают: цикл воркера в любом случае разблокируется и уйдёт в
   обычный `network`-backoff. Сам поток при этом может доработать в фоне
   и корректно освободиться сам — это не "убивает" запрос, а лишь не даёт
   ему держать asyncio-цикл воркера.

---

## 7. Дедуп и надёжность доставки

Двухфазный протокол `SeenCache` (`try_claim → confirm/release`) и
`try/finally` вокруг `send_batch_to_store` в `worker.py` вместе дают такую
гарантию: комментарий считается окончательно "виденным" (и больше никогда
не будет заявлен повторно) **только** после подтверждённой сервером
отправки. Любой сбой на пути — неудачный HTTP-ответ, таймаут, сетевая
ошибка, необработанное исключение внутри `pipeline.py`, отмена задачи
воркера (`CancelledError`) — приводит к `release()`, а не к молчаливой
потере элемента: на следующем цикле опроса (пока комментарий не протух по
`max_age_seconds`) он будет заново подхвачен и отправлен.

Оборотная сторона: если пайплайн (`store_items`) недоступен дольше, чем
`max_age_seconds`, часть комментариев так и не будет отправлена — это
осознанный компромежуточный trade-off свежести против гарантированной
доставки (проект называется "fresh-comments scraper" не просто так).

---

## 8. Конфигурация и hot-reload

`config.yaml` перечитывается `ConfigStore.reload_loop()` каждые
`CONFIG_RELOAD_SECONDS` (дефолт 5с, `env CONFIG_RELOAD_SECONDS`) без
рестарта процесса — `account_worker` каждую итерацию цикла читает
актуальные значения через `config.get(...)`. `accounts.yaml`, наоборот,
читается один раз в `main.py` до старта задач — включение новых
аккаунтов или смена cookie-файлов требует рестарта процесса/контейнера.

Переменные окружения (`CONFIG_PATH`, `ACCOUNTS_PATH`, `PROXY_HOST`,
`STORE_ENDPOINT`, `CONFIG_RELOAD_SECONDS`) позволяют докер-compose файлам
переопределять пути и адреса без правки самого `config.yaml` — см.
`docker-compose.yml`/`docker-compose.bridge.yml`.
