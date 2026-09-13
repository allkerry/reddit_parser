# Reddit fresh-comments scraper

Опрашивает список сабреддитов через `www.reddit.com/r/.../comments.json`
(авторизованно, через cookies) с нескольких аккаунтов параллельно, каждый —
через свой VPN-выход (своя нода из твоей подписки), отбирает только самые
свежие комментарии и льёт их в локальный пайплайн (`POST /store_item`) с
управляемой скоростью.

## Структура

```
reddit_scraper/
├── scraper.py              # основная async-логика
├── config.yaml             # тюнинг: target_rate, max_age, сабы, fetch_limit...
├── accounts.yaml           # 6 слотов аккаунтов, cookie_file + proxy_port
├── mihomo_config.yaml      # 6 VPN-нод, каждая на своём локальном порту
├── cookies/account_N.json  # cookies аккаунтов (Cookie Editor export)
├── Dockerfile               # образ scraper.py
├── docker-compose.yml       # Linux: mihomo + scraper, network_mode: host
├── docker-compose.bridge.yml# Mac/Windows: bridge-сеть + host.docker.internal
├── requirements.txt
└── .gitignore / .dockerignore
```

---

## Вариант 1: Docker (рекомендуется)

### Linux

```bash
docker compose up -d --build
docker compose logs -f scraper
```

`docker-compose.yml` поднимает оба контейнера в `network_mode: host`, поэтому
scraper видит порты mihomo (`127.0.0.1:7891-7896`) и локальный пайплайн
(`127.0.0.1:9000/store_item`) на хосте без дополнительной настройки сети.

### Mac / Windows (Docker Desktop)

`network_mode: host` там не работает как на Linux, поэтому используй bridge-вариант:

```bash
docker compose -f docker-compose.bridge.yml up -d --build
docker compose -f docker-compose.bridge.yml logs -f scraper
```

Здесь scraper обращается к mihomo по DNS-имени контейнера (`PROXY_HOST=mihomo`),
а к пайплайну на хосте — через `host.docker.internal` (уже прописано в
`environment:` этого файла).

### Остановка / рестарт

```bash
docker compose down
docker compose restart scraper   # если поменял accounts.yaml (новые аккаунты)
```

**Важно:** `config.yaml` (target_rate, max_age, сабы, fetch_limit,
poll_interval) **перечитывается на лету каждые 5 секунд** — просто правишь
файл на хосте, рестарт контейнера не нужен, изменения подхватятся сами
(это видно в логах: `config.yaml обновлён: target_rate=... max_age=...`).

А вот `accounts.yaml` (включение новых аккаунтов, смена cookie-файлов)
читается один раз при старте процесса — после правки нужен
`docker compose restart scraper`.

---

## Вариант 2: без Docker, локально

```bash
pip install -r requirements.txt
```

Понадобится бинарник **mihomo** (Clash.Meta core):
https://github.com/MetaCubeX/mihomo/releases

Терминал 1 — прокси:
```bash
mihomo -f mihomo_config.yaml
```

Терминал 2 — скрапер:
```bash
python3 scraper.py
```

---

## Таблица аккаунт -> нода -> порт

| Аккаунт    | Нода          | Локальный порт | Статус на старте |
|------------|---------------|-----------------|-------------------|
| account_1  | Canada        | 7891            | enabled (cookies есть) |
| account_2  | Russia        | 7892            | disabled (нет cookies) |
| account_3  | USA           | 7893            | disabled |
| account_4  | Great Britain | 7894            | disabled |
| account_5  | Germany       | 7895            | disabled |
| account_6  | Netherlands   | 7896            | disabled |

## Добавление остальных аккаунтов

1. Экспортируй cookies (Cookie Editor -> Export -> JSON) для нужного
   аккаунта, сохрани как `cookies/account_2.json` (и т.д.) — лишние поля
   экспорта скрипт сам игнорирует, важны только `name`/`value`.
2. В `accounts.yaml` поставь этому аккаунту `enabled: true`.
3. `docker compose restart scraper` (или просто перезапусти `python3 scraper.py`
   при локальном запуске). mihomo трогать не нужно — все 6 портов уже подняты.

## Тюнинг под нагрузку пайплайна

Всё — в `config.yaml`, подхватывается автоматически (см. выше про hot-reload):

- `target_rate_per_second` — сколько свежих комментариев в секунду слать
  в `store_item`. Смотришь логи пайплайна -> крутишь это значение.
- `max_age_seconds` — жёсткий потолок возраста комментария. Всё старше —
  отбрасывается ещё до отправки.
- `subreddits` — список сабов (сейчас первые 20 из top_subreddits.csv).
- `fetch_limit` — сколько комментариев запрашивать у Reddit за один запрос
  (сейчас 50).
- `poll_interval_seconds` — как часто **каждый** аккаунт дёргает Reddit.
  Эффективная частота опроса всего списка сабов = это значение делить на
  число активных аккаунтов (они идут со сдвигом по фазе).

## Логи

В логах на каждый цикл каждого аккаунта:
```
[account_1] цикл: получено=50 свежих=12 отправлено=8 дублей=2 срезано_лимитом=2
```
- `получено` — сколько всего вернул Reddit;
- `свежих` — сколько прошло фильтр `max_age_seconds`;
- `отправлено` — реально ушло в `store_item`;
- `дублей` — совпало с уже отправленными (дедуп-кэш);
- `срезано_лимитом` — не влезло в `target_rate_per_second`, отброшено
  (это нормально и ожидаемо, если свежего контента больше, чем ты просишь
  в единицу времени).

## Защита от рейт-лимита / банов (v3)

По результатам реального прогона (73 успешных цикла ~3.5 мин, потом
сплошные 429 без самовосстановления) добавлено:

- **Exponential backoff** при 429/5xx/сетевых ошибках:
  `base_backoff_seconds * 2^(подряд ошибок)`, потолок `max_backoff_seconds`,
  плюс случайный джиттер. Если Reddit прислал `Retry-After` — используется
  он, если он больше расчётного backoff. Пока идёт backoff, аккаунт **не
  делает новых запросов вообще** — раньше скрипт продолжал стучаться
  каждые 3 сек прямо во время блокировки, что её только продлевало.
- **Джиттер обычного интервала** (`poll_jitter_ratio`, дефолт ±25%) —
  паттерн запросов больше не идеально ровный "тик-так" каждые 3.000 сек.
- **Реалистичные User-Agent** по одному на аккаунт (`accounts.yaml`) —
  раньше UA был `python:fresh-comment-collector:v1.0`, что прямым текстом
  выдавало скрипт. Теперь — обычные браузерные строки, разные на разных
  аккаунтах.
- **Отдельная обработка 401/403**: если у аккаунта подряд
  `max_consecutive_auth_errors` (дефолт 5) неудачных попыток — воркер
  останавливается совсем с понятным ERROR в логе ("обнови cookies"),
  вместо того чтобы бесконечно долбить, возможно, уже забаненный аккаунт.
- Опция переключиться на `old.reddit.com` через `reddit_base_url` в
  `config.yaml`, если у него окажется отдельный/мягче рейт-лимит.

Это снижает риск, но не убирает его до нуля — многоаккаунтовый скрейпинг
с датацентровых IP в принципе замечаем современными антибот-системами.
Если увидишь в логах регулярные `backoff` — это сигнал снизить
`target_rate_per_second`/увеличить `poll_interval_seconds`, а не игнорировать.

## Известные ограничения / на что смотреть

- Reddit может отдавать `429` при слишком частых запросах — если видишь
  это в логах, увеличивай `poll_interval_seconds` или сокращай список
  сабов на аккаунт.
- `401`/`403` в логах конкретного аккаунта = протухли cookies или бан —
  worker этого аккаунта просто перестаёт слать данные, остальные
  продолжают работать. В Docker это будет видно через
  `docker compose logs -f scraper`.
- Дедуп-кэш (`seen_cache_size`, сейчас 1000) — только в памяти. При
  рестарте контейнера/процесса обнуляется (так и задумано).
- Если `mihomo` без поддержки `listeners` (старая версия) — нужно
  поднимать 6 отдельных инстансов mihomo с 6 отдельными конфигами вместо
  одного файла со списком `listeners`. Скажи, если версия не потянет —
  подготовлю такой вариант (и под Docker тоже, отдельным сервисом на
  каждую ноду).
- `docker-compose.yml` тянет `metacubex/mihomo:latest` — если нужна
  конкретная зафиксированная версия для повторяемых сборок, скажи, подставлю
  тег вместо `latest`.
