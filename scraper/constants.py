import logging
import os
from pathlib import Path

BASE_DIR = Path(__file__).parent.parent
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

# Как часто (в секундах) логировать снапшот занятости http_executor
# (см. scraper/health.py) — фоновая задача в main.py. Не влияет на сам
# счётчик, только на частоту логирования его текущего значения (и на
# частоту, с которой вообще проверяется условие свопа, см. ниже).
EXECUTOR_HEALTH_LOG_INTERVAL_SECONDS = float(os.environ.get("EXECUTOR_HEALTH_LOG_INTERVAL_SECONDS", "300"))

# ---------------------------------------------------------------- #
#  Своп http_executor при деградации (см. scraper/health.py)
#
#  asyncio.wait_for() не может принудительно прервать физический поток
#  curl_cffi (Python не умеет убивать потоки снаружи, см.
#  ARCHITECTURE.md §6) — если прокси/DNS зависают навсегда, поток
#  остаётся "занятым" пулом бесконечно. Единственный способ
#  "вылечиться" без рестарта всего процесса — пересоздать пул целиком.
#  Три ручки ниже управляют тем, КОГДА это делать и когда сдаться.
# ---------------------------------------------------------------- #

# Доля max_workers, при достижении которой (active >= max_workers *
# EXECUTOR_SWAP_THRESHOLD) считаем пул почти исчерпанным и кандидатом
# на пересоздание.
EXECUTOR_SWAP_THRESHOLD = float(os.environ.get("EXECUTOR_SWAP_THRESHOLD", "0.9"))

# Минимальный интервал между двумя свопами подряд. Без него: если
# конкретная VPN-нода дохнет навсегда (не временно подвиснет), новый
# пул почти сразу же дойдёт до того же порога (те же воркеры уйдут в
# те же зависания на той же мёртвой ноде), и без cooldown процесс начал
# бы штамповать новые ThreadPoolExecutor подряд, без остановки.
EXECUTOR_SWAP_COOLDOWN_SECONDS = float(os.environ.get("EXECUTOR_SWAP_COOLDOWN_SECONDS", "300"))

# Если суммарно утёкших (списанных вместе со старыми пулами) потоков
# накопилось >= EXECUTOR_FATAL_LEAK_MULTIPLIER * max_workers — значит
# свопы не успевают за темпом утечки (она не самоограничивается), и
# дальнейшие свопы не помогают. В этом случае процесс сам завершается
# (SystemExit), чтобы docker перезапустил его чисто
# (restart: unless-stopped в docker-compose.yml), вместо того чтобы
# тихо копить ОС-потоки неделями.
EXECUTOR_FATAL_LEAK_MULTIPLIER = float(os.environ.get("EXECUTOR_FATAL_LEAK_MULTIPLIER", "10"))

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] [%(name)s] %(message)s",
)
log = logging.getLogger("scraper")
