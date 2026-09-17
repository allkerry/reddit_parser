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

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] [%(name)s] %(message)s",
)
log = logging.getLogger("scraper")
