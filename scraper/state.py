import asyncio
import random
import time
from collections import deque


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
        # Сколько токенов выдано за всё время — QueueGovernor по разнице
        # считает реальную скорость отправки (токенов/сек).
        self.granted = 0

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
                self.granted += 1
                return True
            return False


# ---------------------------------------------------------------- #
#  Общий дедуп-кэш (двухфазный: claim -> confirm/release)
# ---------------------------------------------------------------- #

class SeenCache:
    """id считается окончательно "виденным" только после confirm().
    До этого он лежит в `_pending` (защита от параллельной отправки
    одного id). release() снимает claim — id снова свободен."""

    def __init__(self, max_size: int):
        self._deque = deque(maxlen=max_size)
        self._set = set()
        self._pending = set()
        self._lock = asyncio.Lock()

    def contains(self, item_id: str) -> bool:
        """Синхронная проверка (event loop однопоточный, лок не нужен):
        id подтверждён или прямо сейчас отправляется."""
        return item_id in self._set or item_id in self._pending

    async def try_claim(self, item_id: str) -> bool:
        async with self._lock:
            if item_id in self._set or item_id in self._pending:
                return False
            self._pending.add(item_id)
            return True

    async def confirm(self, item_id: str):
        async with self._lock:
            self._pending.discard(item_id)
            if item_id not in self._set:
                if len(self._deque) == self._deque.maxlen:
                    old = self._deque.popleft()
                    self._set.discard(old)
                self._deque.append(item_id)
                self._set.add(item_id)

    async def release(self, item_id: str):
        async with self._lock:
            self._pending.discard(item_id)


# ---------------------------------------------------------------- #
#  Backoff-состояние на аккаунт
# ---------------------------------------------------------------- #

class BackoffState:
    """Отдельно считает подряд идущие rate-limit/5xx ошибки и отдельно —
    подряд идущие auth-ошибки (401/403)."""

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