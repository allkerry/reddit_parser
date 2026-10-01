"""
QueueGovernor — авто-подстройка скорости отправки по очереди пайплайна.

Раз в poll_interval_seconds читает GET queue_control.url (например
http://192.168.0.101:9000/queue):

  {"action": "speed_up", "fill": 0.0, "low": 0.2, "high": 0.7,
   "degraded": false, "upipe_queue": 0, "upipe_limit": 200,
   "bpipe_queue": 0, "bpipe_max": 2000, "bpipe_inflight": 0,
   "bpipe_instances": 2}

и двигает потолок общего TokenBucket так, чтобы bpipe_queue держалась
не ниже target_min (пайплайн не простаивает), но не раздувалась до
переполнения.

Управляется только потолок отправки. Частоту опроса Reddit по-прежнему
адаптирует PollScheduler по доле дублей.
"""

import asyncio
import time

import aiohttp

from .state import TokenBucket


def _num(v, default=None):
    try:
        return float(v)
    except (TypeError, ValueError):
        return default


class QueueGovernor:
    def __init__(self, config, bucket: TokenBucket, session: aiohttp.ClientSession, log):
        self.config = config
        self.bucket = bucket
        self.session = session
        self.log = log

        self.rate: float = bucket.rate
        self.observed: float | None = None  # EWMA реально выданных токенов/сек

        self._active = False
        self._fallback = False
        self._active_since = time.monotonic()
        self._last_ok: float | None = None
        self._last_granted = bucket.granted
        self._last_t = time.monotonic()
        self._last_log = 0.0
        self._last_status: dict | None = None
        self._last_reason = "старт"

    def _cfg(self) -> dict:
        return self.config.get("queue_control", {}) or {}

    def _static_rate(self) -> float:
        return float(self.config.get("target_rate_per_second", 80))

    async def _fetch(self, url: str, timeout: float) -> dict | None:
        try:
            async with self.session.get(url, timeout=aiohttp.ClientTimeout(total=timeout)) as resp:
                if resp.status != 200:
                    self.log.warning("[governor] %s ответил %s", url, resp.status)
                    return None
                data = await resp.json(content_type=None)
                return data if isinstance(data, dict) else None
        except (aiohttp.ClientError, asyncio.TimeoutError, ValueError) as e:
            self.log.warning("[governor] не удалось прочитать %s: %s", url, e)
            return None

    def _decide(self, q: dict, c: dict) -> tuple[float, str]:
        """Возвращает (новый потолок, причина)."""
        rate = self.rate
        observed = self.observed or 0.0
        min_rate = float(c.get("min_rate", 5))
        max_rate = float(c.get("max_rate", 400))
        initial = float(c.get("initial_rate", 80))

        bq = _num(q.get("bpipe_queue"))
        if bq is None:
            return rate, "в ответе нет bpipe_queue — держу"
        bmax = _num(q.get("bpipe_max"))
        uq = _num(q.get("upipe_queue"))
        ulim = _num(q.get("upipe_limit"))

        target_min = float(c.get("target_min", 200))
        headroom = float(c.get("headroom_ratio", 1.25))
        up_thr = target_min * headroom

        if c.get("target_high") is not None:
            high = float(c["target_high"])
        elif bmax:
            high = bmax * float(c.get("high_fraction", 0.5))
        else:
            high = up_thr * 4
        high = max(high, up_thr * 1.5)  # мёртвая зона между порогами всегда есть

        panic_frac = float(c.get("panic_fraction", 0.9))
        panic = bool(q.get("degraded")) \
            or bool(bmax and bq >= bmax * panic_frac) \
            or bool(ulim and uq is not None and uq >= ulim * panic_frac)

        action = str(q.get("action", "")).lower()
        slow_action = bool(c.get("respect_server_action", True)) and any(
            w in action for w in ("slow", "pause", "stop")
        )

        # База для торможения — реальная скорость, а не раздутый потолок:
        # иначе ряд тиков уходил бы на "пустое" снижение.
        base = min(rate, max(observed, min_rate))

        if panic:
            new = base * float(c.get("panic_factor", 0.5))
            reason = "ПАНИКА (degraded/очереди почти полные)"
        elif slow_action or bq > high:
            new = base * float(c.get("down_factor", 0.8))
            reason = f"сервер просит притормозить (action={action})" if slow_action \
                else f"bpipe_queue={bq:.0f} > {high:.0f} — тормозим"
        elif bq < up_thr:
            ceiling = max(observed * float(c.get("rate_ceiling_over_observed", 2.0)), initial)
            new = min(max_rate, max(rate, min(rate * float(c.get("up_factor", 1.25)), ceiling)))
            reason = f"bpipe_queue={bq:.0f} < {up_thr:.0f} — ускоряемся"
        else:
            new = rate
            reason = f"bpipe_queue={bq:.0f} в норме — держим"

        return max(min_rate, min(max_rate, new)), reason

    def _apply(self, new_rate: float):
        self.rate = new_rate
        self.bucket.update_rate(new_rate)

    async def run(self):
        while True:
            c = self._cfg()
            interval = max(0.5, float(c.get("poll_interval_seconds", 2.0)))
            now = time.monotonic()

            # реально выдаваемая скорость (токенов/сек) за прошлый тик
            dt = now - self._last_t
            granted = self.bucket.granted
            if dt > 0:
                inst = (granted - self._last_granted) / dt
                self.observed = inst if self.observed is None else 0.5 * inst + 0.5 * self.observed
            self._last_granted, self._last_t = granted, now

            url = c.get("url")
            if not c.get("enabled", True) or not url:
                if self._active:
                    self._active = False
                    self.log.info("[governor] выключен — статичный target_rate_per_second")
                self._apply(self._static_rate())
                await asyncio.sleep(interval)
                continue

            if not self._active:
                self._active = True
                self._fallback = False
                self._active_since = now
                self._last_ok = None
                self._apply(float(c.get("initial_rate", 80)))
                self.log.info("[governor] включён: url=%s target_min=%s стартовый потолок=%.0f/с",
                              url, c.get("target_min", 200), self.rate)

            status = await self._fetch(url, float(c.get("request_timeout_seconds", 2.0)))

            if status is not None:
                self._last_ok = now
                self._last_status = status
                if self._fallback:
                    self._fallback = False
                    self.log.info("[governor] эндпоинт очереди снова доступен")
                old = self.rate
                new, reason = self._decide(status, c)
                self._last_reason = reason
                self._apply(new)
                changed = abs(new - old) > max(1.0, old * 0.05)
                periodic = now - self._last_log >= float(c.get("log_every_seconds", 30))
                if changed or periodic:
                    self._last_log = now
                    self.log.info(
                        "[governor] bpipe_queue=%s/%s upipe=%s/%s degraded=%s | потолок %.0f -> %.0f/с "
                        "(реально %.0f/с) | %s",
                        status.get("bpipe_queue"), status.get("bpipe_max"),
                        status.get("upipe_queue"), status.get("upipe_limit"),
                        status.get("degraded"), old, new, self.observed or 0.0, reason,
                    )
            else:
                since = self._last_ok if self._last_ok is not None else self._active_since
                stale = float(c.get("stale_seconds", 30))
                if not self._fallback and now - since > stale:
                    self._fallback = True
                    fb = self._static_rate()
                    self._apply(fb)
                    self.log.warning(
                        "[governor] очередь недоступна > %.0fs — fallback на статичный "
                        "target_rate_per_second=%.0f/с", stale, fb,
                    )

            await asyncio.sleep(interval)