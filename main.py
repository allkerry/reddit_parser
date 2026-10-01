#!/usr/bin/env python3
"""
Reddit fresh-comments scraper -> POST /store_items (batch)

Схема: все аккаунты опрашивают один поток (по умолчанию r/all), но не
каждый по своему таймеру, а по слотам от единого PollScheduler
(scraper/scheduler.py): слоты выдаются по кругу равномерно, аккаунт со
своим X-Ratelimit-бюджетом пропускается, пока не "отдохнул". Интервал
слотов подстраивается по доле дублей на первой странице ответа.
Пагинация идёт до пересечения с уже виденными комментариями (догоняет
пропуски).

Скорость отправки в пайплайн (потолок TokenBucket) ведёт QueueGovernor
(scraper/governor.py) по состоянию очереди коллектора (queue_control в
config.yaml): цель — чтобы bpipe_queue не опускалась ниже target_min.

Остальное — общий SeenCache, health-мониторинг executor'а, graceful
shutdown — как раньше, см. ARCHITECTURE.md.
"""

import asyncio
import signal
from concurrent.futures import ThreadPoolExecutor

import aiohttp

from scraper.config import ConfigStore, load_accounts
from scraper.constants import (
    CONFIG_PATH,
    CONFIG_RELOAD_SECONDS,
    EXECUTOR_FATAL_LEAK_MULTIPLIER,
    EXECUTOR_HEALTH_LOG_INTERVAL_SECONDS,
    EXECUTOR_SWAP_COOLDOWN_SECONDS,
    EXECUTOR_SWAP_THRESHOLD,
    PROXY_HOST,
    log,
)
from scraper.governor import QueueGovernor
from scraper.health import ExecutorHandle, ExecutorHealth, health_report_loop
from scraper.scheduler import PollScheduler
from scraper.state import SeenCache, TokenBucket
from scraper.worker import account_worker, supervised


def _install_signal_handlers(loop: asyncio.AbstractEventLoop, stop_event: asyncio.Event):
    def _handle_stop_signal(sig_name: str):
        if not stop_event.is_set():
            log.info("Получен сигнал %s — начинаю штатную остановку", sig_name)
            stop_event.set()

    for sig in (signal.SIGTERM, signal.SIGINT):
        try:
            loop.add_signal_handler(sig, _handle_stop_signal, sig.name)
        except NotImplementedError:
            log.debug("add_signal_handler недоступен для %s на этой платформе", sig.name)


async def main():
    config = ConfigStore(CONFIG_PATH, CONFIG_RELOAD_SECONDS)
    config.load_once()

    accounts = [a for a in load_accounts() if a.get("enabled")]
    if not accounts:
        log.error("Нет ни одного enabled:true аккаунта в accounts.yaml — нечего запускать")
        return

    qc = config.get("queue_control", {}) or {}
    qc_enabled = bool(qc.get("enabled", False)) and bool(qc.get("url"))
    start_rate = float(qc.get("initial_rate", 80)) if qc_enabled else float(config.get("target_rate_per_second", 80))

    log.info(
        "Запуск: %d аккаунт(ов), поток=r/%s, скорость=%s, max_age=%ss, "
        "batch_max_items=%s, старт_интервал_слота=%ss, proxy_host=%s",
        len(accounts), "+".join(config.get("stream_subs", ["all"])),
        f"авто по очереди ({qc.get('url')}, target_min={qc.get('target_min', 200)})"
        if qc_enabled else f"статичная {start_rate}/сек",
        config.get("max_age_seconds"),
        config.get("batch_max_items", 500), config.get("stream_start_interval_seconds", 1.5),
        PROXY_HOST,
    )

    bucket = TokenBucket(start_rate)
    seen = SeenCache(config.get("seen_cache_size", 40000))
    scheduler = PollScheduler(config.get)

    http_executor_max_workers = max(32, len(accounts) * 2)
    http_executor_handle = ExecutorHandle(
        factory=lambda: ThreadPoolExecutor(
            max_workers=http_executor_max_workers,
            thread_name_prefix="reddit-fetch",
        ),
        max_workers=http_executor_max_workers,
    )
    executor_health = ExecutorHealth()

    loop = asyncio.get_running_loop()
    stop_event = asyncio.Event()
    _install_signal_handlers(loop, stop_event)

    try:
        store_connector = aiohttp.TCPConnector(ttl_dns_cache=300, keepalive_timeout=60)
        async with aiohttp.ClientSession(connector=store_connector) as store_session:
            governor = QueueGovernor(config, bucket, store_session, log)
            tasks = [
                asyncio.create_task(config.reload_loop(), name="config_reload_loop"),
                asyncio.create_task(governor.run(), name="queue_governor"),
                asyncio.create_task(
                    health_report_loop(
                        executor_health,
                        http_executor_handle,
                        EXECUTOR_HEALTH_LOG_INTERVAL_SECONDS,
                        log,
                        swap_threshold=EXECUTOR_SWAP_THRESHOLD,
                        swap_cooldown_seconds=EXECUTOR_SWAP_COOLDOWN_SECONDS,
                        fatal_leak_multiplier=EXECUTOR_FATAL_LEAK_MULTIPLIER,
                    ),
                    name="health_report_loop",
                ),
                asyncio.create_task(scheduler.run(), name="poll_scheduler"),
                asyncio.create_task(scheduler.stats_loop(log), name="stream_stats"),
            ]
            for account in accounts:
                tasks.append(
                    asyncio.create_task(
                        supervised(
                            account_worker,
                            account, config, bucket, seen, scheduler, store_session,
                            http_executor_handle,
                            executor_health,
                            name=account["name"],
                        ),
                        name=f"account_worker[{account['name']}]",
                    )
                )

            stop_waiter = asyncio.create_task(stop_event.wait(), name="stop_signal_waiter")
            done, _pending = await asyncio.wait(
                [*tasks, stop_waiter], return_when=asyncio.FIRST_COMPLETED
            )

            if stop_waiter in done:
                log.info("Останавливаю %d фоновых задач...", len(tasks))
            else:
                stop_waiter.cancel()
                for t in done:
                    if t is not stop_waiter:
                        exc = t.exception()
                        if exc is not None:
                            log.error("Задача %s завершилась с ошибкой, останавливаю процесс", t.get_name())

            for t in tasks:
                if not t.done():
                    t.cancel()
            await asyncio.gather(*tasks, return_exceptions=True)
    finally:
        http_executor_handle.shutdown(wait=False, cancel_futures=True)

    log.info("Остановлено штатно")


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        log.info("Остановлено пользователем (KeyboardInterrupt)")