#!/usr/bin/env python3
"""
Reddit fresh-comments scraper -> POST /store_items (batch)

См. README.md / ARCHITECTURE.md за полным описанием. Ключевое отличие
этой версии от предыдущей: сабреддиты больше не опрашиваются одним
общим списком, одинаковым для всех аккаунтов. Вместо этого:

- scraper/grouping.py делит весь список сабов (top_subreddits.csv, ранг
  как proxy активности) между аккаунтами так, чтобы каждый саб достался
  РОВНО одному аккаунту (кросс-аккаунтные дубли исчезают почти полностью,
  общий SeenCache остаётся только на случай внутрипроцессной гонки), с
  балансировкой по ожидаемому трафику (1/sqrt(rank)), а не просто по
  количеству сабов на аккаунт.
- Внутри аккаунта сабы бьются на "группы" (multi-sub таргеты) по тирам:
  hot (маленькие группы — иначе активный саб задавит соседей в общем
  ответе Reddit), medium, cold (крупные группы — иначе один запрос на
  дохлый саб возвращает пару комментариев впустую).
- Каждая группа опрашивается со своим адаптивным интервалом
  (scraper/state.py:GroupState) — подстраивается по факту выхода
  (мало комментариев -> реже, много -> чаще), а не остаётся фиксированной
  навсегда.

Также в этой версии добавлен health-мониторинг http_executor (см.
scraper/health.py): asyncio.wait_for() в http_client.py при таймауте не
убивает физический поток curl_cffi (Python не умеет прерывать потоки
снаружи) — если прокси/DNS зависают, поток остаётся занятым executor'ом
навсегда, и за недели непрерывной работы фиксированный пул может
постепенно исчерпаться, приводя к тихому параличу всех воркеров без
единой явной ошибки в логах.

Начиная с этой версии, health-мониторинг не только детектирует
деградацию, но и лечит её: http_executor теперь не голый
ThreadPoolExecutor, а ExecutorHandle (scraper/health.py) — обёртка,
которую health_report_loop() может пересоздать "на лету", если занятость
пула приближается к его размеру. Сам пул почистить нельзя (Python не
умеет прерывать потоки снаружи), поэтому единственный рабочий вариант —
списать старый пул целиком (уже не стартовавшие задачи отменяются, а
реально зависшие потоки просто перестают быть чьей-либо заботой) и
продолжить работу с новым, чистым. Два предохранителя защищают от того,
чтобы сам своп не стал новой формой той же болезни: cooldown между
свопами (иначе постоянно дохнущая VPN-нода заставила бы процесс
штамповать новые пулы без остановки) и аварийный потолок по суммарно
утёкшим потокам (если своп явно не успевает за темпом утечки — процесс
сам завершается через SystemExit, отдавая перезапуск docker'у
(`restart: unless-stopped`), вместо тихого накопления ОС-потоков
неделями). ExecutorHandle передаётся в account_worker и резолвится в
http_client.py на каждый отдельный HTTP-запрос (а не один раз при
старте воркера) — благодаря этому даже долгоживущий воркер, который не
падал и не перезапускался месяцами, подхватывает новый пул на
следующем же цикле опроса, без своего собственного рестарта.

Graceful shutdown: и SIGINT (Ctrl+C), и SIGTERM (docker stop /
docker compose down / docker compose restart) явно перехватываются
через loop.add_signal_handler и штатно отменяют все фоновые задачи —
раньше SIGTERM не обрабатывался вовсе (Python транслирует в
KeyboardInterrupt только SIGINT), и контейнер, скорее всего, просто
убивался по таймауту docker stop (SIGTERM -> 10с -> SIGKILL) без единой
строки в логе о причине остановки.

Остальное — общий TokenBucket/SeenCache/backoff — как раньше, см.
заголовок предыдущей версии этого файла в git-истории и ARCHITECTURE.md.
"""

import asyncio
import signal
from concurrent.futures import ThreadPoolExecutor

import aiohttp

from scraper.config import ConfigStore, load_accounts
from scraper.constants import (
    BASE_DIR,
    CONFIG_PATH,
    CONFIG_RELOAD_SECONDS,
    EXECUTOR_FATAL_LEAK_MULTIPLIER,
    EXECUTOR_HEALTH_LOG_INTERVAL_SECONDS,
    EXECUTOR_SWAP_COOLDOWN_SECONDS,
    EXECUTOR_SWAP_THRESHOLD,
    PROXY_HOST,
    log,
)
from scraper.grouping import build_account_groups, build_account_groups_from_ranked
from scraper.health import ExecutorHandle, ExecutorHealth, health_report_loop
from scraper.state import SeenCache, TokenBucket
from scraper.worker import account_worker, supervised


def _build_groups(config: ConfigStore, account_names: list[str]) -> dict[str, list[dict]]:
    tier_cfg = config.get("tier_sizing", {})
    kwargs = dict(
        hot_max_rank=tier_cfg.get("hot_max_rank", 50),
        medium_max_rank=tier_cfg.get("medium_max_rank", 300),
        hot_group_size=tier_cfg.get("hot_group_size", 2),
        medium_group_size=tier_cfg.get("medium_group_size", 8),
        cold_group_size=tier_cfg.get("cold_group_size", 40),
    )

    csv_name = config.get("subreddit_ranks_csv")
    csv_path = (BASE_DIR / csv_name) if csv_name else None

    if csv_path and csv_path.exists():
        groups = build_account_groups(csv_path, account_names, **kwargs)
        log.info("Раскладка сабов построена из %s", csv_path)
        return groups

    # Fallback: если CSV не задан/не найден — берём плоский список
    # subreddits из config.yaml и присваиваем ранги по порядку в списке
    # (первый = самый "горячий"), чтобы вся логика тиров/группировки
    # работала так же, просто без реальных данных о популярности.
    flat = config.get("subreddits", [])
    if not flat:
        log.warning(
            "Ни subreddit_ranks_csv, ни config.yaml:subreddits не заданы — "
            "аккаунтам не будет назначено ни одного сабреддита"
        )
        return {name: [] for name in account_names}

    log.warning(
        "%s не найден — использую плоский список subreddits из config.yaml "
        "(%d шт.) с рангом по порядку в списке вместо реального CSV",
        csv_path, len(flat),
    )
    ranked = [(i + 1, s) for i, s in enumerate(flat)]
    return build_account_groups_from_ranked(ranked, account_names, **kwargs)


def _install_signal_handlers(loop: asyncio.AbstractEventLoop, stop_event: asyncio.Event):
    """Вешает обработчики SIGTERM и SIGINT на текущий event loop.

    По умолчанию asyncio транслирует в KeyboardInterrupt ТОЛЬКО SIGINT —
    SIGTERM (именно его шлют `docker stop` / `docker compose down` /
    `docker compose restart`) молча убивает процесс мимо всех
    try/finally и async-with, если не перехватить его явно. Здесь
    обработчик не делает саму остановку — он лишь выставляет
    stop_event, а вся логика штатного завершения (отмена задач,
    закрытие сессий) живёт в main() ниже, в обычном асинхронном коде."""

    def _handle_stop_signal(sig_name: str):
        if not stop_event.is_set():
            log.info("Получен сигнал %s — начинаю штатную остановку", sig_name)
            stop_event.set()

    for sig in (signal.SIGTERM, signal.SIGINT):
        try:
            loop.add_signal_handler(sig, _handle_stop_signal, sig.name)
        except NotImplementedError:
            # add_signal_handler недоступен на этой платформе (например,
            # стандартный event loop на Windows) — оставляем дефолтное
            # поведение asyncio для неё (SIGINT -> KeyboardInterrupt).
            log.debug(
                "add_signal_handler недоступен для %s на этой платформе — "
                "используется поведение asyncio по умолчанию", sig.name,
            )


async def main():
    config = ConfigStore(CONFIG_PATH, CONFIG_RELOAD_SECONDS)
    config.load_once()

    accounts = [a for a in load_accounts() if a.get("enabled")]
    if not accounts:
        log.error("Нет ни одного enabled:true аккаунта в accounts.yaml — нечего запускать")
        return

    account_names = [a["name"] for a in accounts]
    groups_by_account = _build_groups(config, account_names)

    for name in account_names:
        groups = groups_by_account.get(name, [])
        total_subs = sum(len(g["subs"]) for g in groups)
        log.info(
            "[%s] назначено %d сабов в %d группах (%s)",
            name, total_subs, len(groups),
            ", ".join(f"{g['tier']}x{len(g['subs'])}" for g in groups) or "пусто",
        )

    log.info(
        "Запуск: %d аккаунт(ов), target_rate=%s/сек, max_age=%ss, batch_max_items=%s, proxy_host=%s",
        len(accounts), config.get("target_rate_per_second"), config.get("max_age_seconds"),
        config.get("batch_max_items", 500), PROXY_HOST,
    )

    bucket = TokenBucket(config.get("target_rate_per_second", 25))
    seen = SeenCache(config.get("seen_cache_size", 1000))

    # ExecutorHandle вместо голого ThreadPoolExecutor — см. docstring
    # модуля выше и scraper/health.py: позволяет health_report_loop
    # пересоздать пул целиком, если он деградировал (зависшие потоки на
    # мёртвых прокси/DNS, которые нельзя прервать снаружи).
    http_executor_max_workers = max(32, len(accounts) * 2)
    http_executor_handle = ExecutorHandle(
        factory=lambda: ThreadPoolExecutor(
            max_workers=http_executor_max_workers,
            thread_name_prefix="reddit-fetch",
        ),
        max_workers=http_executor_max_workers,
    )

    # Общий на процесс счётчик занятости http_executor (см.
    # scraper/health.py) — диагностика зависших потоков curl_cffi,
    # которые asyncio.wait_for() не может прервать принудительно, и
    # источник данных для решения о свопе пула.
    executor_health = ExecutorHealth()

    loop = asyncio.get_running_loop()
    stop_event = asyncio.Event()
    _install_signal_handlers(loop, stop_event)

    try:
        store_connector = aiohttp.TCPConnector(ttl_dns_cache=300, keepalive_timeout=60)
        async with aiohttp.ClientSession(connector=store_connector) as store_session:
            tasks = [
                asyncio.create_task(config.reload_loop(), name="config_reload_loop"),
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
            ]
            for i, account in enumerate(accounts):
                phase_offset = i * (config.get("poll_interval_seconds", 3) / len(accounts))
                tasks.append(
                    asyncio.create_task(
                        supervised(
                            account_worker,
                            account, config, bucket, seen, phase_offset, store_session,
                            http_executor_handle,
                            groups_by_account.get(account["name"], []),
                            executor_health,
                            name=account["name"]
                        ),
                        name=f"account_worker[{account['name']}]",
                    )
                )

            # Гонка между "любая из фоновых задач завершилась сама" и
            # "пришёл сигнал остановки" — что бы ни случилось раньше,
            # ведёт к одному и тому же штатному пути отмены ниже.
            # Сюда же попадает и штатный SystemExit из health_report_loop
            # (аварийный потолок утечки потоков, см. scraper/health.py) —
            # он всплывает как исключение задачи, а не как сигнал, и
            # обрабатывается веткой "одна из задач завершилась сама" ниже.
            stop_waiter = asyncio.create_task(stop_event.wait(), name="stop_signal_waiter")
            done, _pending = await asyncio.wait(
                [*tasks, stop_waiter], return_when=asyncio.FIRST_COMPLETED
            )

            if stop_waiter in done:
                log.info("Останавливаю %d фоновых задач...", len(tasks))
            else:
                stop_waiter.cancel()
                # Одна из задач (воркер/reload_loop/health_report_loop)
                # завершилась сама по себе, не по сигналу — это не должно
                # происходить в штатном режиме (supervised() перезапускает
                # воркеры при исключениях сам, а вложенные reload/health
                # циклы бесконечны, за исключением health_report_loop,
                # который может сам себя остановить через SystemExit при
                # неостановимой утечке потоков — см. scraper/health.py),
                # так что пробрасываем исключение, если оно там было,
                # вместо того чтобы тихо продолжать с одной мёртвой задачей.
                for t in done:
                    if t is not stop_waiter:
                        exc = t.exception()
                        if exc is not None:
                            log.error("Задача %s завершилась с ошибкой, останавливаю процесс", t.get_name())

            # Штатная отмена всего остального, что ещё работает — и в
            # случае сигнала, и в случае неожиданного завершения одной
            # из задач выше. gather(..., return_exceptions=True), чтобы
            # CancelledError отменённых задач не всплыл наружу и не
            # помешал остальным корректно доотмениться.
            for t in tasks:
                if not t.done():
                    t.cancel()
            await asyncio.gather(*tasks, return_exceptions=True)
    finally:
        # Гасим ТЕКУЩИЙ пул handle'а — если за время работы процесса
        # было несколько свопов, все более ранние пулы уже погашены
        # внутри ExecutorHandle.swap() в момент своего свопа.
        http_executor_handle.shutdown(wait=False, cancel_futures=True)

    log.info("Остановлено штатно")


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        # Подстраховка: на платформах без add_signal_handler (Windows)
        # SIGINT по-прежнему приходит сюда стандартным путём asyncio.
        log.info("Остановлено пользователем (KeyboardInterrupt)")
