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

Остальное — общий TokenBucket/SeenCache/ThreadPoolExecutor/backoff — как
раньше, см. заголовок предыдущей версии этого файла в git-истории и
ARCHITECTURE.md.
"""

import asyncio
from concurrent.futures import ThreadPoolExecutor

import aiohttp

from scraper.config import ConfigStore, load_accounts
from scraper.constants import BASE_DIR, CONFIG_PATH, CONFIG_RELOAD_SECONDS, PROXY_HOST, log
from scraper.grouping import build_account_groups, build_account_groups_from_ranked
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

    http_executor = ThreadPoolExecutor(
        max_workers=max(32, len(accounts) * 2),
        thread_name_prefix="reddit-fetch",
    )

    try:
        store_connector = aiohttp.TCPConnector(ttl_dns_cache=300, keepalive_timeout=60)
        async with aiohttp.ClientSession(connector=store_connector) as store_session:
            # reload_loop() тоже обёрнут в supervised(), как и account_worker
            # ниже: без этого необработанное исключение внутри цикла
            # ре-конфига (мимо внутреннего try/except вокруг load_once())
            # пробрасывалось бы прямо в asyncio.gather(), который роняет
            # ВСЕ задачи разом — все воркеры аккаунтов гасли бы из-за
            # одной локальной ошибки в перечитывании config.yaml, а
            # finally ниже ещё и рвал бы исполнение через
            # http_executor.shutdown(cancel_futures=True) прямо под
            # работающими запросами к Reddit. supervised() ловит
            # исключение, логирует и перезапускает только эту задачу —
            # остальные воркеры продолжают работать на последнем
            # успешно загруженном конфиге.
            tasks = [
                asyncio.create_task(
                    supervised(config.reload_loop, name="config_reload")
                )
            ]
            for i, account in enumerate(accounts):
                phase_offset = i * (config.get("poll_interval_seconds", 3) / len(accounts))
                tasks.append(
                    asyncio.create_task(
                        supervised(
                            account_worker,
                            account, config, bucket, seen, phase_offset, store_session, http_executor,
                            groups_by_account.get(account["name"], []),
                            name=account["name"]
                        )
                    )
                )
            await asyncio.gather(*tasks)
    finally:
        http_executor.shutdown(wait=False, cancel_futures=True)


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        log.info("Остановлено пользователем")
