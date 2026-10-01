"""
Распределение сабреддитов между аккаунтами и разбивка на "группы"
(multi-sub таргеты, т.е. отдельные `subs_joined` для запроса к
comments.json).

Цели:
  1. Каждый сабреддит опрашивается ровно ОДНИМ аккаунтом — кросс-аккаунтные
     дубли пропадают почти полностью (SeenCache остаётся только на случай
     внутрипроцессной гонки, не как основной механизм от дублей).
  2. "Горячие" сабы (маленький ранг = много трафика) не тонут в одном
     запросе вместе с кучей других — иначе комментарии активного саба
     будут доминировать в ответе Reddit (он отдаёт максимум `fetch_limit`
     штук по всем сабам группы сразу, отсортированных по времени), и менее
     активные соседи по группе будут регулярно вытесняться за пределы
     этого лимита. Поэтому хотовые сабы держим в маленьких группах
     (по 1-3 штуки), холодные — можно группировать по многу штук в одном
     запросе, там риска "задавить" соседей почти нет.
  3. Холодные сабы группируются достаточно крупно, чтобы один запрос не
     возвращал 3-5 комментариев впустую (тратя впустую rate-limit Reddit
     и токен из общего TokenBucket на почти нулевой выхлоп).

У нас нет реальных данных о трафике по сабам, только ранг (позиция в
top_subreddits.csv). Ранг используется как proxy: чем меньше ранг, тем
выше трафик, причём НЕ линейно — верхние сабы обычно на порядки активнее
нижних, поэтому вес считается как 1/sqrt(rank), а не 1/rank (иначе один
аккаунт получил бы почти весь трафик топ-10 и почти ничего больше)."""

import csv
import math
from pathlib import Path


def load_ranked_subreddits(csv_path: Path) -> list[tuple[int, str]]:
    """Возвращает [(rank, 'AskReddit'), ...] по возрастанию ранга
    (rank=1 — самый популярный/активный)."""
    out = []
    with open(csv_path, "r", encoding="utf-8") as f:
        for row in csv.DictReader(f):
            name = (row.get("subreddit") or "").strip()
            if name.lower().startswith("r/"):
                name = name[2:]
            if not name:
                continue
            out.append((int(row["rank"]), name))
    out.sort(key=lambda x: x[0])
    return out


def _weight(rank: int) -> float:
    return 1.0 / math.sqrt(max(rank, 1))


def assign_to_accounts(
    ranked: list[tuple[int, str]], n_accounts: int
) -> list[list[tuple[int, str]]]:
    """Жадное (LPT) распределение: сабы обрабатываются от самого
    активного к самому неактивному, каждый уходит аккаунту с наименьшей
    текущей суммарной "нагрузкой" (суммой весов уже назначенных ему
    сабов). Балансирует именно по ожидаемому трафику, а не просто по
    количеству сабов на аккаунт — иначе один аккаунт мог бы случайно
    получить непропорционально много топовых сабов."""
    if n_accounts <= 0:
        return []
    buckets: list[list[tuple[int, str]]] = [[] for _ in range(n_accounts)]
    loads = [0.0] * n_accounts
    for rank, name in ranked:
        idx = min(range(n_accounts), key=lambda i: loads[i])
        buckets[idx].append((rank, name))
        loads[idx] += _weight(rank)
    return buckets


def bucket_by_tier(
    account_subs: list[tuple[int, str]],
    hot_max_rank: int,
    medium_max_rank: int,
    hot_group_size: int,
    medium_group_size: int,
    cold_group_size: int,
) -> list[dict]:
    """Делит сабы одного аккаунта на тиры по рангу и чанкует каждый тир
    на группы заданного размера. Каждая группа — это будущий
    multi-sub-таргет (один `subs_joined` на цикл опроса).
    Возвращает [{"tier": "hot"|"medium"|"cold", "subs": [...]}, ...]."""
    hot = [n for r, n in account_subs if r <= hot_max_rank]
    medium = [n for r, n in account_subs if hot_max_rank < r <= medium_max_rank]
    cold = [n for r, n in account_subs if r > medium_max_rank]

    groups = []
    for tier, subs, size in (
        ("hot", hot, max(1, hot_group_size)),
        ("medium", medium, max(1, medium_group_size)),
        ("cold", cold, max(1, cold_group_size)),
    ):
        for i in range(0, len(subs), size):
            chunk = subs[i:i + size]
            if chunk:
                groups.append({"tier": tier, "subs": chunk})
    return groups


def build_account_groups_from_ranked(
    ranked: list[tuple[int, str]],
    account_names: list[str],
    hot_max_rank: int = 50,
    medium_max_rank: int = 300,
    hot_group_size: int = 2,
    medium_group_size: int = 8,
    cold_group_size: int = 40,
) -> dict[str, list[dict]]:
    per_account = assign_to_accounts(ranked, len(account_names))
    result = {}
    for name, subs in zip(account_names, per_account):
        result[name] = bucket_by_tier(
            subs, hot_max_rank, medium_max_rank,
            hot_group_size, medium_group_size, cold_group_size,
        )
    return result


def build_account_groups(
    csv_path: Path,
    account_names: list[str],
    **tier_kwargs,
) -> dict[str, list[dict]]:
    """Читает CSV с рангами и строит раскладку по аккаунтам. См.
    build_account_groups_from_ranked за параметрами тиров."""
    ranked = load_ranked_subreddits(csv_path)
    return build_account_groups_from_ranked(ranked, account_names, **tier_kwargs)
