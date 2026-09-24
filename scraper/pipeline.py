import asyncio
import json
import time

import aiohttp

from .constants import SERVER_HARD_BATCH_LIMIT, log
from .utils import iso_utc


def build_payload(comment_data: dict) -> dict | None:
    comment_id = comment_data.get("id")
    if not comment_id:
        return None

    fullname = comment_data.get("name") or f"t1_{comment_id}"
    created_utc = comment_data.get("created_utc")
    if created_utc is None:
        return None

    body = comment_data.get("body", "") or ""
    author = comment_data.get("author", "") or ""
    permalink = comment_data.get("permalink", "")
    url = f"https://www.reddit.com{permalink}" if permalink else ""

    parent_id = comment_data.get("parent_id", "") or ""
    external_parent_id = parent_id if parent_id.startswith("t1_") else ""

    # communityName в формате "r/<sub>", как в примере с дашборда
    # (Exorde ожидает summary.communityName именно с префиксом "r/").
    subreddit = comment_data.get("subreddit") or ""
    community_name = f"r/{subreddit}" if subreddit else ""

    # summary — компактная JSON-строка с метаданными поста/комментария.
    # upvote_ratio и num_comments у Reddit есть только у постов (t3_),
    # не у комментариев (t1_) — для комментария оставляем их пустыми,
    # а не подставляем выдуманные значения.
    summary = json.dumps({
        "score": str(comment_data.get("score", "")),
        "upvote_ratio": "",
        "num_comments": "",
        "communityName": community_name,
        "data_type": "comment",
    })

    return {
        "content": body,
        "external_id": fullname,
        "created_at": iso_utc(created_utc),
        "domain": "reddit.com",
        "url": url,
        "title": "",
        "author": author,
        "username": author,
        "external_parent_id": external_parent_id,
        "summary": summary,
        "_age_seconds": time.time() - created_utc,
    }


def _item_ok(entry) -> bool:
    """Разбирает один элемент results[] из ответа /store_items.
    Формат отдельного результата коллектором явно не специфицирован,
    поэтому распознаём несколько разумных вариантов и по умолчанию
    считаем успехом, если явного признака ошибки нет."""
    if isinstance(entry, bool):
        return entry
    if isinstance(entry, dict):
        for key in ("ok", "success", "stored", "saved"):
            if key in entry:
                return bool(entry[key])
        if "error" in entry and entry["error"]:
            return False
        return True
    # неизвестный тип — не считаем это ошибкой, чтобы не заспамить логи
    return True


async def send_batch_to_store(
    session: aiohttp.ClientSession,
    endpoint: str,
    items: list[dict],
    account_name: str,
    batch_max_items: int,
) -> list[bool]:
    """Отправляет items одним (или несколькими, если батч больше лимита)
    POST-запросом(и) на /store_items. Возвращает список bool — по одному
    на исходный items[i], в том же порядке. False для элемента означает
    "не подтверждён как сохранённый" — вызывающий код обязан вызвать
    seen.release() для таких элементов, а не считать их отправленными."""
    if not items:
        return []

    chunk_size = max(1, min(batch_max_items, SERVER_HARD_BATCH_LIMIT))
    results: list[bool] = []

    for start in range(0, len(items), chunk_size):
        chunk = items[start:start + chunk_size]
        body = [{k: v for k, v in p.items() if not k.startswith("_")} for p in chunk]

        try:
            async with session.post(
                endpoint, json=body, timeout=aiohttp.ClientTimeout(total=15)
            ) as resp:
                if resp.status == 413:
                    text = await resp.text()
                    log.error(
                        "[%s] store_items ответил 413 (батч %d элементов слишком большой) — "
                        "уменьши batch_max_items в config.yaml: %s",
                        account_name, len(chunk), text[:200],
                    )
                    results.extend([False] * len(chunk))
                    continue

                if resp.status >= 300:
                    text = await resp.text()
                    log.warning(
                        "[%s] store_items ответил %s для батча из %d: %s",
                        account_name, resp.status, len(chunk), text[:200],
                    )
                    results.extend([False] * len(chunk))
                    continue

                try:
                    data = await resp.json(content_type=None)
                except (json.JSONDecodeError, ValueError) as e:
                    # 2xx, но тело не распарсилось как JSON (например,
                    # прокси/балансировщик перед store_endpoint подменил
                    # тело ответа). Раз сервер не подтвердил сохранение
                    # поэлементно — считаем весь чанк неподтверждённым,
                    # а не роняем воркер необработанным исключением: он
                    # уйдёт по обычному пути seen.release() и повторной
                    # отправки в следующем цикле опроса.
                    log.warning(
                        "[%s] store_items вернул %s, но тело не распарсилось как JSON: %s",
                        account_name, resp.status, e,
                    )
                    results.extend([False] * len(chunk))
                    continue
                item_results = data.get("results") if isinstance(data, dict) else None

                if isinstance(item_results, list) and len(item_results) == len(chunk):
                    results.extend(_item_ok(r) for r in item_results)
                else:
                    # Сервер принял запрос, но не вернул results поэлементно —
                    # считаем весь чанк успешным по факту получения 2xx.
                    received = data.get("received") if isinstance(data, dict) else None
                    if received is not None and received != len(chunk):
                        log.warning(
                            "[%s] store_items: received=%s, но отправлено %d — часть могла не сохраниться",
                            account_name, received, len(chunk),
                        )
                    results.extend([True] * len(chunk))

        except aiohttp.ClientError as e:
            log.warning("[%s] Ошибка отправки батча в store_items (%s): %s", account_name, endpoint, e)
            results.extend([False] * len(chunk))
        except asyncio.TimeoutError:
            log.warning("[%s] Таймаут отправки батча в store_items (%s)", account_name, endpoint)
            results.extend([False] * len(chunk))

    return results
