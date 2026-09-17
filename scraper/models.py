# ---------------------------------------------------------------- #
#  Один аккаунт = один воркер
# ---------------------------------------------------------------- #

class FetchResult:
    __slots__ = ("comments", "status", "retry_after", "error_kind", "after",
                 "pages_fetched", "ratelimit")

    def __init__(self, comments=None, status=None, retry_after=None, error_kind=None,
                 after=None, pages_fetched=1, ratelimit=None):
        self.comments = comments or []
        self.status = status
        self.retry_after = retry_after
        # error_kind: None | "rate_or_server" | "auth" | "network"
        self.error_kind = error_kind
        # Курсор пагинации Reddit ("after" из ответа листинга, либо
        # fullname последнего элемента как fallback) — None, если больше
        # страниц нет / страница пустая.
        self.after = after
        # Сколько страниц реально было выкачано за этот fetch_comments()
        # (используется в account_worker для adjusted_interval). По
        # умолчанию 1 — для одиночного вызова _fetch_comments_page().
        self.pages_fetched = pages_fetched
        # dict {"remaining": float, "reset": float, "used": float|None} —
        # разобранные X-Ratelimit-* заголовки последнего запроса, либо
        # None, если сервер их не прислал. См. parse_ratelimit_headers().
        self.ratelimit = ratelimit
