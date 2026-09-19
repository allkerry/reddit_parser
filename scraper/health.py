import asyncio


class ExecutorHealth:
    """Отслеживает реальную занятость http_executor (сколько потоков
    сейчас выполняют curl_cffi-вызов) — независимо от того, дождался ли
    их asyncio.wait_for() или отвалился по таймауту.

    on_submit()/on_future_done() вызываются только из корутин, живущих в
    event loop потоке (никогда — из worker-потока executor'а). Между
    `+= 1`/`-= 1` нет await, а event loop однопоточен, поэтому лишний
    Lock не нужен: конкурентных гонок быть не может, даже когда несколько
    account_worker-корутин чередуются на одном loop'е.

    on_future_done вешается как done_callback на Future от
    run_in_executor, а НЕ выполняется после `await wait_for(...)` —
    таймаут wait_for отменяет только ожидание в asyncio, физический
    поток при этом продолжает работать до конца (Python не умеет
    прерывать потоки снаружи, см. ARCHITECTURE.md §6). Если считать
    "занято" только пока мы явно ждём результат — зависший поток
    искусственно "освобождался" бы в статистике в момент таймаута."""

    def __init__(self):
        self.submitted = 0
        self.completed = 0
        self.timed_out_waits = 0

    @property
    def active(self) -> int:
        return self.submitted - self.completed

    def on_submit(self):
        self.submitted += 1

    def on_future_done(self, _future=None):
        self.completed += 1

    def on_wait_timeout(self):
        self.timed_out_waits += 1

    def snapshot(self, max_workers: int | None = None) -> str:
        parts = [
            f"active={self.active}",
            f"submitted={self.submitted}",
            f"completed={self.completed}",
            f"timed_out_waits={self.timed_out_waits}",
        ]
        if max_workers is not None:
            parts.append(f"pool_size={max_workers}")
        return " ".join(parts)


async def health_report_loop(health: ExecutorHealth, interval_seconds: float, max_workers: int, log):
    """Раз в interval_seconds логирует занятость http_executor. Если
    active приближается к max_workers — это сигнал, что пул постепенно
    забивается зависшими потоками (мёртвые прокси/DNS), и через
    какое-то время новым run_in_executor будет некуда встать — воркеры
    начнут тихо зависать на await без единой ошибки в логе."""
    while True:
        await asyncio.sleep(interval_seconds)
        active = health.active
        if max_workers and active >= max_workers * 0.8:
            log.warning(
                "[health] http_executor: %s ⚠ пул почти исчерпан — похоже, "
                "есть зависшие потоки curl_cffi",
                health.snapshot(max_workers),
            )
        else:
            log.info("[health] http_executor: %s", health.snapshot(max_workers))
