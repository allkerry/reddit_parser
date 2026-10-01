import asyncio
import time
from concurrent.futures import ThreadPoolExecutor
from typing import Callable


# ---------------------------------------------------------------- #
#  Пересоздаваемый ThreadPoolExecutor
# ---------------------------------------------------------------- #

class ExecutorHandle:
    """Держит ТЕКУЩИЙ ThreadPoolExecutor и умеет пересоздавать его на
    лету (см. health_report_loop ниже). Это нужно, потому что
    asyncio.wait_for() в http_client.py не может принудительно
    прервать физический поток curl_cffi (Python не умеет убивать
    потоки снаружи, см. ARCHITECTURE.md §6): если прокси/DNS зависают
    навсегда, поток остаётся "занятым" executor'ом бесконечно, и за
    недели непрерывной работы фиксированный пул постепенно
    исчерпывается. Раз почистить старый пул нельзя — единственный
    рабочий вариант "вылечиться" без рестарта всего процесса — списать
    его целиком и продолжить работу с новым.

    Воркеры (account_worker -> fetch_comments -> _fetch_comments_page)
    получают этот handle, а НЕ голый ThreadPoolExecutor, и на каждый
    отдельный HTTP-запрос резолвят .current заново — иначе воркер,
    который не падал и не перезапускался месяцами (a значит и не
    перезахватывал аргументы функции), держал бы ссылку на уже списанный
    executor до своего собственного следующего рестарта через
    supervised(). Присваивание .current — атомарная операция обычного
    Python-атрибута; читается/пишется только из корутин, живущих в
    event loop потоке, где между чтением и записью нет await, так что
    гонок при переключении на новый пул нет и без явного Lock."""

    def __init__(self, factory: Callable[[], ThreadPoolExecutor], max_workers: int):
        self._factory = factory
        self.max_workers = max_workers
        self.current = factory()
        self.swaps = 0

    def swap(self) -> ThreadPoolExecutor:
        """Создаёт новый пул, переключает .current на него и гасит
        старый. Возвращает старый пул (на случай, если вызывающему коду
        зачем-то нужна ссылка на него — сейчас не используется)."""
        old = self.current
        self.current = self._factory()
        self.swaps += 1
        # cancel_futures=True отменяет только ещё не стартовавшие задачи
        # старого пула. Уже зависшие потоки этим не прерываются (Python
        # не умеет) — они просто перестают быть чьей-либо заботой и
        # остаются висеть как ОС-потоки до (если) естественного
        # завершения, никак больше не влияя на работу процесса.
        old.shutdown(wait=False, cancel_futures=True)
        return old

    def shutdown(self, **kwargs):
        self.current.shutdown(**kwargs)


# ---------------------------------------------------------------- #
#  Здоровье executor'а: занятость + учёт свопов/утечек
# ---------------------------------------------------------------- #

class ExecutorHealth:
    """Отслеживает реальную занятость ТЕКУЩЕГО пула (сколько потоков
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
        # Суммарно потоков, списанных вместе со старыми пулами при
        # свопах (считались "занятыми" на момент свопа) — метрика для
        # диагностики "утекает ли постоянно", а не входной параметр для
        # решения о следующем свопе (для этого используется active +
        # cooldown, см. health_report_loop).
        self.leaked_total = 0
        # time.monotonic() последнего свопа, 0.0 — свопов ещё не было.
        self.last_swap_at = 0.0

    @property
    def active(self) -> int:
        return self.submitted - self.completed

    def on_submit(self):
        self.submitted += 1

    def on_future_done(self, _future=None):
        self.completed += 1

    def on_wait_timeout(self):
        self.timed_out_waits += 1

    def register_swap(self, leaked_now: int):
        """Вызывается сразу после ExecutorHandle.swap(). Обнуляет
        submitted/completed под НОВЫЙ пул — иначе active тут же снова
        показал бы условные 90%+, унаследованные от futures старого,
        уже списанного пула, и следующая проверка health_report_loop
        сочла бы это поводом свопать снова, несмотря на cooldown только
        что вычисленный по времени, а не по метрике."""
        self.leaked_total += leaked_now
        self.submitted = 0
        self.completed = 0
        self.last_swap_at = time.monotonic()

    def snapshot(self, max_workers: int | None = None) -> str:
        parts = [
            f"active={self.active}",
            f"submitted={self.submitted}",
            f"completed={self.completed}",
            f"timed_out_waits={self.timed_out_waits}",
            f"leaked_total={self.leaked_total}",
            f"swaps={self.swaps if hasattr(self, 'swaps') else 0}",
        ]
        if max_workers is not None:
            parts.append(f"pool_size={max_workers}")
        return " ".join(parts)


async def health_report_loop(
    health: ExecutorHealth,
    handle: ExecutorHandle,
    interval_seconds: float,
    log,
    swap_threshold: float = 0.9,
    swap_cooldown_seconds: float = 300.0,
    fatal_leak_multiplier: float = 10.0,
):
    """Раз в interval_seconds логирует занятость http_executor.

    Если active приближается к max_workers (>= swap_threshold доли) —
    это сигнал, что пул постепенно забивается зависшими потоками
    (мёртвые прокси/DNS). Раз почистить сам пул нельзя (см.
    ExecutorHandle) — пул целиком пересоздаётся через handle.swap().

    Два предохранителя, без которых своп сам по себе стал бы новой
    формой той же болезни:

    - swap_cooldown_seconds — не свопать чаще, чем раз в этот интервал.
      Без него: если конкретная VPN-нода дохнет навсегда (а не временно
      подвиснет), active у НОВОГО пула тоже быстро дойдёт до порога
      (те же воркеры почти сразу же уйдут в те же зависания на той же
      мёртвой ноде), и без cooldown процесс начал бы штамповать новые
      ThreadPoolExecutor на каждой итерации этого цикла, плодя реальные
      ОС-потоки без остановки.
    - fatal_leak_multiplier — если суммарная утечка (leaked_total)
      выросла настолько, что свопы явно не успевают за темпом утечки
      (утечка не самоограничивается, а не единичный всплеск) —
      дальнейшие свопы не помогают, и разумнее честно уронить процесс
      (SystemExit), дав docker-compose (`restart: unless-stopped`)
      перезапустить его чисто, чем тихо копить ОС-потоки неделями.
      Регулярные "http_executor пересоздан" в логах — сигнал разобраться
      с конкретной VPN-нодой/аккаунтом руками, а не просто "само
      вылечится"."""
    while True:
        await asyncio.sleep(interval_seconds)
        active = health.active
        max_workers = handle.max_workers

        if not max_workers or active < max_workers * swap_threshold:
            log.info("[health] http_executor: %s", health.snapshot(max_workers))
            continue

        since_last_swap = (
            time.monotonic() - health.last_swap_at
            if health.last_swap_at
            else swap_cooldown_seconds  # свопов ещё не было — cooldown не блокирует первый
        )
        if since_last_swap < swap_cooldown_seconds:
            log.warning(
                "[health] http_executor почти исчерпан (%s) ⚠, но с последнего свопа прошло "
                "только %.0fs из %.0fs cooldown — жду, чтобы не штамповать пулы подряд",
                health.snapshot(max_workers), since_last_swap, swap_cooldown_seconds,
            )
            continue

        leaked_now = active
        handle.swap()
        health.register_swap(leaked_now)
        log.error(
            "[health] http_executor пересоздан (своп #%d): %d поток(ов) старого пула считались "
            "занятыми (вероятно, зависли навсегда на мёртвых прокси/DNS) — новый пул чист. "
            "Суммарно утекло потоков за время жизни процесса: %d. Если это повторяется регулярно — "
            "разберись с конкретной VPN-нодой/аккаунтом (замени/выключи), не полагайся на авто-своп бесконечно.",
            handle.swaps, leaked_now, health.leaked_total,
        )

        if health.leaked_total >= max_workers * fatal_leak_multiplier:
            log.critical(
                "[health] утечка потоков не самоограничивается (leaked_total=%d >= "
                "%.0fx pool_size=%d) — своп больше не помогает, останавливаю процесс, "
                "чтобы docker перезапустил его чисто (restart: unless-stopped в docker-compose.yml)",
                health.leaked_total, fatal_leak_multiplier, max_workers,
            )
            raise SystemExit(1)
