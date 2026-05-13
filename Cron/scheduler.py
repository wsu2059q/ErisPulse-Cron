import asyncio
import time
from datetime import datetime
from typing import Any, Awaitable, Callable, Dict, List, Optional

try:
    from zoneinfo import ZoneInfo
    _HAS_ZONEINFO = True
    try:
        ZoneInfo("Asia/Shanghai")
    except Exception:
        _HAS_ZONEINFO = False
except ImportError:
    _HAS_ZONEINFO = False

from croniter import croniter

from ErisPulse import sdk

from .store import Store

POLL_INTERVAL = 1.0

TriggerHandler = Callable[[Dict[str, Any]], Awaitable[None]]


def _make_cron_dt(timestamp: float, tz_name: str) -> datetime:
    if _HAS_ZONEINFO:
        try:
            tz = ZoneInfo(tz_name)
            return datetime.fromtimestamp(timestamp, tz=tz)
        except Exception:
            pass
    return datetime.fromtimestamp(timestamp)


class Scheduler:
    def __init__(self, store: Store):
        self.store = store
        self.logger = sdk.logger.get_child("Cron").scheduler
        self._running = False
        self._task: Optional[asyncio.Task] = None
        self._handlers: List[TriggerHandler] = []
        if not _HAS_ZONEINFO:
            self.logger.warning("zoneinfo 不可用，Cron 任务将使用系统本地时间")

    @property
    def is_running(self) -> bool:
        return self._running

    def register_handler(self, handler: TriggerHandler):
        if handler not in self._handlers:
            self._handlers.append(handler)
        self.logger.debug(f"Registered trigger handler: {getattr(handler, '__name__', repr(handler))}")

    def unregister_handler(self, handler: TriggerHandler):
        self._handlers = [h for h in self._handlers if h is not handler]

    async def start(self):
        if self._running:
            return
        self._running = True
        self._recover_tasks()
        self._task = asyncio.create_task(self._poll_loop())
        self.logger.info("Scheduler started")

    async def stop(self):
        self._running = False
        if self._task and not self._task.done():
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass
        self._task = None
        self.logger.info("Scheduler stopped")

    def _recover_tasks(self):
        tasks = self.store.get_all_active_tasks()
        now = time.time()
        recovered = 0

        for task in tasks:
            task_type = task.get("type")
            next_run = task.get("next_run", 0)

            if next_run <= now:
                policy = task.get("missed_policy", "fire_immediately")

                if policy == "skip":
                    new_next = self._calc_next_run(task, now)
                    if new_next is not None:
                        self.store.update_task(task["id"], next_run=new_next)
                    else:
                        self.store.mark_completed(task["id"])
                    continue
                elif policy == "reschedule":
                    new_next = self._calc_next_run(task, now)
                    if new_next is not None:
                        self.store.update_task(task["id"], next_run=new_next)
                    else:
                        self.store.mark_completed(task["id"])
                    continue
                else:
                    self.store.update_task(task["id"], next_run=now)

            recovered += 1

        self.logger.info(f"Recovered {recovered} active tasks ({len(tasks)} total loaded)")

    def _calc_next_run(self, task: dict, base_time: Optional[float] = None) -> Optional[float]:
        base_time = base_time or time.time()
        task_type = task.get("type")

        if task_type == "once":
            return None

        if task_type == "interval":
            interval = task.get("interval_seconds", 0)
            if interval <= 0:
                return None
            return base_time + interval

        if task_type == "cron":
            cron_expr = task.get("cron_expr")
            if not cron_expr:
                return None
            tz_name = task.get("cron_timezone", "Asia/Shanghai")
            try:
                dt = _make_cron_dt(base_time, tz_name)
                cron = croniter(cron_expr, dt)
                next_dt = cron.get_next(datetime)
                return next_dt.timestamp()
            except Exception as e:
                self.logger.error(f"Failed to calc next run for cron '{cron_expr}': {e}")
                return None

        return None

    def calc_first_next_run(self, task_type: str, *, trigger_at: Optional[float] = None,
                            interval_seconds: Optional[float] = None,
                            cron_expr: Optional[str] = None,
                            cron_timezone: str = "Asia/Shanghai",
                            delay: Optional[float] = None) -> float:
        now = time.time()

        if task_type == "once":
            if delay is not None and delay > 0:
                return now + delay
            if trigger_at is not None:
                return trigger_at
            return now

        if task_type == "interval":
            interval = interval_seconds or 0
            if interval <= 0:
                raise ValueError("interval_seconds must be > 0")
            if delay is not None and delay > 0:
                return now + delay
            return now + interval

        if task_type == "cron":
            if not cron_expr:
                raise ValueError("cron_expr is required for cron tasks")
            dt = _make_cron_dt(now, cron_timezone)
            cron = croniter(cron_expr, dt)
            next_dt = cron.get_next(datetime)
            return next_dt.timestamp()

        raise ValueError(f"Unknown task type: {task_type}")

    async def _poll_loop(self):
        try:
            while self._running:
                try:
                    await self._tick()
                except asyncio.CancelledError:
                    raise
                except Exception as e:
                    self.logger.error(f"Tick error: {e}")
                await asyncio.sleep(POLL_INTERVAL)
        except asyncio.CancelledError:
            pass

    async def _tick(self):
        pending = self.store.get_pending_tasks()
        if not pending:
            return

        now = time.time()
        for task in pending:
            if task.get("next_run", now + 1) > now:
                continue

            task_type = task.get("type")
            task_id = task.get("id")
            trigger_time = time.time()

            trigger_info = self._build_trigger_info(task, trigger_time)

            await self._fire_handlers(trigger_info)

            if task_type == "once":
                self.store.mark_completed(task_id)
            elif task_type == "interval":
                interval = task.get("interval_seconds", 0)
                next_run = trigger_time + interval
                self.store.increment_run(task_id, next_run=next_run)
            elif task_type == "cron":
                next_run = self._calc_next_run(task, trigger_time)
                if next_run is not None:
                    self.store.increment_run(task_id, next_run=next_run)
                else:
                    self.store.mark_completed(task_id)

    def _build_trigger_info(self, task: dict, trigger_time: float) -> dict:
        return {
            "task_id": task.get("id"),
            "task_type": task.get("type"),
            "callback_data": task.get("callback_data"),
            "label": task.get("label"),
            "source": task.get("source"),
            "run_count": task.get("run_count", 0) + 1,
            "max_runs": task.get("max_runs", 0),
            "created_at": task.get("created_at"),
            "last_run": task.get("last_run"),
            "trigger_time": trigger_time,
        }

    async def _fire_handlers(self, trigger_info: dict):
        for handler in self._handlers:
            try:
                await handler(trigger_info)
            except asyncio.CancelledError:
                raise
            except Exception as e:
                handler_name = getattr(handler, "__name__", repr(handler))
                self.logger.error(f"Handler '{handler_name}' error for task "
                                  f"{trigger_info.get('task_id')}: {e}")

    async def trigger_task_now(self, task_id: str) -> Optional[dict]:
        task = self.store.get_task(task_id)
        if task is None:
            return None
        if task.get("status") not in ("pending", "paused"):
            return None

        trigger_info = self._build_trigger_info(task, time.time())
        await self._fire_handlers(trigger_info)

        task_type = task.get("type")
        if task_type == "once":
            self.store.mark_completed(task_id)
        else:
            next_run = self._calc_next_run(task)
            if next_run is not None:
                self.store.increment_run(task_id, next_run=next_run)
            else:
                self.store.mark_completed(task_id)

        return trigger_info
