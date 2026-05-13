import time
from typing import Any, Awaitable, Callable, Dict, List, Optional

from ErisPulse import sdk
from ErisPulse.Core.Bases import BaseModule
from ErisPulse.loaders import ModuleLoadStrategy

from .scheduler import Scheduler, TriggerHandler
from .store import Store


class Main(BaseModule):
    def __init__(self):
        self.sdk = sdk
        self.logger = sdk.logger.get_child("Cron")
        self._store: Optional[Store] = None
        self._scheduler: Optional[Scheduler] = None

    @staticmethod
    def get_load_strategy():
        return ModuleLoadStrategy(
            lazy_load=False,
            priority=100,
        )

    def _ensure_initialized(self):
        if self._store is None or self._scheduler is None:
            raise RuntimeError("Cron module is not loaded yet")

    async def on_load(self, event):
        self._store = Store()
        self._scheduler = Scheduler(self._store)
        await self._scheduler.start()
        self.logger.info("Cron module loaded")

    async def on_unload(self, event):
        if self._scheduler:
            await self._scheduler.stop()
        self.logger.info("Cron module unloaded")

    # ==================== Public API ====================

    def once(self,
             delay: Optional[float] = None,
             trigger_at: Optional[float] = None,
             callback_data: Any = None,
             label: Optional[str] = None,
             source: Optional[str] = None,
             missed_policy: str = "fire_immediately") -> str:
        self._ensure_initialized()

        if delay is not None and trigger_at is not None:
            raise ValueError("Specify either 'delay' or 'trigger_at', not both")
        if delay is None and trigger_at is None:
            raise ValueError("Must specify either 'delay' or 'trigger_at'")

        next_run = self._scheduler.calc_first_next_run(
            "once", trigger_at=trigger_at, delay=delay
        )

        actual_trigger_at = trigger_at if trigger_at is not None else (time.time() + delay)

        return self._store.create_task(
            task_type="once",
            trigger_at=actual_trigger_at,
            callback_data=callback_data,
            label=label,
            source=source,
            next_run=next_run,
            missed_policy=missed_policy,
        )

    def interval(self,
                 interval_seconds: float,
                 callback_data: Any = None,
                 delay: Optional[float] = None,
                 max_runs: int = 0,
                 label: Optional[str] = None,
                 source: Optional[str] = None,
                 missed_policy: str = "fire_immediately") -> str:
        self._ensure_initialized()

        if interval_seconds <= 0:
            raise ValueError("interval_seconds must be > 0")

        next_run = self._scheduler.calc_first_next_run(
            "interval",
            interval_seconds=interval_seconds,
            delay=delay,
        )

        return self._store.create_task(
            task_type="interval",
            interval_seconds=interval_seconds,
            callback_data=callback_data,
            label=label,
            source=source,
            next_run=next_run,
            max_runs=max_runs,
            missed_policy=missed_policy,
        )

    def cron(self,
             expression: str,
             callback_data: Any = None,
             timezone: str = "Asia/Shanghai",
             max_runs: int = 0,
             label: Optional[str] = None,
             source: Optional[str] = None,
             missed_policy: str = "fire_immediately") -> str:
        self._ensure_initialized()

        next_run = self._scheduler.calc_first_next_run(
            "cron",
            cron_expr=expression,
            cron_timezone=timezone,
        )

        return self._store.create_task(
            task_type="cron",
            cron_expr=expression,
            cron_timezone=timezone,
            callback_data=callback_data,
            label=label,
            source=source,
            next_run=next_run,
            max_runs=max_runs,
            missed_policy=missed_policy,
        )

    def on_trigger(self, handler: TriggerHandler) -> TriggerHandler:
        self._ensure_initialized()
        self._scheduler.register_handler(handler)
        return handler

    def off_trigger(self, handler: TriggerHandler):
        self._ensure_initialized()
        self._scheduler.unregister_handler(handler)

    def cancel(self, task_id: str) -> bool:
        self._ensure_initialized()
        task = self._store.get_task(task_id)
        if task is None:
            return False
        if task.get("status") not in ("pending", "paused"):
            return False
        return self._store.cancel_task(task_id)

    def pause(self, task_id: str) -> bool:
        self._ensure_initialized()
        task = self._store.get_task(task_id)
        if task is None:
            return False
        if task.get("status") != "pending":
            return False
        return self._store.pause_task(task_id)

    def resume(self, task_id: str, *, reschedule: bool = False) -> bool:
        self._ensure_initialized()
        task = self._store.get_task(task_id)
        if task is None:
            return False
        if task.get("status") != "paused":
            return False

        if reschedule:
            now = time.time()
            next_run = self._scheduler._calc_next_run(task, now)
            if next_run is None:
                next_run = now
        else:
            next_run = task.get("next_run", time.time())

        return self._store.resume_task(task_id, next_run)

    def get_task(self, task_id: str) -> Optional[dict]:
        self._ensure_initialized()
        return self._store.get_task(task_id)

    def list_tasks(self,
                   source: Optional[str] = None,
                   status: Optional[str] = None,
                   task_type: Optional[str] = None) -> List[dict]:
        self._ensure_initialized()
        return self._store.list_tasks(source=source, status=status, task_type=task_type)

    async def trigger_now(self, task_id: str) -> Optional[dict]:
        self._ensure_initialized()
        return await self._scheduler.trigger_task_now(task_id)

    def delete_task(self, task_id: str) -> bool:
        self._ensure_initialized()
        return self._store.delete_task(task_id)

    def cleanup(self, max_age_seconds: float = 86400 * 7) -> int:
        self._ensure_initialized()
        return self._store.cleanup_completed(max_age_seconds)
