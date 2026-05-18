import asyncio
import io
import json
import os
import time
from contextlib import redirect_stderr, redirect_stdout
from typing import Any, Awaitable, Callable, Dict, List, Optional
from urllib.request import Request as UrlRequest, urlopen

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
        self._dashboard: Optional[object] = None
        self._builtin_handler: Optional[TriggerHandler] = None

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

        self._register_builtin_actions()

        from .dashboard import DashboardIntegration
        self._dashboard = DashboardIntegration(self)
        self._dashboard.setup()

        self.logger.info("Cron module loaded")

    async def on_unload(self, event):
        if self._dashboard:
            self._dashboard.teardown()
            self._dashboard = None
        if self._builtin_handler:
            self.off_trigger(self._builtin_handler)
            self._builtin_handler = None
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

    # ==================== Built-in Actions ====================

    def _register_builtin_actions(self):
        async def _builtin_handler(trigger_info: dict):
            cb = trigger_info.get("callback_data")
            if not isinstance(cb, dict):
                return
            action = cb.get("__cron_action")
            if not action:
                return

            task_id = trigger_info.get("task_id", "?")
            try:
                if action == "shell":
                    await self._action_shell(cb, task_id)
                elif action == "python":
                    await self._action_python(cb, task_id)
                elif action == "http":
                    await self._action_http(cb, task_id)
                elif action == "message":
                    await self._action_message(cb, task_id)
                else:
                    self.logger.warning(f"Unknown builtin action '{action}' for task {task_id}")
            except Exception as e:
                self.logger.error(f"Builtin action '{action}' failed for task {task_id}: {e}")

        self._builtin_handler = _builtin_handler
        self.on_trigger(_builtin_handler)

    async def _action_shell(self, data: dict, task_id: str):
        command = data.get("command", "")
        if not command:
            return
        try:
            proc = await asyncio.create_subprocess_shell(
                command,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE,
            )
            stdout, stderr = await asyncio.wait_for(proc.communicate(), timeout=300)
            self.logger.info(
                f"[shell] task={task_id[:8]} exit={proc.returncode} "
                f"stdout={stdout[:200].decode(errors='replace')}"
            )
            if proc.returncode != 0:
                self.logger.warning(
                    f"[shell] task={task_id[:8]} stderr={stderr[:200].decode(errors='replace')}"
                )
        except asyncio.TimeoutError:
            self.logger.error(f"[shell] task={task_id[:8]} timed out (300s)")
        except Exception as e:
            self.logger.error(f"[shell] task={task_id[:8]} error: {e}")

    async def _action_python(self, data: dict, task_id: str):
        code = data.get("code", "")
        if not code:
            return
        stdout_buf = io.StringIO()
        stderr_buf = io.StringIO()
        sandbox = {
            "__builtins__": __builtins__,
            "sdk": self.sdk,
            "asyncio": asyncio,
            "json": json,
            "os": os,
            "time": time,
        }
        try:
            with redirect_stdout(stdout_buf), redirect_stderr(stderr_buf):
                exec(code, sandbox)
            out = stdout_buf.getvalue()
            if out:
                self.logger.info(f"[python] task={task_id[:8]} output: {out[:300]}")
        except Exception as e:
            err = stderr_buf.getvalue()
            self.logger.error(f"[python] task={task_id[:8]} error: {e} {err[:200]}")

    async def _action_http(self, data: dict, task_id: str):
        url = data.get("url", "")
        method = data.get("method", "GET").upper()
        headers = data.get("headers", {})
        body = data.get("body")

        if not url:
            return

        try:
            body_bytes = None
            if body is not None:
                body_bytes = json.dumps(body).encode() if not isinstance(body, (bytes, str)) else (
                    body.encode() if isinstance(body, str) else body
                )

            req = UrlRequest(url, data=body_bytes, method=method)
            for k, v in headers.items():
                req.add_header(k, v)
            if body is not None and "Content-Type" not in headers:
                req.add_header("Content-Type", "application/json")

            loop = asyncio.get_event_loop()
            resp = await loop.run_in_executor(None, lambda: urlopen(req, timeout=30))
            status = resp.getcode()
            resp_data = resp.read(1024)
            self.logger.info(f"[http] task={task_id[:8]} {method} {url} -> {status}")
        except Exception as e:
            self.logger.error(f"[http] task={task_id[:8]} {method} {url} error: {e}")

    async def _action_message(self, data: dict, task_id: str):
        platform = data.get("platform", "")
        session_type = data.get("session_type", "user")
        target_id = data.get("target_id", "")
        message = data.get("message", "")

        if not platform or not target_id or not message:
            return

        try:
            adapter = self.sdk.adapter.get(platform)
            if adapter is None:
                self.logger.error(f"[message] task={task_id[:8]} adapter '{platform}' not found")
                return
            await adapter.Send.To(session_type, str(target_id)).Text(str(message))
            self.logger.info(
                f"[message] task={task_id[:8]} sent to {platform}/{session_type}/{target_id}"
            )
        except Exception as e:
            self.logger.error(f"[message] task={task_id[:8]} error: {e}")
