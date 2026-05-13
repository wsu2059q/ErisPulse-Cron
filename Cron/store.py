import json
import time
import uuid
from typing import Any, Dict, List, Optional

from ErisPulse import sdk

TABLE_NAME = "cron_tasks"

COLUMNS = {
    "id": "TEXT PRIMARY KEY",
    "type": "TEXT NOT NULL",
    "status": "TEXT NOT NULL DEFAULT 'pending'",
    "trigger_at": "REAL",
    "interval_seconds": "REAL",
    "cron_expr": "TEXT",
    "cron_timezone": "TEXT DEFAULT 'Asia/Shanghai'",
    "callback_data": "TEXT",
    "label": "TEXT",
    "source": "TEXT",
    "created_at": "REAL NOT NULL",
    "last_run": "REAL",
    "next_run": "REAL NOT NULL",
    "run_count": "INTEGER DEFAULT 0",
    "max_runs": "INTEGER DEFAULT 0",
    "missed_policy": "TEXT DEFAULT 'fire_immediately'",
}

COLUMN_NAMES = list(COLUMNS.keys())

VALID_TYPES = {"once", "interval", "cron"}
VALID_STATUSES = {"pending", "paused", "completed", "cancelled"}
VALID_MISSED_POLICIES = {"fire_immediately", "skip", "reschedule"}


class Store:
    def __init__(self):
        self._ensure_table()

    def _ensure_table(self):
        sdk.storage.CreateTable(TABLE_NAME, COLUMNS)

    @staticmethod
    def _row_to_dict(row) -> Optional[dict]:
        if row is None:
            return None
        if isinstance(row, dict):
            result = dict(row)
        elif isinstance(row, (tuple, list)):
            result = dict(zip(COLUMN_NAMES, row))
        else:
            return None
        if "callback_data" in result and isinstance(result["callback_data"], str):
            try:
                result["callback_data"] = json.loads(result["callback_data"])
            except (json.JSONDecodeError, TypeError):
                pass
        return result

    @staticmethod
    def _rows_to_dicts(rows) -> List[dict]:
        return [Store._row_to_dict(r) for r in rows]

    @staticmethod
    def _rows_to_single(rows, col_index: int = 0) -> list:
        results = []
        for row in rows:
            if isinstance(row, (tuple, list)):
                results.append(row[col_index])
            elif isinstance(row, dict):
                results.append(list(row.values())[col_index])
        return results

    def create_task(self, *, task_type: str, trigger_at: Optional[float] = None,
                    interval_seconds: Optional[float] = None,
                    cron_expr: Optional[str] = None,
                    cron_timezone: str = "Asia/Shanghai",
                    callback_data: Any = None,
                    label: Optional[str] = None,
                    source: Optional[str] = None,
                    next_run: float,
                    max_runs: int = 0,
                    missed_policy: str = "fire_immediately") -> str:
        task_id = uuid.uuid4().hex
        now = time.time()

        if task_type not in VALID_TYPES:
            raise ValueError(f"Invalid task type: {task_type}, must be one of {VALID_TYPES}")
        if missed_policy not in VALID_MISSED_POLICIES:
            raise ValueError(f"Invalid missed_policy: {missed_policy}, must be one of {VALID_MISSED_POLICIES}")

        row = {
            "id": task_id,
            "type": task_type,
            "status": "pending",
            "trigger_at": trigger_at,
            "interval_seconds": interval_seconds,
            "cron_expr": cron_expr,
            "cron_timezone": cron_timezone,
            "callback_data": json.dumps(callback_data, ensure_ascii=False) if callback_data is not None else "null",
            "label": label,
            "source": source,
            "created_at": now,
            "last_run": None,
            "next_run": next_run,
            "run_count": 0,
            "max_runs": max_runs,
            "missed_policy": missed_policy,
        }

        sdk.storage.Table(TABLE_NAME).Insert(row).Execute()
        return task_id

    def get_task(self, task_id: str) -> Optional[dict]:
        row = sdk.storage.Table(TABLE_NAME).Select("*").Where("id = ?", task_id).ExecuteOne()
        return self._row_to_dict(row)

    def list_tasks(self, source: Optional[str] = None,
                   status: Optional[str] = None,
                   task_type: Optional[str] = None) -> List[dict]:
        query = sdk.storage.Table(TABLE_NAME).Select("*")
        conditions = []
        params = []

        if source is not None:
            conditions.append("source = ?")
            params.append(source)
        if status is not None:
            conditions.append("status = ?")
            params.append(status)
        if task_type is not None:
            conditions.append("type = ?")
            params.append(task_type)

        if conditions:
            where_clause = " AND ".join(conditions)
            query = query.Where(where_clause, *params)

        rows = query.OrderBy("next_run").Execute()
        return self._rows_to_dicts(rows)

    def get_pending_tasks(self) -> List[dict]:
        now = time.time()
        rows = (sdk.storage.Table(TABLE_NAME)
                .Select("*")
                .Where("status = ? AND next_run <= ?", "pending", now)
                .OrderBy("next_run")
                .Execute())
        return self._rows_to_dicts(rows)

    def get_all_active_tasks(self) -> List[dict]:
        rows = (sdk.storage.Table(TABLE_NAME)
                .Select("*")
                .Where("status = ?", "pending")
                .OrderBy("next_run")
                .Execute())
        return self._rows_to_dicts(rows)

    def update_task(self, task_id: str, **fields) -> bool:
        if not fields:
            return False
        allowed = {"status", "next_run", "last_run", "run_count", "trigger_at",
                   "interval_seconds", "cron_expr", "cron_timezone", "label",
                   "callback_data", "max_runs", "missed_policy", "source"}
        updates = {}
        for k, v in fields.items():
            if k in allowed:
                if k == "callback_data" and v is not None:
                    v = json.dumps(v, ensure_ascii=False)
                updates[k] = v

        if not updates:
            return False

        sdk.storage.Table(TABLE_NAME).Update(updates).Where("id = ?", task_id).Execute()
        return True

    def mark_completed(self, task_id: str):
        now = time.time()
        self.update_task(task_id, status="completed", last_run=now)

    def increment_run(self, task_id: str, next_run: Optional[float] = None):
        task = self.get_task(task_id)
        if task is None:
            return
        now = time.time()
        new_count = task.get("run_count", 0) + 1
        updates = {
            "run_count": new_count,
            "last_run": now,
        }
        if next_run is not None:
            updates["next_run"] = next_run

        max_runs = task.get("max_runs", 0)
        if max_runs > 0 and new_count >= max_runs:
            updates["status"] = "completed"

        self.update_task(task_id, **updates)

    def delete_task(self, task_id: str) -> bool:
        sdk.storage.Table(TABLE_NAME).Delete().Where("id = ?", task_id).Execute()
        return True

    def cancel_task(self, task_id: str) -> bool:
        return self.update_task(task_id, status="cancelled")

    def pause_task(self, task_id: str) -> bool:
        return self.update_task(task_id, status="paused")

    def resume_task(self, task_id: str, next_run: float) -> bool:
        return self.update_task(task_id, status="pending", next_run=next_run)

    def cleanup_completed(self, max_age_seconds: float = 86400 * 7) -> int:
        cutoff = time.time() - max_age_seconds
        rows = (sdk.storage.Table(TABLE_NAME)
                .Select("id")
                .Where("status IN (?, ?) AND created_at < ?", "completed", "cancelled", cutoff)
                .Execute())
        ids = self._rows_to_single(rows, 0)
        count = 0
        for task_id in ids:
            self.delete_task(task_id)
            count += 1
        return count
