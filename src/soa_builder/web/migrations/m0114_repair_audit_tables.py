"""Repair audit tables that some recorders could never write to.

- decision_instance_audit and condition_assignment_audit were never
  created by current code, so databases initialised from it silently
  dropped every decision-instance and condition-assignment audit row.
- schedule_timelines_audit was created with an ``instance_id`` column
  while _record_schedule_timeline_audit writes ``schedule_timeline_id``,
  so every timeline audit insert failed. No row can have been written
  through the misnamed column, so renaming it loses nothing.

Older databases already have the correct tables and are left as-is.
"""

from ..db import _connect

_MISSING_TABLES = (
    ("decision_instance_audit", "decision_instance_id"),
    ("condition_assignment_audit", "condition_assignment_id"),
)


def _columns(conn, table: str) -> set:
    return {r[1] for r in conn.execute(f"PRAGMA table_info({table})")}


def m0114_repair_audit_tables() -> None:
    conn = _connect()
    try:
        for table, id_col in _MISSING_TABLES:
            conn.execute(
                f"""CREATE TABLE IF NOT EXISTS {table} (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    soa_id INTEGER NOT NULL,
                    {id_col} INTEGER,
                    action TEXT NOT NULL,
                    before_json TEXT,
                    after_json TEXT,
                    performed_at TEXT NOT NULL
                )"""
            )
            conn.execute(
                f"CREATE INDEX IF NOT EXISTS idx_{table}_soa ON {table}(soa_id)"
            )
        cols = _columns(conn, "schedule_timelines_audit")
        if "instance_id" in cols and "schedule_timeline_id" not in cols:
            conn.execute(
                "ALTER TABLE schedule_timelines_audit"
                " RENAME COLUMN instance_id TO schedule_timeline_id"
            )
        conn.commit()
    finally:
        conn.close()
