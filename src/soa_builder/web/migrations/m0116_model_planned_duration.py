"""Add the intervention model and timeline planned duration columns.

USDM v4.0 InterventionalStudyDesign.model is a required Code (C99076
Intervention Model Response, CORE DDF00216), stored on ``soa`` because
the workbench has one study design per SoA.

ScheduleTimeline.plannedDuration is a Duration (DDF00153 expects one on
the main timeline). Its quantity value and unit term are stored on
``schedule_timelines`` so freeze and rollback carry them with the row.
"""

from ..db import _connect

SOA_COLUMNS = (
    ("intervention_model_code", "TEXT"),
    ("intervention_model_decode", "TEXT"),
    ("intervention_model_version", "TEXT"),
)

TIMELINE_COLUMNS = (
    ("planned_duration_text", "TEXT"),
    ("planned_duration_value", "REAL"),
    ("planned_duration_unit_code", "TEXT"),
    ("planned_duration_unit_decode", "TEXT"),
    ("planned_duration_unit_version", "TEXT"),
    ("planned_duration_will_vary", "INTEGER NOT NULL DEFAULT 0"),
    ("planned_duration_reason", "TEXT"),
)


def _add_columns(cur, table: str, columns) -> None:
    existing = {row[1] for row in cur.execute(f"PRAGMA table_info({table})")}
    for name, decl in columns:
        if name not in existing:
            cur.execute(f"ALTER TABLE {table} ADD COLUMN {name} {decl}")


def m0116_model_planned_duration() -> None:
    conn = _connect()
    try:
        cur = conn.cursor()
        _add_columns(cur, "soa", SOA_COLUMNS)
        _add_columns(cur, "schedule_timelines", TIMELINE_COLUMNS)
        conn.commit()
    finally:
        conn.close()
