"""m0114 repairs audit tables on databases created by earlier code."""

import sqlite3

from soa_builder.web.migrations import m0114_repair_audit_tables as m0114


def _columns(conn, table):
    return [r[1] for r in conn.execute(f"PRAGMA table_info({table})")]


def test_repairs_database_from_buggy_init(tmp_path, monkeypatch):
    path = tmp_path / "old.db"
    conn = sqlite3.connect(path)
    conn.execute(
        "CREATE TABLE schedule_timelines_audit (id INTEGER PRIMARY KEY,"
        " soa_id INTEGER NOT NULL, instance_id INTEGER, action TEXT NOT NULL,"
        " before_json TEXT, after_json TEXT, performed_at TEXT NOT NULL)"
    )
    conn.commit()
    conn.close()
    monkeypatch.setattr(m0114, "_connect", lambda: sqlite3.connect(path))

    m0114.m0114_repair_audit_tables()
    m0114.m0114_repair_audit_tables()  # idempotent

    conn = sqlite3.connect(path)
    try:
        assert "schedule_timeline_id" in _columns(conn, "schedule_timelines_audit")
        assert "instance_id" not in _columns(conn, "schedule_timelines_audit")
        assert "decision_instance_id" in _columns(conn, "decision_instance_audit")
        assert "condition_assignment_id" in _columns(conn, "condition_assignment_audit")
    finally:
        conn.close()


def test_leaves_correct_database_alone(tmp_path, monkeypatch):
    path = tmp_path / "good.db"
    conn = sqlite3.connect(path)
    conn.execute(
        "CREATE TABLE schedule_timelines_audit (id INTEGER PRIMARY KEY,"
        " soa_id INTEGER NOT NULL, schedule_timeline_id INTEGER,"
        " action TEXT NOT NULL, before_json TEXT, after_json TEXT,"
        " performed_at TEXT NOT NULL)"
    )
    conn.execute(
        "INSERT INTO schedule_timelines_audit"
        " (soa_id, schedule_timeline_id, action, performed_at)"
        " VALUES (1, 5, 'create', 'now')"
    )
    conn.commit()
    conn.close()
    monkeypatch.setattr(m0114, "_connect", lambda: sqlite3.connect(path))

    m0114.m0114_repair_audit_tables()

    conn = sqlite3.connect(path)
    try:
        rows = conn.execute(
            "SELECT schedule_timeline_id FROM schedule_timelines_audit"
        ).fetchall()
    finally:
        conn.close()
    assert rows == [(5,)]
