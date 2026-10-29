"""Freeze rollback restores instance-keyed matrix cells."""

import json

from fastapi.testclient import TestClient

from soa_builder.web.app import app
from soa_builder.web.db import _connect
from soa_builder.web.routers._freeze_helpers import _rollback_freeze

client = TestClient(app)


def _new_soa():
    return client.post("/soa", json={"name": "Rollback Cells"}).json()["id"]


def _make_instance(soa_id, uid):
    resp = client.post(
        f"/soa/{soa_id}/instances",
        json={"name": uid, "instance_uid": uid},
    )
    assert resp.status_code in (200, 201), resp.text
    return resp.json()["id"]


def _make_activity(soa_id, name):
    resp = client.post(f"/soa/{soa_id}/activities", json={"name": name})
    assert resp.status_code in (200, 201), resp.text
    return resp.json()["activity_id"]


def _toggle(soa_id, instance_id, activity_id):
    resp = client.post(
        f"/ui/soa/{soa_id}/toggle_cell",
        data={"instance_id": instance_id, "activity_id": activity_id},
    )
    assert resp.status_code == 200, resp.text


def _freeze(soa_id):
    client.post(f"/ui/soa/{soa_id}/freeze", data={"version_label": "v1"})
    conn = _connect()
    row = conn.execute(
        "SELECT id FROM soa_freeze WHERE soa_id=? ORDER BY id DESC",
        (soa_id,),
    ).fetchone()
    conn.close()
    return row[0]


def _cells(soa_id):
    conn = _connect()
    rows = conn.execute(
        "SELECT m.instance_id, m.visit_id, a.activity_uid, m.status "
        "FROM matrix_cells m LEFT JOIN activity a "
        "ON a.id = m.activity_id WHERE m.soa_id=?",
        (soa_id,),
    ).fetchall()
    conn.close()
    return rows


def test_rollback_restores_instance_cells_with_activity_uid():
    soa_id = _new_soa()
    inst = _make_instance(soa_id, "ScheduledActivityInstance_RB1")
    act = _make_activity(soa_id, "Vitals")
    conn = _connect()
    original_uid = conn.execute(
        "SELECT activity_uid FROM activity WHERE id=?", (act,)
    ).fetchone()[0]
    conn.close()
    _toggle(soa_id, inst, act)
    freeze_id = _freeze(soa_id)
    _toggle(soa_id, inst, act)  # clear the cell after the freeze
    assert _cells(soa_id) == []

    stats = _rollback_freeze(soa_id, freeze_id)

    assert stats["cells_restored"] == 1
    assert _cells(soa_id) == [(inst, None, original_uid, "X")]


def test_rollback_skips_cell_whose_instance_was_deleted():
    soa_id = _new_soa()
    keep = _make_instance(soa_id, "ScheduledActivityInstance_RB2")
    gone = _make_instance(soa_id, "ScheduledActivityInstance_RB3")
    act = _make_activity(soa_id, "ECG")
    _toggle(soa_id, keep, act)
    _toggle(soa_id, gone, act)
    freeze_id = _freeze(soa_id)
    conn = _connect()
    conn.execute("DELETE FROM matrix_cells WHERE soa_id=?", (soa_id,))
    conn.execute("DELETE FROM instances WHERE id=?", (gone,))
    conn.commit()
    conn.close()

    stats = _rollback_freeze(soa_id, freeze_id)

    assert stats["cells_restored"] == 1
    assert stats["cells_skipped_missing_instance"] == 1
    assert [c[0] for c in _cells(soa_id)] == [keep]


def test_rollback_of_legacy_visit_snapshot_still_restores_cells():
    soa_id = _new_soa()
    snapshot = {
        "soa_id": soa_id,
        "visits": [{"id": 1, "name": "V1", "order_index": 0}],
        "activities": [{"id": 1, "name": "A1", "order_index": 0}],
        "cells": [{"visit_id": 1, "activity_id": 1, "status": "X"}],
    }
    conn = _connect()
    cur = conn.cursor()
    cur.execute(
        "INSERT INTO soa_freeze (soa_id, version_label, created_at, "
        "snapshot_json) VALUES (?,?,?,?)",
        (soa_id, "legacy", "2025-01-01T00:00:00", json.dumps(snapshot)),
    )
    freeze_id = cur.lastrowid
    conn.commit()
    conn.close()

    stats = _rollback_freeze(soa_id, freeze_id)

    assert stats["cells_restored"] == 1
    cells = _cells(soa_id)
    assert len(cells) == 1
    assert cells[0][0] is None and cells[0][1] is not None


def _modal_html(soa_id):
    resp = client.get(f"/ui/soa/{soa_id}/rollback_audit")
    assert resp.status_code == 200, resp.text
    return resp.text


def test_rollback_audit_records_and_shows_skipped_cells():
    soa_id = _new_soa()
    keep = _make_instance(soa_id, "ScheduledActivityInstance_RB4")
    gone = _make_instance(soa_id, "ScheduledActivityInstance_RB5")
    act = _make_activity(soa_id, "Labs")
    _toggle(soa_id, keep, act)
    _toggle(soa_id, gone, act)
    freeze_id = _freeze(soa_id)
    conn = _connect()
    conn.execute("DELETE FROM instances WHERE id=?", (gone,))
    conn.commit()
    conn.close()

    client.post(f"/ui/soa/{soa_id}/freeze/{freeze_id}/rollback")

    audit = client.get(f"/soa/{soa_id}/rollback_audit").json()["audit"]
    assert audit[0]["cells_restored"] == 1
    assert audit[0]["cells_skipped"] == 1
    html = _modal_html(soa_id)
    assert "Skipped" in html


def test_rollback_counts_unmatched_legacy_cells_as_skipped():
    soa_id = _new_soa()
    snapshot = {
        "soa_id": soa_id,
        "visits": [{"id": 1, "name": "V1", "order_index": 0}],
        "activities": [{"id": 1, "name": "A1", "order_index": 0}],
        "cells": [
            {"visit_id": 1, "activity_id": 1, "status": "X"},
            {"visit_id": 99, "activity_id": 1, "status": "X"},
            {"visit_id": 1, "activity_id": 77, "status": "X"},
            {"visit_id": 1, "activity_id": 1, "status": ""},
        ],
    }
    conn = _connect()
    cur = conn.cursor()
    cur.execute(
        "INSERT INTO soa_freeze (soa_id, version_label, created_at, "
        "snapshot_json) VALUES (?,?,?,?)",
        (soa_id, "legacy2", "2025-01-01T00:00:00", json.dumps(snapshot)),
    )
    freeze_id = cur.lastrowid
    conn.commit()
    conn.close()

    stats = _rollback_freeze(soa_id, freeze_id)

    assert stats["cells_restored"] == 1
    # blank cells are not counted; two non-blank cells could not map
    assert stats["cells_skipped"] == 2


def test_rollback_restores_optional_activity_and_cell_fields():
    soa_id = _new_soa()
    inst = _make_instance(soa_id, "ScheduledActivityInstance_RB6")
    act = _make_activity(soa_id, "Dosing")
    conn = _connect()
    conn.execute(
        "UPDATE activity SET label=?, description=?, superscript=? WHERE id=?",
        ("Dose label", "Dose description", "b", act),
    )
    conn.commit()
    original_uid = conn.execute(
        "SELECT activity_uid FROM activity WHERE id=?", (act,)
    ).fetchone()[0]
    conn.close()
    _toggle(soa_id, inst, act)
    resp = client.post(
        f"/ui/soa/{soa_id}/cell_superscript/{inst}/{act}",
        data={"superscript": "a"},
    )
    assert resp.status_code == 200, resp.text
    freeze_id = _freeze(soa_id)
    conn = _connect()
    conn.execute(
        "UPDATE activity SET label=NULL, description=NULL, superscript=NULL WHERE id=?",
        (act,),
    )
    conn.execute("DELETE FROM matrix_cells WHERE soa_id=?", (soa_id,))
    conn.commit()
    conn.close()

    _rollback_freeze(soa_id, freeze_id)

    conn = _connect()
    activity = conn.execute(
        "SELECT activity_uid, label, description, superscript "
        "FROM activity WHERE soa_id=?",
        (soa_id,),
    ).fetchall()
    cell = conn.execute(
        "SELECT superscript FROM matrix_cells WHERE soa_id=?", (soa_id,)
    ).fetchall()
    conn.close()
    assert activity == [(original_uid, "Dose label", "Dose description", "b")]
    assert cell == [("a",)]


def test_cells_skipped_migration_adds_column_once(tmp_path, monkeypatch):
    import sqlite3

    from soa_builder.web import migrate_database

    path = str(tmp_path / "old_schema.db")
    conn = sqlite3.connect(path)
    conn.execute(
        "CREATE TABLE rollback_audit (id INTEGER PRIMARY KEY, "
        "soa_id INTEGER, cells_restored INTEGER)"
    )
    conn.commit()
    conn.close()
    monkeypatch.setattr(migrate_database, "_connect", lambda: sqlite3.connect(path))

    migrate_database._migrate_rollback_add_cells_skipped()
    migrate_database._migrate_rollback_add_cells_skipped()

    conn = sqlite3.connect(path)
    cols = [r[1] for r in conn.execute("PRAGMA table_info(rollback_audit)")]
    conn.close()
    assert cols.count("cells_skipped") == 1


def _legacy_freeze(soa_id, cells, label="legacy3"):
    snapshot = {
        "soa_id": soa_id,
        "visits": [{"id": 1, "name": "V1", "order_index": 0}],
        "activities": [{"id": 1, "name": "A1", "order_index": 0}],
        "cells": cells,
    }
    conn = _connect()
    cur = conn.cursor()
    cur.execute(
        "INSERT INTO soa_freeze (soa_id, version_label, created_at, "
        "snapshot_json) VALUES (?,?,?,?)",
        (soa_id, label, "2025-01-01T00:00:00", json.dumps(snapshot)),
    )
    freeze_id = cur.lastrowid
    conn.commit()
    conn.close()
    return freeze_id


def test_rollback_tolerates_null_cell_status():
    soa_id = _new_soa()
    freeze_id = _legacy_freeze(
        soa_id,
        [
            {"visit_id": 1, "activity_id": 1, "status": "X"},
            {"visit_id": 1, "activity_id": 1, "status": None},
        ],
    )

    stats = _rollback_freeze(soa_id, freeze_id)

    # a null status is blank: neither restored nor counted as skipped
    assert stats["cells_restored"] == 1
    assert stats["cells_skipped"] == 0


def test_rollback_preview_tolerates_null_cell_status():
    from soa_builder.web.routers._freeze_helpers import _rollback_preview

    soa_id = _new_soa()
    freeze_id = _legacy_freeze(
        soa_id,
        [
            {"visit_id": 1, "activity_id": 1, "status": "X"},
            {"visit_id": 1, "activity_id": 1, "status": None},
        ],
        label="legacy4",
    )

    assert _rollback_preview(soa_id, freeze_id)["cells_to_restore"] == 1


def test_skipped_count_covers_every_unrestorable_cell_kind():
    soa_id = _new_soa()
    freeze_id = _legacy_freeze(
        soa_id,
        [
            {"visit_id": 1, "activity_id": 1, "status": "X"},
            {"visit_id": 99, "activity_id": 1, "status": "X"},
            {"visit_id": 1, "activity_id": 77, "status": "X"},
            {"visit_id": 1, "activity_id": 1, "status": ""},
            {"visit_id": 1, "activity_id": 1, "status": None},
        ],
        label="legacy5",
    )

    stats = _rollback_freeze(soa_id, freeze_id)

    assert stats["cells_restored"] == 1
    assert stats["cells_skipped"] == 2


def test_audit_list_includes_elements_restored():
    from soa_builder.web.routers._freeze_helpers import (
        _list_rollback_audit,
        _record_rollback_audit,
    )

    soa_id = _new_soa()
    freeze_id = _legacy_freeze(soa_id, [], label="legacy6")
    _record_rollback_audit(
        soa_id,
        freeze_id,
        {"cells_restored": 2, "elements_restored": 3, "cells_skipped": 1},
    )

    row = _list_rollback_audit(soa_id)[0]

    assert row["elements_restored"] == 3
    assert "Elements" in _modal_html(soa_id)


def test_audit_columns_constant_matches_list_and_empty_export():
    import io

    import pandas as pd

    from soa_builder.web.routers._freeze_helpers import (
        ROLLBACK_AUDIT_COLUMNS,
        _list_rollback_audit,
        _record_rollback_audit,
    )

    soa_id = _new_soa()
    freeze_id = _legacy_freeze(soa_id, [], label="legacy7")
    resp = client.get(f"/soa/{soa_id}/rollback_audit/export/xlsx")
    empty = pd.read_excel(io.BytesIO(resp.content))
    assert list(empty.columns) == list(ROLLBACK_AUDIT_COLUMNS)

    _record_rollback_audit(soa_id, freeze_id, {"cells_restored": 1})
    assert list(_list_rollback_audit(soa_id)[0]) == list(ROLLBACK_AUDIT_COLUMNS)
