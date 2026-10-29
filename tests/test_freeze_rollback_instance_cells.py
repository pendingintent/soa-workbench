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
