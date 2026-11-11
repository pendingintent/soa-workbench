"""Freeze rollback restores encounters and instances (issue #371)."""

import json

from fastapi.testclient import TestClient

from soa_builder.web.app import app
from soa_builder.web.db import _connect
from soa_builder.web.routers._freeze_helpers import _rollback_freeze

client = TestClient(app)


def _new_soa():
    resp = client.post("/soa", json={"name": "Rollback Enc/Inst"})
    return resp.json()["id"]


def _make_encounter(soa_id, name):
    resp = client.post(f"/soa/{soa_id}/visits", json={"name": name})
    assert resp.status_code in (200, 201), resp.text
    return resp.json().get("id")


def _make_instance(soa_id, name, encounter_uid=None):
    payload = {"name": name}
    if encounter_uid:
        payload["encounter_uid"] = encounter_uid
    resp = client.post(f"/soa/{soa_id}/instances", json=payload)
    assert resp.status_code in (200, 201), resp.text
    return resp.json()["id"]


def _freeze(soa_id):
    client.post(f"/ui/soa/{soa_id}/freeze", data={"version_label": "v1"})
    conn = _connect()
    row = conn.execute(
        "SELECT id FROM soa_freeze WHERE soa_id=? ORDER BY id DESC",
        (soa_id,),
    ).fetchone()
    conn.close()
    return row[0]


def _encounters(soa_id):
    conn = _connect()
    rows = conn.execute(
        "SELECT encounter_uid, name, label, type FROM visit WHERE soa_id=? ORDER BY id",
        (soa_id,),
    ).fetchall()
    conn.close()
    return rows


def _instances(soa_id):
    conn = _connect()
    rows = conn.execute(
        "SELECT instance_uid, name, encounter_uid, order_index, "
        "default_condition_uid, timeline_exit_id FROM instances "
        "WHERE soa_id=? ORDER BY id",
        (soa_id,),
    ).fetchall()
    conn.close()
    return rows


def _save_snapshot(soa_id, snapshot):
    conn = _connect()
    cur = conn.cursor()
    cur.execute(
        "INSERT INTO soa_freeze (soa_id, version_label, created_at, "
        "snapshot_json) VALUES (?,?,?,?)",
        (soa_id, "crafted", "2025-01-01T00:00:00", json.dumps(snapshot)),
    )
    freeze_id = cur.lastrowid
    conn.commit()
    conn.close()
    return freeze_id


def test_round_trip_restores_encounters_and_instances():
    soa_id = _new_soa()
    _make_encounter(soa_id, "Screening")
    _make_encounter(soa_id, "Week 1")
    conn = _connect()
    enc_uid = conn.execute(
        "SELECT encounter_uid FROM visit WHERE soa_id=? ORDER BY id",
        (soa_id,),
    ).fetchone()[0]
    conn.close()
    _make_instance(soa_id, "Inst A", enc_uid)
    _make_instance(soa_id, "Inst B")
    enc_before = _encounters(soa_id)
    inst_before = _instances(soa_id)
    freeze_id = _freeze(soa_id)

    conn = _connect()
    conn.execute(
        "UPDATE visit SET name='Renamed' WHERE soa_id=?",
        (soa_id,),
    )
    conn.execute(
        "DELETE FROM visit WHERE soa_id=? AND name='Renamed' "
        "AND id=(SELECT MIN(id) FROM visit WHERE soa_id=?)",
        (soa_id, soa_id),
    )
    conn.execute(
        "DELETE FROM instances WHERE soa_id=? AND name='Inst B'",
        (soa_id,),
    )
    conn.commit()
    conn.close()
    _make_instance(soa_id, "Post-freeze")

    stats = _rollback_freeze(soa_id, freeze_id)

    assert _encounters(soa_id) == enc_before
    assert _instances(soa_id) == inst_before
    assert stats["encounters_restored"] == 2
    assert stats["instances_restored"] == 2
    conn = _connect()
    junk = conn.execute(
        "SELECT COUNT(*) FROM visit WHERE soa_id=? AND encounter_uid IS NULL",
        (soa_id,),
    ).fetchone()[0]
    conn.close()
    assert junk == 0


def test_instance_axis_entries_are_never_inserted_into_visit():
    soa_id = _new_soa()
    _make_encounter(soa_id, "Only Encounter")
    enc_before = _encounters(soa_id)
    snapshot = {
        "soa_id": soa_id,
        "visits": [
            {
                "id": 1,
                "name": "Inst X",
                "order_index": 1,
                "instance_uid": "ScheduledActivityInstance_9",
            }
        ],
        "activities": [],
        "cells": [],
    }
    freeze_id = _save_snapshot(soa_id, snapshot)

    stats = _rollback_freeze(soa_id, freeze_id)

    assert _encounters(soa_id) == enc_before
    assert stats["encounters_untouched"] is True
    assert stats["visits_restored"] == 0


def test_snapshot_without_instances_leaves_live_instances():
    soa_id = _new_soa()
    _make_instance(soa_id, "Live")
    inst_before = _instances(soa_id)
    snapshot = {
        "soa_id": soa_id,
        "visits": [
            {
                "id": 1,
                "name": "x",
                "order_index": 1,
                "instance_uid": "ScheduledActivityInstance_77",
            }
        ],
        "activities": [],
        "cells": [],
    }
    freeze_id = _save_snapshot(soa_id, snapshot)

    stats = _rollback_freeze(soa_id, freeze_id)

    assert _instances(soa_id) == inst_before
    assert stats["instances_untouched"] is True


def test_old_instance_capture_restores_with_null_new_columns():
    soa_id = _new_soa()
    snapshot = {
        "soa_id": soa_id,
        "visits": [],
        "activities": [],
        "cells": [],
        "encounters_full": [
            {
                "id": 5,
                "name": "E",
                "label": None,
                "description": None,
                "order_index": 1,
                "encounter_uid": "Encounter_4",
                "type": None,
            }
        ],
        "instances": [
            {
                "instance_uid": "ScheduledActivityInstance_3",
                "name": "I",
                "label": None,
                "description": None,
                "epoch_uid": None,
                "timeline_id": None,
                "encounter_uid": "Encounter_4",
                "member_of_timeline": None,
            }
        ],
    }
    freeze_id = _save_snapshot(soa_id, snapshot)

    stats = _rollback_freeze(soa_id, freeze_id)

    assert stats["instances_restored"] == 1
    assert _instances(soa_id) == [
        (
            "ScheduledActivityInstance_3",
            "I",
            "Encounter_4",
            None,
            None,
            None,
        )
    ]
    assert [e[0] for e in _encounters(soa_id)] == ["Encounter_4"]


def test_restored_instances_bring_cells_back():
    soa_id = _new_soa()
    inst = _make_instance(soa_id, "Inst")
    act = client.post(f"/soa/{soa_id}/activities", json={"name": "Vitals"}).json()[
        "activity_id"
    ]
    client.post(
        f"/ui/soa/{soa_id}/toggle_cell",
        data={"instance_id": inst, "activity_id": act},
    )
    freeze_id = _freeze(soa_id)
    conn = _connect()
    conn.execute("DELETE FROM matrix_cells WHERE soa_id=?", (soa_id,))
    conn.execute("DELETE FROM instances WHERE soa_id=?", (soa_id,))
    conn.commit()
    conn.close()

    stats = _rollback_freeze(soa_id, freeze_id)

    assert stats["cells_restored"] == 1
    assert stats["cells_skipped"] == 0


def test_audit_and_preview_report_encounters_and_instances():
    from soa_builder.web.routers._freeze_helpers import (
        ROLLBACK_AUDIT_COLUMNS,
        _list_rollback_audit,
        _record_rollback_audit,
        _rollback_preview,
    )

    soa_id = _new_soa()
    _make_encounter(soa_id, "E")
    _make_instance(soa_id, "I")
    freeze_id = _freeze(soa_id)

    preview = _rollback_preview(soa_id, freeze_id)
    assert preview["encounters_to_restore"] == 1
    assert preview["instances_to_restore"] == 1

    _record_rollback_audit(
        soa_id,
        freeze_id,
        {"encounters_restored": 4, "instances_restored": 5},
    )
    row = _list_rollback_audit(soa_id)[0]
    assert row["encounters_restored"] == 4
    assert row["instances_restored"] == 5
    assert "encounters_restored" in ROLLBACK_AUDIT_COLUMNS
    assert "instances_restored" in ROLLBACK_AUDIT_COLUMNS
    html = client.get(f"/ui/soa/{soa_id}/rollback_audit").text
    assert "Encounters" in html and "Instances" in html


def test_old_audit_rows_render_dash_for_new_counts():
    from soa_builder.web.routers._freeze_helpers import _list_rollback_audit

    soa_id = _new_soa()
    conn = _connect()
    conn.execute(
        "INSERT INTO rollback_audit (soa_id, freeze_id, performed_at,"
        " visits_restored) VALUES (?,?,?,?)",
        (soa_id, 1, "2025-01-01T00:00:00", 2),
    )
    conn.commit()
    conn.close()
    row = _list_rollback_audit(soa_id)[0]
    assert row["encounters_restored"] is None
    html = client.get(f"/ui/soa/{soa_id}/rollback_audit").text
    assert "–" in html
