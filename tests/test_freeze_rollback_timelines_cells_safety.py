"""Rollback restores timelines, decision instances, study cells, and
takes a safety freeze first (issue #371 follow-up)."""

import json

from fastapi.testclient import TestClient

from soa_builder.web.app import app
from soa_builder.web.db import _connect
from soa_builder.web.routers._freeze_helpers import _rollback_freeze

client = TestClient(app)


def _new_soa():
    return client.post("/soa", json={"name": "Rollback TL"}).json()["id"]


def _exec(sql, params=()):
    conn = _connect()
    conn.execute(sql, params)
    conn.commit()
    conn.close()


def _rows(sql, params=()):
    conn = _connect()
    rows = conn.execute(sql, params).fetchall()
    conn.close()
    return rows


def _seed(soa_id):
    _exec(
        "INSERT INTO element (soa_id, element_id, name, order_index) VALUES (?,?,?,?)",
        (soa_id, "StudyElement_4", "Elem", 1),
    )
    _exec(
        "INSERT INTO study_cell (soa_id, study_cell_uid, arm_uid,"
        " epoch_uid, element_uid, order_index) VALUES (?,?,?,?,?,?)",
        (soa_id, "StudyCell_2", "StudyArm_1", "StudyEpoch_1", "StudyElement_4", 1),
    )
    _exec(
        "INSERT INTO schedule_timelines (soa_id, schedule_timeline_uid,"
        " name, main_timeline, order_index) VALUES (?,?,?,?,?)",
        (soa_id, "ScheduleTimeline_3", "Main", 1, 1),
    )
    _exec(
        "INSERT INTO decision_instances (soa_id, instance_uid, name,"
        " default_condition_uid, member_of_timeline, order_index)"
        " VALUES (?,?,?,?,?,?)",
        (
            soa_id,
            "ScheduledDecisionInstance_5",
            "Dec",
            "ScheduledActivityInstance_1",
            "ScheduleTimeline_3",
            1,
        ),
    )


def _state(soa_id):
    return {
        "elements": _rows(
            "SELECT element_id, name FROM element WHERE soa_id=? ORDER BY id",
            (soa_id,),
        ),
        "cells": _rows(
            "SELECT study_cell_uid, arm_uid, epoch_uid, element_uid,"
            " order_index FROM study_cell WHERE soa_id=? ORDER BY id",
            (soa_id,),
        ),
        "timelines": _rows(
            "SELECT schedule_timeline_uid, name, main_timeline,"
            " order_index FROM schedule_timelines WHERE soa_id=?"
            " ORDER BY id",
            (soa_id,),
        ),
        "decisions": _rows(
            "SELECT instance_uid, name, default_condition_uid,"
            " member_of_timeline, order_index FROM decision_instances"
            " WHERE soa_id=? ORDER BY id",
            (soa_id,),
        ),
    }


def _freeze(soa_id, label="v1"):
    client.post(f"/ui/soa/{soa_id}/freeze", data={"version_label": label})
    return _rows(
        "SELECT id FROM soa_freeze WHERE soa_id=? AND version_label=?",
        (soa_id, label),
    )[0][0]


def _save_snapshot(soa_id, snapshot):
    conn = _connect()
    cur = conn.cursor()
    cur.execute(
        "INSERT INTO soa_freeze (soa_id, version_label, created_at,"
        " snapshot_json) VALUES (?,?,?,?)",
        (soa_id, "crafted", "2025-01-01T00:00:00", json.dumps(snapshot)),
    )
    fid = cur.lastrowid
    conn.commit()
    conn.close()
    return fid


def test_round_trip_restores_timelines_decisions_study_cells():
    soa_id = _new_soa()
    _seed(soa_id)
    before = _state(soa_id)
    freeze_id = _freeze(soa_id)
    _exec(
        "UPDATE element SET name='X', element_id='StudyElement_9' WHERE soa_id=?",
        (soa_id,),
    )
    _exec("DELETE FROM study_cell WHERE soa_id=?", (soa_id,))
    _exec("UPDATE schedule_timelines SET name='Renamed' WHERE soa_id=?", (soa_id,))
    _exec("DELETE FROM decision_instances WHERE soa_id=?", (soa_id,))
    _exec(
        "INSERT INTO decision_instances (soa_id, instance_uid, name) VALUES (?,?,?)",
        (soa_id, "ScheduledDecisionInstance_6", "Post"),
    )

    stats = _rollback_freeze(soa_id, freeze_id)

    assert _state(soa_id) == before
    assert stats["timelines_restored"] == 1
    assert stats["decision_instances_restored"] == 1
    assert stats["study_cells_restored"] == 1


def test_old_snapshots_leave_live_rows_untouched():
    soa_id = _new_soa()
    _seed(soa_id)
    before = _state(soa_id)
    fid = _save_snapshot(
        soa_id,
        {"soa_id": soa_id, "visits": [], "activities": [], "cells": []},
    )

    stats = _rollback_freeze(soa_id, fid)

    assert _state(soa_id)["timelines"] == before["timelines"]
    assert _state(soa_id)["decisions"] == before["decisions"]
    assert _state(soa_id)["cells"] == before["cells"]
    assert stats["timelines_untouched"] is True
    assert stats["decision_instances_untouched"] is True
    assert stats["study_cells_untouched"] is True


def test_study_cells_without_element_uid_are_not_restored():
    soa_id = _new_soa()
    _seed(soa_id)
    before = _state(soa_id)["cells"]
    fid = _save_snapshot(
        soa_id,
        {
            "soa_id": soa_id,
            "visits": [],
            "activities": [],
            "cells": [],
            "study_cells": [
                {
                    "study_cell_uid": "StudyCell_1",
                    "arm_uid": "a",
                    "epoch_uid": "e",
                    "order_index": 1,
                }
            ],
        },
    )

    stats = _rollback_freeze(soa_id, fid)

    assert _state(soa_id)["cells"] == before
    assert stats["study_cells_untouched"] is True


def test_safety_freeze_is_taken_and_can_undo_the_rollback():
    soa_id = _new_soa()
    _seed(soa_id)
    freeze_id = _freeze(soa_id)
    _exec("UPDATE schedule_timelines SET name='After' WHERE soa_id=?", (soa_id,))
    after = _state(soa_id)

    stats = _rollback_freeze(soa_id, freeze_id)

    assert _state(soa_id)["timelines"][0][1] == "Main"
    safety_id = stats["safety_freeze_id"]
    assert safety_id and safety_id != freeze_id
    assert stats["safety_freeze_label"].startswith("pre-rollback")

    _rollback_freeze(soa_id, safety_id)

    assert _state(soa_id) == after


def test_safety_freeze_labels_are_unique_across_rollbacks():
    soa_id = _new_soa()
    _seed(soa_id)
    freeze_id = _freeze(soa_id)

    first = _rollback_freeze(soa_id, freeze_id)
    second = _rollback_freeze(soa_id, freeze_id)

    assert first["safety_freeze_label"] != second["safety_freeze_label"]


def test_safety_freeze_failure_aborts_rollback(monkeypatch):
    import soa_builder.web.routers._freeze_helpers as fh

    soa_id = _new_soa()
    _seed(soa_id)
    freeze_id = _freeze(soa_id)
    _exec("UPDATE schedule_timelines SET name='After' WHERE soa_id=?", (soa_id,))

    def boom(*args, **kwargs):
        raise RuntimeError("no freeze")

    monkeypatch.setattr(fh, "_create_freeze", boom)
    try:
        _rollback_freeze(soa_id, freeze_id)
    except RuntimeError:
        pass
    else:  # pragma: no cover
        raise AssertionError("rollback should abort")
    assert _state(soa_id)["timelines"][0][1] == "After"


def test_timeline_and_decision_uids_never_reused():
    soa_id = _new_soa()
    r = client.post(f"/soa/{soa_id}/schedule_timelines", json={"name": "T1"})
    assert r.status_code in (200, 201), r.text
    _exec("DELETE FROM schedule_timelines WHERE soa_id=?", (soa_id,))
    client.post(f"/soa/{soa_id}/schedule_timelines", json={"name": "T2"})
    assert _rows(
        "SELECT schedule_timeline_uid FROM schedule_timelines WHERE soa_id=?",
        (soa_id,),
    ) == [("ScheduleTimeline_2",)]

    r = client.post(f"/soa/{soa_id}/decision_instances", json={"name": "D1"})
    assert r.status_code in (200, 201), r.text
    _exec("DELETE FROM decision_instances WHERE soa_id=?", (soa_id,))
    client.post(f"/soa/{soa_id}/decision_instances", json={"name": "D2"})
    assert _rows(
        "SELECT instance_uid FROM decision_instances WHERE soa_id=?",
        (soa_id,),
    ) == [("ScheduledDecisionInstance_2",)]


def test_audit_and_preview_report_new_counts():
    from soa_builder.web.routers._freeze_helpers import (
        _list_rollback_audit,
        _record_rollback_audit,
        _rollback_preview,
    )

    soa_id = _new_soa()
    _seed(soa_id)
    freeze_id = _freeze(soa_id)
    preview = _rollback_preview(soa_id, freeze_id)
    assert preview["timelines_to_restore"] == 1
    assert preview["decision_instances_to_restore"] == 1
    assert preview["study_cells_to_restore"] == 1

    _record_rollback_audit(
        soa_id,
        freeze_id,
        {
            "timelines_restored": 2,
            "decision_instances_restored": 3,
            "study_cells_restored": 4,
            "safety_freeze_id": 99,
        },
    )
    row = _list_rollback_audit(soa_id)[0]
    assert row["timelines_restored"] == 2
    assert row["decision_instances_restored"] == 3
    assert row["study_cells_restored"] == 4
    assert row["safety_freeze_id"] == 99
    html = client.get(f"/ui/soa/{soa_id}/rollback_audit").text
    assert "Timelines" in html and "Safety" in html
