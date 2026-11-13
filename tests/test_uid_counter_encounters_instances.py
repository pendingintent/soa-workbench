"""Encounter and instance UIDs come from uid_counter (issue #371)."""

import json

from fastapi.testclient import TestClient

from soa_builder.web.app import app
from soa_builder.web.db import _connect
from soa_builder.web.routers._freeze_helpers import _rollback_freeze

client = TestClient(app)


def _new_soa():
    return client.post("/soa", json={"name": "UID Counter"}).json()["id"]


def _enc_uids(soa_id):
    conn = _connect()
    rows = conn.execute(
        "SELECT encounter_uid FROM visit WHERE soa_id=? ORDER BY id",
        (soa_id,),
    ).fetchall()
    conn.close()
    return [r[0] for r in rows]


def _inst_uids(soa_id):
    conn = _connect()
    rows = conn.execute(
        "SELECT instance_uid FROM instances WHERE soa_id=? ORDER BY id",
        (soa_id,),
    ).fetchall()
    conn.close()
    return [r[0] for r in rows]


def _add_encounter(soa_id, name):
    resp = client.post(f"/soa/{soa_id}/visits", json={"name": name})
    assert resp.status_code in (200, 201), resp.text


def _add_instance(soa_id, name):
    resp = client.post(f"/soa/{soa_id}/instances", json={"name": name})
    assert resp.status_code in (200, 201), resp.text


def _delete_all(soa_id, table):
    conn = _connect()
    conn.execute(f"DELETE FROM {table} WHERE soa_id=?", (soa_id,))
    conn.commit()
    conn.close()


def test_encounter_uid_never_reused_after_delete():
    soa_id = _new_soa()
    _add_encounter(soa_id, "A")
    _add_encounter(soa_id, "B")
    assert _enc_uids(soa_id) == ["Encounter_1", "Encounter_2"]
    _delete_all(soa_id, "visit")
    _add_encounter(soa_id, "C")
    assert _enc_uids(soa_id) == ["Encounter_3"]


def test_instance_uid_never_reused_after_delete():
    soa_id = _new_soa()
    _add_instance(soa_id, "A")
    _add_instance(soa_id, "B")
    assert _inst_uids(soa_id) == [
        "ScheduledActivityInstance_1",
        "ScheduledActivityInstance_2",
    ]
    _delete_all(soa_id, "instances")
    _add_instance(soa_id, "C")
    assert _inst_uids(soa_id) == ["ScheduledActivityInstance_3"]


def test_counter_seeds_from_existing_rows():
    soa_id = _new_soa()
    conn = _connect()
    conn.execute(
        "INSERT INTO instances (soa_id, instance_uid, name) VALUES (?,?,?)",
        (soa_id, "ScheduledActivityInstance_7", "pre-existing"),
    )
    conn.commit()
    conn.close()
    _add_instance(soa_id, "next")
    assert _inst_uids(soa_id)[-1] == "ScheduledActivityInstance_8"


def test_legacy_soa_scoped_uid_format_is_ignored_by_seed():
    soa_id = _new_soa()
    conn = _connect()
    conn.execute(
        "INSERT INTO instances (soa_id, instance_uid, name) VALUES (?,?,?)",
        (soa_id, f"ScheduledActivityInstance_{soa_id}_5", "legacy"),
    )
    conn.commit()
    conn.close()
    _add_instance(soa_id, "next")
    assert _inst_uids(soa_id)[-1] == "ScheduledActivityInstance_1"


def test_uid_after_rollback_is_above_prior_maxima():
    soa_id = _new_soa()
    _add_encounter(soa_id, "E1")
    _add_instance(soa_id, "I1")
    client.post(f"/ui/soa/{soa_id}/freeze", data={"version_label": "v1"})
    conn = _connect()
    freeze_id = conn.execute(
        "SELECT id FROM soa_freeze WHERE soa_id=?", (soa_id,)
    ).fetchone()[0]
    conn.close()
    # post-freeze entities that rollback will remove
    _add_encounter(soa_id, "E2")
    _add_instance(soa_id, "I2")
    # simulate a SoA whose counters were never seeded
    conn = _connect()
    conn.execute("DELETE FROM uid_counter WHERE soa_id=?", (soa_id,))
    conn.commit()
    conn.close()

    _rollback_freeze(soa_id, freeze_id)
    assert _enc_uids(soa_id) == ["Encounter_1"]
    _add_encounter(soa_id, "E3")
    _add_instance(soa_id, "I3")

    assert _enc_uids(soa_id)[-1] == "Encounter_3"
    assert _inst_uids(soa_id)[-1] == "ScheduledActivityInstance_3"


def test_bulk_created_instances_use_standard_uid_format():
    soa_id = _new_soa()
    from soa_builder.web.utils import get_next_instance_uid

    conn = _connect()
    cur = conn.cursor()
    first = get_next_instance_uid(cur, soa_id)
    second = get_next_instance_uid(cur, soa_id)
    conn.commit()
    conn.close()
    assert first == "ScheduledActivityInstance_1"
    assert second == "ScheduledActivityInstance_2"
    assert json.dumps(first)
