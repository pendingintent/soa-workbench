"""activityIds on ScheduledActivityInstance come from instance cells."""

from fastapi.testclient import TestClient

from soa_builder.web.app import app
from soa_builder.web.db import _connect
from usdm.generate_scheduled_activity_instances import (
    build_usdm_instances,
)

SOA_ID = 987001
client = TestClient(app)


def _seed():
    client.get("/soa")  # ensure schema is initialized
    conn = _connect()
    cur = conn.cursor()
    for table in ("matrix_cells", "instances", "activity"):
        cur.execute(f"DELETE FROM {table} WHERE soa_id=?", (SOA_ID,))
    for num in (1, 2):
        cur.execute(
            "INSERT INTO activity (soa_id, name, activity_uid) VALUES (?,?,?)",
            (SOA_ID, f"A{num}", f"Activity_{num}"),
        )
    # two instances share one encounter
    for num in (1, 2):
        cur.execute(
            "INSERT INTO instances (soa_id, instance_uid, name, "
            "encounter_uid) VALUES (?,?,?,?)",
            (SOA_ID, f"ScheduledActivityInstance_{num}", f"I{num}", "Encounter_1"),
        )
    ids = {
        r[0]: r[1]
        for r in cur.execute(
            "SELECT instance_uid, id FROM instances WHERE soa_id=?",
            (SOA_ID,),
        )
    }
    act = {
        r[0]: r[1]
        for r in cur.execute(
            "SELECT activity_uid, id FROM activity WHERE soa_id=?",
            (SOA_ID,),
        )
    }
    cur.execute(
        "INSERT INTO matrix_cells (soa_id, instance_id, activity_id, "
        "status) VALUES (?,?,?,?)",
        (SOA_ID, ids["ScheduledActivityInstance_1"], act["Activity_1"], "X"),
    )
    conn.commit()
    conn.close()


def test_activity_ids_populated_from_instance_cells():
    _seed()
    out = {i["id"]: i for i in build_usdm_instances(SOA_ID)}
    first = out["ScheduledActivityInstance_1"]
    second = out["ScheduledActivityInstance_2"]
    assert first["activityIds"] == ["Activity_1"]
    assert second["activityIds"] == []


def _seed_legacy():
    """Legacy visit-only cells: one unique encounter, one shared."""
    client.get("/soa")
    conn = _connect()
    cur = conn.cursor()
    soa = SOA_ID + 1
    for table in ("matrix_cells", "instances", "activity", "visit"):
        cur.execute(f"DELETE FROM {table} WHERE soa_id=?", (soa,))
    for num in (1, 2, 3):
        cur.execute(
            "INSERT INTO activity (soa_id, name, activity_uid) VALUES (?,?,?)",
            (soa, f"A{num}", f"Activity_{num}"),
        )
    # Encounter_1 -> one instance; Encounter_2 -> two instances
    layout = [
        ("ScheduledActivityInstance_1", "Encounter_1"),
        ("ScheduledActivityInstance_2", "Encounter_2"),
        ("ScheduledActivityInstance_3", "Encounter_2"),
    ]
    for uid, enc in layout:
        cur.execute(
            "INSERT INTO instances (soa_id, instance_uid, name, "
            "encounter_uid) VALUES (?,?,?,?)",
            (soa, uid, uid, enc),
        )
    visit_ids = {}
    for enc in ("Encounter_1", "Encounter_2"):
        cur.execute(
            "INSERT INTO visit (soa_id, name, encounter_uid) VALUES (?,?,?)",
            (soa, enc, enc),
        )
        visit_ids[enc] = cur.lastrowid
    act = {
        r[0]: r[1]
        for r in cur.execute(
            "SELECT activity_uid, id FROM activity WHERE soa_id=?",
            (soa,),
        )
    }
    for enc, act_uid in (
        ("Encounter_1", "Activity_1"),
        ("Encounter_2", "Activity_2"),
    ):
        cur.execute(
            "INSERT INTO matrix_cells (soa_id, visit_id, activity_id, "
            "status) VALUES (?,?,?,?)",
            (soa, visit_ids[enc], act[act_uid], "X"),
        )
    conn.commit()
    conn.close()
    return soa


def test_legacy_visit_cell_resolving_to_one_instance_is_kept():
    soa = _seed_legacy()
    out = {i["id"]: i for i in build_usdm_instances(soa)}
    assert out["ScheduledActivityInstance_1"]["activityIds"] == ["Activity_1"]


def test_legacy_visit_cell_on_shared_encounter_is_excluded():
    soa = _seed_legacy()
    out = {i["id"]: i for i in build_usdm_instances(soa)}
    assert out["ScheduledActivityInstance_2"]["activityIds"] == []
    assert out["ScheduledActivityInstance_3"]["activityIds"] == []
