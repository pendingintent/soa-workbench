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
