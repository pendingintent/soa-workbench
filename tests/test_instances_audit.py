from fastapi.testclient import TestClient

from soa_builder.web.app import app
from soa_builder.web.db import _connect

client = TestClient(app)


def _ensure_soa_clean(soa_id: int) -> int:
    conn = _connect()
    cur = conn.cursor()
    # Ensure SOA exists
    cur.execute(
        "INSERT OR IGNORE INTO soa (id, name) VALUES (?, ?)",
        (soa_id, f"Test SOA {soa_id}"),
    )
    # Clean related tables
    cur.execute("DELETE FROM instances WHERE soa_id=?", (soa_id,))
    cur.execute("DELETE FROM instance_audit WHERE soa_id=?", (soa_id,))
    conn.commit()
    conn.close()
    return soa_id


def _audit_rows(soa_id: int):
    conn = _connect()
    cur = conn.cursor()
    cur.execute(
        "SELECT action, before_json, after_json FROM instance_audit WHERE soa_id=? ORDER BY id",
        (soa_id,),
    )
    rows = cur.fetchall() or []
    conn.close()
    return rows


def _list_instances(soa_id: int):
    r = client.get(f"/soa/{soa_id}/instances")
    assert r.status_code == 200
    return r.json()


def test_instance_create_update_delete_audit_flow():
    soa_id = _ensure_soa_clean(18001)

    # CREATE
    r = client.post(
        f"/soa/{soa_id}/instances",
        json={
            "name": "Inst A",
            "label": None,
            "description": None,
            "default_condition_uid": None,
            "epoch_uid": None,
            "timeline_id": None,
            "timeline_exit_id": None,
        },
    )
    assert r.status_code == 201, r.text
    created = r.json()
    instance_id = created["id"]

    insts = _list_instances(soa_id)
    assert any(i["id"] == instance_id for i in insts)

    rows = _audit_rows(soa_id)
    assert len(rows) == 1
    action, before_json, after_json = rows[0]
    assert action == "create"
    assert before_json is None
    assert after_json is not None and "Inst A" in after_json

    # UPDATE
    r2 = client.patch(
        f"/soa/{soa_id}/instances/{instance_id}",
        json={
            "name": "Inst A+",
            "label": "L1",
            "description": None,
            "default_condition_uid": None,
            "epoch_uid": None,
            "timeline_id": None,
            "timeline_exit_id": None,
        },
    )
    assert r2.status_code == 200, r2.text
    updated = r2.json()
    assert updated["name"] == "Inst A+"
    assert updated["label"] == "L1"

    rows2 = _audit_rows(soa_id)
    assert len(rows2) == 2
    action2, before_json2, after_json2 = rows2[-1]
    assert action2 == "update"
    assert before_json2 is not None
    assert after_json2 is not None and "updated_fields" in after_json2

    # DELETE
    r3 = client.delete(f"/soa/{soa_id}/instances/{instance_id}")
    assert r3.status_code == 200
    rows3 = _audit_rows(soa_id)
    assert len(rows3) == 3
    action3, before_json3, after_json3 = rows3[-1]
    assert action3 == "delete"
    assert before_json3 is not None
    assert after_json3 is None
