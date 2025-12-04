import os
import json
import sqlite3
from typing import List

from fastapi.testclient import TestClient

from soa_builder.web.app import app


client = TestClient(app)


def _db_path() -> str:
    return os.environ.get("SOA_BUILDER_DB", "soa_builder_web.db")


def _fetch_epoch_audits(soa_id: int) -> List[dict]:
    conn = sqlite3.connect(_db_path())
    cur = conn.cursor()
    cur.execute(
        "SELECT action, before_json, after_json FROM epoch_audit WHERE soa_id=? ORDER BY id",
        (soa_id,),
    )
    rows = cur.fetchall()
    conn.close()
    return [
        {
            "action": r[0],
            "before": json.loads(r[1]) if r[1] else None,
            "after": json.loads(r[2]) if r[2] else None,
        }
        for r in rows
    ]


def _create_soa(name="EpochReorderAuditAPI") -> int:
    r = client.post("/soa", json={"name": name})
    assert r.status_code == 200
    return r.json()["id"]


def test_epoch_reorder_audit_api_structure():
    soa_id = _create_soa()
    # Create three epochs via JSON API router
    for idx, nm in enumerate(["E1", "E2", "E3"], start=1):
        r_add = client.post(
            f"/soa/{soa_id}/epochs",
            json={
                "name": nm,
                "epoch_label": f"L{idx}",
                "epoch_description": f"D{idx}",
            },
        )
        assert r_add.status_code == 200

    # List epochs to get current order
    r_list = client.get(f"/soa/{soa_id}/epochs")
    assert r_list.status_code == 200
    epochs = r_list.json()["epochs"]
    assert len(epochs) == 3
    old_order = [e["id"] for e in epochs]

    # New order: reverse
    new_order = list(reversed(old_order))
    r_reorder = client.post(f"/soa/{soa_id}/epochs/reorder", json=new_order)
    assert r_reorder.status_code == 200

    audits = _fetch_epoch_audits(soa_id)
    # Find the last reorder audit
    reorder_audits = [a for a in audits if a["action"] == "reorder"]
    assert len(reorder_audits) >= 1
    last = reorder_audits[-1]
    assert last["before"] is not None and last["after"] is not None
    # Validate before.old_order and before.types exist and types is a list of {id,type}
    assert "old_order" in last["before"]
    assert last["before"]["old_order"] == old_order
    assert "types" in last["before"]
    assert isinstance(last["before"]["types"], list)
    if last["before"]["types"]:
        sample = last["before"]["types"][0]
        assert set(sample.keys()) == {"id", "type"}
    # Validate after.new_order equals our new_order
    assert "new_order" in last["after"]
    assert last["after"]["new_order"] == new_order
