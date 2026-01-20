import os
import json
import sqlite3
from typing import List

from fastapi.testclient import TestClient

from soa_builder.web.app import app


client = TestClient(app)


def _db_path() -> str:
    """Get database path for tests.

    CRITICAL: This must only use the test database set by conftest.py.
    If SOA_BUILDER_DB is not set, tests are misconfigured.
    """
    db_path = os.environ.get("SOA_BUILDER_DB")
    if not db_path:
        raise RuntimeError(
            "SOA_BUILDER_DB environment variable not set - tests must use test database"
        )
    if "soa_builder_web.db" in db_path and "test" not in db_path:
        raise RuntimeError(
            f"DANGER: Test trying to use production database: {db_path}. "
            "Expected test database (soa_builder_web_tests.db)"
        )
    return db_path


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

    # API now returns a bare list of epochs, not {"epochs": [...]}
    # epochs = r_list.json()
    # API may return a bare list of epochs, or {"epochs": [...]}
    epochs_payload = r_list.json()
    if isinstance(epochs_payload, dict) and "epochs" in epochs_payload:
        epochs = epochs_payload["epochs"]
    else:
        epochs = epochs_payload

    old_order_ids = [e["id"] for e in epochs]
    old_order_names = [e["name"] for e in epochs]

    # New desired order (reverse)
    new_order_ids = list(reversed(old_order_ids))
    new_order_names = list(reversed(old_order_names))

    # /epochs/reorder expects {"order": [...]} in the JSON body
    r_reorder = client.post(
        f"/soa/{soa_id}/epochs/reorder",
        json={"order": new_order_ids},
    )
    assert r_reorder.status_code == 200

    # Fetch audits and validate structure
    audits = _fetch_epoch_audits(soa_id)
    reorder_audits = [a for a in audits if a["action"] == "reorder"]
    assert len(reorder_audits) == 1

    rec = reorder_audits[0]
    before = rec["before"]
    after = rec["after"]

    # Audit now records epoch NAMES, not IDs
    assert before["old_order"] == old_order_names
    assert after["new_order"] == new_order_names

    # "types" snapshot is still present in before
    assert "types" in before
    assert isinstance(before["types"], list)
