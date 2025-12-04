import os
import json
import sqlite3
from pathlib import Path
from unittest.mock import patch

from fastapi.testclient import TestClient

from soa_builder.web.app import app


client = TestClient(app)


def _db_path():
    return os.environ.get("SOA_BUILDER_DB", str(Path("soa_builder_web.db").absolute()))


def _fetch_epoch_audits(soa_id: int):
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


def _create_soa(name="AuditTest"):
    r = client.post("/soa", json={"name": name})
    assert r.status_code == 200
    return r.json()["id"]


def test_epoch_type_audit_create_update_delete_contains_type():
    soa_id = _create_soa()

    # Mock the epoch type map to resolve submission values to conceptIds
    fake_map = {"CID1": "TREATMENT", "CID2": "FOLLOW-UP"}
    # Also mock parent package href to avoid relying on network
    with (
        patch("soa_builder.web.utils.load_epoch_type_map", return_value=fake_map),
        patch(
            "soa_builder.web.utils.get_epoch_parent_package_href_cached",
            return_value="https://library.cdisc.org/api/mdr/ct/packages/sdtmct-2025-09-26",
        ),
    ):
        # Create epoch with type 'TREATMENT'
        r_add = client.post(
            f"/ui/soa/{soa_id}/add_epoch",
            data={
                "name": "Screening",
                "epoch_label": "SCR",
                "epoch_description": "Initial screening",
                "epoch_type_submission_value": "TREATMENT",
            },
        )
        assert r_add.status_code == 200

        audits = _fetch_epoch_audits(soa_id)
        assert any(a["action"] == "create" for a in audits)
        create_audit = next(a for a in audits if a["action"] == "create")
        assert create_audit["after"] is not None
        # Type should be present in after snapshot (code_uid value)
        assert "type" in create_audit["after"]
        created_type_uid = create_audit["after"]["type"]
        assert created_type_uid is None or str(created_type_uid).startswith("Code_")

        # Update epoch type to 'FOLLOW-UP' and ensure audit after contains type
        # Need the epoch_id for update; read from list epochs
        rl = client.get(f"/soa/{soa_id}/epochs")
        assert rl.status_code == 200
        epoch_id = rl.json()["epochs"][0]["id"]

        r_up = client.post(
            f"/ui/soa/{soa_id}/update_epoch",
            data={
                "epoch_id": epoch_id,
                "name": "Screening",
                "epoch_label": "SCR",
                "epoch_description": "Initial screening",
                "epoch_type_submission_value": "FOLLOW-UP",
            },
        )
        assert r_up.status_code == 200

        audits2 = _fetch_epoch_audits(soa_id)
        assert any(a["action"] == "update" for a in audits2)
        update_audit = [a for a in audits2 if a["action"] == "update"][-1]
        assert update_audit["after"] is not None
        assert "type" in update_audit["after"]
        updated_type_uid = update_audit["after"]["type"]
        # Should allow same or new; our handler creates new Code_N, so expect change
        if created_type_uid:
            assert updated_type_uid != created_type_uid

        # Delete epoch and ensure delete audit before has type
        r_del = client.post(
            f"/ui/soa/{soa_id}/delete_epoch",
            data={"epoch_id": epoch_id},
        )
        assert r_del.status_code == 200

        audits3 = _fetch_epoch_audits(soa_id)
        assert any(a["action"] == "delete" for a in audits3)
        delete_audit = [a for a in audits3 if a["action"] == "delete"][-1]
        assert delete_audit["before"] is not None
        assert "type" in delete_audit["before"]
        assert delete_audit["before"]["type"] is not None
