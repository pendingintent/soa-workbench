from fastapi.testclient import TestClient

from soa_builder.web.app import app


client = TestClient(app)


def test_timing_audit_endpoint_has_rows():
    # Create study
    resp = client.post("/soa", json={"name": "TimingAuditCheck"})
    assert resp.status_code == 200
    soa_id = resp.json()["id"]

    # Create timing (records create audit)
    r1 = client.post(f"/soa/{soa_id}/timings", json={"name": "T1"})
    assert r1.status_code == 201
    tid = r1.json()["id"]

    # Update timing
    r2 = client.patch(
        f"/soa/{soa_id}/timings/{tid}",
        json={"name": "T1-upd"},
    )
    assert r2.status_code == 200

    # Delete timing
    r3 = client.delete(f"/soa/{soa_id}/timings/{tid}")
    assert r3.status_code == 200

    # Call timing audit listing endpoint
    audit = client.get(f"/soa/{soa_id}/timing_audit")
    assert audit.status_code == 200
    rows = audit.json()
    assert isinstance(rows, list)
    # Should contain at least 3 rows for create, update, delete
    assert len(rows) >= 3
    actions = [r.get("action") for r in rows]
    assert "create" in actions and "update" in actions and "delete" in actions
