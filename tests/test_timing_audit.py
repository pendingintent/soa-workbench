from fastapi.testclient import TestClient

from soa_builder.web.app import app
from soa_builder.web.db import _connect


client = TestClient(app)


def test_timing_audit_rows_created():
    # Create SOA
    r = client.post("/soa", json={"name": "TimingAudit"})
    assert r.status_code == 200
    soa_id = r.json()["id"]

    # Create timing (should record create audit)
    r1 = client.post(f"/soa/{soa_id}/timings", json={"name": "T1"})
    assert r1.status_code == 201, r1.text
    tid = r1.json()["id"]

    # Update timing (should record update audit)
    r2 = client.patch(
        f"/soa/{soa_id}/timings/{tid}",
        json={"name": "T1-upd"},
    )
    assert r2.status_code == 200, r2.text

    # Delete timing (should record delete audit)
    r3 = client.delete(f"/soa/{soa_id}/timings/{tid}")
    assert r3.status_code == 200, r3.text

    # Verify timing_audit contains entries for create, update, delete
    conn = _connect()
    cur = conn.cursor()
    cur.execute(
        "SELECT action FROM timing_audit WHERE soa_id=? ORDER BY id",
        (soa_id,),
    )
    actions = [row[0] for row in cur.fetchall()]
    conn.close()

    assert "create" in actions
    assert "update" in actions
    assert "delete" in actions
