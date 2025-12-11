from fastapi.testclient import TestClient

from soa_builder.web.app import app

client = TestClient(app)


def test_element_audit_endpoint_has_rows():
    # Create study
    resp = client.post("/soa", json={"name": "AuditCheck"})
    assert resp.status_code == 200
    soa_id = resp.json()["id"]

    # Create element via UI endpoint (records create audit)
    r1 = client.post(f"/ui/soa/{soa_id}/add_element", data={"name": "E1"})
    assert r1.status_code == 200

    # Update element
    # Update element: get element id directly from DB to avoid UI joins
    from soa_builder.web.app import _connect

    conn = _connect()
    cur = conn.cursor()
    cur.execute("SELECT id FROM element WHERE soa_id=? ORDER BY id LIMIT 1", (soa_id,))
    row = cur.fetchone()
    assert row is not None, "Element was not created"
    eid = int(row[0])
    conn.close()

    r2 = client.post(
        f"/ui/soa/{soa_id}/update_element",
        data={"element_id": eid, "name": "E1-upd"},
    )
    assert r2.status_code == 200

    # Delete element
    r3 = client.post(
        f"/ui/soa/{soa_id}/delete_element",
        data={"element_id": eid},
    )
    assert r3.status_code == 200

    # Call element audit listing endpoint
    audit = client.get(f"/soa/{soa_id}/element_audit")
    assert audit.status_code == 200
    rows = audit.json()
    assert isinstance(rows, list)
    # Should contain at least 3 rows for create, update, delete
    assert len(rows) >= 3
    actions = [r.get("action") for r in rows]
    assert "create" in actions and "update" in actions and "delete" in actions
