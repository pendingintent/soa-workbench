from fastapi.testclient import TestClient
from soa_builder.web.app import app, _connect

client = TestClient(app)

def create_study(name="Test Study"):
    resp = client.post("/soa", json={"name": name})
    assert resp.status_code == 200, resp.text
    return resp.json()["id"]

def check_crud_audit():
    soa_id = create_study()
    r = client.post(f"/soa/{soa_id}/elements", json={"name": "ElemA", "label": "A", "description": "Desc", "testrl": "start X", "teenrl": "end Y"})
    assert r.status_code == 200, r.text
    eid = r.json()["element_id"]
    r2 = client.patch(f"/soa/{soa_id}/elements/{eid}", json={"name": "ElemA2"})
    assert r2.status_code == 200, r2.text
    assert "name" in r2.json()["updated_fields"]
    r2b = client.patch(f"/soa/{soa_id}/elements/{eid}", json={"testrl": "start Z"})
    assert r2b.status_code == 200, r2b.text
    assert "testrl" in r2b.json()["updated_fields"]
    r3 = client.delete(f"/soa/{soa_id}/elements/{eid}")
    assert r3.status_code == 200, r3.text
    audit = client.get(f"/soa/{soa_id}/element_audit").json()["audit"]
    actions = {a["action"] for a in audit}
    assert actions == {"create", "update", "delete"}, actions


def check_freeze_elements():
    soa_id = create_study("Freeze Study")
    client.post(f"/soa/{soa_id}/elements", json={"name": "E1", "label": "L1", "description": "D1", "testrl": "R1"})
    client.post(f"/soa/{soa_id}/elements", json={"name": "E2", "label": "Label2", "description": "Desc2", "teenrl": "R2"})
    fr = client.post(f"/ui/soa/{soa_id}/freeze", data={"version_label": "vTest"})
    assert fr.status_code == 200, fr.text
    conn = _connect()
    cur = conn.cursor()
    cur.execute("SELECT id FROM soa_freeze WHERE soa_id=? ORDER BY id DESC", (soa_id,))
    fid = cur.fetchone()[0]
    conn.close()
    snap = client.get(f"/soa/{soa_id}/freeze/{fid}").json()
    assert "elements" in snap
    assert len(snap["elements"]) == 2
    assert all("etcd" not in e for e in snap["elements"])  # ensure legacy field gone
    assert any(e.get("testrl") == "R1" for e in snap["elements"]) and any(e.get("teenrl") == "R2" for e in snap["elements"])

def check_mandatory_enforcement():
    soa_id = create_study("Mandatory Study")
    # Missing label
    r = client.post(f"/soa/{soa_id}/elements", json={"name": "X", "description": "D"})
    assert r.status_code == 422, r.text
    # Blank description
    r2 = client.post(f"/soa/{soa_id}/elements", json={"name": "X", "label": "L", "description": "   "})
    assert r2.status_code == 400
    # Valid element with rule
    r3 = client.post(f"/soa/{soa_id}/elements", json={"name": "X", "label": "L", "description": "D", "testrl": "alpha"})
    assert r3.status_code == 200
    eid = r3.json()["element_id"]
    # attempt to clear mandatory label
    r4 = client.patch(f"/soa/{soa_id}/elements/{eid}", json={"label": "   "})
    assert r4.status_code == 400
    # blank rule allowed -> stored as null
    r5 = client.post(f"/soa/{soa_id}/elements", json={"name": "Y", "label": "L2", "description": "D2", "testrl": "   ", "teenrl": ""})
    assert r5.status_code == 200
    # too long rule rejected
    long_val = "x" * 201
    r6 = client.post(f"/soa/{soa_id}/elements", json={"name": "Z", "label": "L3", "description": "D3", "testrl": long_val})
    assert r6.status_code == 400

if __name__ == "__main__":
    check_crud_audit()
    check_freeze_elements()
    check_mandatory_enforcement()
    print("Element checks passed.")
