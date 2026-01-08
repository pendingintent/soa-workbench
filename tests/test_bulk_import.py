from fastapi.testclient import TestClient

from soa_builder.web.app import app

client = TestClient(app)


def reset_db():
    # Disabled: preserve persistent DB across tests
    return


def test_bulk_activities_endpoint():
    reset_db()
    r = client.post("/soa", json={"name": "Bulk Trial"})
    soa_id = r.json()["id"]
    payload = {"names": ["Hematology", "Chemistry", "ECG", "Hematology", " ", "MRI"]}
    resp = client.post(f"/soa/{soa_id}/activities/bulk", json=payload)
    assert resp.status_code == 200
    data = resp.json()
    assert data["added"] == 4  # Hematology counted once, blank skipped
    assert "Hematology" in data["details"]["added"]
    # Ensure matrix shows 4 activities
    m = client.get(f"/soa/{soa_id}/matrix").json()
    assert len(m["activities"]) == 4


def test_matrix_import_endpoint():
    reset_db()
    r = client.post("/soa", json={"name": "Matrix Trial"})
    soa_id = r.json()["id"]
    payload = {
        "instances": [
            {"name": "SCREEN1", "label": "Screening 1 instance"},
            {"name": "SCREEN2"},
            {"name": "BASELINE"},
        ],
        "activities": [
            {"name": "Hematology", "statuses": ["X", "X", "O"]},
            {"name": "Chemistry", "statuses": ["", "X", ""]},
            {"name": "ECG", "statuses": ["O", "", "O"]},
        ],
        "reset": True,
    }
    resp = client.post(f"/soa/{soa_id}/matrix/import", json=payload)
    assert resp.status_code == 200, resp.text
    data = resp.json()
    assert data["instances_added"] == 3
    assert data["activities_added"] == 3
    # matrix_cells: Hematology (3 non-empty), Chemistry (1), ECG (2) => 6
    assert data["cells_inserted"] == 6
    # verify matrix fetch
    m = client.get(f"/soa/{soa_id}/matrix").json()
    assert len(m["instances"]) == 3
    assert len(m["activities"]) == 3
    # Ensure a specific matrix_cell present (C1D15, Hematology -> O)
    # Need to map instance/activity names to ids then check matrix_cell list
    instance_map = {i["name"]: i["id"] for i in m["instances"]}
    activity_map = {a["name"]: a["id"] for a in m["activities"]}
    target_matrix_cells = [
        c
        for c in m["cells"]
        if c["instance_id"] == instance_map["BASELINE"]
        and c["activity_id"] == activity_map["Hematology"]
    ]
    assert target_matrix_cells and target_matrix_cells[0]["status"] == "O"
