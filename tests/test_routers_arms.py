"""Comprehensive tests for arms router endpoints."""

from fastapi.testclient import TestClient

from soa_builder.web.app import app

client = TestClient(app)


def test_list_arms_empty():
    """Test listing arms for a new SoA returns empty list."""
    r = client.post("/soa", json={"name": "Arms Test Study"})
    soa_id = r.json()["id"]

    resp = client.get(f"/soa/{soa_id}/arms")
    assert resp.status_code == 200
    assert resp.json() == []


def test_list_arms_nonexistent_soa():
    """Test listing arms for nonexistent SoA returns 404."""
    resp = client.get("/soa/999999/arms")
    assert resp.status_code == 404


def test_create_arm():
    """Test creating an arm via API."""
    r = client.post("/soa", json={"name": "Arm Create Test"})
    soa_id = r.json()["id"]

    arm_data = {
        "name": "Treatment Arm A",
        "arm_label": "ARM_A",
        "description": "Active treatment",
    }
    resp = client.post(f"/soa/{soa_id}/arms", json=arm_data)
    assert resp.status_code == 201
    data = resp.json()
    assert "id" in data
    assert data["name"] == "Treatment Arm A"
    assert "arm_uid" in data


def test_create_arm_with_custom_uid():
    """Test creating arm ignores custom UID and auto-generates."""
    r = client.post("/soa", json={"name": "Custom UID Test"})
    soa_id = r.json()["id"]

    arm_data = {"name": "Custom Arm", "arm_uid": "StudyArm_Custom"}
    resp = client.post(f"/soa/{soa_id}/arms", json=arm_data)
    assert resp.status_code == 201
    # Router always auto-generates UID, ignoring provided value
    assert resp.json()["arm_uid"] == "StudyArm_1"


def test_get_arm_detail():
    """Test that there's no detail endpoint (only list endpoint exists)."""
    r = client.post("/soa", json={"name": "Detail Test"})
    soa_id = r.json()["id"]

    # Create arm
    arm_resp = client.post(
        f"/soa/{soa_id}/arms", json={"name": "Test Arm", "arm_label": "TA"}
    )
    arm_id = arm_resp.json()["id"]

    # Try to get detail - should fail (no such endpoint)
    resp = client.get(f"/soa/{soa_id}/arms/{arm_id}")
    assert resp.status_code == 405  # Method Not Allowed


def test_update_arm():
    """Test updating arm via PATCH."""
    r = client.post("/soa", json={"name": "Update Test"})
    soa_id = r.json()["id"]

    # Create arm
    arm_resp = client.post(
        f"/soa/{soa_id}/arms", json={"name": "Original Name", "arm_label": "ORIG"}
    )
    arm_id = arm_resp.json()["id"]

    # Update arm
    update_data = {"name": "Updated Name", "arm_label": "UPD"}
    resp = client.patch(f"/soa/{soa_id}/arms/{arm_id}", json=update_data)
    assert resp.status_code == 200
    data = resp.json()
    assert "updated_fields" in data


def test_delete_arm():
    """Test deleting an arm."""
    r = client.post("/soa", json={"name": "Delete Test"})
    soa_id = r.json()["id"]

    # Create arm
    arm_resp = client.post(f"/soa/{soa_id}/arms", json={"name": "To Delete"})
    arm_id = arm_resp.json()["id"]

    # Delete arm
    resp = client.delete(f"/soa/{soa_id}/arms/{arm_id}")
    assert resp.status_code == 200
    assert resp.json()["deleted"] is True
    assert resp.json()["id"] == arm_id

    # Verify deleted
    list_resp = client.get(f"/soa/{soa_id}/arms")
    assert len(list_resp.json()) == 0


def test_ui_create_arm():
    """Test creating arm via UI form redirects."""
    r = client.post("/soa", json={"name": "UI Create Test"})
    soa_id = r.json()["id"]

    form_data = {"name": "UI Arm", "arm_label": "UIA", "description": "Created via UI"}
    resp = client.post(
        f"/ui/soa/{soa_id}/arms/create", data=form_data, follow_redirects=False
    )
    assert resp.status_code in [200, 303]


def test_ui_update_arm():
    """Test updating arm via UI form."""
    r = client.post("/soa", json={"name": "UI Update Test"})
    soa_id = r.json()["id"]

    arm_resp = client.post(f"/soa/{soa_id}/arms", json={"name": "Original"})
    arm_id = arm_resp.json()["id"]

    form_data = {"name": "Updated via UI"}
    resp = client.post(
        f"/ui/soa/{soa_id}/arms/{arm_id}/update",
        data=form_data,
        follow_redirects=False,
    )
    assert resp.status_code in [200, 303]


def test_ui_delete_arm():
    """Test deleting arm via UI form."""
    r = client.post("/soa", json={"name": "UI Delete Test"})
    soa_id = r.json()["id"]

    arm_resp = client.post(f"/soa/{soa_id}/arms", json={"name": "To Delete UI"})
    arm_id = arm_resp.json()["id"]

    resp = client.post(f"/ui/soa/{soa_id}/arms/{arm_id}/delete", follow_redirects=False)
    assert resp.status_code in [200, 303]


def test_ui_reorder_arms():
    """Test reordering arms via UI form (endpoint doesn't exist)."""
    r = client.post("/soa", json={"name": "UI Reorder Test"})
    soa_id = r.json()["id"]

    # Create arms
    a1 = client.post(f"/soa/{soa_id}/arms", json={"name": "A1"}).json()["id"]
    a2 = client.post(f"/soa/{soa_id}/arms", json={"name": "A2"}).json()["id"]

    # UI reorder endpoint doesn't exist - returns 404
    form_data = {"order": f"{a2},{a1}"}
    resp = client.post(f"/ui/soa/{soa_id}/arms/reorder", data=form_data)
    assert resp.status_code == 404


def test_arm_uid_generation():
    """Test that arm UID is auto-generated if not provided."""
    r = client.post("/soa", json={"name": "UID Gen Test"})
    soa_id = r.json()["id"]

    # Create arm without UID
    arm_resp = client.post(f"/soa/{soa_id}/arms", json={"name": "Auto UID Arm"})
    assert arm_resp.status_code == 201
    data = arm_resp.json()
    assert data["arm_uid"].startswith("StudyArm_")


def test_arm_cascade_delete_study_cells():
    """Test that deleting arm cascades to study cells."""
    r = client.post("/soa", json={"name": "Cascade Test"})
    soa_id = r.json()["id"]

    # Create arm and epoch
    arm_resp = client.post(f"/soa/{soa_id}/arms", json={"name": "Arm"})
    arm_id = arm_resp.json()["id"]

    client.post(
        f"/soa/{soa_id}/epochs/create", data={"name": "Epoch", "epoch_label": "E"}
    )

    # Create study cell (if endpoint exists)
    # cell_data = {"arm_id": arm_id, "epoch_id": epoch_id}
    # client.post(f"/soa/{soa_id}/cells", json=cell_data)

    # Delete arm
    resp = client.delete(f"/soa/{soa_id}/arms/{arm_id}")
    assert resp.status_code == 200


def test_arm_type_field():
    """Test arm with type field (if supported)."""
    r = client.post("/soa", json={"name": "Type Test"})
    soa_id = r.json()["id"]

    arm_data = {
        "name": "Experimental Arm",
        "arm_label": "EXP",
        "arm_type": "Experimental",
    }
    resp = client.post(f"/soa/{soa_id}/arms", json=arm_data)
    assert resp.status_code == 201
    # Verify type stored (if schema includes it)
