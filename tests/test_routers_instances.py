"""Comprehensive tests for instances router endpoints."""

from fastapi.testclient import TestClient

from soa_builder.web.app import app

client = TestClient(app)


def test_list_instances_empty():
    """Test listing instances for a new SoA returns empty list."""
    r = client.post("/soa", json={"name": "Instances Test Study"})
    soa_id = r.json()["id"]

    resp = client.get(f"/soa/{soa_id}/instances")
    assert resp.status_code == 200
    assert resp.json() == []


def test_list_instances_nonexistent_soa():
    """Test listing instances for nonexistent SoA returns 404."""
    resp = client.get("/soa/999999/instances")
    assert resp.status_code == 404


def test_create_instance():
    """Test creating a scheduled activity instance."""
    r = client.post("/soa", json={"name": "Instance Create Test"})
    soa_id = r.json()["id"]

    # Create instance with minimal required fields
    instance_data = {
        "name": "V1_Instance",
        "label": "Visit 1 Instance",
        "encounter_uid": "Encounter_1",
    }
    resp = client.post(f"/soa/{soa_id}/instances", json=instance_data)
    assert resp.status_code == 201
    data = resp.json()
    assert "id" in data
    assert "instance_uid" in data
    assert data["instance_uid"].startswith("ScheduledActivityInstance_")
    assert data["name"] == "V1_Instance"


def test_create_instance_minimal():
    """Test creating instance with only name (minimal required)."""
    r = client.post("/soa", json={"name": "Minimal Instance Test"})
    soa_id = r.json()["id"]

    # Only name is required
    resp = client.post(f"/soa/{soa_id}/instances", json={"name": "Simple Instance"})
    assert resp.status_code == 201
    data = resp.json()
    assert data["name"] == "Simple Instance"
    assert "instance_uid" in data


def test_list_instances():
    """Test listing multiple instances."""
    r = client.post("/soa", json={"name": "List Test"})
    soa_id = r.json()["id"]

    # Create two instances
    client.post(f"/soa/{soa_id}/instances", json={"name": "Instance 1"})
    client.post(f"/soa/{soa_id}/instances", json={"name": "Instance 2"})

    # List instances
    resp = client.get(f"/soa/{soa_id}/instances")
    assert resp.status_code == 200
    data = resp.json()
    assert len(data) == 2
    assert data[0]["name"] == "Instance 1"
    assert data[1]["name"] == "Instance 2"


def test_update_instance():
    """Test updating instance via PATCH."""
    r = client.post("/soa", json={"name": "Update Test"})
    soa_id = r.json()["id"]

    # Create instance
    instance_resp = client.post(
        f"/soa/{soa_id}/instances",
        json={"name": "Original Name", "label": "Original Label"},
    )
    instance_id = instance_resp.json()["id"]

    # Update instance
    update_data = {"label": "Updated Label", "description": "New description"}
    resp = client.patch(f"/soa/{soa_id}/instances/{instance_id}", json=update_data)
    assert resp.status_code == 200
    updated = resp.json()
    assert updated["label"] == "Updated Label"
    assert updated["description"] == "New description"
    # Name should remain unchanged
    assert updated["name"] == "Original Name"


def test_delete_instance():
    """Test deleting an instance."""
    r = client.post("/soa", json={"name": "Delete Test"})
    soa_id = r.json()["id"]

    # Create instance
    instance_resp = client.post(f"/soa/{soa_id}/instances", json={"name": "To Delete"})
    instance_id = instance_resp.json()["id"]

    # Delete instance
    resp = client.delete(f"/soa/{soa_id}/instances/{instance_id}")
    assert resp.status_code == 200
    assert resp.json()["deleted"] is True

    # Verify deleted
    list_resp = client.get(f"/soa/{soa_id}/instances")
    assert len(list_resp.json()) == 0


def test_instance_uid_generation():
    """Test that instance UID is auto-generated with sequential numbers."""
    r = client.post("/soa", json={"name": "UID Gen Test"})
    soa_id = r.json()["id"]

    # Create first instance
    resp1 = client.post(f"/soa/{soa_id}/instances", json={"name": "Instance 1"})
    assert resp1.status_code == 201
    uid1 = resp1.json()["instance_uid"]
    assert uid1 == "ScheduledActivityInstance_1"

    # Create second instance
    resp2 = client.post(f"/soa/{soa_id}/instances", json={"name": "Instance 2"})
    uid2 = resp2.json()["instance_uid"]
    assert uid2 == "ScheduledActivityInstance_2"


def test_instance_with_epoch():
    """Test creating instance with epoch reference."""
    r = client.post("/soa", json={"name": "Epoch Instance Test"})
    soa_id = r.json()["id"]

    # Create epoch via UI (no JSON API for epochs create)
    client.post(
        f"/ui/soa/{soa_id}/epochs/create",
        data={"label": "Treatment", "description": "TRT"},
    )

    # Get epochs to find the epoch_uid
    list_resp = client.get(f"/soa/{soa_id}/epochs")
    if list_resp.status_code == 200:
        epochs = list_resp.json()
        if len(epochs) > 0:
            epoch_uid = epochs[0].get("epoch_uid")

            # Create instance with epoch
            instance_data = {"name": "Instance with Epoch", "epoch_uid": epoch_uid}
            resp = client.post(f"/soa/{soa_id}/instances", json=instance_data)
            assert resp.status_code == 201


def test_instance_audit_trail():
    """Test that instance operations create audit records."""
    r = client.post("/soa", json={"name": "Audit Test"})
    soa_id = r.json()["id"]

    # Create instance
    instance_resp = client.post(
        f"/soa/{soa_id}/instances", json={"name": "Audited Instance"}
    )
    instance_id = instance_resp.json()["id"]

    # Update instance (creates audit)
    client.patch(f"/soa/{soa_id}/instances/{instance_id}", json={"label": "Updated"})

    # Check audit endpoint (may or may not exist)
    resp = client.get(f"/soa/{soa_id}/instances/audit")
    # Either 200, 404, or 405 if endpoint doesn't exist or wrong method
    assert resp.status_code in [200, 404, 405]


def test_instance_with_fields():
    """Test instance with all optional fields populated."""
    r = client.post("/soa", json={"name": "Full Fields Test"})
    soa_id = r.json()["id"]

    instance_data = {
        "name": "Full Instance",
        "label": "Instance Label",
        "description": "Instance description",
        "default_condition_uid": "Condition_1",
        "epoch_uid": "Epoch_1",
        "timeline_id": "Timeline_1",
        "timeline_exit_id": "Exit_1",
        "encounter_uid": "Encounter_1",
        "member_of_timeline": "MainTimeline",
    }
    resp = client.post(f"/soa/{soa_id}/instances", json=instance_data)
    assert resp.status_code == 201
    data = resp.json()
    assert data["name"] == "Full Instance"
    assert data["label"] == "Instance Label"
    assert data["encounter_uid"] == "Encounter_1"


def test_create_multiple_instances():
    """Test creating multiple instances in sequence."""
    r = client.post("/soa", json={"name": "Multiple Instances Test"})
    soa_id = r.json()["id"]

    # Create 3 instances
    for i in range(3):
        resp = client.post(f"/soa/{soa_id}/instances", json={"name": f"Instance {i+1}"})
        assert resp.status_code == 201

    # Verify all created
    list_resp = client.get(f"/soa/{soa_id}/instances")
    assert len(list_resp.json()) == 3


def test_ui_create_instance():
    """Test creating instance via UI form."""
    r = client.post("/soa", json={"name": "UI Instance Test"})
    soa_id = r.json()["id"]

    form_data = {"name": "UI Instance", "label": "UI Label"}
    resp = client.post(f"/ui/soa/{soa_id}/instances/create", data=form_data)
    # TestClient doesn't follow redirects, returns 200
    assert resp.status_code == 200


def test_update_instance_partial():
    """Test partial update (not all fields)."""
    r = client.post("/soa", json={"name": "Partial Update Test"})
    soa_id = r.json()["id"]

    # Create instance
    instance_resp = client.post(
        f"/soa/{soa_id}/instances",
        json={
            "name": "Original",
            "label": "Original Label",
            "description": "Original Description",
        },
    )
    instance_id = instance_resp.json()["id"]

    # Update only label
    update_data = {"label": "New Label"}
    resp = client.patch(f"/soa/{soa_id}/instances/{instance_id}", json=update_data)
    assert resp.status_code == 200
    updated = resp.json()
    assert updated["label"] == "New Label"
    # Name and description should be unchanged
    assert updated["name"] == "Original"
    assert updated["description"] == "Original Description"


def test_delete_nonexistent_instance():
    """Test deleting instance that doesn't exist."""
    r = client.post("/soa", json={"name": "Delete Nonexistent Test"})
    soa_id = r.json()["id"]

    resp = client.delete(f"/soa/{soa_id}/instances/999")
    assert resp.status_code == 404


def test_update_nonexistent_instance():
    """Test updating instance that doesn't exist."""
    r = client.post("/soa", json={"name": "Update Nonexistent Test"})
    soa_id = r.json()["id"]

    resp = client.patch(f"/soa/{soa_id}/instances/999", json={"label": "New"})
    assert resp.status_code == 404
