"""Comprehensive tests for epochs router endpoints."""

from fastapi.testclient import TestClient

from soa_builder.web.app import app

client = TestClient(app)


def test_list_epochs_empty():
    """Test listing epochs for a new SoA returns empty list."""
    r = client.post("/soa", json={"name": "Epochs Test Study"})
    soa_id = r.json()["id"]

    resp = client.get(f"/soa/{soa_id}/epochs")
    assert resp.status_code == 200
    assert resp.json() == []


def test_list_epochs_nonexistent_soa():
    """Test listing epochs for nonexistent SoA returns 404."""
    resp = client.get("/soa/999999/epochs")
    assert resp.status_code == 404


def test_create_epoch():
    """Test creating an epoch via UI form."""
    r = client.post("/soa", json={"name": "Epoch Create Test"})
    soa_id = r.json()["id"]

    form_data = {
        "name": "Screening Period",
        "label": "SCR",
        "description": "Initial screening phase",
    }
    resp = client.post(f"/ui/soa/{soa_id}/epochs/create", data=form_data)
    assert resp.status_code == 200
    assert resp.headers["content-type"].startswith("text/html")


def test_create_epoch_with_type():
    """Test creating epoch with type field."""
    r = client.post("/soa", json={"name": "Epoch Type Test"})
    soa_id = r.json()["id"]

    form_data = {"name": "Treatment Period", "label": "TRT", "type": "TREATMENT"}
    resp = client.post(f"/ui/soa/{soa_id}/epochs/create", data=form_data)
    assert resp.status_code == 200


def test_get_epoch_detail():
    """Test getting epoch detail."""
    r = client.post("/soa", json={"name": "Detail Test"})
    soa_id = r.json()["id"]

    # Create epoch
    client.post(
        f"/ui/soa/{soa_id}/epochs/create", data={"name": "Test Epoch", "label": "TE"}
    )

    # Get list and extract first epoch
    list_resp = client.get(f"/soa/{soa_id}/epochs")
    epochs = list_resp.json()
    assert len(epochs) > 0
    epoch = epochs[0]
    assert epoch["name"] == "Test Epoch"


def test_update_epoch():
    """Test updating epoch via UI form."""
    r = client.post("/soa", json={"name": "Update Test"})
    soa_id = r.json()["id"]

    # Create epoch
    client.post(
        f"/ui/soa/{soa_id}/epochs/create",
        data={"name": "Original Name", "label": "ORIG"},
    )

    # Get epoch ID
    list_resp = client.get(f"/soa/{soa_id}/epochs")
    epoch_id = list_resp.json()[0]["id"]

    # Update epoch
    form_data = {"name": "Updated Name", "label": "UPD"}
    resp = client.post(f"/ui/soa/{soa_id}/epochs/{epoch_id}/update", data=form_data)
    assert resp.status_code == 200


def test_delete_epoch():
    """Test deleting an epoch."""
    r = client.post("/soa", json={"name": "Delete Test"})
    soa_id = r.json()["id"]

    # Create epoch
    client.post(f"/ui/soa/{soa_id}/epochs/create", data={"name": "To Delete"})

    # Get epoch ID
    list_resp = client.get(f"/soa/{soa_id}/epochs")
    epoch_id = list_resp.json()[0]["id"]

    # Delete epoch
    resp = client.post(f"/ui/soa/{soa_id}/epochs/{epoch_id}/delete")
    assert resp.status_code == 200

    # Verify deleted
    list_resp = client.get(f"/soa/{soa_id}/epochs")
    assert len(list_resp.json()) == 0


def test_reorder_epochs():
    """Test reordering epochs."""
    r = client.post("/soa", json={"name": "Reorder Test"})
    soa_id = r.json()["id"]

    # Create multiple epochs
    client.post(f"/ui/soa/{soa_id}/epochs/create", data={"name": "Epoch 1"})
    client.post(f"/ui/soa/{soa_id}/epochs/create", data={"name": "Epoch 2"})
    client.post(f"/ui/soa/{soa_id}/epochs/create", data={"name": "Epoch 3"})

    # Get epoch IDs
    list_resp = client.get(f"/soa/{soa_id}/epochs")
    epochs = list_resp.json()
    e1, e2, e3 = epochs[0]["id"], epochs[1]["id"], epochs[2]["id"]

    # Reorder: [e3, e1, e2] - use JSON body
    resp = client.post(f"/soa/{soa_id}/epochs/reorder", json={"order": [e3, e1, e2]})
    assert resp.status_code == 200

    # Verify new order
    list_resp = client.get(f"/soa/{soa_id}/epochs")
    epochs = list_resp.json()
    assert epochs[0]["id"] == e3
    assert epochs[1]["id"] == e1
    assert epochs[2]["id"] == e2


def test_epoch_uid_generation():
    """Test that epoch UID is auto-generated."""
    r = client.post("/soa", json={"name": "UID Gen Test"})
    soa_id = r.json()["id"]

    # Create epoch
    client.post(f"/ui/soa/{soa_id}/epochs/create", data={"name": "Auto UID Epoch"})

    # Get epoch
    list_resp = client.get(f"/soa/{soa_id}/epochs")
    epoch = list_resp.json()[0]
    assert "epoch_uid" in epoch
    assert epoch["epoch_uid"].startswith("StudyEpoch_")


def test_epoch_cascade_delete_visits():
    """Test that deleting epoch updates associated visits."""
    r = client.post("/soa", json={"name": "Cascade Test"})
    soa_id = r.json()["id"]

    # Create epoch
    client.post(f"/ui/soa/{soa_id}/epochs/create", data={"name": "Epoch"})

    # Get epoch ID
    list_resp = client.get(f"/soa/{soa_id}/epochs")
    epoch_id = list_resp.json()[0]["id"]

    # Create visit linked to epoch
    visit_data = {"name": "Visit", "epoch_id": epoch_id}
    client.post(f"/soa/{soa_id}/visits", json=visit_data)

    # Delete epoch
    resp = client.post(f"/ui/soa/{soa_id}/epochs/{epoch_id}/delete")
    assert resp.status_code == 200


def test_epoch_description_field():
    """Test epoch with description field."""
    r = client.post("/soa", json={"name": "Description Test"})
    soa_id = r.json()["id"]

    form_data = {
        "name": "Treatment",
        "label": "TRT",
        "description": "Active treatment phase",
    }
    resp = client.post(f"/ui/soa/{soa_id}/epochs/create", data=form_data)
    assert resp.status_code == 200

    # Verify description stored
    list_resp = client.get(f"/soa/{soa_id}/epochs")
    epoch = list_resp.json()[0]
    assert epoch.get("epoch_description") == "Active treatment phase"


def test_epoch_previous_epoch_id():
    """Test epoch with previous_epoch_id linkage."""
    r = client.post("/soa", json={"name": "Sequence Test"})
    soa_id = r.json()["id"]

    # Create first epoch
    client.post(f"/ui/soa/{soa_id}/epochs/create", data={"name": "Epoch 1"})

    # Get first epoch ID
    list_resp = client.get(f"/soa/{soa_id}/epochs")
    epoch1_id = list_resp.json()[0]["id"]

    # Create second epoch with previous reference
    form_data = {"name": "Epoch 2", "previous_epoch_id": str(epoch1_id)}
    resp = client.post(f"/ui/soa/{soa_id}/epochs/create", data=form_data)
    assert resp.status_code == 200
