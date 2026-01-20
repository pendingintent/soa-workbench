"""Comprehensive tests for visits router endpoints."""

from fastapi.testclient import TestClient

from soa_builder.web.app import app

client = TestClient(app)


def test_list_visits_empty():
    """Test listing visits for a new SoA returns empty list."""
    r = client.post("/soa", json={"name": "Visits Test Study"})
    soa_id = r.json()["id"]

    resp = client.get(f"/soa/{soa_id}/visits")
    assert resp.status_code == 200
    assert resp.json() == []


def test_list_visits_nonexistent_soa():
    """Test listing visits for nonexistent SoA returns 404."""
    resp = client.get("/soa/999999/visits")
    assert resp.status_code == 404


def test_create_visit():
    """Test creating a visit via API."""
    r = client.post("/soa", json={"name": "Visit Create Test"})
    soa_id = r.json()["id"]

    visit_data = {
        "name": "Screening Visit",
        "label": "SCR",
        "description": "Initial screening",
    }
    resp = client.post(f"/soa/{soa_id}/visits", json=visit_data)
    assert resp.status_code == 201
    data = resp.json()
    assert "id" in data
    assert data["name"] == "Screening Visit"
    assert data["label"] == "SCR"


def test_create_visit_minimal():
    """Test creating visit with only required name field."""
    r = client.post("/soa", json={"name": "Minimal Visit Test"})
    soa_id = r.json()["id"]

    visit_data = {"name": "Basic Visit"}
    resp = client.post(f"/soa/{soa_id}/visits", json=visit_data)
    assert resp.status_code == 201
    data = resp.json()
    assert data["name"] == "Basic Visit"


def test_list_visits_with_data():
    """Test listing visits returns created visits."""
    r = client.post("/soa", json={"name": "List Test"})
    soa_id = r.json()["id"]

    # Create visit
    client.post(f"/soa/{soa_id}/visits", json={"name": "Test Visit", "label": "TV"})

    # List visits
    resp = client.get(f"/soa/{soa_id}/visits")
    assert resp.status_code == 200
    visits = resp.json()
    assert len(visits) == 1
    assert visits[0]["name"] == "Test Visit"


def test_get_visit_detail():
    """Test getting visit detail (note: endpoint takes soa_id as query param)."""
    r = client.post("/soa", json={"name": "Detail Test"})
    soa_id = r.json()["id"]

    # Create visit
    visit_resp = client.post(
        f"/soa/{soa_id}/visits", json={"name": "Test Visit", "label": "TV"}
    )
    visit_id = visit_resp.json()["id"]

    # Get detail - endpoint needs soa_id as query param
    resp = client.get(f"/soa/visits/{visit_id}?soa_id={soa_id}")
    assert resp.status_code == 200
    data = resp.json()
    assert data["id"] == visit_id
    assert data["name"] == "Test Visit"
    assert "encounter_uid" in data


def test_update_visit():
    """Test updating visit via PATCH."""
    r = client.post("/soa", json={"name": "Update Test"})
    soa_id = r.json()["id"]

    # Create visit
    visit_resp = client.post(f"/soa/{soa_id}/visits", json={"name": "Original"})
    visit_id = visit_resp.json()["id"]

    # Update it
    update_data = {"name": "Updated Name", "label": "UPD"}
    resp = client.patch(f"/soa/{soa_id}/visits/{visit_id}", json=update_data)
    assert resp.status_code == 200
    data = resp.json()
    assert data["name"] == "Updated Name"
    assert data["label"] == "UPD"


def test_delete_visit():
    """Test deleting a visit."""
    r = client.post("/soa", json={"name": "Delete Test"})
    soa_id = r.json()["id"]

    # Create visit
    visit_resp = client.post(f"/soa/{soa_id}/visits", json={"name": "To Delete"})
    visit_id = visit_resp.json()["id"]

    # Delete visit
    resp = client.delete(f"/soa/{soa_id}/visits/{visit_id}")
    assert resp.status_code == 200
    data = resp.json()
    assert data["deleted"] is True
    assert data["id"] == visit_id


def test_reorder_visits():
    """Test reordering visits via API."""
    r = client.post("/soa", json={"name": "Reorder Test"})
    soa_id = r.json()["id"]

    # Create visits
    v1_resp = client.post(f"/soa/{soa_id}/visits", json={"name": "Visit 1"})
    v2_resp = client.post(f"/soa/{soa_id}/visits", json={"name": "Visit 2"})
    v1_id = v1_resp.json()["id"]
    v2_id = v2_resp.json()["id"]

    # Reorder them
    resp = client.post(
        "/visits/reorder", params={"soa_id": soa_id}, json=[v2_id, v1_id]
    )
    assert resp.status_code == 200
    data = resp.json()
    assert data["new_order"] == [v2_id, v1_id]


def test_create_visit_with_environmental_settings():
    """Test creating visit with environmental settings."""
    r = client.post("/soa", json={"name": "Env Settings Test"})
    soa_id = r.json()["id"]

    visit_data = {
        "name": "Clinical Visit",
        "environmentalSettings": "C174215",  # Clinical site
    }
    resp = client.post(f"/soa/{soa_id}/visits", json=visit_data)
    assert resp.status_code == 201
    data = resp.json()
    assert data["environmental_settings"] == "C174215"


def test_create_visit_with_contact_modes():
    """Test creating visit with contact modes."""
    r = client.post("/soa", json={"name": "Contact Modes Test"})
    soa_id = r.json()["id"]

    visit_data = {"name": "Virtual Visit", "contactModes": "C171441"}  # Virtual
    resp = client.post(f"/soa/{soa_id}/visits", json=visit_data)
    assert resp.status_code == 201
    data = resp.json()
    assert data["contactModes"] == "C171441"


def test_create_visit_with_transition_rules():
    """Test creating visit with transition rules."""
    r = client.post("/soa", json={"name": "Transition Test"})
    soa_id = r.json()["id"]

    visit_data = {
        "name": "Scheduled Visit",
        "transitionStartRule": "After enrollment",
        "transitionEndRule": "Visit complete",
    }
    resp = client.post(f"/soa/{soa_id}/visits", json=visit_data)
    assert resp.status_code == 201
    data = resp.json()
    assert data["transitionStartRule"] == "After enrollment"
    assert data["transitionEndRule"] == "Visit complete"


def test_delete_nonexistent_visit():
    """Test deleting nonexistent visit returns 404."""
    r = client.post("/soa", json={"name": "Delete Nonexistent Test"})
    soa_id = r.json()["id"]

    resp = client.delete(f"/soa/{soa_id}/visits/999999")
    assert resp.status_code == 404


def test_update_nonexistent_visit():
    """Test updating nonexistent visit returns 404."""
    r = client.post("/soa", json={"name": "Update Nonexistent Test"})
    soa_id = r.json()["id"]

    resp = client.patch(f"/soa/{soa_id}/visits/999999", json={"name": "New Name"})
    assert resp.status_code == 404


def test_create_visit_empty_name():
    """Test creating visit with empty name fails."""
    r = client.post("/soa", json={"name": "Empty Name Test"})
    soa_id = r.json()["id"]

    visit_data = {"name": ""}
    resp = client.post(f"/soa/{soa_id}/visits", json=visit_data)
    assert resp.status_code == 400


def test_create_visit_nonexistent_soa():
    """Test creating visit for nonexistent SoA returns 404."""
    visit_data = {"name": "Test Visit"}
    resp = client.post("/soa/999999/visits", json=visit_data)
    assert resp.status_code == 404


def test_visit_all_fields():
    """Test creating visit with all fields populated."""
    r = client.post("/soa", json={"name": "All Fields Test"})
    soa_id = r.json()["id"]

    visit_data = {
        "name": "Complete Visit",
        "label": "COMP",
        "description": "A complete visit with all fields",
        "type": "SCREENING",
        "environmentalSettings": "C174215",
        "contactModes": "C171440",
        "transitionStartRule": "Start rule",
        "transitionEndRule": "End rule",
        "scheduledAtId": "Instance_1",
    }
    resp = client.post(f"/soa/{soa_id}/visits", json=visit_data)
    assert resp.status_code == 201
    data = resp.json()
    assert data["name"] == "Complete Visit"
    assert data["label"] == "COMP"
    assert data["description"] == "A complete visit with all fields"


def test_reorder_empty_list():
    """Test reordering with empty list fails."""
    r = client.post("/soa", json={"name": "Empty Reorder Test"})
    soa_id = r.json()["id"]

    resp = client.post("/visits/reorder", params={"soa_id": soa_id}, json=[])
    assert resp.status_code == 400


def test_reorder_invalid_visit_id():
    """Test reordering with invalid visit ID fails."""
    r = client.post("/soa", json={"name": "Invalid Reorder Test"})
    soa_id = r.json()["id"]

    # Create one visit
    v1_resp = client.post(f"/soa/{soa_id}/visits", json={"name": "Visit 1"})
    v1_id = v1_resp.json()["id"]

    # Try to reorder with invalid ID
    resp = client.post(
        "/visits/reorder", params={"soa_id": soa_id}, json=[v1_id, 999999]
    )
    assert resp.status_code == 400


def test_visit_order_index_resequenced_after_delete():
    """Test that order_index is resequenced after deleting a visit."""
    r = client.post("/soa", json={"name": "Order Index Test"})
    soa_id = r.json()["id"]

    # Create 3 visits
    client.post(f"/soa/{soa_id}/visits", json={"name": "Visit 1"})
    v2 = client.post(f"/soa/{soa_id}/visits", json={"name": "Visit 2"})
    client.post(f"/soa/{soa_id}/visits", json={"name": "Visit 3"})

    v2_id = v2.json()["id"]

    # Delete middle visit
    client.delete(f"/soa/{soa_id}/visits/{v2_id}")

    # List remaining visits
    resp = client.get(f"/soa/{soa_id}/visits")
    visits = resp.json()

    # Should be 2 visits left
    assert len(visits) == 2

    # Order indices should be sequential (1, 2)
    indices = sorted([v["order_index"] for v in visits])
    assert indices == [1, 2]
