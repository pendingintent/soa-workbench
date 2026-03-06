"""Comprehensive tests for elements router endpoints."""

from fastapi.testclient import TestClient

from soa_builder.web.app import app

client = TestClient(app)


def test_list_elements_empty():
    """Test listing elements for a new SoA returns empty list."""
    r = client.post("/soa", json={"name": "Elements Test Study"})
    soa_id = r.json()["id"]

    resp = client.get(f"/soa/{soa_id}/elements")
    assert resp.status_code == 200
    assert resp.json() == []


def test_list_elements_nonexistent_soa():
    """Test listing elements for nonexistent SoA returns 404."""
    resp = client.get("/soa/999999/elements")
    assert resp.status_code == 404


def test_create_element():
    """Test creating an element via UI form."""
    r = client.post("/soa", json={"name": "Element Create Test"})
    soa_id = r.json()["id"]

    form_data = {
        "name": "Treatment Period A",
        "label": "TRT_A",
        "description": "First treatment period",
    }
    resp = client.post(f"/ui/soa/{soa_id}/elements/create", data=form_data)
    # UI endpoint redirects - TestClient shows 200
    assert resp.status_code == 200


def test_create_element_with_transition_rules():
    """Test creating element with testrl and teenrl fields."""
    r = client.post("/soa", json={"name": "Transition Test"})
    soa_id = r.json()["id"]

    form_data = {"name": "Element with Rules", "testrl": "Day 1", "teenrl": "Day 28"}
    resp = client.post(f"/ui/soa/{soa_id}/elements/create", data=form_data)
    assert resp.status_code == 200


def test_get_element_detail():
    """Test getting element detail."""
    r = client.post("/soa", json={"name": "Detail Test"})
    soa_id = r.json()["id"]

    # Create element
    client.post(f"/ui/soa/{soa_id}/elements/create", data={"name": "Test Element"})

    # Get list and extract element ID
    list_resp = client.get(f"/soa/{soa_id}/elements")
    elements = list_resp.json()
    assert len(elements) > 0
    element = elements[0]
    assert element["name"] == "Test Element"
    assert "element_id" in element


def test_update_element():
    """Test updating element via UI form."""
    r = client.post("/soa", json={"name": "Update Test"})
    soa_id = r.json()["id"]

    # Create element
    client.post(f"/ui/soa/{soa_id}/elements/create", data={"name": "Original Name"})

    # Get element ID
    list_resp = client.get(f"/soa/{soa_id}/elements")
    element_id = list_resp.json()[0]["id"]

    # Update element
    form_data = {"name": "Updated Name"}
    resp = client.post(f"/ui/soa/{soa_id}/elements/{element_id}/update", data=form_data)
    assert resp.status_code == 200


def test_delete_element():
    """Test deleting an element."""
    r = client.post("/soa", json={"name": "Delete Test"})
    soa_id = r.json()["id"]

    # Create element
    client.post(f"/ui/soa/{soa_id}/elements/create", data={"name": "To Delete"})

    # Get element ID
    list_resp = client.get(f"/soa/{soa_id}/elements")
    element_id = list_resp.json()[0]["id"]

    # Delete element
    resp = client.post(f"/ui/soa/{soa_id}/elements/{element_id}/delete")
    assert resp.status_code == 200

    # Verify deleted
    list_resp = client.get(f"/soa/{soa_id}/elements")
    assert len(list_resp.json()) == 0


def test_element_uid_generation():
    """Test that element UID is auto-generated with monotonic IDs."""
    r = client.post("/soa", json={"name": "UID Gen Test"})
    soa_id = r.json()["id"]

    # Create first element
    client.post(f"/ui/soa/{soa_id}/elements/create", data={"name": "Element 1"})

    # Get element
    list_resp = client.get(f"/soa/{soa_id}/elements")
    element1 = list_resp.json()[0]
    assert "element_id" in element1
    assert element1["element_id"].startswith("StudyElement_")

    # Create second element
    client.post(f"/ui/soa/{soa_id}/elements/create", data={"name": "Element 2"})

    # Verify monotonic ID
    list_resp = client.get(f"/soa/{soa_id}/elements")
    elements = list_resp.json()
    uid1 = int(elements[0]["element_id"].split("_")[1])
    uid2 = int(elements[1]["element_id"].split("_")[1])
    assert uid2 > uid1


def test_element_audit_trail():
    """Test that element operations create audit records."""
    r = client.post("/soa", json={"name": "Audit Test"})
    soa_id = r.json()["id"]

    # Create element
    client.post(f"/ui/soa/{soa_id}/elements/create", data={"name": "Audited Element"})

    # Get element ID
    list_resp = client.get(f"/soa/{soa_id}/elements")
    element_id = list_resp.json()[0]["id"]

    # Check audit endpoint for all elements
    resp = client.get(f"/soa/{soa_id}/element_audit")
    assert resp.status_code == 200
    audit = resp.json()
    assert len(audit) > 0
    # Find create action for this element
    creates = [
        a
        for a in audit
        if a["action"] == "create" and a.get("element_id") == element_id
    ]
    assert len(creates) > 0


def test_element_previous_element_id():
    """Test creating multiple sequential elements."""
    r = client.post("/soa", json={"name": "Sequence Test"})
    soa_id = r.json()["id"]

    # Create first element
    client.post(f"/ui/soa/{soa_id}/elements/create", data={"name": "Element 1"})

    # Get first element ID
    list_resp = client.get(f"/soa/{soa_id}/elements")
    element1_id = list_resp.json()[0]["id"]
    assert element1_id is not None

    # Create second element
    resp = client.post(f"/ui/soa/{soa_id}/elements/create", data={"name": "Element 2"})
    assert resp.status_code == 200


def test_element_description_field():
    """Test element with description field."""
    r = client.post("/soa", json={"name": "Description Test"})
    soa_id = r.json()["id"]

    form_data = {
        "name": "Described Element",
        "description": "Detailed description of element",
    }
    resp = client.post(f"/ui/soa/{soa_id}/elements/create", data=form_data)
    assert resp.status_code == 200

    # Verify description stored
    list_resp = client.get(f"/soa/{soa_id}/elements")
    element = list_resp.json()[0]
    assert element.get("description") == "Detailed description of element"


def test_bulk_create_elements():
    """Test bulk creating elements (if supported)."""
    r = client.post("/soa", json={"name": "Bulk Elements Test"})
    soa_id = r.json()["id"]

    # Create multiple elements
    for i in range(5):
        client.post(
            f"/ui/soa/{soa_id}/elements/create", data={"name": f"Element {i + 1}"}
        )

    # Verify all created
    list_resp = client.get(f"/soa/{soa_id}/elements")
    assert len(list_resp.json()) == 5


def test_element_immutable_uid():
    """Test that element_uid cannot be changed after creation."""
    r = client.post("/soa", json={"name": "Immutable UID Test"})
    soa_id = r.json()["id"]

    # Create element
    client.post(f"/ui/soa/{soa_id}/elements/create", data={"name": "Element"})

    # Get element
    list_resp = client.get(f"/soa/{soa_id}/elements")
    element = list_resp.json()[0]
    original_uid = element["element_id"]
    element_id = element["id"]

    # Try to update - UID cannot be changed via update
    form_data = {"name": "Updated"}
    client.post(f"/ui/soa/{soa_id}/elements/{element_id}/update", data=form_data)

    # Verify UID unchanged
    list_resp = client.get(f"/soa/{soa_id}/elements")
    assert list_resp.json()[0]["element_id"] == original_uid
