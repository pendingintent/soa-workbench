"""Comprehensive tests for schedule_timelines router endpoints."""

from fastapi.testclient import TestClient

from soa_builder.web.app import app

client = TestClient(app)


def test_create_timeline():
    """Test creating a schedule timeline via API."""
    r = client.post("/soa", json={"name": "Timeline Create Test"})
    soa_id = r.json()["id"]

    timeline_data = {"name": "Main Timeline", "main_timeline": True}
    resp = client.post(f"/soa/{soa_id}/schedule_timelines", json=timeline_data)
    assert resp.status_code == 201
    data = resp.json()
    assert "id" in data
    assert data["name"] == "Main Timeline"
    assert "schedule_timeline_uid" in data
    assert data["main_timeline"] is True


def test_create_timeline_minimal():
    """Test creating timeline with only required name field."""
    r = client.post("/soa", json={"name": "Minimal Timeline Test"})
    soa_id = r.json()["id"]

    timeline_data = {"name": "Basic Timeline"}
    resp = client.post(f"/soa/{soa_id}/schedule_timelines", json=timeline_data)
    assert resp.status_code == 201
    data = resp.json()
    assert data["name"] == "Basic Timeline"


def test_create_timeline_with_entry_condition():
    """Test creating timeline with entry condition."""
    r = client.post("/soa", json={"name": "Entry Condition Test"})
    soa_id = r.json()["id"]

    timeline_data = {
        "name": "Conditional Timeline",
        "entry_condition": "Patient enrolled",
    }
    resp = client.post(f"/soa/{soa_id}/schedule_timelines", json=timeline_data)
    assert resp.status_code == 201
    data = resp.json()
    assert data["entry_condition"] == "Patient enrolled"


def test_create_timeline_with_entry_id():
    """Test creating timeline with entry_id."""
    r = client.post("/soa", json={"name": "Entry ID Test"})
    soa_id = r.json()["id"]

    # Create an instance
    instance_resp = client.post(
        f"/soa/{soa_id}/instances", json={"name": "Entry Instance"}
    )
    instance_uid = instance_resp.json()["instance_uid"]

    timeline_data = {"name": "Timeline with Entry", "entry_id": instance_uid}
    resp = client.post(f"/soa/{soa_id}/schedule_timelines", json=timeline_data)
    assert resp.status_code == 201
    data = resp.json()
    assert data["entry_id"] == instance_uid


def test_create_timeline_with_exit_id():
    """Test creating timeline with exit_id."""
    r = client.post("/soa", json={"name": "Exit ID Test"})
    soa_id = r.json()["id"]

    # Create an instance
    instance_resp = client.post(
        f"/soa/{soa_id}/instances", json={"name": "Exit Instance"}
    )
    instance_uid = instance_resp.json()["instance_uid"]

    timeline_data = {"name": "Timeline with Exit", "exit_id": instance_uid}
    resp = client.post(f"/soa/{soa_id}/schedule_timelines", json=timeline_data)
    assert resp.status_code == 201
    data = resp.json()
    assert data["exit_id"] == instance_uid


def test_update_timeline():
    """Test updating timeline via PATCH."""
    r = client.post("/soa", json={"name": "Update Test"})
    soa_id = r.json()["id"]

    # Create timeline
    timeline_resp = client.post(
        f"/soa/{soa_id}/schedule_timelines", json={"name": "Original"}
    )
    timeline_id = timeline_resp.json()["id"]

    # Update it
    update_data = {"name": "Updated Name", "label": "New Label"}
    resp = client.patch(
        f"/soa/{soa_id}/schedule_timelines/{timeline_id}", json=update_data
    )
    assert resp.status_code == 200
    data = resp.json()
    assert data["name"] == "Updated Name"
    assert data["label"] == "New Label"


def test_delete_timeline():
    """Test deleting a timeline."""
    r = client.post("/soa", json={"name": "Delete Test"})
    soa_id = r.json()["id"]

    # Create timeline
    timeline_resp = client.post(
        f"/soa/{soa_id}/schedule_timelines", json={"name": "To Delete"}
    )
    timeline_id = timeline_resp.json()["id"]

    # Delete it
    resp = client.delete(f"/soa/{soa_id}/schedule_timelines/{timeline_id}")
    assert resp.status_code == 200
    assert resp.json()["deleted"] is True


def test_timeline_uid_generation():
    """Test that schedule_timeline_uid is auto-generated."""
    r = client.post("/soa", json={"name": "UID Gen Test"})
    soa_id = r.json()["id"]

    # Create first timeline
    resp1 = client.post(
        f"/soa/{soa_id}/schedule_timelines", json={"name": "Timeline 1"}
    )
    uid1 = resp1.json()["schedule_timeline_uid"]
    assert uid1.startswith("ScheduleTimeline_")

    # Create second timeline
    resp2 = client.post(
        f"/soa/{soa_id}/schedule_timelines", json={"name": "Timeline 2"}
    )
    uid2 = resp2.json()["schedule_timeline_uid"]
    assert uid2.startswith("ScheduleTimeline_")

    # UIDs should be different
    assert uid1 != uid2


def test_main_timeline_flag():
    """Test main_timeline boolean flag."""
    r = client.post("/soa", json={"name": "Main Timeline Test"})
    soa_id = r.json()["id"]

    # Create main timeline
    timeline_data = {"name": "Main", "main_timeline": True}
    resp = client.post(f"/soa/{soa_id}/schedule_timelines", json=timeline_data)
    assert resp.status_code == 201
    data = resp.json()
    assert data["main_timeline"] is True


def test_only_one_main_timeline():
    """Test that only one timeline can be marked as main."""
    r = client.post("/soa", json={"name": "Single Main Test"})
    soa_id = r.json()["id"]

    # Create first main timeline
    client.post(
        f"/soa/{soa_id}/schedule_timelines",
        json={"name": "Main 1", "main_timeline": True},
    )

    # Try to create second main timeline
    resp = client.post(
        f"/soa/{soa_id}/schedule_timelines",
        json={"name": "Main 2", "main_timeline": True},
    )
    # Should fail with 400
    assert resp.status_code == 400


def test_ui_list_timelines():
    """Test UI view for listing timelines."""
    r = client.post("/soa", json={"name": "UI List Test"})
    soa_id = r.json()["id"]

    resp = client.get(f"/ui/soa/{soa_id}/schedule_timelines")
    assert resp.status_code == 200
    assert "text/html" in resp.headers.get("content-type", "")


def test_ui_create_timeline():
    """Test creating timeline via UI form."""
    r = client.post("/soa", json={"name": "UI Create Test"})
    soa_id = r.json()["id"]

    form_data = {"name": "UI Timeline", "description": "Created via UI"}
    resp = client.post(f"/ui/soa/{soa_id}/schedule_timelines/create", data=form_data)
    assert resp.status_code == 200  # TestClient doesn't follow redirects


def test_ui_update_timeline():
    """Test updating timeline via UI form."""
    r = client.post("/soa", json={"name": "UI Update Test"})
    soa_id = r.json()["id"]

    # Create timeline
    timeline_resp = client.post(
        f"/soa/{soa_id}/schedule_timelines", json={"name": "Original"}
    )
    timeline_id = timeline_resp.json()["id"]

    # Update via UI
    form_data = {"name": "Updated via UI"}
    resp = client.post(
        f"/ui/soa/{soa_id}/schedule_timelines/{timeline_id}/update", data=form_data
    )
    assert resp.status_code == 200  # TestClient doesn't follow redirects


def test_ui_delete_timeline():
    """Test deleting timeline via UI form."""
    r = client.post("/soa", json={"name": "UI Delete Test"})
    soa_id = r.json()["id"]

    # Create timeline
    timeline_resp = client.post(
        f"/soa/{soa_id}/schedule_timelines", json={"name": "To Delete"}
    )
    timeline_id = timeline_resp.json()["id"]

    # Delete via UI
    resp = client.post(f"/ui/soa/{soa_id}/schedule_timelines/{timeline_id}/delete")
    assert resp.status_code == 200  # TestClient doesn't follow redirects


def test_create_timeline_all_fields():
    """Test creating timeline with all fields populated."""
    r = client.post("/soa", json={"name": "All Fields Test"})
    soa_id = r.json()["id"]

    timeline_data = {
        "name": "Complete Timeline",
        "label": "Test Label",
        "description": "Test Description",
        "main_timeline": False,
        "entry_condition": "Condition text",
        "entry_id": "Instance_1",
        "exit_id": "Instance_2",
    }
    resp = client.post(f"/soa/{soa_id}/schedule_timelines", json=timeline_data)
    assert resp.status_code == 201
    data = resp.json()
    assert data["name"] == "Complete Timeline"
    assert data["label"] == "Test Label"
    assert data["description"] == "Test Description"
    assert data["main_timeline"] is False
    assert data["entry_condition"] == "Condition text"


def test_delete_nonexistent_timeline():
    """Test deleting nonexistent timeline returns 404."""
    r = client.post("/soa", json={"name": "Delete Nonexistent Test"})
    soa_id = r.json()["id"]

    resp = client.delete(f"/soa/{soa_id}/schedule_timelines/999999")
    assert resp.status_code == 404


def test_update_nonexistent_timeline():
    """Test updating nonexistent timeline returns 404."""
    r = client.post("/soa", json={"name": "Update Nonexistent Test"})
    soa_id = r.json()["id"]

    resp = client.patch(
        f"/soa/{soa_id}/schedule_timelines/999999", json={"name": "New Name"}
    )
    assert resp.status_code == 404


def test_create_timeline_empty_name():
    """Test creating timeline with empty name fails."""
    r = client.post("/soa", json={"name": "Empty Name Test"})
    soa_id = r.json()["id"]

    timeline_data = {"name": ""}
    resp = client.post(f"/soa/{soa_id}/schedule_timelines", json=timeline_data)
    assert resp.status_code == 400


def test_create_timeline_nonexistent_soa():
    """Test creating timeline for nonexistent SoA returns 404."""
    timeline_data = {"name": "Test Timeline"}
    resp = client.post("/soa/999999/schedule_timelines", json=timeline_data)
    assert resp.status_code == 404


def test_ui_list_nonexistent_soa():
    """Test UI list for nonexistent SoA returns 404."""
    resp = client.get("/ui/soa/999999/schedule_timelines")
    assert resp.status_code == 404
