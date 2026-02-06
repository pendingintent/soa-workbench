"""Comprehensive tests for timings router endpoints."""

from fastapi.testclient import TestClient

from soa_builder.web.app import app

client = TestClient(app)


def test_list_timings_empty():
    """Test listing timings for a new SoA returns empty list."""
    r = client.post("/soa", json={"name": "Timings Test Study"})
    soa_id = r.json()["id"]

    resp = client.get(f"/soa/{soa_id}/timings")
    assert resp.status_code == 200
    assert resp.json() == []


def test_list_timings_nonexistent_soa():
    """Test listing timings for nonexistent SoA returns 404."""
    resp = client.get("/soa/999999/timings")
    assert resp.status_code == 404


def test_create_timing():
    """Test creating a timing via API."""
    r = client.post("/soa", json={"name": "Timing Create Test"})
    soa_id = r.json()["id"]

    timing_data = {"name": "Day 1", "value": "P1D"}
    resp = client.post(f"/soa/{soa_id}/timings", json=timing_data)
    assert resp.status_code == 201
    data = resp.json()
    assert "id" in data
    assert data["name"] == "Day 1"
    assert "timing_uid" in data


def test_create_timing_minimal():
    """Test creating timing with only required name field."""
    r = client.post("/soa", json={"name": "Minimal Timing Test"})
    soa_id = r.json()["id"]

    timing_data = {"name": "Basic Timing"}
    resp = client.post(f"/soa/{soa_id}/timings", json=timing_data)
    assert resp.status_code == 201
    data = resp.json()
    assert data["name"] == "Basic Timing"


def test_create_timing_with_iso8601():
    """Test creating timing with ISO 8601 duration."""
    r = client.post("/soa", json={"name": "ISO8601 Test"})
    soa_id = r.json()["id"]

    timing_data = {"name": "Week 2", "value": "P2W"}
    resp = client.post(f"/soa/{soa_id}/timings", json=timing_data)
    assert resp.status_code == 201
    data = resp.json()
    assert data["value"] == "P2W"


def test_list_timings_with_data():
    """Test listing timings returns created timings."""
    r = client.post("/soa", json={"name": "List Test"})
    soa_id = r.json()["id"]

    # Create timing
    client.post(f"/soa/{soa_id}/timings", json={"name": "Test Timing", "value": "P7D"})

    # List timings
    resp = client.get(f"/soa/{soa_id}/timings")
    assert resp.status_code == 200
    timings = resp.json()
    assert len(timings) == 1
    assert timings[0]["name"] == "Test Timing"


def test_update_timing():
    """Test updating timing via PATCH."""
    r = client.post("/soa", json={"name": "Update Test"})
    soa_id = r.json()["id"]

    # Create timing
    timing_resp = client.post(f"/soa/{soa_id}/timings", json={"name": "Original"})
    timing_id = timing_resp.json()["id"]

    # Update it
    update_data = {"name": "Updated Name", "label": "New Label"}
    resp = client.patch(f"/soa/{soa_id}/timings/{timing_id}", json=update_data)
    assert resp.status_code == 200
    data = resp.json()
    assert data["name"] == "Updated Name"
    assert data["label"] == "New Label"


def test_delete_timing():
    """Test deleting a timing."""
    r = client.post("/soa", json={"name": "Delete Test"})
    soa_id = r.json()["id"]

    # Create timing
    timing_resp = client.post(f"/soa/{soa_id}/timings", json={"name": "To Delete"})
    timing_id = timing_resp.json()["id"]

    # Delete it
    resp = client.delete(f"/soa/{soa_id}/timings/{timing_id}")
    assert resp.status_code == 200
    assert resp.json()["deleted"] is True


def test_timing_uid_generation():
    """Test that timing_uid is auto-generated."""
    r = client.post("/soa", json={"name": "UID Gen Test"})
    soa_id = r.json()["id"]

    # Create first timing
    resp1 = client.post(f"/soa/{soa_id}/timings", json={"name": "Timing 1"})
    uid1 = resp1.json()["timing_uid"]
    assert uid1.startswith("Timing_")

    # Create second timing
    resp2 = client.post(f"/soa/{soa_id}/timings", json={"name": "Timing 2"})
    uid2 = resp2.json()["timing_uid"]
    assert uid2.startswith("Timing_")

    # UIDs should be different
    assert uid1 != uid2


def test_timing_with_relative_reference():
    """Test timing with relative_from_schedule_instance."""
    r = client.post("/soa", json={"name": "Relative Timing Test"})
    soa_id = r.json()["id"]

    # Create instance
    instance_resp = client.post(
        f"/soa/{soa_id}/instances", json={"name": "Reference Instance"}
    )
    instance_uid = instance_resp.json()["instance_uid"]

    # Create timing with reference
    timing_data = {
        "name": "Relative Timing",
        "value": "P7D",
        "relative_from_schedule_instance": instance_uid,
    }
    resp = client.post(f"/soa/{soa_id}/timings", json=timing_data)
    assert resp.status_code == 201
    data = resp.json()
    assert data["relative_from_schedule_instance"] == instance_uid


def test_timing_audit_trail():
    """Test that timing operations create audit records."""
    r = client.post("/soa", json={"name": "Audit Test"})
    soa_id = r.json()["id"]

    # Create timing
    client.post(f"/soa/{soa_id}/timings", json={"name": "Audited"})

    # Get audit trail
    audit_resp = client.get(f"/soa/{soa_id}/timing_audit")
    assert audit_resp.status_code == 200
    audit_data = audit_resp.json()

    # Should have at least one audit entry for create
    assert len(audit_data) > 0


def test_timing_with_window_fields():
    """Test timing with window_upper/window_lower fields."""
    r = client.post("/soa", json={"name": "Window Test"})
    soa_id = r.json()["id"]

    timing_data = {
        "name": "Windowed Timing",
        "value": "P7D",
        "window_lower": "P-2D",
        "window_upper": "P3D",
        "window_label": "Visit Window",
    }
    resp = client.post(f"/soa/{soa_id}/timings", json=timing_data)
    assert resp.status_code == 201
    data = resp.json()
    assert data["window_lower"] == "P-2D"
    assert data["window_upper"] == "P3D"
    assert data["window_label"] == "Visit Window"


def test_ui_list_timings():
    """Test UI view for listing timings."""
    r = client.post("/soa", json={"name": "UI List Test"})
    soa_id = r.json()["id"]

    resp = client.get(f"/ui/soa/{soa_id}/timings")
    assert resp.status_code == 200
    assert "text/html" in resp.headers.get("content-type", "")


def test_ui_create_timing():
    """Test creating timing via UI form."""
    r = client.post("/soa", json={"name": "UI Timing Test"})
    soa_id = r.json()["id"]

    form_data = {"name": "UI Timing", "value": "P1D"}
    resp = client.post(f"/ui/soa/{soa_id}/timings/create", data=form_data)
    assert resp.status_code == 200  # TestClient doesn't follow redirects


def test_ui_update_timing():
    """Test updating timing via UI form."""
    r = client.post("/soa", json={"name": "UI Update Test"})
    soa_id = r.json()["id"]

    # Create timing
    timing_resp = client.post(f"/soa/{soa_id}/timings", json={"name": "Original"})
    timing_id = timing_resp.json()["id"]

    # Update via UI
    form_data = {"name": "Updated via UI"}
    resp = client.post(f"/ui/soa/{soa_id}/timings/{timing_id}/update", data=form_data)
    assert resp.status_code == 200  # TestClient doesn't follow redirects


def test_ui_delete_timing():
    """Test deleting timing via UI form."""
    r = client.post("/soa", json={"name": "UI Delete Test"})
    soa_id = r.json()["id"]

    # Create timing
    timing_resp = client.post(f"/soa/{soa_id}/timings", json={"name": "To Delete"})
    timing_id = timing_resp.json()["id"]

    # Delete via UI
    resp = client.post(f"/ui/soa/{soa_id}/timings/{timing_id}/delete")
    assert resp.status_code == 200  # TestClient doesn't follow redirects


def test_timing_all_fields():
    """Test creating timing with all fields populated."""
    r = client.post("/soa", json={"name": "All Fields Test"})
    soa_id = r.json()["id"]

    timing_data = {
        "name": "Complete Timing",
        "label": "Test Label",
        "description": "Test Description",
        "type": "RELATIVE",
        "value": "P7D",
        "value_label": "7 days",
        "window_label": "Visit Window",
        "window_upper": "P2D",
        "window_lower": "P-2D",
    }
    resp = client.post(f"/soa/{soa_id}/timings", json=timing_data)
    assert resp.status_code == 201
    data = resp.json()
    assert data["name"] == "Complete Timing"
    assert data["label"] == "Test Label"
    assert data["type"] == "RELATIVE"
    assert data["value"] == "P7D"


def test_delete_nonexistent_timing():
    """Test deleting nonexistent timing returns 404."""
    r = client.post("/soa", json={"name": "Delete Nonexistent Test"})
    soa_id = r.json()["id"]

    resp = client.delete(f"/soa/{soa_id}/timings/999999")
    assert resp.status_code == 404


def test_update_nonexistent_timing():
    """Test updating nonexistent timing returns 404."""
    r = client.post("/soa", json={"name": "Update Nonexistent Test"})
    soa_id = r.json()["id"]

    resp = client.patch(f"/soa/{soa_id}/timings/999999", json={"name": "New Name"})
    assert resp.status_code == 404


def test_create_timing_empty_name():
    """Test creating timing with empty name fails."""
    r = client.post("/soa", json={"name": "Empty Name Test"})
    soa_id = r.json()["id"]

    timing_data = {"name": ""}
    resp = client.post(f"/soa/{soa_id}/timings", json=timing_data)
    assert resp.status_code == 400


def test_create_timing_nonexistent_soa():
    """Test creating timing for nonexistent SoA returns 404."""
    timing_data = {"name": "Test Timing"}
    resp = client.post("/soa/999999/timings", json=timing_data)
    assert resp.status_code == 404


def test_timing_bulk_create():
    """Test bulk creating timings."""
    r = client.post("/soa", json={"name": "Bulk Timings Test"})
    soa_id = r.json()["id"]

    # Create multiple timings
    for day in range(1, 8):
        timing_data = {"name": f"Day {day}", "value": f"P{day}D"}
        resp = client.post(f"/soa/{soa_id}/timings", json=timing_data)
        assert resp.status_code == 201

    # Verify all created
    list_resp = client.get(f"/soa/{soa_id}/timings")
    timings = list_resp.json()
    assert len(timings) == 7


def test_timing_member_of_timeline():
    """Test timing with member_of_timeline field."""
    r = client.post("/soa", json={"name": "Timeline Member Test"})
    soa_id = r.json()["id"]

    # Create timeline
    timeline_resp = client.post(
        f"/soa/{soa_id}/schedule_timelines", json={"name": "Main Timeline"}
    )
    timeline_uid = timeline_resp.json()["schedule_timeline_uid"]

    # Create timing as member
    timing_data = {
        "name": "Timeline Timing",
        "value": "P1D",
        "member_of_timeline": timeline_uid,
    }
    resp = client.post(f"/soa/{soa_id}/timings", json=timing_data)
    assert resp.status_code == 201
    data = resp.json()
    assert data["member_of_timeline"] == timeline_uid


def test_window_lower_rejects_non_iso8601():
    """Test that window_lower rejects non-ISO 8601 values."""
    r = client.post("/soa", json={"name": "Window Validate Lower"})
    soa_id = r.json()["id"]

    timing_data = {"name": "Bad Lower", "window_lower": "2 days"}
    resp = client.post(f"/soa/{soa_id}/timings", json=timing_data)
    assert resp.status_code == 422


def test_window_upper_rejects_non_iso8601():
    """Test that window_upper rejects non-ISO 8601 values."""
    r = client.post("/soa", json={"name": "Window Validate Upper"})
    soa_id = r.json()["id"]

    timing_data = {"name": "Bad Upper", "window_upper": "+3"}
    resp = client.post(f"/soa/{soa_id}/timings", json=timing_data)
    assert resp.status_code == 422


def test_window_accepts_valid_iso8601_durations():
    """Test that various valid ISO 8601 durations are accepted."""
    r = client.post("/soa", json={"name": "Window Valid ISO"})
    soa_id = r.json()["id"]

    valid_durations = [
        ("P1D", "P1D"),
        ("P2W", "P2W"),
        ("PT8H", "PT8H"),
        ("-P2D", "-P2D"),
        ("P-2D", "P-2D"),
        ("P1Y2M3D", "P1Y2M3D"),
        ("PT1H30M", "PT1H30M"),
    ]
    for i, (lower, upper) in enumerate(valid_durations):
        timing_data = {
            "name": f"Valid Duration {i}",
            "window_lower": lower,
            "window_upper": upper,
            "window_label": f"Window {i}",
        }
        resp = client.post(f"/soa/{soa_id}/timings", json=timing_data)
        assert resp.status_code == 201, f"Failed for duration: {lower}/{upper}"
        data = resp.json()
        assert data["window_lower"] == lower
        assert data["window_upper"] == upper


def test_window_rejects_bare_p():
    """Test that bare 'P' without any components is rejected."""
    r = client.post("/soa", json={"name": "Window Bare P"})
    soa_id = r.json()["id"]

    timing_data = {"name": "Bare P", "window_lower": "P"}
    resp = client.post(f"/soa/{soa_id}/timings", json=timing_data)
    assert resp.status_code == 422


def test_update_timing_rejects_invalid_window():
    """Test that PATCH update also validates window fields."""
    r = client.post("/soa", json={"name": "Update Window Validate"})
    soa_id = r.json()["id"]

    timing_resp = client.post(f"/soa/{soa_id}/timings", json={"name": "Good"})
    timing_id = timing_resp.json()["id"]

    resp = client.patch(
        f"/soa/{soa_id}/timings/{timing_id}",
        json={"name": "Good", "window_upper": "bad"},
    )
    assert resp.status_code == 422


def test_ui_create_timing_rejects_invalid_window():
    """Test that UI create form rejects non-ISO 8601 window values."""
    r = client.post("/soa", json={"name": "UI Window Validate"})
    soa_id = r.json()["id"]

    form_data = {"name": "Bad Window", "window_lower": "not-iso"}
    resp = client.post(
        f"/ui/soa/{soa_id}/timings/create", data=form_data, follow_redirects=False
    )
    assert resp.status_code == 400


def test_value_rejects_non_iso8601():
    """Test that value rejects non-ISO 8601 values."""
    r = client.post("/soa", json={"name": "Value Validate"})
    soa_id = r.json()["id"]

    timing_data = {"name": "Bad Value", "value": "5 days"}
    resp = client.post(f"/soa/{soa_id}/timings", json=timing_data)
    assert resp.status_code == 422


def test_value_rejects_plain_number():
    """Test that a plain number like '5' is rejected for value."""
    r = client.post("/soa", json={"name": "Value Plain Number"})
    soa_id = r.json()["id"]

    timing_data = {"name": "Plain Num", "value": "5"}
    resp = client.post(f"/soa/{soa_id}/timings", json=timing_data)
    assert resp.status_code == 422


def test_value_accepts_valid_iso8601_durations():
    """Test that various valid ISO 8601 durations are accepted for value."""
    r = client.post("/soa", json={"name": "Value Valid ISO"})
    soa_id = r.json()["id"]

    valid_values = ["P1D", "P2W", "PT8H", "-P2D", "P-2D", "P1Y2M3D", "PT1H30M"]
    for i, val in enumerate(valid_values):
        timing_data = {"name": f"Valid Value {i}", "value": val}
        resp = client.post(f"/soa/{soa_id}/timings", json=timing_data)
        assert resp.status_code == 201, f"Failed for value: {val}"
        assert resp.json()["value"] == val


def test_update_timing_rejects_invalid_value():
    """Test that PATCH update also validates value field."""
    r = client.post("/soa", json={"name": "Update Value Validate"})
    soa_id = r.json()["id"]

    timing_resp = client.post(
        f"/soa/{soa_id}/timings", json={"name": "Good", "value": "P1D"}
    )
    timing_id = timing_resp.json()["id"]

    resp = client.patch(
        f"/soa/{soa_id}/timings/{timing_id}",
        json={"name": "Good", "value": "not-a-duration"},
    )
    assert resp.status_code == 422


def test_ui_create_timing_rejects_invalid_value():
    """Test that UI create form rejects non-ISO 8601 value."""
    r = client.post("/soa", json={"name": "UI Value Validate"})
    soa_id = r.json()["id"]

    form_data = {"name": "Bad Value", "value": "two weeks"}
    resp = client.post(
        f"/ui/soa/{soa_id}/timings/create", data=form_data, follow_redirects=False
    )
    assert resp.status_code == 400


def test_window_all_or_none_accepts_all_three():
    """Test that providing all three window fields is accepted."""
    r = client.post("/soa", json={"name": "Window Complete"})
    soa_id = r.json()["id"]

    resp = client.post(
        f"/soa/{soa_id}/timings",
        json={
            "name": "T1",
            "window_lower": "-P1D",
            "window_upper": "P2D",
            "window_label": "Visit Window",
        },
    )
    assert resp.status_code == 201


def test_window_all_or_none_accepts_none():
    """Test that providing no window fields is accepted."""
    r = client.post("/soa", json={"name": "Window None"})
    soa_id = r.json()["id"]

    resp = client.post(
        f"/soa/{soa_id}/timings",
        json={"name": "No Window"},
    )
    assert resp.status_code == 201
    data = resp.json()
    assert data["window_lower"] is None
    assert data["window_upper"] is None
    assert data["window_label"] is None
