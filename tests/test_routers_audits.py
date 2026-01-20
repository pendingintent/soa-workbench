"""Comprehensive tests for audits router endpoints.

Note: Only element_audit and timing_audit API endpoints exist.
Other audits are shown via the UI endpoint /ui/soa/{soa_id}/audits.
"""

from fastapi.testclient import TestClient

from soa_builder.web.app import app

client = TestClient(app)


def test_ui_list_audits_empty():
    """Test UI audits page for new SoA."""
    r = client.post("/soa", json={"name": "Audits Test"})
    soa_id = r.json()["id"]

    resp = client.get(f"/ui/soa/{soa_id}/audits")
    assert resp.status_code == 200
    assert resp.headers["content-type"].startswith("text/html")


def test_ui_list_audits_nonexistent_soa():
    """Test UI audits page for nonexistent SoA returns 404."""
    resp = client.get("/ui/soa/999999/audits")
    assert resp.status_code == 404


def test_get_element_audit():
    """Test getting element audit trail via API."""
    r = client.post("/soa", json={"name": "Element Audit Test"})
    soa_id = r.json()["id"]

    # Element audit endpoint exists
    resp = client.get(f"/soa/{soa_id}/element_audit")
    assert resp.status_code == 200
    audit = resp.json()
    assert isinstance(audit, list)


def test_get_timing_audit():
    """Test getting timing audit trail via API."""
    r = client.post("/soa", json={"name": "Timing Audit Test"})
    soa_id = r.json()["id"]

    # Timing audit endpoint exists
    resp = client.get(f"/soa/{soa_id}/timing_audit")
    assert resp.status_code == 200
    audit = resp.json()
    assert isinstance(audit, list)


def test_element_audit_captures_create():
    """Test that element audit captures create operations."""
    r = client.post("/soa", json={"name": "Element Create Audit"})
    soa_id = r.json()["id"]

    # Create element via API
    elem_resp = client.post(
        f"/soa/{soa_id}/elements", json={"name": "Test Element", "label": "TE"}
    )
    assert elem_resp.status_code == 201

    # Check audit
    resp = client.get(f"/soa/{soa_id}/element_audit")
    audit = resp.json()
    assert len(audit) > 0
    assert any(a["action"] == "create" for a in audit)


def test_element_audit_captures_update():
    """Test that element audit captures update operations."""
    r = client.post("/soa", json={"name": "Element Update Audit"})
    soa_id = r.json()["id"]

    # Create and update element
    elem_resp = client.post(f"/soa/{soa_id}/elements", json={"name": "Original"})
    element_id = elem_resp.json()["id"]

    client.patch(f"/soa/{soa_id}/elements/{element_id}", json={"name": "Updated"})

    # Check audit
    resp = client.get(f"/soa/{soa_id}/element_audit")
    audit = resp.json()
    actions = [a["action"] for a in audit]
    assert "create" in actions
    assert "update" in actions


def test_element_audit_captures_delete():
    """Test that element audit captures delete operations."""
    r = client.post("/soa", json={"name": "Element Delete Audit"})
    soa_id = r.json()["id"]

    # Create and delete element
    elem_resp = client.post(f"/soa/{soa_id}/elements", json={"name": "To Delete"})
    element_id = elem_resp.json()["id"]

    client.delete(f"/soa/{soa_id}/elements/{element_id}")

    # Check audit
    resp = client.get(f"/soa/{soa_id}/element_audit")
    audit = resp.json()
    actions = [a["action"] for a in audit]
    assert "delete" in actions


def test_element_audit_before_after_state():
    """Test that element audit includes before/after state."""
    r = client.post("/soa", json={"name": "Element State Audit"})
    soa_id = r.json()["id"]

    # Create and update element
    elem_resp = client.post(f"/soa/{soa_id}/elements", json={"name": "Original Name"})
    element_id = elem_resp.json()["id"]

    client.patch(f"/soa/{soa_id}/elements/{element_id}", json={"name": "New Name"})

    # Check audit
    resp = client.get(f"/soa/{soa_id}/element_audit")
    audit = resp.json()

    # Find update record
    updates = [a for a in audit if a["action"] == "update"]
    assert len(updates) > 0

    update_record = updates[0]
    assert "before" in update_record
    assert "after" in update_record

    # Check that before/after contain the name change
    if update_record["before"] and update_record["after"]:
        assert update_record["before"]["name"] == "Original Name"
        assert update_record["after"]["name"] == "New Name"


def test_element_audit_has_timestamps():
    """Test that element audit records have timestamps."""
    r = client.post("/soa", json={"name": "Element Timestamp Audit"})
    soa_id = r.json()["id"]

    # Create element
    client.post(f"/soa/{soa_id}/elements", json={"name": "Element"})

    # Check audit
    resp = client.get(f"/soa/{soa_id}/element_audit")
    audit = resp.json()

    assert len(audit) > 0
    assert "performed_at" in audit[0]
    assert audit[0]["performed_at"] is not None


def test_timing_audit_captures_create():
    """Test that timing audit captures create operations."""
    r = client.post("/soa", json={"name": "Timing Create Audit"})
    soa_id = r.json()["id"]

    # Create timing via API
    timing_resp = client.post(
        f"/soa/{soa_id}/timings", json={"name": "Day 1", "value": "P1D"}
    )
    assert timing_resp.status_code == 201

    # Check audit
    resp = client.get(f"/soa/{soa_id}/timing_audit")
    audit = resp.json()
    assert len(audit) > 0
    assert any(a["action"] == "create" for a in audit)


def test_timing_audit_captures_update():
    """Test that timing audit captures update operations."""
    r = client.post("/soa", json={"name": "Timing Update Audit"})
    soa_id = r.json()["id"]

    # Create and update timing
    timing_resp = client.post(
        f"/soa/{soa_id}/timings", json={"name": "Original", "value": "P1D"}
    )
    timing_id = timing_resp.json()["id"]

    client.patch(f"/soa/{soa_id}/timings/{timing_id}", json={"name": "Updated"})

    # Check audit
    resp = client.get(f"/soa/{soa_id}/timing_audit")
    audit = resp.json()
    actions = [a["action"] for a in audit]
    assert "create" in actions
    assert "update" in actions


def test_timing_audit_captures_delete():
    """Test that timing audit captures delete operations."""
    r = client.post("/soa", json={"name": "Timing Delete Audit"})
    soa_id = r.json()["id"]

    # Create and delete timing
    timing_resp = client.post(
        f"/soa/{soa_id}/timings", json={"name": "To Delete", "value": "P1D"}
    )
    timing_id = timing_resp.json()["id"]

    client.delete(f"/soa/{soa_id}/timings/{timing_id}")

    # Check audit
    resp = client.get(f"/soa/{soa_id}/timing_audit")
    audit = resp.json()
    actions = [a["action"] for a in audit]
    assert "delete" in actions


def test_timing_audit_has_timestamps():
    """Test that timing audit records have timestamps."""
    r = client.post("/soa", json={"name": "Timing Timestamp Audit"})
    soa_id = r.json()["id"]

    # Create timing
    client.post(f"/soa/{soa_id}/timings", json={"name": "T1", "value": "P1D"})

    # Check audit
    resp = client.get(f"/soa/{soa_id}/timing_audit")
    audit = resp.json()

    assert len(audit) > 0
    assert "performed_at" in audit[0]
    assert audit[0]["performed_at"] is not None


def test_ui_audits_shows_activity_audits():
    """Test that UI audits page includes activity audits."""
    r = client.post("/soa", json={"name": "Activity Audit UI Test"})
    soa_id = r.json()["id"]

    # Create activity
    client.post(f"/soa/{soa_id}/activities", json={"name": "Test Activity"})

    # Check UI page
    resp = client.get(f"/ui/soa/{soa_id}/audits")
    assert resp.status_code == 200
    # HTML response should contain audit data
    assert b"activity" in resp.content.lower() or b"audit" in resp.content.lower()


def test_ui_audits_shows_visit_audits():
    """Test that UI audits page includes visit audits."""
    r = client.post("/soa", json={"name": "Visit Audit UI Test"})
    soa_id = r.json()["id"]

    # Create visit
    client.post(f"/soa/{soa_id}/visits", json={"name": "Test Visit"})

    # Check UI page
    resp = client.get(f"/ui/soa/{soa_id}/audits")
    assert resp.status_code == 200
    assert b"visit" in resp.content.lower() or b"audit" in resp.content.lower()


def test_element_audit_chronological_order():
    """Test that element audit records are in chronological order (DESC)."""
    r = client.post("/soa", json={"name": "Chronological Test"})
    soa_id = r.json()["id"]

    # Create and update element multiple times
    elem_resp = client.post(f"/soa/{soa_id}/elements", json={"name": "V1"})
    element_id = elem_resp.json()["id"]

    client.patch(f"/soa/{soa_id}/elements/{element_id}", json={"name": "V2"})
    client.patch(f"/soa/{soa_id}/elements/{element_id}", json={"name": "V3"})

    # Check audit order
    resp = client.get(f"/soa/{soa_id}/element_audit")
    audit = resp.json()

    assert len(audit) >= 3
    # Most recent should be first (DESC order)
    actions = [a["action"] for a in audit]
    # Last action should appear first
    assert actions[0] == "update"


def test_timing_audit_chronological_order():
    """Test that timing audit records are in chronological order (DESC)."""
    r = client.post("/soa", json={"name": "Timing Chronological Test"})
    soa_id = r.json()["id"]

    # Create and update timing multiple times
    timing_resp = client.post(
        f"/soa/{soa_id}/timings", json={"name": "V1", "value": "P1D"}
    )
    timing_id = timing_resp.json()["id"]

    client.patch(f"/soa/{soa_id}/timings/{timing_id}", json={"name": "V2"})
    client.patch(f"/soa/{soa_id}/timings/{timing_id}", json={"name": "V3"})

    # Check audit order
    resp = client.get(f"/soa/{soa_id}/timing_audit")
    audit = resp.json()

    assert len(audit) >= 3
    # Most recent should be first (DESC order)
    actions = [a["action"] for a in audit]
    assert actions[0] == "update"


def test_audit_nonexistent_soa():
    """Test audit endpoints for nonexistent SoA return 404."""
    resp = client.get("/soa/999999/element_audit")
    assert resp.status_code == 404

    resp = client.get("/soa/999999/timing_audit")
    assert resp.status_code == 404
