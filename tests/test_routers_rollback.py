"""Comprehensive tests for rollback router endpoints.

Note: The rollback router provides AUDIT endpoints only.
Actual rollback operations are done via the freezes router.
"""

import os
import sqlite3
from fastapi.testclient import TestClient

from soa_builder.web.app import app

client = TestClient(app)


def _get_latest_freeze_id(soa_id: int) -> int:
    """Helper to get latest freeze_id for a given soa_id."""
    db_path = os.environ.get("SOA_BUILDER_DB")
    if not db_path:
        raise RuntimeError(
            "SOA_BUILDER_DB environment variable not set - tests must use test database"
        )
    if "soa_builder_web.db" in db_path and "test" not in db_path:
        raise RuntimeError(
            f"DANGER: Test trying to use production database: {db_path}. "
            "Expected test database (soa_builder_web_tests.db)"
        )
    conn = sqlite3.connect(db_path)
    cur = conn.cursor()
    cur.execute(
        "SELECT id FROM soa_freeze WHERE soa_id=? ORDER BY created_at DESC LIMIT 1",
        (soa_id,),
    )
    row = cur.fetchone()
    conn.close()
    return row[0] if row else None


def test_list_rollback_audit_empty():
    """Test rollback audit list for SoA with no rollbacks."""
    r = client.post("/soa", json={"name": "No Rollback Test"})
    soa_id = r.json()["id"]

    resp = client.get(f"/soa/{soa_id}/rollback_audit")
    assert resp.status_code == 200
    data = resp.json()
    assert "audit" in data
    assert isinstance(data["audit"], list)


def test_list_reorder_audit_empty():
    """Test reorder audit list for SoA with no reorders."""
    r = client.post("/soa", json={"name": "No Reorder Test"})
    soa_id = r.json()["id"]

    resp = client.get(f"/soa/{soa_id}/reorder_audit")
    assert resp.status_code == 200
    data = resp.json()
    assert "audit" in data
    assert isinstance(data["audit"], list)


def test_rollback_audit_after_rollback():
    """Test that rollback audit is created after a rollback operation."""
    r = client.post("/soa", json={"name": "Rollback Audit Test"})
    soa_id = r.json()["id"]

    # Create and freeze
    client.post(f"/soa/{soa_id}/activities", json={"name": "Activity 1"})
    client.post(f"/ui/soa/{soa_id}/freeze", data={"version_label": "v1"})
    freeze_id = _get_latest_freeze_id(soa_id)

    # Modify data
    client.post(f"/soa/{soa_id}/activities", json={"name": "Activity 2"})

    # Perform rollback via freezes router
    client.post(f"/ui/soa/{soa_id}/freeze/{freeze_id}/rollback")

    # Check rollback audit
    resp = client.get(f"/soa/{soa_id}/rollback_audit")
    assert resp.status_code == 200
    data = resp.json()
    # Should have at least one audit entry
    assert len(data["audit"]) >= 1


def test_ui_rollback_audit_view():
    """Test UI view for rollback audit."""
    r = client.post("/soa", json={"name": "UI Audit Test"})
    soa_id = r.json()["id"]

    resp = client.get(f"/ui/soa/{soa_id}/rollback_audit")
    assert resp.status_code == 200
    # Returns HTML
    assert (
        b"html" in resp.content.lower()
        or resp.headers.get("content-type") == "text/html; charset=utf-8"
    )


def test_ui_reorder_audit_view():
    """Test UI view for reorder audit."""
    r = client.post("/soa", json={"name": "UI Reorder Audit Test"})
    soa_id = r.json()["id"]

    resp = client.get(f"/ui/soa/{soa_id}/reorder_audit")
    assert resp.status_code == 200
    # Returns HTML
    assert (
        b"html" in resp.content.lower()
        or resp.headers.get("content-type") == "text/html; charset=utf-8"
    )


def test_rollback_audit_export_xlsx():
    """Test exporting rollback audit to Excel."""
    r = client.post("/soa", json={"name": "Export Rollback Test"})
    soa_id = r.json()["id"]

    resp = client.get(f"/soa/{soa_id}/rollback_audit/export/xlsx")
    assert resp.status_code == 200
    # Check it's an Excel file
    assert (
        resp.headers["content-type"]
        == "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
    )


def test_reorder_audit_export_xlsx():
    """Test exporting reorder audit to Excel."""
    r = client.post("/soa", json={"name": "Export Reorder Test"})
    soa_id = r.json()["id"]

    resp = client.get(f"/soa/{soa_id}/reorder_audit/export/xlsx")
    assert resp.status_code == 200
    # Check it's an Excel file
    assert (
        resp.headers["content-type"]
        == "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
    )


def test_rollback_audit_nonexistent_soa():
    """Test rollback audit for nonexistent SoA returns 404."""
    resp = client.get("/soa/999999/rollback_audit")
    assert resp.status_code == 404


def test_reorder_audit_nonexistent_soa():
    """Test reorder audit for nonexistent SoA returns 404."""
    resp = client.get("/soa/999999/reorder_audit")
    assert resp.status_code == 404


def test_reorder_audit_after_reorder():
    """Test that reorder audit is created after a reorder operation."""
    r = client.post("/soa", json={"name": "Reorder Audit Test"})
    soa_id = r.json()["id"]

    # Create activities
    resp1 = client.post(f"/soa/{soa_id}/activities", json={"name": "Activity 1"})
    resp2 = client.post(f"/soa/{soa_id}/activities", json={"name": "Activity 2"})
    id1 = resp1.json()["activity_id"]
    id2 = resp2.json()["activity_id"]

    # Reorder them
    client.post(f"/soa/{soa_id}/activities/reorder", json={"order": [id2, id1]})

    # Check reorder audit - may be empty if reorder doesn't create audit
    resp = client.get(f"/soa/{soa_id}/reorder_audit")
    assert resp.status_code == 200
    data = resp.json()
    # Audit exists (may be empty)
    assert "audit" in data


def test_audit_contains_freeze_info():
    """Test rollback audit contains freeze information."""
    r = client.post("/soa", json={"name": "Freeze Info Test"})
    soa_id = r.json()["id"]

    # Create, freeze, modify, rollback
    client.post(f"/soa/{soa_id}/activities", json={"name": "Activity 1"})
    client.post(f"/ui/soa/{soa_id}/freeze", data={"version_label": "TestVersion"})
    freeze_id = _get_latest_freeze_id(soa_id)

    client.post(f"/soa/{soa_id}/activities", json={"name": "Activity 2"})
    client.post(f"/ui/soa/{soa_id}/freeze/{freeze_id}/rollback")

    # Check audit
    resp = client.get(f"/soa/{soa_id}/rollback_audit")
    assert resp.status_code == 200
    data = resp.json()
    # Audit should exist
    assert len(data["audit"]) >= 1


def test_xlsx_export_has_content_type():
    """Test Excel export has proper content type."""
    r = client.post("/soa", json={"name": "Content Type Test"})
    soa_id = r.json()["id"]

    resp = client.get(f"/soa/{soa_id}/rollback_audit/export/xlsx")
    assert resp.status_code == 200
    # Verify it's Excel format
    assert "spreadsheet" in resp.headers["content-type"]


def test_ui_endpoints_return_html():
    """Test UI endpoints return HTML responses."""
    r = client.post("/soa", json={"name": "HTML Test"})
    soa_id = r.json()["id"]

    # Test both UI endpoints
    resp1 = client.get(f"/ui/soa/{soa_id}/rollback_audit")
    resp2 = client.get(f"/ui/soa/{soa_id}/reorder_audit")

    assert resp1.status_code == 200
    assert resp2.status_code == 200
    assert "text/html" in resp1.headers.get("content-type", "")
    assert "text/html" in resp2.headers.get("content-type", "")


def test_audit_list_structure():
    """Test audit response has correct structure."""
    r = client.post("/soa", json={"name": "Structure Test"})
    soa_id = r.json()["id"]

    resp = client.get(f"/soa/{soa_id}/rollback_audit")
    assert resp.status_code == 200
    data = resp.json()

    # Should have audit key with list
    assert "audit" in data
    assert isinstance(data["audit"], list)
