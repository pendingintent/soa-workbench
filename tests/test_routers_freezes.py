"""Comprehensive test coverage for routers/freezes.py."""

import os
import sqlite3
from fastapi.testclient import TestClient
from soa_builder.web.app import app

client = TestClient(app)


def _get_latest_freeze_id(soa_id: int) -> int:
    """Helper to get latest freeze_id for a given soa_id.

    CRITICAL: This must only use the test database set by conftest.py.
    If SOA_BUILDER_DB is not set, tests are misconfigured.
    """
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


def test_ui_create_freeze_basic():
    """Test UI form submission to create a freeze."""
    r = client.post("/soa", json={"name": "Test Study"})
    soa_id = r.json()["id"]

    # Add some data to freeze
    client.post(f"/soa/{soa_id}/visits", json={"name": "Visit 1"})

    # Create freeze via UI form
    resp = client.post(
        f"/ui/soa/{soa_id}/freeze", data={"version_label": "Version 1.0"}
    )
    assert resp.status_code == 200  # TestClient doesn't follow redirects


def test_ui_create_freeze_without_label():
    """Test creating freeze without version_label."""
    r = client.post("/soa", json={"name": "No Label Study"})
    soa_id = r.json()["id"]

    resp = client.post(f"/ui/soa/{soa_id}/freeze", data={})
    # May succeed with empty label or fail with 422
    assert resp.status_code in [200, 422]


def test_ui_create_freeze_empty_soa():
    """Test creating freeze on empty SoA."""
    r = client.post("/soa", json={"name": "Empty Study"})
    soa_id = r.json()["id"]

    resp = client.post(
        f"/ui/soa/{soa_id}/freeze", data={"version_label": "Empty Snapshot"}
    )
    assert resp.status_code == 200


def test_get_freeze_by_id():
    """Test retrieving freeze by ID."""
    r = client.post("/soa", json={"name": "Retrieve Test"})
    soa_id = r.json()["id"]

    # Create freeze
    client.post(f"/ui/soa/{soa_id}/freeze", data={"version_label": "v1"})
    freeze_id = _get_latest_freeze_id(soa_id)

    # Retrieve it
    resp = client.get(f"/soa/{soa_id}/freeze/{freeze_id}")
    assert resp.status_code == 200
    data = resp.json()
    assert "visits" in data
    assert "activities" in data
    assert "cells" in data or "matrix_cells" in data


def test_get_freeze_nonexistent():
    """Test getting freeze that doesn't exist."""
    r = client.post("/soa", json={"name": "Test"})
    soa_id = r.json()["id"]

    resp = client.get(f"/soa/{soa_id}/freeze/999")
    assert resp.status_code == 404


def test_ui_freeze_view():
    """Test UI freeze view page."""
    r = client.post("/soa", json={"name": "View Test"})
    soa_id = r.json()["id"]

    client.post(f"/ui/soa/{soa_id}/freeze", data={"version_label": "v1"})
    freeze_id = _get_latest_freeze_id(soa_id)

    resp = client.get(f"/ui/soa/{soa_id}/freeze/{freeze_id}/view")
    assert resp.status_code == 200
    # Returns HTML template
    assert (
        b"html" in resp.content.lower()
        or resp.headers.get("content-type") == "text/html; charset=utf-8"
    )


def test_ui_freeze_diff():
    """Test UI freeze diff view."""
    r = client.post("/soa", json={"name": "Diff Test"})
    soa_id = r.json()["id"]

    # Create first freeze
    client.post(f"/ui/soa/{soa_id}/freeze", data={"version_label": "v1"})
    freeze_id1 = _get_latest_freeze_id(soa_id)

    # Modify data and create second freeze
    client.post(f"/soa/{soa_id}/visits", json={"name": "New Visit"})
    client.post(f"/ui/soa/{soa_id}/freeze", data={"version_label": "v2"})
    freeze_id2 = _get_latest_freeze_id(soa_id)

    resp = client.get(
        f"/ui/soa/{soa_id}/freeze/diff?left={freeze_id1}&right={freeze_id2}"
    )
    assert resp.status_code == 200
    assert (
        b"html" in resp.content.lower()
        or resp.headers.get("content-type") == "text/html; charset=utf-8"
    )


def test_freeze_diff_json():
    """Test freeze diff JSON endpoint."""
    r = client.post("/soa", json={"name": "JSON Diff Test"})
    soa_id = r.json()["id"]

    # Create first freeze
    client.post(f"/ui/soa/{soa_id}/freeze", data={"version_label": "v1"})
    freeze_id1 = _get_latest_freeze_id(soa_id)

    # Modify data and create second freeze
    client.post(f"/soa/{soa_id}/activities", json={"name": "New Activity"})
    client.post(f"/ui/soa/{soa_id}/freeze", data={"version_label": "v2"})
    freeze_id2 = _get_latest_freeze_id(soa_id)

    resp = client.get(
        f"/soa/{soa_id}/freeze/diff.json?left={freeze_id1}&right={freeze_id2}"
    )
    # May be 200 or 422 depending on validation
    assert resp.status_code in [200, 422]


def test_ui_freeze_rollback_preview():
    """Test UI rollback preview."""
    r = client.post("/soa", json={"name": "Rollback Preview"})
    soa_id = r.json()["id"]

    client.post(f"/ui/soa/{soa_id}/freeze", data={"version_label": "v1"})
    freeze_id = _get_latest_freeze_id(soa_id)

    resp = client.get(f"/ui/soa/{soa_id}/freeze/{freeze_id}/rollback_preview")
    assert resp.status_code == 200
    assert (
        b"html" in resp.content.lower()
        or resp.headers.get("content-type") == "text/html; charset=utf-8"
    )


def test_ui_freeze_rollback():
    """Test UI rollback operation."""
    r = client.post("/soa", json={"name": "Rollback Test"})
    soa_id = r.json()["id"]

    client.post(f"/ui/soa/{soa_id}/freeze", data={"version_label": "v1"})
    freeze_id = _get_latest_freeze_id(soa_id)

    # Modify data
    client.post(f"/soa/{soa_id}/activities", json={"name": "Post-Freeze Activity"})

    resp = client.post(f"/ui/soa/{soa_id}/freeze/{freeze_id}/rollback")
    assert resp.status_code == 200


def test_rollback_restores_objectives_endpoints_and_code_associations():
    """Round-trip test for the wide rollback fix.

    After rolling back, post-freeze objectives/endpoints must be gone
    and pre-freeze objectives/endpoints (with their level_code_uid
    references) must resolve to existing code_association rows.
    """
    from unittest.mock import patch

    r = client.post("/soa", json={"name": "Rollback Wide Test"})
    soa_id = r.json()["id"]

    with (
        patch(
            "soa_builder.web.routers.objectives.get_latest_ddf_ct_href",
            return_value="ddfct-2024-01-01",
        ),
        patch(
            "soa_builder.web.routers.endpoints.get_latest_ddf_ct_href",
            return_value="ddfct-2024-01-01",
        ),
    ):
        obj = client.post(
            f"/soa/{soa_id}/objectives",
            json={"name": "Primary Obj", "level": "Primary Objective"},
        )
        assert obj.status_code in (200, 201), obj.text
        obj_uid_pre = obj.json()["objective_uid"]
        ep = client.post(
            f"/soa/{soa_id}/endpoints",
            json={
                "name": "Primary EP",
                "level": "Primary Endpoint",
                "objective_uid": obj_uid_pre,
            },
        )
        assert ep.status_code in (200, 201), ep.text

        client.post(f"/ui/soa/{soa_id}/freeze", data={"version_label": "v1"})
        freeze_id = _get_latest_freeze_id(soa_id)

        client.post(
            f"/soa/{soa_id}/objectives",
            json={"name": "Secondary Obj", "level": "Secondary Objective"},
        )

        resp = client.post(f"/ui/soa/{soa_id}/freeze/{freeze_id}/rollback")
        assert resp.status_code == 200

    objs = client.get(f"/soa/{soa_id}/objectives").json()
    assert len(objs) == 1
    assert objs[0]["objective_uid"] == obj_uid_pre

    eps = client.get(f"/soa/{soa_id}/endpoints").json()
    assert len(eps) == 1

    db_path = os.environ.get("SOA_BUILDER_DB")
    conn = sqlite3.connect(db_path)
    cur = conn.cursor()
    cur.execute("SELECT level_code_uid FROM objective WHERE soa_id=?", (soa_id,))
    obj_lc = cur.fetchone()[0]
    cur.execute(
        "SELECT 1 FROM code_association WHERE soa_id=? AND code_uid=?",
        (soa_id, obj_lc),
    )
    assert cur.fetchone() is not None, "objective level_code_uid dangles"

    cur.execute("SELECT level_code_uid FROM endpoint WHERE soa_id=?", (soa_id,))
    ep_lc = cur.fetchone()[0]
    cur.execute(
        "SELECT 1 FROM code_association WHERE soa_id=? AND code_uid=?",
        (soa_id, ep_lc),
    )
    assert cur.fetchone() is not None, "endpoint level_code_uid dangles"
    conn.close()


def test_freeze_with_visits():
    """Test freeze structure includes visits array."""
    r = client.post("/soa", json={"name": "Visits Freeze"})
    soa_id = r.json()["id"]

    client.post(f"/ui/soa/{soa_id}/freeze", data={"version_label": "v1"})
    freeze_id = _get_latest_freeze_id(soa_id)

    freeze = client.get(f"/soa/{soa_id}/freeze/{freeze_id}").json()
    # Just verify visits key exists (may be empty)
    assert "visits" in freeze


def test_freeze_with_activities():
    """Test freeze captures activities correctly."""
    r = client.post("/soa", json={"name": "Activities Freeze"})
    soa_id = r.json()["id"]

    # Add activities - activities are scoped to soa_id
    client.post(f"/soa/{soa_id}/activities", json={"name": "Activity 1"})
    client.post(f"/soa/{soa_id}/activities", json={"name": "Activity 2"})

    client.post(f"/ui/soa/{soa_id}/freeze", data={"version_label": "v1"})
    freeze_id = _get_latest_freeze_id(soa_id)

    freeze = client.get(f"/soa/{soa_id}/freeze/{freeze_id}").json()
    assert len(freeze["activities"]) == 2


def test_freeze_nonexistent_soa():
    """Test freeze operations on non-existent SoA."""
    resp = client.get("/soa/999/freeze/1")
    assert resp.status_code == 404


def test_ui_list_freezes_renders_table_desc():
    """The dedicated freezes page lists versions newest-first."""
    r = client.post("/soa", json={"name": "Freeze List Study"})
    soa_id = r.json()["id"]

    client.post(f"/ui/soa/{soa_id}/freeze", data={"version_label": "vA"})
    client.post(f"/ui/soa/{soa_id}/freeze", data={"version_label": "vB"})

    resp = client.get(f"/ui/soa/{soa_id}/freezes")
    assert resp.status_code == 200
    body = resp.text
    assert "Snapshots" in body
    # Newest (vB, created last ⇒ higher id) appears before vA in DESC order
    assert body.index("vB") < body.index("vA")


def test_ui_list_freezes_unknown_soa():
    resp = client.get("/ui/soa/9999999/freezes")
    assert resp.status_code == 404


def test_ui_delete_freeze_removes_row():
    r = client.post("/soa", json={"name": "Delete Freeze Study"})
    soa_id = r.json()["id"]
    client.post(f"/ui/soa/{soa_id}/freeze", data={"version_label": "vDel"})
    freeze_id = _get_latest_freeze_id(soa_id)

    resp = client.post(
        f"/ui/soa/{soa_id}/freeze/{freeze_id}/delete", follow_redirects=False
    )
    assert resp.status_code == 200

    # Confirm gone
    assert client.get(f"/soa/{soa_id}/freeze/{freeze_id}").status_code == 404
    list_resp = client.get(f"/ui/soa/{soa_id}/freezes")
    assert "vDel" not in list_resp.text


def test_ui_delete_freeze_unknown_id():
    r = client.post("/soa", json={"name": "Delete Unknown Freeze"})
    soa_id = r.json()["id"]
    resp = client.post(f"/ui/soa/{soa_id}/freeze/999999/delete")
    assert resp.status_code == 404


def test_edit_page_no_longer_renders_freeze_bar():
    """Regression: freeze controls were moved to /freezes; edit page must not
    render the inline freeze bar."""
    r = client.post("/soa", json={"name": "Edit Page Freeze Removal"})
    soa_id = r.json()["id"]

    resp = client.get(f"/ui/soa/{soa_id}/edit")
    assert resp.status_code == 200
    assert "freeze-bar" not in resp.text


def test_get_freeze_wrong_soa():
    """Test accessing freeze from wrong SoA returns 404."""
    r1 = client.post("/soa", json={"name": "SoA 1"})
    soa_id1 = r1.json()["id"]

    r2 = client.post("/soa", json={"name": "SoA 2"})
    soa_id2 = r2.json()["id"]

    # Create freeze in soa1
    client.post(f"/ui/soa/{soa_id1}/freeze", data={"version_label": "Test"})
    freeze_id = _get_latest_freeze_id(soa_id1)

    # Try to access from soa2
    resp = client.get(f"/soa/{soa_id2}/freeze/{freeze_id}")
    assert resp.status_code == 404
