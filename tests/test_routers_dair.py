"""Tests for the DAIR (Digital Amendment Impact Report) router."""

import os
import sqlite3

from fastapi.testclient import TestClient

from soa_builder.web.app import app

client = TestClient(app)

_DOCX_CONTENT_TYPE = (
    "application/vnd.openxmlformats-officedocument.wordprocessingml.document"
)


def _get_db_path() -> str:
    db_path = os.environ.get("SOA_BUILDER_DB")
    if not db_path:
        raise RuntimeError("SOA_BUILDER_DB not set")
    return db_path


def _create_freeze(soa_id: int, label: str) -> int:
    """Create a freeze and return its id."""
    resp = client.post(f"/ui/soa/{soa_id}/freeze", data={"version_label": label})
    assert resp.status_code in (200, 302, 303)
    db = _get_db_path()
    conn = sqlite3.connect(db)
    cur = conn.cursor()
    cur.execute(
        "SELECT id FROM soa_freeze WHERE soa_id=? AND version_label=?",
        (soa_id, label),
    )
    row = cur.fetchone()
    conn.close()
    assert row, f"Freeze '{label}' not found after creation"
    return row[0]


# ---------------------------------------------------------------------------
# UI page tests
# ---------------------------------------------------------------------------


def test_ui_dair_404_nonexistent_soa():
    resp = client.get("/ui/soa/999999/dair")
    assert resp.status_code == 404


def test_ui_dair_no_freezes():
    r = client.post("/soa", json={"name": "DAIR No Freezes"})
    soa_id = r.json()["id"]
    resp = client.get(f"/ui/soa/{soa_id}/dair")
    assert resp.status_code == 200
    assert "text/html" in resp.headers.get("content-type", "")
    assert "snapshots" in resp.text.lower() or "snapshot" in resp.text.lower()


def test_ui_dair_one_freeze_shows_warning():
    r = client.post("/soa", json={"name": "DAIR One Freeze"})
    soa_id = r.json()["id"]
    _create_freeze(soa_id, "v1")
    resp = client.get(f"/ui/soa/{soa_id}/dair")
    assert resp.status_code == 200
    assert "base_freeze_id" not in resp.text


def test_ui_dair_with_two_freezes_shows_form():
    r = client.post("/soa", json={"name": "DAIR Two Freezes"})
    soa_id = r.json()["id"]
    _create_freeze(soa_id, "v1")
    client.post(f"/soa/{soa_id}/visits", json={"name": "Visit 1"})
    _create_freeze(soa_id, "v2")
    resp = client.get(f"/ui/soa/{soa_id}/dair")
    assert resp.status_code == 200
    assert "base_freeze_id" in resp.text
    assert "revised_freeze_id" in resp.text
    assert "v1" in resp.text
    assert "v2" in resp.text


# ---------------------------------------------------------------------------
# Download endpoint tests
# ---------------------------------------------------------------------------


def test_download_dair_404_nonexistent_soa():
    resp = client.get("/soa/999999/dair/download?base_freeze_id=1&revised_freeze_id=2")
    assert resp.status_code == 404


def test_download_dair_same_freeze_ids():
    r = client.post("/soa", json={"name": "DAIR Same ID"})
    soa_id = r.json()["id"]
    fid = _create_freeze(soa_id, "v1")
    resp = client.get(
        f"/soa/{soa_id}/dair/download?base_freeze_id={fid}&revised_freeze_id={fid}"
    )
    assert resp.status_code == 400


def test_download_dair_invalid_freeze():
    r = client.post("/soa", json={"name": "DAIR Bad Freeze"})
    soa_id = r.json()["id"]
    resp = client.get(
        f"/soa/{soa_id}/dair/download?base_freeze_id=99998&revised_freeze_id=99999"
    )
    assert resp.status_code == 404


def test_download_dair_returns_docx():
    r = client.post("/soa", json={"name": "DAIR Download Test"})
    soa_id = r.json()["id"]

    # Create base freeze
    fid1 = _create_freeze(soa_id, "baseline")

    # Add some data then create revised freeze
    client.post(f"/soa/{soa_id}/visits", json={"name": "Visit A"})
    client.post(f"/soa/{soa_id}/activities", json={"name": "Activity X"})
    fid2 = _create_freeze(soa_id, "revised")

    resp = client.get(
        f"/soa/{soa_id}/dair/download?base_freeze_id={fid1}&revised_freeze_id={fid2}"
    )
    assert resp.status_code == 200
    ct = resp.headers.get("content-type", "")
    assert "wordprocessingml" in ct or "octet-stream" in ct
    assert "attachment" in resp.headers.get("content-disposition", "")
    assert f"DAIR_{soa_id}_{fid1}_vs_{fid2}.docx" in resp.headers.get(
        "content-disposition", ""
    )
    # DOCX starts with PK (ZIP magic bytes)
    assert resp.content[:2] == b"PK"


def test_download_dair_no_changes():
    """DAIR should still generate successfully when no changes exist."""
    r = client.post("/soa", json={"name": "DAIR No Changes"})
    soa_id = r.json()["id"]
    fid1 = _create_freeze(soa_id, "snap1")
    fid2 = _create_freeze(soa_id, "snap2")
    resp = client.get(
        f"/soa/{soa_id}/dair/download?base_freeze_id={fid1}&revised_freeze_id={fid2}"
    )
    assert resp.status_code == 200
    assert resp.content[:2] == b"PK"
