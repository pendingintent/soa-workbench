"""Tests for the combined study timing UI route."""

from fastapi.testclient import TestClient

from soa_builder.web.app import app

client = TestClient(app)


def test_ui_study_timing_200():
    """GET /ui/soa/{soa_id}/study_timing returns 200 HTML for a valid SoA."""
    r = client.post("/soa", json={"name": "Study Timing 200 Test"})
    soa_id = r.json()["id"]

    resp = client.get(f"/ui/soa/{soa_id}/study_timing")
    assert resp.status_code == 200
    assert "text/html" in resp.headers.get("content-type", "")


def test_ui_study_timing_contains_all_sections():
    """Response HTML includes all three section containers."""
    r = client.post("/soa", json={"name": "Study Timing Sections Test"})
    soa_id = r.json()["id"]

    resp = client.get(f"/ui/soa/{soa_id}/study_timing")
    assert resp.status_code == 200
    assert "study-timing-schedule-timelines" in resp.text
    assert "study-timing-instances" in resp.text
    assert "study-timing-timings" in resp.text


def test_ui_study_timing_404_nonexistent_soa():
    """GET /ui/soa/999999/study_timing returns 404 for nonexistent SoA."""
    resp = client.get("/ui/soa/999999/study_timing")
    assert resp.status_code == 404


def test_ui_study_timing_with_data():
    """Created entities appear in the rendered page."""
    r = client.post("/soa", json={"name": "Study Timing Data Test"})
    soa_id = r.json()["id"]

    # Create one of each entity via API
    client.post(
        f"/soa/{soa_id}/schedule_timelines",
        json={"name": "My Test Timeline"},
    )
    client.post(
        f"/soa/{soa_id}/instances",
        json={"name": "My Test Instance"},
    )
    client.post(
        f"/soa/{soa_id}/timings",
        json={"name": "My Test Timing"},
    )

    resp = client.get(f"/ui/soa/{soa_id}/study_timing")
    assert resp.status_code == 200
    assert "My Test Timeline" in resp.text
    assert "My Test Instance" in resp.text
    assert "My Test Timing" in resp.text
