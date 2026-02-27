"""Tests for the USDM JSON generation routes."""

from fastapi.testclient import TestClient

from soa_builder.web.app import app

client = TestClient(app)


def test_ui_usdm_json_200():
    """GET /ui/soa/{soa_id}/usdm_json returns 200 HTML for a valid SoA."""
    r = client.post("/soa", json={"name": "USDM JSON 200 Test"})
    soa_id = r.json()["id"]

    resp = client.get(f"/ui/soa/{soa_id}/usdm_json")
    assert resp.status_code == 200
    assert "text/html" in resp.headers.get("content-type", "")


def test_ui_usdm_json_contains_all_components():
    """Response HTML lists all 10 component rows."""
    r = client.post("/soa", json={"name": "USDM JSON Components Test"})
    soa_id = r.json()["id"]

    resp = client.get(f"/ui/soa/{soa_id}/usdm_json")
    assert resp.status_code == 200
    for label in [
        "Full USDM Document",
        "Arms",
        "Activities",
        "Study Elements",
        "Encounters",
        "Study Epochs",
        "Schedule Timelines",
        "Timings",
        "Scheduled Activity Instances",
        "Study Cells",
    ]:
        assert label in resp.text


def test_ui_usdm_json_404_nonexistent_soa():
    """GET /ui/soa/999999/usdm_json returns 404 for a nonexistent SoA."""
    resp = client.get("/ui/soa/999999/usdm_json")
    assert resp.status_code == 404


def test_download_usdm_component_arms():
    """Download arms component returns JSON with Content-Disposition attachment."""
    r = client.post("/soa", json={"name": "USDM Arms Download Test"})
    soa_id = r.json()["id"]

    resp = client.get(f"/soa/{soa_id}/usdm_json/arms")
    assert resp.status_code == 200
    assert "application/json" in resp.headers.get("content-type", "")
    assert "attachment" in resp.headers.get("content-disposition", "")
    assert "usdm_arms.json" in resp.headers.get("content-disposition", "")
    assert isinstance(resp.json(), list)


def test_download_usdm_component_404_nonexistent_soa():
    """Download endpoint returns 404 for a nonexistent SoA."""
    resp = client.get("/soa/999999/usdm_json/arms")
    assert resp.status_code == 404


def test_download_usdm_component_400_unknown_component():
    """Download endpoint returns 400 for an unknown component key."""
    r = client.post("/soa", json={"name": "USDM Bad Component Test"})
    soa_id = r.json()["id"]

    resp = client.get(f"/soa/{soa_id}/usdm_json/bogus")
    assert resp.status_code == 400
