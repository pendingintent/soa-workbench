"""Tests for the Define-JSON generator route and UI page."""

from fastapi.testclient import TestClient

from soa_builder.web.app import app

client = TestClient(app)


def _create_soa(name: str) -> int:
    r = client.post("/soa", json={"name": name})
    return r.json()["id"]


def test_route_missing_sdtmct_returns_422():
    """GET without required sdtmct query param returns 422."""
    soa_id = _create_soa("DefineJSON No Params")
    resp = client.get(f"/soa/{soa_id}/usdm_json/define_json")
    assert resp.status_code == 422


def test_route_404_nonexistent_soa():
    """GET /soa/999999/usdm_json/define_json returns 404."""
    resp = client.get(
        "/soa/999999/usdm_json/define_json",
        params={"sdtmct": "2025-03-28"},
    )
    assert resp.status_code == 404


def test_ui_define_json_page_200():
    """GET /ui/soa/{soa_id}/define_json returns 200 HTML."""
    soa_id = _create_soa("DefineJSON UI Test")
    resp = client.get(f"/ui/soa/{soa_id}/define_json")

    assert resp.status_code == 200
    assert "text/html" in resp.headers.get("content-type", "")
    assert "Define-JSON" in resp.text


def test_ui_define_json_page_contains_form_fields():
    """The Define-JSON UI page has sdtmct, sdtmig, and cosmosversion inputs."""
    soa_id = _create_soa("DefineJSON Form Fields Test")
    resp = client.get(f"/ui/soa/{soa_id}/define_json")

    assert resp.status_code == 200
    assert 'name="sdtmct"' in resp.text
    assert 'name="sdtmig"' in resp.text
    assert 'name="cosmosversion"' in resp.text


def test_ui_define_json_page_404_nonexistent_soa():
    """GET /ui/soa/999999/define_json returns 404."""
    resp = client.get("/ui/soa/999999/define_json")
    assert resp.status_code == 404
