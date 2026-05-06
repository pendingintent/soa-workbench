"""Tests for the Objectives & Endpoints UI page."""

from fastapi.testclient import TestClient

from soa_builder.web.app import app

client = TestClient(app)


def test_ui_list_objectives_200():
    r = client.post("/soa", json={"name": "Objectives UI 200 Test"})
    soa_id = r.json()["id"]

    resp = client.get(f"/ui/soa/{soa_id}/objectives")
    assert resp.status_code == 200
    assert "text/html" in resp.headers.get("content-type", "")
    assert "Objectives &amp; Endpoints" in resp.text
    assert f"/ui/soa/{soa_id}/objectives/create" in resp.text


def test_ui_list_objectives_404():
    resp = client.get("/ui/soa/999999/objectives")
    assert resp.status_code == 404


def test_edit_page_no_longer_includes_objectives_section():
    r = client.post("/soa", json={"name": "Edit Strip Objectives Test"})
    soa_id = r.json()["id"]

    resp = client.get(f"/ui/soa/{soa_id}/edit")
    assert resp.status_code == 200
    assert "objectives-section" not in resp.text
