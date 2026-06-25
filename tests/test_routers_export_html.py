"""Tests for the HTML matrix export endpoint GET /soa/{soa_id}/export/html."""

from fastapi.testclient import TestClient
from soa_builder.web.app import app

client = TestClient(app)


def _make_soa(name="HTML Export Test"):
    r = client.post("/soa", json={"name": name})
    assert r.status_code in (200, 201), r.text
    return r.json()["id"]


def test_export_html_returns_200():
    soa_id = _make_soa()
    resp = client.get(f"/soa/{soa_id}/export/html")
    assert resp.status_code == 200
    assert "text/html" in resp.headers["content-type"]


def test_export_html_content_disposition():
    soa_id = _make_soa()
    resp = client.get(f"/soa/{soa_id}/export/html")
    assert resp.status_code == 200
    cd = resp.headers.get("content-disposition", "")
    assert "attachment" in cd
    assert ".html" in cd


def test_export_html_contains_matrix_class():
    soa_id = _make_soa()
    # An instance is required to trigger a timeline and render the matrix table
    client.post(
        f"/soa/{soa_id}/instances",
        json={"name": "V1", "instance_uid": "SI_mc_1"},
    )
    resp = client.get(f"/soa/{soa_id}/export/html")
    assert resp.status_code == 200
    assert b'class="matrix"' in resp.content


def test_export_html_shows_activity_name():
    soa_id = _make_soa("Activity Name Test")
    client.post(f"/soa/{soa_id}/activities", json={"name": "Vital Signs CBC"})

    # Need an instance so the timeline renders
    client.post(
        f"/soa/{soa_id}/instances",
        json={"name": "Screening Visit", "instance_uid": "SI_test_1"},
    )

    resp = client.get(f"/soa/{soa_id}/export/html")
    assert resp.status_code == 200
    assert b"Vital Signs CBC" in resp.content


def test_export_html_concepts_in_details_element():
    """Concepts per activity are wrapped in <details> for expand/collapse."""
    soa_id = _make_soa("Concepts Details Test")
    # Create instance and activity so the matrix renders
    client.post(
        f"/soa/{soa_id}/instances",
        json={"name": "V1", "instance_uid": "SI_cd_1"},
    )
    act_r = client.post(f"/soa/{soa_id}/activities", json={"name": "ECG"})
    assert act_r.status_code in (200, 201), act_r.text
    act_id = act_r.json()["activity_id"]

    # Pre-populate activity_concept directly so we don't need CDISC API
    import os
    import sqlite3

    db = os.environ["SOA_BUILDER_DB"]
    conn = sqlite3.connect(db)
    conn.execute(
        "INSERT INTO activity_concept (activity_id, concept_code, concept_title, soa_id)"
        " VALUES (?,?,?,?)",
        (act_id, "C49677", "Heart Rate", soa_id),
    )
    conn.commit()
    conn.close()

    resp = client.get(f"/soa/{soa_id}/export/html")
    assert resp.status_code == 200
    assert b"<details>" in resp.content
    assert b"Heart Rate" in resp.content
    assert b"C49677" in resp.content


def test_export_html_superscript_rendered():
    """Cell superscripts appear as <sup> tags in the exported HTML."""
    soa_id = _make_soa("Superscript Test")
    inst_r = client.post(
        f"/soa/{soa_id}/instances",
        json={"name": "Day 1", "instance_uid": "SI_sup_1"},
    )
    assert inst_r.status_code in (200, 201), inst_r.text
    inst_id = inst_r.json()["id"]

    act_r = client.post(f"/soa/{soa_id}/activities", json={"name": "Vitals"})
    assert act_r.status_code in (200, 201), act_r.text
    act_id = act_r.json()["activity_id"]

    # Toggle cell on (X)
    client.post(
        f"/ui/soa/{soa_id}/toggle_cell",
        data={"instance_id": inst_id, "activity_id": act_id},
    )

    # Set superscript
    client.post(
        f"/ui/soa/{soa_id}/cell_superscript/{inst_id}/{act_id}",
        data={"superscript": "a"},
    )

    resp = client.get(f"/soa/{soa_id}/export/html")
    assert resp.status_code == 200
    assert b"<sup>a</sup>" in resp.content


def test_export_html_footnotes_rendered():
    """Footnotes section appears at the bottom of the exported HTML."""
    soa_id = _make_soa("Footnote Test")
    fn_r = client.post(
        f"/soa/{soa_id}/footnotes",
        json={"name": "a", "text": "See protocol section 5.1"},
    )
    assert fn_r.status_code in (200, 201), fn_r.text

    resp = client.get(f"/soa/{soa_id}/export/html")
    assert resp.status_code == 200
    assert b"Footnotes" in resp.content
    assert b"See protocol section 5.1" in resp.content


def test_export_html_unknown_soa_returns_404():
    resp = client.get("/soa/999999/export/html")
    assert resp.status_code == 404


def test_export_html_embedded_css():
    """The exported file contains embedded CSS (style block)."""
    soa_id = _make_soa()
    resp = client.get(f"/soa/{soa_id}/export/html")
    assert resp.status_code == 200
    assert b"<style>" in resp.content
    # CSS custom property from design system
    assert b"--am-purple" in resp.content
