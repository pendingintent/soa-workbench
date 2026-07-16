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


def test_export_html_activity_superscript_rendered():
    """Activity superscripts appear as <sup> tags in the exported HTML."""
    soa_id = _make_soa("Activity Superscript Export Test")
    client.post(
        f"/soa/{soa_id}/instances",
        json={"name": "Day 1", "instance_uid": "SI_asup_1"},
    )
    act_r = client.post(f"/soa/{soa_id}/activities", json={"name": "Vitals"})
    assert act_r.status_code in (200, 201), act_r.text
    act_id = act_r.json()["activity_id"]

    client.post(
        f"/ui/soa/{soa_id}/activity_superscript/{act_id}",
        data={"superscript": "b"},
    )

    resp = client.get(f"/soa/{soa_id}/export/html")
    assert resp.status_code == 200
    assert b"<sup>b</sup>" in resp.content


def test_activity_superscript_edit_save_view():
    """Full edit/save/cancel cycle for an activity-level superscript."""
    soa_id = _make_soa("Activity Superscript Cycle Test")
    act_r = client.post(f"/soa/{soa_id}/activities", json={"name": "ECG"})
    assert act_r.status_code in (200, 201), act_r.text
    act_id = act_r.json()["activity_id"]

    # Edit mode returns an inline input form
    edit_resp = client.get(f"/ui/soa/{soa_id}/activity_superscript_edit/{act_id}")
    assert edit_resp.status_code == 200
    assert b'name="superscript"' in edit_resp.content

    # Save persists and returns the rendered <sup> marker
    save_resp = client.post(
        f"/ui/soa/{soa_id}/activity_superscript/{act_id}",
        data={"superscript": "c"},
    )
    assert save_resp.status_code == 200
    assert b"<sup>c</sup>" in save_resp.content
    assert b"ECG" in save_resp.content

    # View mode (cancel path) re-renders the same persisted value
    view_resp = client.get(f"/ui/soa/{soa_id}/activity_superscript_view/{act_id}")
    assert view_resp.status_code == 200
    assert b"<sup>c</sup>" in view_resp.content

    # Clearing the value removes the marker
    clear_resp = client.post(
        f"/ui/soa/{soa_id}/activity_superscript/{act_id}",
        data={"superscript": ""},
    )
    assert clear_resp.status_code == 200
    assert b"<sup>" not in clear_resp.content


def test_activity_superscript_unknown_activity_returns_404():
    soa_id = _make_soa("Activity Superscript 404 Test")
    resp = client.post(
        f"/ui/soa/{soa_id}/activity_superscript/999999",
        data={"superscript": "a"},
    )
    assert resp.status_code == 404


def _make_epoch_encounter_instance(soa_name):
    """Create a SoA with one epoch, one visit, and one instance linking both.

    Returns (soa_id, epoch_id, encounter_id).
    """
    soa_id = _make_soa(soa_name)

    epoch_r = client.post(f"/soa/{soa_id}/epochs", json={"name": "Screening"})
    assert epoch_r.status_code in (200, 201), epoch_r.text
    epoch_data = epoch_r.json()
    epoch_id = epoch_data["id"]
    epoch_uid = epoch_data["epoch_uid"]

    visit_r = client.post(f"/soa/{soa_id}/visits", json={"name": "Visit 1"})
    assert visit_r.status_code in (200, 201), visit_r.text
    encounter_id = visit_r.json()["id"]
    detail_r = client.get(f"/soa/visits/{encounter_id}?soa_id={soa_id}")
    assert detail_r.status_code == 200
    encounter_uid = detail_r.json()["encounter_uid"]

    inst_r = client.post(
        f"/soa/{soa_id}/instances",
        json={
            "name": "V1",
            "instance_uid": "SI_epenc_1",
            "epoch_uid": epoch_uid,
            "encounter_uid": encounter_uid,
        },
    )
    assert inst_r.status_code in (200, 201), inst_r.text

    return soa_id, epoch_id, encounter_id


def test_epoch_superscript_edit_save_view():
    """Full edit/save/cancel cycle for an epoch superscript, colspan preserved."""
    soa_id, epoch_id, _encounter_id = _make_epoch_encounter_instance(
        "Epoch Superscript Cycle Test"
    )

    # Edit mode returns an inline input form and preserves colspan
    edit_resp = client.get(
        f"/ui/soa/{soa_id}/epoch_superscript_edit/{epoch_id}?colspan=3"
    )
    assert edit_resp.status_code == 200
    assert b'name="superscript"' in edit_resp.content
    assert b'colspan="3"' in edit_resp.content

    # Save persists and returns the rendered <sup> marker, colspan preserved
    save_resp = client.post(
        f"/ui/soa/{soa_id}/epoch_superscript/{epoch_id}",
        data={"superscript": "1", "colspan": "3"},
    )
    assert save_resp.status_code == 200
    assert b"<sup>1</sup>" in save_resp.content
    assert b'colspan="3"' in save_resp.content
    assert b"Screening" in save_resp.content

    # View mode (cancel path) re-renders the same persisted value + colspan
    view_resp = client.get(
        f"/ui/soa/{soa_id}/epoch_superscript_view/{epoch_id}?colspan=3"
    )
    assert view_resp.status_code == 200
    assert b"<sup>1</sup>" in view_resp.content
    assert b'colspan="3"' in view_resp.content

    # Clearing the value removes the marker but keeps colspan
    clear_resp = client.post(
        f"/ui/soa/{soa_id}/epoch_superscript/{epoch_id}",
        data={"superscript": "", "colspan": "3"},
    )
    assert clear_resp.status_code == 200
    assert b"<sup>" not in clear_resp.content
    assert b'colspan="3"' in clear_resp.content


def test_epoch_superscript_unknown_epoch_returns_404():
    soa_id = _make_soa("Epoch Superscript 404 Test")
    resp = client.post(
        f"/ui/soa/{soa_id}/epoch_superscript/999999",
        data={"superscript": "a"},
    )
    assert resp.status_code == 404


def test_encounter_superscript_edit_save_view():
    """Full edit/save/cancel cycle for an encounter (visit) superscript."""
    soa_id, _epoch_id, encounter_id = _make_epoch_encounter_instance(
        "Encounter Superscript Cycle Test"
    )

    edit_resp = client.get(
        f"/ui/soa/{soa_id}/encounter_superscript_edit/{encounter_id}"
    )
    assert edit_resp.status_code == 200
    assert b'name="superscript"' in edit_resp.content

    save_resp = client.post(
        f"/ui/soa/{soa_id}/encounter_superscript/{encounter_id}",
        data={"superscript": "2"},
    )
    assert save_resp.status_code == 200
    assert b"<sup>2</sup>" in save_resp.content
    assert b"Visit 1" in save_resp.content

    view_resp = client.get(
        f"/ui/soa/{soa_id}/encounter_superscript_view/{encounter_id}"
    )
    assert view_resp.status_code == 200
    assert b"<sup>2</sup>" in view_resp.content

    clear_resp = client.post(
        f"/ui/soa/{soa_id}/encounter_superscript/{encounter_id}",
        data={"superscript": ""},
    )
    assert clear_resp.status_code == 200
    assert b"<sup>" not in clear_resp.content


def test_encounter_superscript_unknown_encounter_returns_404():
    soa_id = _make_soa("Encounter Superscript 404 Test")
    resp = client.post(
        f"/ui/soa/{soa_id}/encounter_superscript/999999",
        data={"superscript": "a"},
    )
    assert resp.status_code == 404


def test_export_html_epoch_and_encounter_superscript_rendered():
    """Epoch and encounter superscripts appear as <sup> tags in the exported HTML."""
    soa_id, epoch_id, encounter_id = _make_epoch_encounter_instance(
        "Epoch Encounter Export Test"
    )

    client.post(
        f"/ui/soa/{soa_id}/epoch_superscript/{epoch_id}",
        data={"superscript": "x", "colspan": "1"},
    )
    client.post(
        f"/ui/soa/{soa_id}/encounter_superscript/{encounter_id}",
        data={"superscript": "y"},
    )

    resp = client.get(f"/soa/{soa_id}/export/html")
    assert resp.status_code == 200
    assert b"<sup>x</sup>" in resp.content
    assert b"<sup>y</sup>" in resp.content


def test_edit_page_shows_epoch_and_encounter_edit_affordance():
    """The matrix edit page renders pencil-icon affordances for epoch/encounter headers."""
    soa_id, epoch_id, encounter_id = _make_epoch_encounter_instance(
        "Epoch Encounter Edit Page Test"
    )

    resp = client.get(f"/ui/soa/{soa_id}/edit")
    assert resp.status_code == 200
    assert (
        f"/ui/soa/{soa_id}/epoch_superscript_edit/{epoch_id}".encode() in resp.content
    )
    assert (
        f"/ui/soa/{soa_id}/encounter_superscript_edit/{encounter_id}".encode()
        in resp.content
    )


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
