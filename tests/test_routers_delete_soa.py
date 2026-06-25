"""Tests for the delete SOA endpoint POST /ui/soa/{soa_id}/delete."""

from fastapi.testclient import TestClient

from soa_builder.web.app import app

client = TestClient(app)


def _create_soa(name="Delete Test Study", study_id="DEL-001"):
    r = client.post("/soa", json={"name": name, "study_id": study_id})
    assert r.status_code == 200
    return r.json()["id"]


def test_delete_soa_success():
    """POST with the correct study_id deletes the study and redirects."""
    soa_id = _create_soa("Delete Success", "DEL-OK-001")

    resp = client.post(
        f"/ui/soa/{soa_id}/delete",
        data={"confirm_study_id": "DEL-OK-001"},
    )
    assert resp.status_code == 200
    assert "window.location='/';" in resp.text

    # Study must no longer appear on the home page
    home = client.get("/")
    assert f"/ui/soa/{soa_id}/edit" not in home.text


def test_delete_soa_wrong_study_id_returns_400():
    """POST with a mismatched study_id is rejected with 400."""
    soa_id = _create_soa("Delete Wrong ID", "DEL-WRONG-001")

    resp = client.post(
        f"/ui/soa/{soa_id}/delete",
        data={"confirm_study_id": "NOT-THE-RIGHT-ID"},
    )
    assert resp.status_code == 400
    assert "does not match" in resp.json()["detail"]


def test_delete_soa_unknown_soa_returns_404():
    """POST to a nonexistent soa_id returns 404."""
    resp = client.post(
        "/ui/soa/999999/delete",
        data={"confirm_study_id": "ANYTHING"},
    )
    assert resp.status_code == 404


def test_delete_soa_cascades_activities():
    """Deleting a study also removes its activities."""
    soa_id = _create_soa("Delete Cascade", "DEL-CASCADE-001")

    # Create an activity
    act_r = client.post(
        f"/soa/{soa_id}/activities",
        json={"name": "Cascade Activity"},
    )
    assert act_r.status_code == 200

    # Delete the study
    resp = client.post(
        f"/ui/soa/{soa_id}/delete",
        data={"confirm_study_id": "DEL-CASCADE-001"},
    )
    assert resp.status_code == 200

    # Activities endpoint for a deleted study returns 404
    acts = client.get(f"/soa/{soa_id}/activities")
    assert acts.status_code == 404
