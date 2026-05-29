"""Tests for Study Titles CRUD and USDM output."""

from fastapi.testclient import TestClient

from soa_builder.web.app import app

client = TestClient(app)


def _create_soa(name="Titles Test", study_id=None):
    payload = {"name": name}
    if study_id:
        payload["study_id"] = study_id
    r = client.post("/soa", json=payload)
    assert r.status_code == 200
    return r.json()["id"]


def _create_title(
    soa_id,
    text="My Title",
    concept_id="C99905x2",
    preferred_term="Official Study Title",
):
    r = client.post(
        f"/soa/{soa_id}/titles",
        params={
            "text": text,
            "type_concept_id": concept_id,
            "type_preferred_term": preferred_term,
            "type_version": "2022-09-30",
        },
    )
    assert r.status_code == 201
    return r.json()


def test_create_title_returns_uid():
    """POST creates a title with StudyTitle_N UID."""
    soa_id = _create_soa("Title UID Test")
    result = _create_title(soa_id)
    assert result["study_title_uid"].startswith("StudyTitle_")
    assert result["text"] == "My Title"


def test_create_title_uid_monotonic():
    """UIDs are monotonic — delete first, third is still StudyTitle_3."""
    soa_id = _create_soa("Title Monotonic Test")
    t1 = _create_title(soa_id, text="T1")
    t2 = _create_title(soa_id, text="T2")
    # Delete first title
    r = client.delete(f"/soa/{soa_id}/titles/{t1['id']}")
    assert r.status_code == 200
    # Third title must be StudyTitle_3, never reuse StudyTitle_1
    t3 = _create_title(soa_id, text="T3")
    assert t3["study_title_uid"] == "StudyTitle_3"
    _ = t2  # silence unused warning


def test_list_titles():
    """GET /soa/{soa_id}/titles returns the list of titles."""
    soa_id = _create_soa("Title List Test")
    _create_title(soa_id, text="Alpha")
    _create_title(soa_id, text="Beta")
    r = client.get(f"/soa/{soa_id}/titles")
    assert r.status_code == 200
    texts = [t["text"] for t in r.json()]
    assert "Alpha" in texts
    assert "Beta" in texts


def test_delete_title():
    """DELETE removes the title; list is then empty."""
    soa_id = _create_soa("Title Delete Test")
    t = _create_title(soa_id, text="ToDelete")
    r = client.delete(f"/soa/{soa_id}/titles/{t['id']}")
    assert r.status_code == 200
    assert r.json()["deleted_title_id"] == t["id"]
    listing = client.get(f"/soa/{soa_id}/titles").json()
    assert listing == []


def test_unknown_soa_returns_404():
    """POST to a nonexistent soa_id returns 404."""
    r = client.post(
        "/soa/999999/titles",
        params={"text": "x", "type_concept_id": "C1"},
    )
    assert r.status_code == 404


def test_usdm_titles_in_output():
    """After creating a title, USDM output contains the saved title text."""
    soa_id = _create_soa("Title USDM Test", study_id="TITLE-USDM-001")
    _create_title(soa_id, text="Phase 2 Primary Title")
    r = client.get(f"/soa/{soa_id}/usdm_json/full")
    assert r.status_code == 200
    usdm = r.json()
    study_version = usdm["study"]["versions"][0]
    titles = study_version["titles"]
    assert any(t["text"] == "Phase 2 Primary Title" for t in titles)
    # Type code must carry codeSystem
    for t in titles:
        if t["text"] == "Phase 2 Primary Title":
            assert t["type"]["codeSystem"] == "http://www.cdisc.org"
            assert t["type"]["code"] == "C99905x2"
            assert t["type"]["decode"] == "Official Study Title"
            assert t["type"]["codeSystemVersion"] == "2022-09-30"


def test_usdm_fallback_when_no_titles():
    """With no titles in DB, USDM output still contains a valid StudyTitle."""
    soa_id = _create_soa("Title Fallback Test", study_id="TITLE-FALLBACK-001")
    r = client.get(f"/soa/{soa_id}/usdm_json/full")
    assert r.status_code == 200
    usdm = r.json()
    study_version = usdm["study"]["versions"][0]
    titles = study_version["titles"]
    assert len(titles) >= 1
    assert all(t["instanceType"] == "StudyTitle" for t in titles)
