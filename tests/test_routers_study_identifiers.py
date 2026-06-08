"""Tests for the study_identifiers router (API + UI endpoints)."""

from fastapi.testclient import TestClient

from soa_builder.web.app import app

client = TestClient(app)


def _new_soa(name: str) -> int:
    r = client.post("/soa", json={"name": name})
    return r.json()["id"]


def _new_org(soa_id: int, name: str) -> dict:
    r = client.post(f"/soa/{soa_id}/organizations", json={"name": name})
    assert r.status_code == 201
    return r.json()


# ---------------------------------------------------------------------------
# API – list
# ---------------------------------------------------------------------------


def test_list_study_identifiers_empty():
    soa_id = _new_soa("SI List Empty")
    resp = client.get(f"/soa/{soa_id}/study-identifiers")
    assert resp.status_code == 200
    assert resp.json() == []


def test_list_study_identifiers_nonexistent_soa():
    resp = client.get("/soa/999999/study-identifiers")
    assert resp.status_code == 404


# ---------------------------------------------------------------------------
# API – create
# ---------------------------------------------------------------------------


def test_create_study_identifier_minimal():
    soa_id = _new_soa("SI Create Minimal")
    resp = client.post(
        f"/soa/{soa_id}/study-identifiers",
        params={"text": "NCT12345678"},
    )
    assert resp.status_code == 201
    body = resp.json()
    assert body["study_identifier_uid"] == "StudyIdentifier_1"
    assert body["text"] == "NCT12345678"
    assert body["scope_org_uid"] == ""


def test_create_study_identifier_with_org():
    soa_id = _new_soa("SI Create With Org")
    org = _new_org(soa_id, "Sponsor Inc.")
    resp = client.post(
        f"/soa/{soa_id}/study-identifiers",
        params={"text": "PROTO-001", "scope_org_uid": org["organization_uid"]},
    )
    assert resp.status_code == 201
    body = resp.json()
    assert body["scope_org_uid"] == org["organization_uid"]


def test_create_study_identifier_requires_text():
    soa_id = _new_soa("SI Requires Text")
    resp = client.post(
        f"/soa/{soa_id}/study-identifiers",
        params={"text": "   "},
    )
    assert resp.status_code == 400


def test_create_study_identifier_nonexistent_soa():
    resp = client.post(
        "/soa/999999/study-identifiers",
        params={"text": "X"},
    )
    assert resp.status_code == 404


# ---------------------------------------------------------------------------
# API – delete
# ---------------------------------------------------------------------------


def test_delete_study_identifier():
    soa_id = _new_soa("SI Delete")
    r = client.post(f"/soa/{soa_id}/study-identifiers", params={"text": "ID-001"})
    si_id = r.json()["id"]

    resp = client.delete(f"/soa/{soa_id}/study-identifiers/{si_id}")
    assert resp.status_code == 200

    remaining = client.get(f"/soa/{soa_id}/study-identifiers").json()
    assert remaining == []


def test_delete_study_identifier_not_found():
    soa_id = _new_soa("SI Delete 404")
    resp = client.delete(f"/soa/{soa_id}/study-identifiers/999999")
    assert resp.status_code == 404


# ---------------------------------------------------------------------------
# UID monotonicity
# ---------------------------------------------------------------------------


def test_study_identifier_uid_is_one():
    soa_id = _new_soa("SI UID One")
    r1 = client.post(f"/soa/{soa_id}/study-identifiers", params={"text": "A"})
    assert r1.json()["study_identifier_uid"] == "StudyIdentifier_1"


def test_only_one_identifier_allowed():
    soa_id = _new_soa("SI Only One")
    client.post(f"/soa/{soa_id}/study-identifiers", params={"text": "A"})
    r2 = client.post(f"/soa/{soa_id}/study-identifiers", params={"text": "B"})
    assert r2.status_code == 409


def test_study_identifier_uid_never_recycled():
    soa_id = _new_soa("SI UID Monotonic")
    r1 = client.post(f"/soa/{soa_id}/study-identifiers", params={"text": "A"})
    assert r1.json()["study_identifier_uid"] == "StudyIdentifier_1"

    client.delete(f"/soa/{soa_id}/study-identifiers/{r1.json()['id']}")

    r2 = client.post(f"/soa/{soa_id}/study-identifiers", params={"text": "B"})
    assert r2.json()["study_identifier_uid"] == "StudyIdentifier_2"


def test_study_identifier_uids_scoped_per_soa():
    soa_a = _new_soa("SI Scope SOA-A")
    soa_b = _new_soa("SI Scope SOA-B")
    ra = client.post(f"/soa/{soa_a}/study-identifiers", params={"text": "A"})
    rb = client.post(f"/soa/{soa_b}/study-identifiers", params={"text": "B"})
    assert ra.json()["study_identifier_uid"] == "StudyIdentifier_1"
    assert rb.json()["study_identifier_uid"] == "StudyIdentifier_1"


# ---------------------------------------------------------------------------
# UI (HTMX) endpoints
# ---------------------------------------------------------------------------


def test_ui_add_study_identifier_returns_html():
    soa_id = _new_soa("SI UI Add")
    resp = client.post(
        f"/ui/soa/{soa_id}/study-identifiers-add",
        data={"text": "UINCTID-001", "scope_org_uid": ""},
    )
    assert resp.status_code == 200
    assert "study-identifiers-section" in resp.text
    assert "UINCTID-001" in resp.text


def test_ui_add_study_identifier_empty_text_returns_partial():
    soa_id = _new_soa("SI UI Add Empty")
    resp = client.post(
        f"/ui/soa/{soa_id}/study-identifiers-add",
        data={"text": "", "scope_org_uid": ""},
    )
    assert resp.status_code == 200
    assert "study-identifiers-section" in resp.text


def test_ui_delete_study_identifier_returns_html():
    soa_id = _new_soa("SI UI Delete")
    client.post(
        f"/ui/soa/{soa_id}/study-identifiers-add",
        data={"text": "TO-DELETE", "scope_org_uid": ""},
    )
    rows = client.get(f"/soa/{soa_id}/study-identifiers").json()
    si_id = rows[0]["id"]

    resp = client.post(f"/ui/soa/{soa_id}/study-identifiers/{si_id}/delete")
    assert resp.status_code == 200
    assert "study-identifiers-section" in resp.text

    remaining = client.get(f"/soa/{soa_id}/study-identifiers").json()
    assert remaining == []
