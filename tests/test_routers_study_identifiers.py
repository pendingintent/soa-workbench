"""Tests for the study_identifiers router (API + UI endpoints).

A study version may carry several StudyIdentifiers (USDM
StudyVersion.studyIdentifiers is 1..*). The router enforces the
USDM v4 CORE rules on entry:

- DDF00172 (ERROR): exactly one sponsor-scoped identifier
- DDF00173 (ERROR): text unique within an organization's scope
- DDF00174 (WARNING): one identifier per organization expected;
  allowed, but flagged
"""

from fastapi.testclient import TestClient

from soa_builder.web.app import app

client = TestClient(app)

_SPONSOR = ("C70793", "Clinical Study Sponsor")
_REGISTRY = ("C93453", "Study Registry")


def _new_soa(name: str) -> int:
    r = client.post("/soa", json={"name": name})
    return r.json()["id"]


def _new_org(soa_id: int, name: str, org_type: tuple = _REGISTRY) -> str:
    """Create an organization and return its organization_uid."""
    r = client.post(
        f"/soa/{soa_id}/organizations",
        json={
            "name": name,
            "type_concept_id": org_type[0],
            "type_preferred_term": org_type[1],
            "type_version": "2025-09-26",
        },
    )
    assert r.status_code == 201
    return r.json()["organization_uid"]


def _create(soa_id: int, text: str, org_uid: str):
    return client.post(
        f"/soa/{soa_id}/study-identifiers",
        params={"text": text, "scope_org_uid": org_uid},
    )


def _uids(soa_id: int) -> list:
    rows = client.get(f"/soa/{soa_id}/study-identifiers").json()
    return [r["study_identifier_uid"] for r in rows]


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


def test_create_study_identifier_with_org():
    soa_id = _new_soa("SI Create With Org")
    org = _new_org(soa_id, "Sponsor Inc.", _SPONSOR)
    resp = _create(soa_id, "PROTO-001", org)
    assert resp.status_code == 201
    body = resp.json()
    assert body["study_identifier_uid"] == "StudyIdentifier_1"
    assert body["text"] == "PROTO-001"
    assert body["scope_org_uid"] == org
    assert body["warnings"] == []


def test_create_study_identifier_requires_text():
    soa_id = _new_soa("SI Requires Text")
    org = _new_org(soa_id, "Sponsor Inc.", _SPONSOR)
    resp = _create(soa_id, "   ", org)
    assert resp.status_code == 400


def test_create_study_identifier_requires_org():
    # USDM StudyIdentifier.scopeId has cardinality 1.
    soa_id = _new_soa("SI Requires Org")
    resp = client.post(
        f"/soa/{soa_id}/study-identifiers", params={"text": "NCT00000001"}
    )
    assert resp.status_code == 400
    assert "organization" in resp.json()["detail"].lower()


def test_create_study_identifier_rejects_unknown_org():
    soa_id = _new_soa("SI Unknown Org")
    resp = _create(soa_id, "NCT00000001", "Organization_999")
    assert resp.status_code == 400


def test_create_study_identifier_rejects_org_from_other_soa():
    soa_a = _new_soa("SI Org SOA-A")
    soa_b = _new_soa("SI Org SOA-B")
    org_a = _new_org(soa_a, "Registry A")  # soa_b has no organizations
    assert _create(soa_b, "NCT00000001", org_a).status_code == 400
    assert _create(soa_a, "NCT00000001", org_a).status_code == 201


def test_create_study_identifier_nonexistent_soa():
    resp = _create(999999, "X", "Organization_1")
    assert resp.status_code == 404


def test_multiple_identifiers_allowed():
    soa_id = _new_soa("SI Multiple")
    sponsor = _new_org(soa_id, "Sponsor Inc.", _SPONSOR)
    ctgov = _new_org(soa_id, "ClinicalTrials.gov")
    ctis = _new_org(soa_id, "CTIS")
    assert _create(soa_id, "PROTO-001", sponsor).status_code == 201
    assert _create(soa_id, "NCT00000001", ctgov).status_code == 201
    assert _create(soa_id, "2022-500000-11", ctis).status_code == 201
    assert _uids(soa_id) == [
        "StudyIdentifier_1",
        "StudyIdentifier_2",
        "StudyIdentifier_3",
    ]


def test_second_sponsor_identifier_rejected():
    # DDF00172: exactly one sponsor study identifier.
    soa_id = _new_soa("SI Second Sponsor")
    sponsor = _new_org(soa_id, "Sponsor Inc.", _SPONSOR)
    other_sponsor = _new_org(soa_id, "Co-Sponsor", _SPONSOR)
    assert _create(soa_id, "PROTO-001", sponsor).status_code == 201
    resp = _create(soa_id, "PROTO-002", other_sponsor)
    assert resp.status_code == 409
    assert "DDF00172" in resp.json()["detail"]


def test_duplicate_text_in_same_org_rejected():
    # DDF00173: identifier text unique within an organization's scope.
    soa_id = _new_soa("SI Duplicate Text")
    ctis = _new_org(soa_id, "CTIS")
    assert _create(soa_id, "2022-500000-11", ctis).status_code == 201
    resp = _create(soa_id, "2022-500000-11", ctis)
    assert resp.status_code == 409
    assert "DDF00173" in resp.json()["detail"]


def test_same_text_in_different_orgs_allowed():
    soa_id = _new_soa("SI Same Text Diff Org")
    reg_a = _new_org(soa_id, "Registry A")
    reg_b = _new_org(soa_id, "Registry B")
    assert _create(soa_id, "ABC-1", reg_a).status_code == 201
    assert _create(soa_id, "ABC-1", reg_b).status_code == 201


def test_second_identifier_for_same_org_warns():
    # DDF00174 is a WARNING: allowed, but reported.
    soa_id = _new_soa("SI Same Org Warns")
    ctis = _new_org(soa_id, "CTIS")
    assert _create(soa_id, "2022-500000-11", ctis).json()["warnings"] == []
    resp = _create(soa_id, "2018-001795-38", ctis)
    assert resp.status_code == 201
    assert any("DDF00174" in w for w in resp.json()["warnings"])


# ---------------------------------------------------------------------------
# API – delete
# ---------------------------------------------------------------------------


def test_delete_study_identifier():
    soa_id = _new_soa("SI Delete")
    org = _new_org(soa_id, "Registry")
    si_id = _create(soa_id, "ID-001", org).json()["id"]

    resp = client.delete(f"/soa/{soa_id}/study-identifiers/{si_id}")
    assert resp.status_code == 200
    assert client.get(f"/soa/{soa_id}/study-identifiers").json() == []


def test_delete_study_identifier_not_found():
    soa_id = _new_soa("SI Delete 404")
    resp = client.delete(f"/soa/{soa_id}/study-identifiers/999999")
    assert resp.status_code == 404


# ---------------------------------------------------------------------------
# API – reorder
# ---------------------------------------------------------------------------


def _three_identifiers(name: str) -> int:
    soa_id = _new_soa(name)
    sponsor = _new_org(soa_id, "Sponsor Inc.", _SPONSOR)
    ctgov = _new_org(soa_id, "ClinicalTrials.gov")
    ctis = _new_org(soa_id, "CTIS")
    _create(soa_id, "PROTO-001", sponsor)
    _create(soa_id, "NCT00000001", ctgov)
    _create(soa_id, "2022-500000-11", ctis)
    return soa_id


def test_reorder_study_identifiers():
    soa_id = _three_identifiers("SI Reorder")
    new_order = ["StudyIdentifier_3", "StudyIdentifier_1", "StudyIdentifier_2"]
    resp = client.put(
        f"/soa/{soa_id}/study-identifiers/order",
        json={"study_identifier_uids": new_order},
    )
    assert resp.status_code == 200
    assert [r["study_identifier_uid"] for r in resp.json()] == new_order
    assert _uids(soa_id) == new_order


def test_reorder_requires_every_uid_once():
    soa_id = _three_identifiers("SI Reorder Bad")
    resp = client.put(
        f"/soa/{soa_id}/study-identifiers/order",
        json={"study_identifier_uids": ["StudyIdentifier_1"]},
    )
    assert resp.status_code == 400


def test_reorder_is_audited():
    soa_id = _three_identifiers("SI Reorder Audit")
    client.put(
        f"/soa/{soa_id}/study-identifiers/order",
        json={
            "study_identifier_uids": [
                "StudyIdentifier_2",
                "StudyIdentifier_1",
                "StudyIdentifier_3",
            ]
        },
    )
    from soa_builder.web.db import _connect

    conn = _connect()
    try:
        actions = [
            r[0]
            for r in conn.execute(
                "SELECT action FROM study_identifier_audit WHERE soa_id=?",
                (soa_id,),
            )
        ]
    finally:
        conn.close()
    assert "reorder" in actions


# ---------------------------------------------------------------------------
# UID monotonicity
# ---------------------------------------------------------------------------


def test_study_identifier_uid_never_recycled():
    soa_id = _new_soa("SI UID Monotonic")
    org = _new_org(soa_id, "Registry")
    r1 = _create(soa_id, "A", org)
    assert r1.json()["study_identifier_uid"] == "StudyIdentifier_1"

    client.delete(f"/soa/{soa_id}/study-identifiers/{r1.json()['id']}")

    r2 = _create(soa_id, "B", org)
    assert r2.json()["study_identifier_uid"] == "StudyIdentifier_2"


def test_study_identifier_uids_scoped_per_soa():
    soa_a = _new_soa("SI Scope SOA-A")
    soa_b = _new_soa("SI Scope SOA-B")
    ra = _create(soa_a, "A", _new_org(soa_a, "Registry A"))
    rb = _create(soa_b, "B", _new_org(soa_b, "Registry B"))
    assert ra.json()["study_identifier_uid"] == "StudyIdentifier_1"
    assert rb.json()["study_identifier_uid"] == "StudyIdentifier_1"


# ---------------------------------------------------------------------------
# UI (HTMX) endpoints
# ---------------------------------------------------------------------------


def _ui_add(soa_id: int, text: str, org_uid: str):
    return client.post(
        f"/ui/soa/{soa_id}/study-identifiers-add",
        data={"text": text, "scope_org_uid": org_uid},
    )


def test_ui_add_study_identifier_returns_html():
    soa_id = _new_soa("SI UI Add")
    org = _new_org(soa_id, "Registry")
    resp = _ui_add(soa_id, "UINCTID-001", org)
    assert resp.status_code == 200
    assert "study-identifiers-section" in resp.text
    assert "UINCTID-001" in resp.text


def test_ui_add_form_stays_visible_after_first_identifier():
    soa_id = _new_soa("SI UI Form Visible")
    org = _new_org(soa_id, "Registry")
    resp = _ui_add(soa_id, "NCT00000001", org)
    assert "study-identifiers-add" in resp.text


def test_ui_add_second_identifier():
    soa_id = _new_soa("SI UI Add Second")
    sponsor = _new_org(soa_id, "Sponsor Inc.", _SPONSOR)
    ctgov = _new_org(soa_id, "ClinicalTrials.gov")
    _ui_add(soa_id, "PROTO-001", sponsor)
    resp = _ui_add(soa_id, "NCT00000001", ctgov)
    assert "PROTO-001" in resp.text
    assert "NCT00000001" in resp.text
    assert len(_uids(soa_id)) == 2


def test_ui_add_empty_text_shows_error():
    soa_id = _new_soa("SI UI Add Empty")
    org = _new_org(soa_id, "Registry")
    resp = _ui_add(soa_id, "", org)
    assert resp.status_code == 200
    assert "study-identifiers-section" in resp.text
    assert "am-api-notice-error" in resp.text
    assert _uids(soa_id) == []


def test_ui_add_missing_org_shows_error():
    soa_id = _new_soa("SI UI Add No Org")
    resp = _ui_add(soa_id, "NCT00000001", "")
    assert resp.status_code == 200
    assert "am-api-notice-error" in resp.text
    assert _uids(soa_id) == []


def test_ui_add_second_sponsor_shows_error():
    soa_id = _new_soa("SI UI Second Sponsor")
    sponsor = _new_org(soa_id, "Sponsor Inc.", _SPONSOR)
    _ui_add(soa_id, "PROTO-001", sponsor)
    resp = _ui_add(soa_id, "PROTO-002", sponsor)
    assert resp.status_code == 200
    assert "DDF00172" in resp.text
    assert len(_uids(soa_id)) == 1


def test_ui_lists_all_organizations_with_sponsor_first():
    soa_id = _new_soa("SI UI All Orgs")
    _new_org(soa_id, "ClinicalTrials.gov")
    _new_org(soa_id, "Sponsor Inc.", _SPONSOR)
    resp = _ui_add(soa_id, "", "")
    text = resp.text
    assert "ClinicalTrials.gov" in text
    assert "Sponsor Inc." in text
    assert text.index("Sponsor Inc.") < text.index("ClinicalTrials.gov")


def test_ui_flags_org_with_more_than_one_identifier():
    soa_id = _new_soa("SI UI DDF00174")
    ctis = _new_org(soa_id, "CTIS")
    _ui_add(soa_id, "2022-500000-11", ctis)
    resp = _ui_add(soa_id, "2018-001795-38", ctis)
    assert "DDF00174" in resp.text
    assert len(_uids(soa_id)) == 2


def test_ui_move_down_and_up():
    soa_id = _three_identifiers("SI UI Move")
    rows = client.get(f"/soa/{soa_id}/study-identifiers").json()
    first_id = rows[0]["id"]

    resp = client.post(f"/ui/soa/{soa_id}/study-identifiers/{first_id}/move_down")
    assert resp.status_code == 200
    assert "study-identifiers-section" in resp.text
    assert _uids(soa_id) == [
        "StudyIdentifier_2",
        "StudyIdentifier_1",
        "StudyIdentifier_3",
    ]

    client.post(f"/ui/soa/{soa_id}/study-identifiers/{first_id}/move_up")
    assert _uids(soa_id) == [
        "StudyIdentifier_1",
        "StudyIdentifier_2",
        "StudyIdentifier_3",
    ]


def test_ui_move_past_ends_is_noop():
    soa_id = _three_identifiers("SI UI Move Ends")
    rows = client.get(f"/soa/{soa_id}/study-identifiers").json()
    before = _uids(soa_id)
    client.post(f"/ui/soa/{soa_id}/study-identifiers/{rows[0]['id']}/move_up")
    client.post(f"/ui/soa/{soa_id}/study-identifiers/{rows[-1]['id']}/move_down")
    assert _uids(soa_id) == before


def test_ui_delete_study_identifier_returns_html():
    soa_id = _new_soa("SI UI Delete")
    org = _new_org(soa_id, "Registry")
    _ui_add(soa_id, "TO-DELETE", org)
    si_id = client.get(f"/soa/{soa_id}/study-identifiers").json()[0]["id"]

    resp = client.post(f"/ui/soa/{soa_id}/study-identifiers/{si_id}/delete")
    assert resp.status_code == 200
    assert "study-identifiers-section" in resp.text
    assert client.get(f"/soa/{soa_id}/study-identifiers").json() == []
