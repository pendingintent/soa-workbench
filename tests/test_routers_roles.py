"""Tests for the roles router (API + USDM output)."""

import re
from unittest.mock import patch

from fastapi.testclient import TestClient

from soa_builder.web.app import app

client = TestClient(app)

_MOCK_DDF_SLUG = "ddfct-2022-09-30"
_MOCK_DDF_ROWS = [
    {
        "codelist_code": "C215480",
        "code": "C70793",
        "preferred_term": "Sponsor",
        "submission_value": "Sponsor",
    },
    {
        "codelist_code": "C215480",
        "code": "C70901",
        "preferred_term": "Contract Research Organization",
        "submission_value": "CRO",
    },
]
_PATCH_SLUG = patch(
    "soa_builder.web.routers.roles.get_latest_ddf_ct_href",
    return_value=_MOCK_DDF_SLUG,
)
_PATCH_ROWS = patch(
    "soa_builder.web.routers.roles.get_ddf_ct_rows",
    return_value={"rows": _MOCK_DDF_ROWS},
)


def _new_soa(name: str) -> int:
    r = client.post("/soa", json={"name": name})
    assert r.status_code in (200, 201), r.text
    return r.json()["id"]


def _create_role(soa_id: int, **kwargs) -> dict:
    # Investigator by default: a sponsor role is constrained (one per
    # study, exactly one organization), see the DDF00201/2 tests.
    body = {
        "name": "Study Investigator",
        "type_concept_id": "C25936",
        "type_preferred_term": "Investigator",
        "type_version": "2022-09-30",
    }
    body.update(kwargs)
    with _PATCH_SLUG, _PATCH_ROWS:
        r = client.post(f"/soa/{soa_id}/roles", json=body)
    assert r.status_code == 201, r.text
    return r.json()


def test_create_role_returns_uid():
    soa_id = _new_soa("Role Create UID Test")
    body = _create_role(soa_id)
    assert re.match(r"^StudyRole_\d+$", body["role_uid"]), body["role_uid"]
    assert body["name"] == "Study Investigator"


def test_role_uid_monotonic():
    soa_id = _new_soa("Role Monotonic Test")
    r1 = _create_role(soa_id, name="Role A")
    r2 = _create_role(soa_id, name="Role B")
    assert r1["role_uid"] == "StudyRole_1"
    assert r2["role_uid"] == "StudyRole_2"

    r = client.delete(f"/soa/{soa_id}/roles/{r1['id']}")
    assert r.status_code == 200

    r3 = _create_role(soa_id, name="Role C")
    assert r3["role_uid"] == "StudyRole_3"


def test_list_roles():
    soa_id = _new_soa("Role List Test")
    _create_role(soa_id, name="Alpha Role")
    _create_role(soa_id, name="Beta Role")
    resp = client.get(f"/soa/{soa_id}/roles")
    assert resp.status_code == 200
    data = resp.json()
    assert len(data) == 2
    assert data[0]["name"] == "Alpha Role"
    assert data[1]["name"] == "Beta Role"


def test_list_roles_empty():
    soa_id = _new_soa("Role List Empty Test")
    resp = client.get(f"/soa/{soa_id}/roles")
    assert resp.status_code == 200
    assert resp.json() == []


def test_delete_role():
    soa_id = _new_soa("Role Delete Test")
    body = _create_role(soa_id, name="To Delete")
    role_id = body["id"]

    r = client.delete(f"/soa/{soa_id}/roles/{role_id}")
    assert r.status_code == 200

    resp = client.get(f"/soa/{soa_id}/roles")
    assert resp.json() == []


def test_unknown_soa_returns_404():
    r = client.post("/soa/999999/roles", json={"name": "X"})
    assert r.status_code == 404

    r = client.get("/soa/999999/roles")
    assert r.status_code == 404

    r = client.delete("/soa/999999/roles/1")
    assert r.status_code == 404


def test_usdm_roles_in_output():
    soa_id = _new_soa("Role USDM Output Test")
    org_uid = _create_org(soa_id, "Sponsor Inc")
    _create_role(
        soa_id,
        name="Sponsor Role",
        type_concept_id="C70793",
        type_preferred_term="Sponsor",
        type_version="2022-09-30",
        organization_ids=[org_uid],
    )
    resp = client.get(f"/soa/{soa_id}/usdm_json/full")
    assert resp.status_code == 200
    doc = resp.json()
    roles = doc["study"]["versions"][0]["roles"]
    assert len(roles) == 1
    role = roles[0]
    assert role["name"] == "Sponsor Role"
    assert role["instanceType"] == "StudyRole"
    assert role["code"]["code"] == "C70793"
    assert role["code"]["decode"] == "Sponsor"
    assert role["code"]["codeSystem"] == "http://www.cdisc.org"
    assert role["code"]["codeSystemVersion"] == "2022-09-30"
    assert role["masking"] is None
    assert role["organizationIds"] == [org_uid]
    # Roles apply to the study version (DDF00189, DDF00203).
    assert role["appliesToIds"] == ["StudyVersion_1"]
    assert role["assignedPersons"] == []


def test_role_with_masking():
    soa_id = _new_soa("Role Masking Test")
    _create_role(soa_id, name="Blinded Role", masking=True)
    resp = client.get(f"/soa/{soa_id}/usdm_json/full")
    assert resp.status_code == 200
    roles = resp.json()["study"]["versions"][0]["roles"]
    assert len(roles) == 1
    masking = roles[0]["masking"]
    assert masking is not None
    assert masking["isMasked"] is True
    assert masking["text"] == "Masked"
    assert masking["instanceType"] == "Masking"
    assert masking["id"].startswith("Masking_")


def test_role_with_organization_ids():
    soa_id = _new_soa("Role Org IDs Test")
    # Create an organization first
    org_r = client.post(
        f"/soa/{soa_id}/organizations",
        json={"name": "Test Org", "type_concept_id": "", "type_preferred_term": ""},
    )
    assert org_r.status_code == 201, org_r.text
    org_uid = org_r.json()["organization_uid"]

    _create_role(
        soa_id,
        name="Org Role",
        organization_ids=[org_uid],
    )
    resp = client.get(f"/soa/{soa_id}/usdm_json/full")
    assert resp.status_code == 200
    roles = resp.json()["study"]["versions"][0]["roles"]
    assert len(roles) == 1
    assert org_uid in roles[0]["organizationIds"]


# ---------------------------------------------------------------------------
# Mutual-exclusion: role.organizationIds vs assignedPerson.organizationId
# ---------------------------------------------------------------------------


def _create_org(soa_id: int, name: str = "Test Org") -> str:
    r = client.post(
        f"/soa/{soa_id}/organizations",
        json={"name": name, "type_concept_id": "", "type_preferred_term": ""},
    )
    assert r.status_code == 201, r.text
    return r.json()["organization_uid"]


def _create_person(soa_id: int, **kwargs) -> dict:
    body = {"name": "Test Person"}
    body.update(kwargs)
    r = client.post(f"/soa/{soa_id}/persons", json=body)
    assert r.status_code == 201, r.text
    return r.json()


def test_create_role_api_org_and_person_with_org_returns_422():
    """JSON create_role: role org + person org → 422."""
    soa_id = _new_soa("Role API Org+PersonOrg Conflict")
    org_uid = _create_org(soa_id)
    person = _create_person(soa_id, organization_uid=org_uid)

    with _PATCH_SLUG, _PATCH_ROWS:
        r = client.post(
            f"/soa/{soa_id}/roles",
            json={
                "name": "Conflicting Role",
                "organization_ids": [org_uid],
                "person_uids": [person["person_uid"]],
            },
        )
    assert r.status_code == 422


def test_create_role_api_org_and_person_without_org_ok():
    """JSON create_role: role org + person without org → 201."""
    soa_id = _new_soa("Role API Org+PersonNoOrg OK")
    org_uid = _create_org(soa_id)
    person = _create_person(soa_id)

    with _PATCH_SLUG, _PATCH_ROWS:
        r = client.post(
            f"/soa/{soa_id}/roles",
            json={
                "name": "Valid Role",
                "organization_ids": [org_uid],
                "person_uids": [person["person_uid"]],
            },
        )
    assert r.status_code == 201


def test_ui_roles_update_org_and_person_with_org_returns_422():
    """HTMX roles update: adding role org to role that has a person with org → 422."""
    soa_id = _new_soa("Role Update Org+PersonOrg Conflict")
    org_uid = _create_org(soa_id)
    person = _create_person(soa_id, organization_uid=org_uid)
    role = _create_role(soa_id, name="Role To Update")
    role_id = role["id"]

    with _PATCH_SLUG, _PATCH_ROWS:
        r = client.post(
            f"/ui/soa/{soa_id}/roles/{role_id}/update",
            data={
                "name": "Updated Role",
                "organization_ids": [org_uid],
                "person_uids": [person["person_uid"]],
            },
        )
    assert r.status_code == 422


# ---------------------------------------------------------------------------
# Sponsor role rules (USDM v4)
#   DDF00201: exactly one study role with a code of sponsor
#   DDF00202: the sponsor study role points to exactly one organization
# ---------------------------------------------------------------------------

_SPONSOR = {
    "type_concept_id": "C70793",
    "type_preferred_term": "Sponsor",
    "type_version": "2022-09-30",
}


def _post_role(soa_id: int, **kwargs):
    body = {"name": "Role", **kwargs}
    with _PATCH_SLUG, _PATCH_ROWS:
        return client.post(f"/soa/{soa_id}/roles", json=body)


def _ui_role(soa_id: int, path: str, **data):
    form = {"name": "Role", **data}
    with _PATCH_SLUG, _PATCH_ROWS:
        return client.post(f"/ui/soa/{soa_id}/{path}", data=form)


def test_sponsor_role_with_one_org_ok():
    soa_id = _new_soa("Sponsor Role One Org")
    org_uid = _create_org(soa_id, "Sponsor Inc")
    r = _post_role(soa_id, organization_ids=[org_uid], **_SPONSOR)
    assert r.status_code == 201, r.text


def test_second_sponsor_role_rejected():
    soa_id = _new_soa("Sponsor Role Twice")
    org_a = _create_org(soa_id, "Sponsor A")
    org_b = _create_org(soa_id, "Sponsor B")
    assert _post_role(soa_id, organization_ids=[org_a], **_SPONSOR).status_code == 201
    r = _post_role(soa_id, organization_ids=[org_b], **_SPONSOR)
    assert r.status_code == 422
    assert "DDF00201" in r.json()["detail"]


def test_sponsor_role_without_org_rejected():
    soa_id = _new_soa("Sponsor Role No Org")
    r = _post_role(soa_id, **_SPONSOR)
    assert r.status_code == 422
    assert "DDF00202" in r.json()["detail"]


def test_sponsor_role_with_two_orgs_rejected():
    soa_id = _new_soa("Sponsor Role Two Orgs")
    org_a = _create_org(soa_id, "Sponsor A")
    org_b = _create_org(soa_id, "Sponsor B")
    r = _post_role(soa_id, organization_ids=[org_a, org_b], **_SPONSOR)
    assert r.status_code == 422
    assert "DDF00202" in r.json()["detail"]


def test_co_sponsor_roles_not_limited():
    soa_id = _new_soa("Co-Sponsor Roles")
    co = {"type_concept_id": "C215669", "type_preferred_term": "Co-Sponsor"}
    assert _post_role(soa_id, **co).status_code == 201
    assert _post_role(soa_id, **co).status_code == 201


def test_ui_add_second_sponsor_role_rejected():
    soa_id = _new_soa("UI Sponsor Role Twice")
    org_a = _create_org(soa_id, "Sponsor A")
    org_b = _create_org(soa_id, "Sponsor B")
    sponsor_form = {"role_type_code": "C70793", "role_type_decode": "Sponsor"}
    r1 = _ui_role(soa_id, "roles-add", organization_ids=[org_a], **sponsor_form)
    assert r1.status_code == 200
    r2 = _ui_role(soa_id, "roles-add", organization_ids=[org_b], **sponsor_form)
    assert r2.status_code == 422
    # The roles section is re-rendered with the error, and swapped in
    # despite the 422 (HTMX 1.x skips 4xx swaps unless told to).
    assert "roles-section" in r2.text
    assert "am-api-notice-error" in r2.text
    assert "DDF00201" in r2.text
    assert "before-swap" in r2.text
    roles = client.get(f"/soa/{soa_id}/roles").json()
    assert len(roles) == 1


def test_ui_add_sponsor_role_without_org_rejected():
    soa_id = _new_soa("UI Sponsor Role No Org")
    r = _ui_role(
        soa_id, "roles-add", role_type_code="C70793", role_type_decode="Sponsor"
    )
    assert r.status_code == 422
    assert client.get(f"/soa/{soa_id}/roles").json() == []


def test_ui_update_sponsor_role_keeps_itself_valid():
    # Re-saving the one sponsor role must not count it as a second one.
    soa_id = _new_soa("UI Sponsor Role Resave")
    org_uid = _create_org(soa_id, "Sponsor Inc")
    role = _post_role(soa_id, organization_ids=[org_uid], **_SPONSOR).json()
    r = _ui_role(
        soa_id,
        f"roles/{role['id']}/update",
        name="Sponsor (renamed)",
        role_type_code="C70793",
        role_type_decode="Sponsor",
        organization_ids=[org_uid],
    )
    assert r.status_code == 200, r.text


def test_ui_update_to_second_sponsor_role_rejected():
    soa_id = _new_soa("UI Sponsor Role Update Twice")
    org_a = _create_org(soa_id, "Sponsor A")
    org_b = _create_org(soa_id, "Investigator Site")
    _post_role(soa_id, organization_ids=[org_a], **_SPONSOR)
    other = _create_role(soa_id, name="Investigator")
    r = _ui_role(
        soa_id,
        f"roles/{other['id']}/update",
        role_type_code="C70793",
        role_type_decode="Sponsor",
        organization_ids=[org_b],
    )
    assert r.status_code == 422
    assert "DDF00201" in r.text


def test_ui_role_person_org_conflict_shows_error():
    soa_id = _new_soa("UI Role Org+Person Conflict Message")
    org_uid = _create_org(soa_id)
    person = _create_person(soa_id, organization_uid=org_uid)
    r = _ui_role(
        soa_id,
        "roles-add",
        organization_ids=[org_uid],
        person_uids=[person["person_uid"]],
    )
    assert r.status_code == 422
    assert "am-api-notice-error" in r.text
    assert client.get(f"/soa/{soa_id}/roles").json() == []
