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
    body = {
        "name": "Study Sponsor",
        "type_concept_id": "C70793",
        "type_preferred_term": "Sponsor",
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
    assert body["name"] == "Study Sponsor"


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
    _create_role(
        soa_id,
        name="Sponsor Role",
        type_concept_id="C70793",
        type_preferred_term="Sponsor",
        type_version="2022-09-30",
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
    assert role["organizationIds"] == []
    assert role["appliesToIds"] == []
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
