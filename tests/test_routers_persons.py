"""Tests for the PersonName router (API + USDM output)."""

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
    body = {"name": "Sponsor Role"}
    body.update(kwargs)
    with _PATCH_SLUG, _PATCH_ROWS:
        r = client.post(f"/soa/{soa_id}/roles", json=body)
    assert r.status_code == 201, r.text
    return r.json()


def _create_person(soa_id: int, **kwargs) -> dict:
    body = {"name": "Jane Smith"}
    body.update(kwargs)
    r = client.post(f"/soa/{soa_id}/persons", json=body)
    assert r.status_code == 201, r.text
    return r.json()


# ---------------------------------------------------------------------------
# CRUD
# ---------------------------------------------------------------------------


def test_create_person_returns_uid():
    soa_id = _new_soa("PersonName Create UID Test")
    body = _create_person(soa_id)
    assert re.match(r"^Person_\d+$", body["person_uid"]), body["person_uid"]
    assert body["name"] == "Jane Smith"


def test_person_uid_monotonic():
    soa_id = _new_soa("PersonName Monotonic Test")
    p1 = _create_person(soa_id, name="Alice")
    p2 = _create_person(soa_id, name="Bob")
    assert p1["person_uid"] == "Person_1"
    assert p2["person_uid"] == "Person_2"

    r = client.delete(f"/soa/{soa_id}/persons/{p1['id']}")
    assert r.status_code == 200

    p3 = _create_person(soa_id, name="Carol")
    assert p3["person_uid"] == "Person_3"


def test_list_persons():
    soa_id = _new_soa("PersonName List Test")
    _create_person(soa_id, name="Alpha")
    _create_person(soa_id, name="Beta")
    resp = client.get(f"/soa/{soa_id}/persons")
    assert resp.status_code == 200
    data = resp.json()
    assert len(data) == 2
    assert data[0]["name"] == "Alpha"
    assert data[1]["name"] == "Beta"


def test_list_persons_empty():
    soa_id = _new_soa("PersonName List Empty Test")
    resp = client.get(f"/soa/{soa_id}/persons")
    assert resp.status_code == 200
    assert resp.json() == []


def test_delete_person():
    soa_id = _new_soa("PersonName Delete Test")
    body = _create_person(soa_id, name="To Delete")
    person_id = body["id"]

    r = client.delete(f"/soa/{soa_id}/persons/{person_id}")
    assert r.status_code == 200

    resp = client.get(f"/soa/{soa_id}/persons")
    assert resp.json() == []


def test_unknown_soa_returns_404():
    r = client.post("/soa/999999/persons", json={"name": "X"})
    assert r.status_code == 404

    r = client.get("/soa/999999/persons")
    assert r.status_code == 404

    r = client.delete("/soa/999999/persons/1")
    assert r.status_code == 404


def test_create_person_missing_name_returns_400():
    soa_id = _new_soa("PersonName Bad Name Test")
    r = client.post(f"/soa/{soa_id}/persons", json={})
    assert r.status_code == 400


def test_person_name_fields_stored_and_returned():
    soa_id = _new_soa("PersonName Fields Test")
    _create_person(
        soa_id,
        name="Noah Kornblum",
        text="Noah Kornblum, MD",
        family_name="Kornblum",
        given_names=["Noah"],
        prefixes=["Dr"],
        suffixes=["MD"],
    )
    resp = client.get(f"/soa/{soa_id}/persons")
    assert resp.status_code == 200
    p = resp.json()[0]
    assert p["text"] == "Noah Kornblum, MD"
    assert p["family_name"] == "Kornblum"
    assert p["given_names"] == ["Noah"]
    assert p["prefixes"] == ["Dr"]
    assert p["suffixes"] == ["MD"]


def test_person_name_uid_suffix_matches():
    soa_id = _new_soa("PersonName UID Suffix Test")
    _create_person(soa_id, name="First")
    _create_person(soa_id, name="Second")

    resp = client.get(f"/soa/{soa_id}/persons")
    persons = resp.json()
    for p in persons:
        person_n = p["person_uid"].split("_")[-1]
        pn_n = p["person_name_uid"].split("_")[-1]
        assert person_n == pn_n, (
            f"Suffix mismatch: {p['person_uid']} vs {p['person_name_uid']}"
        )


# ---------------------------------------------------------------------------
# Role assignment (now from the ROLE side)
# ---------------------------------------------------------------------------


def test_assign_person_to_role_via_role_form():
    """Persons are assigned through the role, not through the person form."""
    soa_id = _new_soa("PersonName Assign Via Role Test")
    person = _create_person(soa_id, name="Noah Kornblum")
    person_uid = person["person_uid"]

    with _PATCH_SLUG, _PATCH_ROWS:
        r = client.post(
            f"/soa/{soa_id}/roles",
            json={"name": "Study Chair Role", "role_uids": [person_uid]},
        )
    assert r.status_code == 201, r.text


def test_delete_person_removes_role_assignments():
    soa_id = _new_soa("PersonName Delete Cascade Test")
    person = _create_person(soa_id, name="Eve")
    person_id = person["id"]

    r = client.delete(f"/soa/{soa_id}/persons/{person_id}")
    assert r.status_code == 200

    resp = client.get(f"/soa/{soa_id}/persons")
    assert resp.json() == []


# ---------------------------------------------------------------------------
# USDM output
# ---------------------------------------------------------------------------


def test_usdm_assigned_persons_empty_without_assignment():
    soa_id = _new_soa("PersonName USDM Empty Test")
    _create_role(soa_id, name="Sponsor Role")

    resp = client.get(f"/soa/{soa_id}/usdm_json/full")
    assert resp.status_code == 200
    roles = resp.json()["study"]["versions"][0]["roles"]
    assert roles[0]["assignedPersons"] == []


def test_usdm_assigned_persons_populated_via_htmx_add():
    """
    The HTMX roles-add endpoint accepts person_uids and creates
    role_person rows. Verify the USDM output reflects them.
    """
    soa_id = _new_soa("PersonName USDM Populated Test")
    person = _create_person(
        soa_id,
        name="Noah Kornblum",
        job_title="Study Chair",
        text="Noah Kornblum, MD",
        family_name="Kornblum",
        given_names=["Noah"],
        prefixes=["Dr"],
        suffixes=["MD"],
    )
    person_uid = person["person_uid"]

    with _PATCH_SLUG, _PATCH_ROWS:
        r = client.post(
            "/ui/soa/{}/roles-add".format(soa_id),
            data={
                "name": "Study Chair Role",
                "person_uids": [person_uid],
            },
        )
    assert r.status_code == 200, r.text

    resp = client.get(f"/soa/{soa_id}/usdm_json/full")
    assert resp.status_code == 200
    roles = resp.json()["study"]["versions"][0]["roles"]
    assert len(roles) == 1
    assigned = roles[0]["assignedPersons"]
    assert len(assigned) == 1

    ap = assigned[0]
    assert ap["name"] == "Noah Kornblum"
    assert ap["jobTitle"] == "Study Chair"
    assert ap["instanceType"] == "AssignedPerson"
    assert re.match(r"^Person_\d+$", ap["id"])

    pn = ap["personName"]
    assert pn["instanceType"] == "PersonName"
    assert pn["text"] == "Noah Kornblum, MD"
    assert pn["familyName"] == "Kornblum"
    assert pn["givenNames"] == ["Noah"]
    assert pn["prefixes"] == ["Dr"]
    assert pn["suffixes"] == ["MD"]
    assert re.match(r"^PersonName_\d+$", pn["id"])


def test_usdm_person_with_organization():
    soa_id = _new_soa("PersonName USDM Org Test")
    org_r = client.post(
        f"/soa/{soa_id}/organizations",
        json={
            "name": "Test Org",
            "type_concept_id": "",
            "type_preferred_term": "",
        },
    )
    assert org_r.status_code == 201, org_r.text
    org_uid = org_r.json()["organization_uid"]

    person = _create_person(
        soa_id,
        name="Alice",
        organization_uid=org_uid,
    )
    person_uid = person["person_uid"]

    with _PATCH_SLUG, _PATCH_ROWS:
        r = client.post(
            "/ui/soa/{}/roles-add".format(soa_id),
            data={
                "name": "Sponsor",
                "person_uids": [person_uid],
            },
        )
    assert r.status_code == 200

    resp = client.get(f"/soa/{soa_id}/usdm_json/full")
    assert resp.status_code == 200
    roles = resp.json()["study"]["versions"][0]["roles"]
    ap = roles[0]["assignedPersons"][0]
    assert ap["organizationId"] == org_uid
