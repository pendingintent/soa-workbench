"""Tests for the organizations router (API + USDM output)."""

from unittest.mock import patch

from fastapi.testclient import TestClient

from soa_builder.web.app import app

client = TestClient(app)

_MOCK_DDF_SLUG = "ddfct-2022-09-30"
_PATCH_SLUG = patch(
    "soa_builder.web.routers.organizations.get_latest_ddf_ct_href",
    return_value=_MOCK_DDF_SLUG,
)


def _new_soa(name: str) -> int:
    r = client.post("/soa", json={"name": name})
    return r.json()["id"]


def _create_org(soa_id: int, **kwargs) -> dict:
    body = {
        "name": "Test Org",
        "type_concept_id": "C70793",
        "type_preferred_term": "Sponsor",
        "type_version": "2022-09-30",
    }
    body.update(kwargs)
    r = client.post(f"/soa/{soa_id}/organizations", json=body)
    assert r.status_code == 201, r.text
    return r.json()


def test_create_organization_returns_uid():
    soa_id = _new_soa("Org Create UID Test")
    with _PATCH_SLUG:
        body = _create_org(soa_id)
    assert body["organization_uid"].startswith("Organization_")
    assert body["name"] == "Test Org"


def test_create_organization_uid_monotonic():
    soa_id = _new_soa("Org Monotonic Test")
    with _PATCH_SLUG:
        o1 = _create_org(soa_id, name="Org A")
        o2 = _create_org(soa_id, name="Org B")
    assert o1["organization_uid"] == "Organization_1"
    assert o2["organization_uid"] == "Organization_2"

    # Delete first
    r = client.delete(f"/soa/{soa_id}/organizations/{o1['id']}")
    assert r.status_code == 200

    # Third must be Organization_3 (never reuses 1)
    with _PATCH_SLUG:
        o3 = _create_org(soa_id, name="Org C")
    assert o3["organization_uid"] == "Organization_3"


def test_list_organizations():
    soa_id = _new_soa("Org List Test")
    with _PATCH_SLUG:
        _create_org(soa_id, name="Alpha Org")
        _create_org(soa_id, name="Beta Org")
    resp = client.get(f"/soa/{soa_id}/organizations")
    assert resp.status_code == 200
    data = resp.json()
    assert len(data) == 2
    assert data[0]["name"] == "Alpha Org"
    assert data[1]["name"] == "Beta Org"


def test_list_organizations_empty():
    soa_id = _new_soa("Org List Empty Test")
    resp = client.get(f"/soa/{soa_id}/organizations")
    assert resp.status_code == 200
    assert resp.json() == []


def test_delete_organization():
    soa_id = _new_soa("Org Delete Test")
    with _PATCH_SLUG:
        body = _create_org(soa_id, name="To Delete")
    org_id = body["id"]

    r = client.delete(f"/soa/{soa_id}/organizations/{org_id}")
    assert r.status_code == 200

    resp = client.get(f"/soa/{soa_id}/organizations")
    assert resp.json() == []


def test_unknown_soa_returns_404():
    r = client.post("/soa/999999/organizations", json={"name": "X"})
    assert r.status_code == 404

    r = client.get("/soa/999999/organizations")
    assert r.status_code == 404

    r = client.delete("/soa/999999/organizations/1")
    assert r.status_code == 404


def test_usdm_organizations_in_output():
    soa_id = _new_soa("Org USDM Output Test")
    with _PATCH_SLUG:
        _create_org(
            soa_id,
            name="CDISC",
            identifier="12345",
            identifier_scheme="DUNS",
            type_concept_id="C70793",
            type_preferred_term="Sponsor",
        )
    resp = client.get(f"/soa/{soa_id}/usdm_json/full")
    assert resp.status_code == 200
    doc = resp.json()
    orgs = doc["study"]["versions"][0]["organizations"]
    assert len(orgs) == 1
    org = orgs[0]
    assert org["name"] == "CDISC"
    assert org["identifier"] == "12345"
    assert org["identifierScheme"] == "DUNS"
    assert org["instanceType"] == "Organization"
    assert org["type"]["code"] == "C70793"
    assert org["type"]["decode"] == "Sponsor"
    assert org["type"]["codeSystem"] == "http://www.cdisc.org"
    assert org["type"]["codeSystemVersion"] == "2022-09-30"
    assert org["legalAddress"] is None


def test_organization_with_address():
    soa_id = _new_soa("Org Address Test")
    with _PATCH_SLUG:
        _create_org(
            soa_id,
            name="Global HQ",
            addr_text="123 Main St",
            addr_lines=["Suite 100", "Floor 3"],
            addr_city="Austin",
            addr_state="TX",
            addr_postal_code="78701",
            addr_country_numeric="840",
            addr_country_name="United States of America",
        )
    resp = client.get(f"/soa/{soa_id}/usdm_json/full")
    assert resp.status_code == 200
    orgs = resp.json()["study"]["versions"][0]["organizations"]
    assert len(orgs) == 1
    addr = orgs[0]["legalAddress"]
    assert addr is not None
    assert addr["instanceType"] == "Address"
    assert addr["text"] == "123 Main St"
    assert addr["lines"] == ["Suite 100", "Floor 3"]
    assert addr["city"] == "Austin"
    assert addr["state"] == "TX"
    assert addr["postalCode"] == "78701"
    assert addr["country"]["code"] == "840"
    assert addr["country"]["codeSystem"] == "ISO 3166 1 Numeric Code"
    assert addr["country"]["decode"] == "United States of America"


def test_usdm_empty_organizations():
    soa_id = _new_soa("Org USDM Empty Test")
    resp = client.get(f"/soa/{soa_id}/usdm_json/full")
    assert resp.status_code == 200
    orgs = resp.json()["study"]["versions"][0]["organizations"]
    assert orgs == []
