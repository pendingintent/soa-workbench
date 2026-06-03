"""Tests for the indications router (API + USDM output)."""

import re

from fastapi.testclient import TestClient

from soa_builder.web.app import app

client = TestClient(app)


def _new_soa(name: str) -> int:
    r = client.post("/soa", json={"name": name})
    assert r.status_code in (200, 201), r.text
    return r.json()["id"]


def _create_indication(soa_id: int, **kwargs) -> dict:
    body = {"name": "Indication Alpha"}
    body.update(kwargs)
    r = client.post(f"/soa/{soa_id}/indications", json=body)
    assert r.status_code == 201, r.text
    return r.json()


# ---------------------------------------------------------------------------
# UID and CRUD
# ---------------------------------------------------------------------------


def test_create_indication_returns_uid():
    soa_id = _new_soa("Ind Create UID Test")
    body = _create_indication(soa_id, name="Ind 1")
    assert re.match(r"^Indication_\d+$", body["indication_uid"])
    assert body["name"] == "Ind 1"


def test_indication_uid_monotonic():
    soa_id = _new_soa("Ind Monotonic Test")
    r1 = _create_indication(soa_id, name="Ind A")
    r2 = _create_indication(soa_id, name="Ind B")
    assert r1["indication_uid"] == "Indication_1"
    assert r2["indication_uid"] == "Indication_2"

    r = client.delete(f"/soa/{soa_id}/indications/{r1['id']}")
    assert r.status_code == 200

    r3 = _create_indication(soa_id, name="Ind C")
    assert r3["indication_uid"] == "Indication_3"


def test_list_indications():
    soa_id = _new_soa("Ind List Test")
    _create_indication(soa_id, name="Alpha")
    _create_indication(soa_id, name="Beta")
    resp = client.get(f"/soa/{soa_id}/indications")
    assert resp.status_code == 200
    data = resp.json()
    assert len(data) == 2
    assert data[0]["name"] == "Alpha"
    assert data[1]["name"] == "Beta"


def test_list_indications_empty():
    soa_id = _new_soa("Ind Empty Test")
    resp = client.get(f"/soa/{soa_id}/indications")
    assert resp.status_code == 200
    assert resp.json() == []


def test_delete_indication():
    soa_id = _new_soa("Ind Delete Test")
    body = _create_indication(soa_id, name="To Delete")
    iid = body["id"]
    r = client.delete(f"/soa/{soa_id}/indications/{iid}")
    assert r.status_code == 200
    assert client.get(f"/soa/{soa_id}/indications").json() == []


def test_unknown_soa_returns_404():
    r = client.post("/soa/999999/indications", json={"name": "X"})
    assert r.status_code == 404

    r = client.get("/soa/999999/indications")
    assert r.status_code == 404

    r = client.delete("/soa/999999/indications/1")
    assert r.status_code == 404


# ---------------------------------------------------------------------------
# is_rare_disease field
# ---------------------------------------------------------------------------


def test_is_rare_disease_stored():
    soa_id = _new_soa("Ind Rare Disease Test")
    _create_indication(soa_id, name="Rare", is_rare_disease=True)
    _create_indication(soa_id, name="Common", is_rare_disease=False)

    data = client.get(f"/soa/{soa_id}/indications").json()
    assert data[0]["is_rare_disease"] is True
    assert data[1]["is_rare_disease"] is False


# ---------------------------------------------------------------------------
# Codes sub-resource
# ---------------------------------------------------------------------------


def test_add_and_delete_code():
    soa_id = _new_soa("Ind Code Test")
    ind = _create_indication(soa_id, name="Ind Code")
    iid = ind["id"]

    r = client.post(
        f"/soa/{soa_id}/indications/{iid}/codes",
        json={
            "code": "C12345",
            "code_system": "http://www.cdisc.org",
            "code_system_version": "2024-09-27",
            "decode": "Some Indication",
        },
    )
    assert r.status_code == 201, r.text
    code_entry = r.json()
    assert re.match(r"^Code_\d+$", code_entry["code_uid"])

    data = client.get(f"/soa/{soa_id}/indications").json()
    assert len(data[0]["codes"]) == 1
    c = data[0]["codes"][0]
    assert c["code"] == "C12345"
    assert c["decode"] == "Some Indication"

    # Delete
    code_id = data[0]["codes"][0]["id"]
    r = client.delete(f"/soa/{soa_id}/indications/{iid}/codes/{code_id}")
    assert r.status_code == 200

    data = client.get(f"/soa/{soa_id}/indications").json()
    assert data[0]["codes"] == []


def test_delete_indication_cascades():
    soa_id = _new_soa("Ind Cascade Test")
    ind = _create_indication(soa_id, name="Ind Cascade")
    iid = ind["id"]

    client.post(
        f"/soa/{soa_id}/indications/{iid}/codes",
        json={"code": "C99999", "decode": "Test"},
    )
    client.post(
        f"/soa/{soa_id}/indications/{iid}/codes",
        json={"code": "C88888", "decode": "Test 2"},
    )

    r = client.delete(f"/soa/{soa_id}/indications/{iid}")
    assert r.status_code == 200
    assert client.get(f"/soa/{soa_id}/indications").json() == []


# ---------------------------------------------------------------------------
# USDM output
# ---------------------------------------------------------------------------


def test_usdm_indication_in_output():
    soa_id = _new_soa("Ind USDM Test")
    _create_indication(
        soa_id,
        name="Type 2 Diabetes",
        label="T2DM",
        is_rare_disease=False,
    )

    resp = client.get(f"/soa/{soa_id}/usdm_json/full")
    assert resp.status_code == 200
    doc = resp.json()
    design = doc["study"]["versions"][0]["studyDesigns"][0]

    indications = design["indications"]
    assert len(indications) == 1
    ind = indications[0]
    assert ind["instanceType"] == "Indication"
    assert ind["name"] == "Type 2 Diabetes"
    assert ind["label"] == "T2DM"
    assert ind["id"].startswith("Indication_")
    assert ind["isRareDisease"] is False
    assert ind["codes"] == []
    assert ind["notes"] == []


def test_usdm_indication_rare_disease_flag():
    soa_id = _new_soa("Ind USDM Rare Test")
    _create_indication(soa_id, name="Orphan Disease", is_rare_disease=True)

    design = client.get(f"/soa/{soa_id}/usdm_json/full").json()["study"]["versions"][0][
        "studyDesigns"
    ][0]
    assert design["indications"][0]["isRareDisease"] is True


def test_usdm_indication_code_shape():
    soa_id = _new_soa("Ind USDM Code Test")
    ind = _create_indication(soa_id, name="Hypertension")
    iid = ind["id"]

    client.post(
        f"/soa/{soa_id}/indications/{iid}/codes",
        json={
            "code": "C3117",
            "code_system": "http://ncithesaurus.nci.nih.gov",
            "code_system_version": "23.10e",
            "decode": "Hypertensive Disorder",
        },
    )

    design = client.get(f"/soa/{soa_id}/usdm_json/full").json()["study"]["versions"][0][
        "studyDesigns"
    ][0]
    ind_out = design["indications"][0]
    assert len(ind_out["codes"]) == 1
    c = ind_out["codes"][0]
    assert c["instanceType"] == "Code"
    assert c["id"].startswith("Code_")
    assert c["code"] == "C3117"
    assert c["codeSystem"] == "http://ncithesaurus.nci.nih.gov"
    assert c["codeSystemVersion"] == "23.10e"
    assert c["decode"] == "Hypertensive Disorder"
