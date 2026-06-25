"""Tests for the study_interventions router (API + USDM output)."""

import re
from unittest.mock import patch

from fastapi.testclient import TestClient

from soa_builder.web.app import app

client = TestClient(app)

_MOCK_DDF_SLUG = "ddfct-2026-03-27"
_MOCK_DDF_ROWS = [
    {
        "codelist_code": "C207417",
        "code": "C1909",
        "preferred_term": "Investigational Product",
        "submission_value": "INVESTIGATIONAL PRODUCT",
    },
    {
        "codelist_code": "C207417",
        "code": "C29143",
        "preferred_term": "Placebo",
        "submission_value": "PLACEBO",
    },
]
_MOCK_PROTO_SLUG = "protocolct-2025-09-26"
_MOCK_PROTO_ROWS = [
    {
        "codelist_code": "C99078",
        "code": "C1909",
        "preferred_term": "Drug",
        "submission_value": "Drug",
    },
]
_MOCK_SDTM_SLUG = "sdtmct-2026-03-27"
_MOCK_SDTM_ROWS = [
    {
        "codelist_code": "C66781",
        "code": "C25301",
        "preferred_term": "Day",
        "submission_value": "DAY",
    },
    {
        "codelist_code": "C66781",
        "code": "C29844",
        "preferred_term": "Week",
        "submission_value": "WEEK",
    },
]

_PATCH_DDF_SLUG = patch(
    "soa_builder.web.routers.study_interventions.get_latest_ddf_ct_href",
    return_value=_MOCK_DDF_SLUG,
)
_PATCH_DDF_ROWS = patch(
    "soa_builder.web.routers.study_interventions.get_ddf_ct_rows",
    return_value={"rows": _MOCK_DDF_ROWS},
)
_PATCH_PROTO_SLUG = patch(
    "soa_builder.web.routers.study_interventions.get_latest_protocol_ct_href",
    return_value=_MOCK_PROTO_SLUG,
)
_PATCH_PROTO_ROWS = patch(
    "soa_builder.web.routers.study_interventions.get_protocol_ct_rows",
    return_value={"rows": _MOCK_PROTO_ROWS},
)
_PATCH_SDTM_SLUG = patch(
    "soa_builder.web.routers.study_interventions.get_latest_sdtm_ct_href",
    return_value=_MOCK_SDTM_SLUG,
)
_PATCH_SDTM_ROWS = patch(
    "soa_builder.web.routers.study_interventions.get_sdtm_ct_rows",
    return_value={"rows": _MOCK_SDTM_ROWS},
)

_ALL_PATCHES = (
    _PATCH_DDF_SLUG,
    _PATCH_DDF_ROWS,
    _PATCH_PROTO_SLUG,
    _PATCH_PROTO_ROWS,
    _PATCH_SDTM_SLUG,
    _PATCH_SDTM_ROWS,
)


def _new_soa(name: str) -> int:
    r = client.post("/soa", json={"name": name})
    assert r.status_code in (200, 201), r.text
    return r.json()["id"]


def _create_intervention(soa_id: int, **kwargs) -> dict:
    body = {
        "name": "Drug A",
        "role_concept_id": "C1909",
        "role_decode": "Investigational Product",
        "role_version": "2026-03-27",
        "type_concept_id": "C1909",
        "type_decode": "Drug",
        "type_version": "2025-09-26",
    }
    body.update(kwargs)
    with _PATCH_DDF_SLUG, _PATCH_DDF_ROWS, _PATCH_PROTO_SLUG, _PATCH_PROTO_ROWS:
        r = client.post(f"/soa/{soa_id}/study-interventions", json=body)
    assert r.status_code == 201, r.text
    return r.json()


def test_create_intervention_returns_uid():
    soa_id = _new_soa("SI Create UID Test")
    body = _create_intervention(soa_id)
    assert re.match(r"^StudyIntervention_\d+$", body["intervention_uid"])
    assert body["name"] == "Drug A"


def test_intervention_uid_monotonic():
    soa_id = _new_soa("SI Monotonic Test")
    r1 = _create_intervention(soa_id, name="Drug A")
    r2 = _create_intervention(soa_id, name="Drug B")
    assert r1["intervention_uid"] == "StudyIntervention_1"
    assert r2["intervention_uid"] == "StudyIntervention_2"

    r = client.delete(f"/soa/{soa_id}/study-interventions/{r1['id']}")
    assert r.status_code == 200

    r3 = _create_intervention(soa_id, name="Drug C")
    assert r3["intervention_uid"] == "StudyIntervention_3"


def test_list_interventions():
    soa_id = _new_soa("SI List Test")
    _create_intervention(soa_id, name="Alpha Drug")
    _create_intervention(soa_id, name="Beta Drug")
    resp = client.get(f"/soa/{soa_id}/study-interventions")
    assert resp.status_code == 200
    data = resp.json()
    assert len(data) == 2
    assert data[0]["name"] == "Alpha Drug"
    assert data[1]["name"] == "Beta Drug"


def test_list_interventions_empty():
    soa_id = _new_soa("SI List Empty Test")
    resp = client.get(f"/soa/{soa_id}/study-interventions")
    assert resp.status_code == 200
    assert resp.json() == []


def test_delete_intervention():
    soa_id = _new_soa("SI Delete Test")
    body = _create_intervention(soa_id, name="To Delete")
    iid = body["id"]

    r = client.delete(f"/soa/{soa_id}/study-interventions/{iid}")
    assert r.status_code == 200

    resp = client.get(f"/soa/{soa_id}/study-interventions")
    assert resp.json() == []


def test_unknown_soa_returns_404():
    r = client.post("/soa/999999/study-interventions", json={"name": "X"})
    assert r.status_code == 404

    r = client.get("/soa/999999/study-interventions")
    assert r.status_code == 404

    r = client.delete("/soa/999999/study-interventions/1")
    assert r.status_code == 404


def test_usdm_interventions_in_output():
    soa_id = _new_soa("SI USDM Output Test")
    _create_intervention(
        soa_id,
        name="Drug A",
        role_concept_id="C1909",
        role_decode="Investigational Product",
        role_version="2026-03-27",
        type_concept_id="C1909",
        type_decode="Drug",
        type_version="2025-09-26",
    )
    resp = client.get(f"/soa/{soa_id}/usdm_json/full")
    assert resp.status_code == 200
    doc = resp.json()
    version = doc["study"]["versions"][0]

    interventions = version["studyInterventions"]
    assert len(interventions) == 1
    si = interventions[0]
    assert si["instanceType"] == "StudyIntervention"
    assert si["name"] == "Drug A"
    assert si["id"].startswith("StudyIntervention_")

    role = si["role"]
    assert role["instanceType"] == "Code"
    assert role["code"] == "C1909"
    assert role["decode"] == "Investigational Product"
    assert role["codeSystem"] == "http://www.cdisc.org"
    assert role["codeSystemVersion"] == "2026-03-27"

    type_ = si["type"]
    assert type_["instanceType"] == "Code"
    assert type_["code"] == "C1909"
    assert type_["decode"] == "Drug"
    assert type_["codeSystem"] == "http://www.cdisc.org"
    assert type_["codeSystemVersion"] == "2025-09-26"

    assert si["minimumResponseDuration"] is None
    assert si["codes"] == []
    assert si["administrations"] == []
    assert si["notes"] == []

    # studyInterventionIds in study design must reference the intervention
    design = version["studyDesigns"][0]
    assert si["id"] in design["studyInterventionIds"]


def test_usdm_intervention_with_mrd():
    soa_id = _new_soa("SI MRD Test")
    body = {
        "name": "Drug MRD",
        "role_concept_id": "C1909",
        "role_decode": "Investigational Product",
        "role_version": "2026-03-27",
        "type_concept_id": "C1909",
        "type_decode": "Drug",
        "type_version": "2025-09-26",
        "mrd_value": 28,
        "mrd_unit_concept_id": "C25301",
        "mrd_unit_decode": "Day",
        "mrd_unit_version": "2026-03-27",
    }
    with (
        _PATCH_DDF_SLUG,
        _PATCH_DDF_ROWS,
        _PATCH_PROTO_SLUG,
        _PATCH_PROTO_ROWS,
        _PATCH_SDTM_SLUG,
        _PATCH_SDTM_ROWS,
    ):
        r = client.post(f"/soa/{soa_id}/study-interventions", json=body)
    assert r.status_code == 201, r.text

    resp = client.get(f"/soa/{soa_id}/usdm_json/full")
    assert resp.status_code == 200
    si = resp.json()["study"]["versions"][0]["studyInterventions"][0]

    mrd = si["minimumResponseDuration"]
    assert mrd is not None
    assert mrd["instanceType"] == "Quantity"
    assert mrd["value"] == 28.0
    assert mrd["id"].startswith("Quantity_")

    unit = mrd["unit"]
    assert unit["instanceType"] == "AliasCode"
    assert unit["id"].startswith("AliasCode_")
    std = unit["standardCode"]
    assert std["instanceType"] == "Code"
    assert std["code"] == "C25301"
    assert std["decode"] == "Day"
    assert std["codeSystem"] == "http://www.cdisc.org"
    assert std["codeSystemVersion"] == "2026-03-27"


def test_intervention_codes_add_and_delete():
    soa_id = _new_soa("SI Codes Test")
    si = _create_intervention(soa_id, name="Drug Coded")
    iid = si["id"]

    # Add a code entry
    r = client.post(
        f"/soa/{soa_id}/study-interventions/{iid}/codes",
        json={
            "code": "L01XC",
            "code_system": "http://www.whocc.no/atc",
            "code_system_version": "2024",
            "decode": "Monoclonal antibodies",
        },
    )
    assert r.status_code == 201, r.text
    entry = r.json()
    assert "id" in entry
    assert entry["code_uid"].startswith("Code_")

    # Verify code appears in USDM output
    resp = client.get(f"/soa/{soa_id}/usdm_json/full")
    si_out = resp.json()["study"]["versions"][0]["studyInterventions"][0]
    assert len(si_out["codes"]) == 1
    code = si_out["codes"][0]
    assert code["code"] == "L01XC"
    assert code["codeSystem"] == "http://www.whocc.no/atc"
    assert code["codeSystemVersion"] == "2024"
    assert code["decode"] == "Monoclonal antibodies"
    assert code["instanceType"] == "Code"

    # Delete the code entry
    r = client.delete(f"/soa/{soa_id}/study-interventions/{iid}/codes/{entry['id']}")
    assert r.status_code == 200

    resp = client.get(f"/soa/{soa_id}/usdm_json/full")
    si_out = resp.json()["study"]["versions"][0]["studyInterventions"][0]
    assert si_out["codes"] == []


def test_delete_intervention_cascades_codes():
    soa_id = _new_soa("SI Cascade Delete Test")
    si = _create_intervention(soa_id, name="Drug Cascade")
    iid = si["id"]

    client.post(
        f"/soa/{soa_id}/study-interventions/{iid}/codes",
        json={
            "code": "X01",
            "code_system": "http://example.com",
            "code_system_version": "1.0",
            "decode": "Test code",
        },
    )

    r = client.delete(f"/soa/{soa_id}/study-interventions/{iid}")
    assert r.status_code == 200

    resp = client.get(f"/soa/{soa_id}/study-interventions")
    assert resp.json() == []
