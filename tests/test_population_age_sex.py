"""StudyDesignPopulation planned sex and planned age (USDM v4.0).

DDF00098 needs plannedSex and DDF00097 plannedAge on the population
(or on cohorts, not both). plannedSex holds up to two codes from the
Sex of Participants codelist (C66732). plannedAge is a Range whose
bounds are Quantities with an age unit (C66781).

A protocol often states only one bound ("aged 18 years or over").
USDM v4.0 requires both bounds; v4.1 is expected to allow either to be
absent, so a bound given as n/a is exported as null.
"""

import pytest
from fastapi.testclient import TestClient

from soa_builder.web import utils
from soa_builder.web.app import app

client = TestClient(app)

POP = "StudyDesignPopulation_1"


def _term(codelist: str, code: str, value: str) -> dict:
    return {"codelist_code": codelist, "code": code, "submission_value": value}


@pytest.fixture(autouse=True)
def sdtm_ct(monkeypatch):
    payload = {
        "slug": "sdtmct-2026-09-25",
        "rows": [
            _term("C66732", "C16576", "F"),
            _term("C66732", "C20197", "M"),
            _term("C66732", "C49636", "BOTH"),
            _term("C66781", "C29848", "YEARS"),
            _term("C66781", "C29846", "MONTHS"),
        ],
    }
    monkeypatch.setattr(utils, "get_sdtm_ct_rows", lambda *a, **k: payload)


def _new_soa(name: str) -> int:
    return client.post("/soa", json={"name": name}).json()["id"]


def _population(soa_id: int) -> dict:
    resp = client.get(f"/soa/{soa_id}/usdm_json/full")
    assert resp.status_code == 200
    return resp.json()["study"]["versions"][0]["studyDesigns"][0]["population"]


def _code(code_id: str, code: str, decode: str) -> dict:
    return {
        "id": code_id,
        "extensionAttributes": [],
        "code": code,
        "codeSystem": "http://www.cdisc.org",
        "codeSystemVersion": "2026-09-25",
        "decode": decode,
        "instanceType": "Code",
    }


def _quantity(prefix: str, value: float) -> dict:
    return {
        "id": prefix,
        "extensionAttributes": [],
        "value": value,
        "unit": {
            "id": f"{prefix}Unit",
            "extensionAttributes": [],
            "standardCode": _code(f"{prefix}UnitCode", "C29848", "YEARS"),
            "standardCodeAliases": [],
            "instanceType": "AliasCode",
        },
        "instanceType": "Quantity",
    }


# --- Planned sex --------------------------------------------------------


def test_planned_sex_exports_codes():
    soa_id = _new_soa("Sex both")

    resp = client.put(
        f"/soa/{soa_id}/population/planned_sex",
        json={"codes": ["C16576", "C20197"]},
    )

    assert resp.status_code == 200, resp.text
    assert _population(soa_id)["plannedSex"] == [
        _code(f"{POP}_PlannedSex_1", "C16576", "F"),
        _code(f"{POP}_PlannedSex_2", "C20197", "M"),
    ]


@pytest.mark.parametrize(
    "codes",
    [
        [],
        ["C29848"],
        ["C16576", "C16576"],
        ["C16576", "C20197", "C49636"],
    ],
)
def test_planned_sex_rejects_invalid_codes(codes):
    soa_id = _new_soa("Sex invalid")

    resp = client.put(f"/soa/{soa_id}/population/planned_sex", json={"codes": codes})

    assert resp.status_code == 400
    assert _population(soa_id)["plannedSex"] == []


def test_clear_planned_sex():
    soa_id = _new_soa("Sex clear")
    client.put(f"/soa/{soa_id}/population/planned_sex", json={"codes": ["C16576"]})

    resp = client.delete(f"/soa/{soa_id}/population/planned_sex")

    assert resp.status_code == 204
    assert _population(soa_id)["plannedSex"] == []


# --- Planned age --------------------------------------------------------


def test_planned_age_with_both_bounds():
    soa_id = _new_soa("Age both")

    resp = client.put(
        f"/soa/{soa_id}/population/planned_age",
        json={"min_value": 18, "max_value": 75, "unit_code": "C29848"},
    )

    assert resp.status_code == 200, resp.text
    assert _population(soa_id)["plannedAge"] == {
        "id": f"{POP}_PlannedAge",
        "extensionAttributes": [],
        "minValue": _quantity(f"{POP}_PlannedAgeMin", 18.0),
        "maxValue": _quantity(f"{POP}_PlannedAgeMax", 75.0),
        "isApproximate": False,
        "instanceType": "Range",
    }


def test_planned_age_without_maximum_exports_null_bound():
    soa_id = _new_soa("Age no max")

    client.put(
        f"/soa/{soa_id}/population/planned_age",
        json={"min_value": 18, "unit_code": "C29848"},
    )

    age = _population(soa_id)["plannedAge"]
    assert age["minValue"] == _quantity(f"{POP}_PlannedAgeMin", 18.0)
    assert age["maxValue"] is None


def test_planned_age_without_minimum_exports_null_bound():
    soa_id = _new_soa("Age no min")

    client.put(
        f"/soa/{soa_id}/population/planned_age",
        json={"max_value": 65, "unit_code": "C29848", "is_approximate": True},
    )

    age = _population(soa_id)["plannedAge"]
    assert age["minValue"] is None
    assert age["maxValue"]["value"] == 65.0
    assert age["isApproximate"] is True


@pytest.mark.parametrize(
    "body",
    [
        {"unit_code": "C29848"},
        {"min_value": 18},
        {"min_value": 18, "unit_code": "C16576"},
        {"min_value": -1, "unit_code": "C29848"},
        {"min_value": 65, "max_value": 18, "unit_code": "C29848"},
    ],
)
def test_planned_age_rejects_invalid_input(body):
    soa_id = _new_soa("Age invalid")

    resp = client.put(f"/soa/{soa_id}/population/planned_age", json=body)

    assert resp.status_code == 400
    assert _population(soa_id)["plannedAge"] is None


def test_clear_planned_age():
    soa_id = _new_soa("Age clear")
    client.put(
        f"/soa/{soa_id}/population/planned_age",
        json={"min_value": 18, "unit_code": "C29848"},
    )

    resp = client.delete(f"/soa/{soa_id}/population/planned_age")

    assert resp.status_code == 204
    assert _population(soa_id)["plannedAge"] is None


def test_population_404_for_missing_soa():
    resp = client.put("/soa/999999/population/planned_sex", json={"codes": ["C16576"]})
    assert resp.status_code == 404


# --- UI -----------------------------------------------------------------


def test_population_ui_saves_sex_and_age():
    soa_id = _new_soa("Population UI")

    page = client.get(f"/ui/soa/{soa_id}/population")
    assert page.status_code == 200
    assert "YEARS" in page.text and "BOTH" in page.text

    sex = client.post(
        f"/ui/soa/{soa_id}/population/planned_sex",
        data={"codes": ["C16576", "C20197"]},
    )
    assert sex.status_code == 200
    age = client.post(
        f"/ui/soa/{soa_id}/population/planned_age",
        data={"min_value": "18", "max_value": "", "unit_code": "C29848"},
    )
    assert age.status_code == 200

    population = _population(soa_id)
    assert [c["code"] for c in population["plannedSex"]] == ["C16576", "C20197"]
    assert population["plannedAge"]["minValue"]["value"] == 18.0
    assert population["plannedAge"]["maxValue"] is None


def test_population_ui_shows_validation_error():
    soa_id = _new_soa("Population UI error")

    resp = client.post(
        f"/ui/soa/{soa_id}/population/planned_age",
        data={"min_value": "", "max_value": "", "unit_code": "C29848"},
    )

    assert resp.status_code == 200
    assert "at least one" in resp.text
    assert _population(soa_id)["plannedAge"] is None
