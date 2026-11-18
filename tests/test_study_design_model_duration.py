"""Intervention model and planned duration (USDM v4.0).

InterventionalStudyDesign.model is a required Code from the SDTM
Intervention Model Response codelist (C99076, DDF00216).
ScheduleTimeline.plannedDuration is a Duration (DDF00153 expects one on
the main timeline) whose quantity unit is a CDISC unit term.
"""

import pytest
from fastapi.testclient import TestClient

from soa_builder.web import utils
from soa_builder.web.app import app
from soa_builder.web.db import _connect
from soa_builder.web.routers._freeze_helpers.capture import (
    _capture_schedule_timelines,
)
from usdm.generate_schedule_timelines import build_usdm_schedule_timelines

client = TestClient(app)

SDTM_SLUG = "sdtmct-2026-09-25"


def _term(codelist: str, code: str, value: str) -> dict:
    return {"codelist_code": codelist, "code": code, "submission_value": value}


@pytest.fixture(autouse=True)
def sdtm_ct(monkeypatch):
    payload = {
        "slug": SDTM_SLUG,
        "rows": [
            _term("C99076", "C82639", "PARALLEL"),
            _term("C99076", "C82637", "CROSSOVER"),
            _term("C66781", "C29844", "WEEKS"),
            _term("C66781", "C25301", "DAYS"),
        ],
    }
    monkeypatch.setattr(utils, "get_sdtm_ct_rows", lambda *a, **k: payload)


def _new_soa(name: str) -> int:
    return client.post("/soa", json={"name": name}).json()["id"]


def _design(soa_id: int) -> dict:
    resp = client.get(f"/soa/{soa_id}/usdm_json/full")
    assert resp.status_code == 200
    return resp.json()["study"]["versions"][0]["studyDesigns"][0]


def _timeline(soa_id: int) -> tuple[int, str]:
    resp = client.post(
        f"/soa/{soa_id}/schedule_timelines",
        json={"name": "Main", "main_timeline": True},
    )
    assert resp.status_code in (200, 201), resp.text
    body = resp.json()
    return body["id"], body["schedule_timeline_uid"]


# --- Intervention model -------------------------------------------------


def test_set_intervention_model_exports_cdisc_code():
    soa_id = _new_soa("Model set")

    resp = client.put(f"/soa/{soa_id}/intervention_model", json={"code": "C82639"})

    assert resp.status_code == 200, resp.text
    assert resp.json() == {
        "code": "C82639",
        "decode": "PARALLEL",
        "code_system_version": "2026-09-25",
    }
    model = _design(soa_id)["model"]
    assert model["code"] == "C82639"
    assert model["decode"] == "PARALLEL"
    assert model["codeSystem"] == "http://www.cdisc.org"
    assert model["codeSystemVersion"] == "2026-09-25"
    assert model["instanceType"] == "Code"


def test_intervention_model_must_be_in_codelist():
    soa_id = _new_soa("Model invalid")

    resp = client.put(f"/soa/{soa_id}/intervention_model", json={"code": "C29844"})

    assert resp.status_code == 400
    assert client.get(f"/soa/{soa_id}/intervention_model").json() is None


def test_clear_intervention_model():
    soa_id = _new_soa("Model clear")
    client.put(f"/soa/{soa_id}/intervention_model", json={"code": "C82637"})

    resp = client.delete(f"/soa/{soa_id}/intervention_model")

    assert resp.status_code == 204
    assert client.get(f"/soa/{soa_id}/intervention_model").json() is None
    assert _design(soa_id)["model"]["code"] == ""


def test_intervention_model_404_for_missing_soa():
    resp = client.put("/soa/999999/intervention_model", json={"code": "C82639"})
    assert resp.status_code == 404


def test_intervention_model_ui_partial_saves_selection():
    soa_id = _new_soa("Model UI")

    page = client.get(f"/ui/soa/{soa_id}/intervention_model")
    assert page.status_code == 200
    assert "PARALLEL" in page.text

    saved = client.post(f"/ui/soa/{soa_id}/intervention_model", data={"code": "C82639"})
    assert saved.status_code == 200
    assert 'value="C82639" selected' in saved.text
    cleared = client.post(f"/ui/soa/{soa_id}/intervention_model", data={"code": ""})
    assert cleared.status_code == 200
    assert client.get(f"/soa/{soa_id}/intervention_model").json() is None


# --- Planned duration ---------------------------------------------------


def _duration_url(soa_id: int, timeline_id: int) -> str:
    return f"/soa/{soa_id}/schedule_timelines/{timeline_id}/planned_duration"


def test_planned_duration_exports_duration_with_quantity():
    soa_id = _new_soa("Duration set")
    tl_id, tl_uid = _timeline(soa_id)

    resp = client.put(
        _duration_url(soa_id, tl_id),
        json={
            "text": "About 85 weeks",
            "value": 85,
            "unit_code": "C29844",
            "duration_will_vary": True,
            "reason_duration_will_vary": "Premature discontinuation",
        },
    )

    assert resp.status_code == 200, resp.text
    timeline = next(
        t for t in build_usdm_schedule_timelines(soa_id) if t["id"] == tl_uid
    )
    assert timeline["plannedDuration"] == {
        "id": f"{tl_uid}_PlannedDuration",
        "extensionAttributes": [],
        "text": "About 85 weeks",
        "quantity": {
            "id": f"{tl_uid}_PlannedDurationQuantity",
            "extensionAttributes": [],
            "value": 85.0,
            "unit": {
                "id": f"{tl_uid}_PlannedDurationUnit",
                "extensionAttributes": [],
                "standardCode": {
                    "id": f"{tl_uid}_PlannedDurationUnitCode",
                    "extensionAttributes": [],
                    "code": "C29844",
                    "codeSystem": "http://www.cdisc.org",
                    "codeSystemVersion": "2026-09-25",
                    "decode": "WEEKS",
                    "instanceType": "Code",
                },
                "standardCodeAliases": [],
                "instanceType": "AliasCode",
            },
            "instanceType": "Quantity",
        },
        "durationWillVary": True,
        "reasonDurationWillVary": "Premature discontinuation",
        "instanceType": "Duration",
    }


def test_planned_duration_text_only_has_no_quantity():
    soa_id = _new_soa("Duration text")
    tl_id, tl_uid = _timeline(soa_id)

    client.put(_duration_url(soa_id, tl_id), json={"text": "Until progression"})

    duration = next(
        t for t in build_usdm_schedule_timelines(soa_id) if t["id"] == tl_uid
    )["plannedDuration"]
    assert duration["text"] == "Until progression"
    assert duration["quantity"] is None
    assert duration["durationWillVary"] is False


@pytest.mark.parametrize(
    "body",
    [
        {},
        {"value": 85},
        {"value": 85, "unit_code": "C82639"},
        {"value": 0, "unit_code": "C29844"},
        {"unit_code": "C29844"},
    ],
)
def test_planned_duration_rejects_invalid_input(body):
    soa_id = _new_soa("Duration invalid")
    tl_id, _ = _timeline(soa_id)

    resp = client.put(_duration_url(soa_id, tl_id), json=body)

    assert resp.status_code in (400, 422)


def test_clear_planned_duration_and_audit():
    soa_id = _new_soa("Duration clear")
    tl_id, tl_uid = _timeline(soa_id)
    client.put(_duration_url(soa_id, tl_id), json={"value": 2, "unit_code": "C25301"})

    resp = client.delete(_duration_url(soa_id, tl_id))

    assert resp.status_code == 204
    timeline = next(
        t for t in build_usdm_schedule_timelines(soa_id) if t["id"] == tl_uid
    )
    assert timeline["plannedDuration"] is None
    conn = _connect()
    try:
        actions = [
            r[0]
            for r in conn.execute(
                "SELECT action FROM schedule_timelines_audit"
                " WHERE soa_id=? AND schedule_timeline_id=? ORDER BY id",
                (soa_id, tl_id),
            )
        ]
    finally:
        conn.close()
    assert actions[-2:] == ["update", "update"]


def test_planned_duration_404_for_other_studys_timeline():
    owner = _new_soa("Duration owner")
    tl_id, _ = _timeline(owner)
    other = _new_soa("Duration other")

    resp = client.put(
        _duration_url(other, tl_id), json={"value": 2, "unit_code": "C25301"}
    )

    assert resp.status_code == 404


def test_freeze_capture_includes_planned_duration():
    soa_id = _new_soa("Duration freeze")
    tl_id, _ = _timeline(soa_id)
    client.put(
        _duration_url(soa_id, tl_id),
        json={"value": 85, "unit_code": "C29844", "text": "85 weeks"},
    )

    conn = _connect()
    try:
        captured = _capture_schedule_timelines(conn.cursor(), soa_id)
    finally:
        conn.close()

    assert captured[0]["planned_duration_value"] == 85
    assert captured[0]["planned_duration_unit_code"] == "C29844"
    assert captured[0]["planned_duration_text"] == "85 weeks"


def test_planned_duration_ui_partial_saves_form():
    soa_id = _new_soa("Duration UI")
    tl_id, tl_uid = _timeline(soa_id)
    url = f"/ui/soa/{soa_id}/schedule_timelines/{tl_id}/planned_duration"

    page = client.get(url)
    assert page.status_code == 200
    assert "WEEKS" in page.text

    saved = client.post(
        url,
        data={"value": "85", "unit_code": "C29844", "duration_will_vary": "1"},
    )
    assert saved.status_code == 200
    duration = next(
        t for t in build_usdm_schedule_timelines(soa_id) if t["id"] == tl_uid
    )["plannedDuration"]
    assert duration["quantity"]["value"] == 85.0
    assert duration["durationWillVary"] is True
