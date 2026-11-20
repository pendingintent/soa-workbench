"""Intervention model, planned duration and population age and sex.

USDM v4.0 attributes with CDISC-coded values:

- InterventionalStudyDesign.model: a term from the SDTM Intervention
  Model Response codelist (INTERVENTION_MODEL_CODELIST, C99076),
  stored on ``soa``.
- ScheduleTimeline.plannedDuration: a Duration with optional text and
  a Quantity whose unit is a term from DURATION_UNIT_CODELIST
  (C66781), stored on ``schedule_timelines``.
- StudyDesignPopulation.plannedSex (up to two terms from
  PLANNED_SEX_CODELIST, C66732) and plannedAge (a Range whose bounds
  use AGE_UNIT_CODELIST, C66781), stored on ``soa``.

Codes are checked against the latest SDTM CT package, and that
package's date is stored as the code system version.
"""

import json
import logging
import os
import sqlite3
from typing import List, Optional

from fastapi import APIRouter, Form, HTTPException, Request, Response
from fastapi.responses import HTMLResponse
from fastapi.templating import Jinja2Templates
from pydantic import BaseModel

from .. import utils as web_utils
from ..audit import _record_schedule_timeline_audit
from ..codelist_config import (
    AGE_UNIT_CODELIST,
    DURATION_UNIT_CODELIST,
    INTERVENTION_MODEL_CODELIST,
    PLANNED_SEX_CODELIST,
)
from ..db import _connect
from ..utils import soa_exists

logger = logging.getLogger("soa_builder.web.routers.study_design")

router = APIRouter()
templates = Jinja2Templates(
    directory=os.path.join(os.path.dirname(__file__), "..", "templates")
)

_DURATION_COLUMNS = (
    "planned_duration_text",
    "planned_duration_value",
    "planned_duration_unit_code",
    "planned_duration_unit_decode",
    "planned_duration_unit_version",
    "planned_duration_will_vary",
    "planned_duration_reason",
)


class InterventionModelIn(BaseModel):
    code: str


class PlannedSexIn(BaseModel):
    codes: List[str]


class PlannedAgeIn(BaseModel):
    min_value: Optional[float] = None
    max_value: Optional[float] = None
    unit_code: Optional[str] = None
    is_approximate: bool = False


class PlannedDurationIn(BaseModel):
    text: Optional[str] = None
    value: Optional[float] = None
    unit_code: Optional[str] = None
    duration_will_vary: bool = False
    reason_duration_will_vary: Optional[str] = None


# --- CDISC terms --------------------------------------------------------


def _sdtm_terms(codelist_code: str) -> tuple[dict[str, str], str]:
    """Return ({code: submission value}, package date) for a codelist.

    Called through the module so tests can patch get_sdtm_ct_rows.
    """
    payload = web_utils.get_sdtm_ct_rows() or {}
    parts = (payload.get("slug") or "").split("-")
    version = "-".join(parts[-3:]) if len(parts) >= 4 else ""
    terms = {
        str(r.get("code")): str(r.get("submission_value") or "")
        for r in payload.get("rows") or []
        if r.get("codelist_code") == codelist_code and r.get("code")
    }
    return terms, version


def _term_options(terms: dict[str, str]) -> list[dict[str, str]]:
    return sorted(
        ({"code": code, "decode": decode} for code, decode in terms.items()),
        key=lambda t: t["decode"],
    )


def _clean(value: Optional[str]) -> Optional[str]:
    value = (value or "").strip()
    return value or None


def _require_soa(soa_id: int) -> None:
    if not soa_exists(soa_id):
        raise HTTPException(404, "SOA not found")


# --- Intervention model -------------------------------------------------


def get_intervention_model(soa_id: int) -> Optional[dict]:
    conn = _connect()
    try:
        row = conn.execute(
            "SELECT intervention_model_code, intervention_model_decode,"
            " intervention_model_version FROM soa WHERE id=?",
            (soa_id,),
        ).fetchone()
    finally:
        conn.close()
    if not row or not row[0]:
        return None
    return {"code": row[0], "decode": row[1] or "", "code_system_version": row[2] or ""}


def _write_intervention_model(soa_id: int, values: tuple) -> None:
    conn = _connect()
    try:
        conn.execute(
            "UPDATE soa SET intervention_model_code=?,"
            " intervention_model_decode=?, intervention_model_version=?"
            " WHERE id=?",
            (*values, soa_id),
        )
        conn.commit()
    finally:
        conn.close()


def set_intervention_model(soa_id: int, code: str) -> dict:
    terms, version = _sdtm_terms(INTERVENTION_MODEL_CODELIST)
    code = (code or "").strip()
    if code not in terms:
        raise HTTPException(
            400,
            f"{code or 'Code'} is not in the intervention model codelist"
            f" {INTERVENTION_MODEL_CODELIST}",
        )
    _write_intervention_model(soa_id, (code, terms[code], version))
    return {"code": code, "decode": terms[code], "code_system_version": version}


@router.get("/soa/{soa_id}/intervention_model")
def api_get_intervention_model(soa_id: int):
    _require_soa(soa_id)
    return get_intervention_model(soa_id)


@router.put("/soa/{soa_id}/intervention_model")
def api_set_intervention_model(soa_id: int, payload: InterventionModelIn):
    _require_soa(soa_id)
    return set_intervention_model(soa_id, payload.code)


@router.delete("/soa/{soa_id}/intervention_model", status_code=204)
def api_clear_intervention_model(soa_id: int):
    _require_soa(soa_id)
    _write_intervention_model(soa_id, (None, None, None))
    return Response(status_code=204)


def _model_partial(request: Request, soa_id: int, error: str = ""):
    terms, _ = _sdtm_terms(INTERVENTION_MODEL_CODELIST)
    return templates.TemplateResponse(
        request,
        "_intervention_model_partial.html",
        {
            "soa_id": soa_id,
            "model": get_intervention_model(soa_id),
            "options": _term_options(terms),
            "codelist": INTERVENTION_MODEL_CODELIST,
            "error": error,
        },
    )


@router.get("/ui/soa/{soa_id}/intervention_model", response_class=HTMLResponse)
def ui_intervention_model(request: Request, soa_id: int):
    _require_soa(soa_id)
    return _model_partial(request, soa_id)


@router.post("/ui/soa/{soa_id}/intervention_model", response_class=HTMLResponse)
def ui_save_intervention_model(
    request: Request, soa_id: int, code: Optional[str] = Form(None)
):
    _require_soa(soa_id)
    error = ""
    if _clean(code):
        try:
            set_intervention_model(soa_id, code)
        except HTTPException as exc:
            error = str(exc.detail)
    else:
        _write_intervention_model(soa_id, (None, None, None))
    return _model_partial(request, soa_id, error)


# --- Planned duration ---------------------------------------------------


def _get_timeline(soa_id: int, timeline_id: int) -> dict:
    conn = _connect()
    try:
        row = conn.execute(
            "SELECT schedule_timeline_uid, name, "
            + ", ".join(_DURATION_COLUMNS)
            + " FROM schedule_timelines WHERE soa_id=? AND id=?",
            (soa_id, timeline_id),
        ).fetchone()
    finally:
        conn.close()
    if not row:
        raise HTTPException(404, "Schedule timeline not found")
    duration = dict(zip(_DURATION_COLUMNS, row[2:]))
    duration["planned_duration_will_vary"] = bool(
        duration["planned_duration_will_vary"]
    )
    return {"id": timeline_id, "uid": row[0], "name": row[1], "duration": duration}


def _validate_duration(payload: PlannedDurationIn) -> dict:
    """Return the column values for a valid planned duration.

    CORE DDF00039: a duration that will vary has no quantity (describe
    it with text and a reason), and a fixed duration must have one. A
    reason is only meaningful for a duration that will vary.
    """
    text = _clean(payload.text)
    unit_code = _clean(payload.unit_code)
    reason = _clean(payload.reason_duration_will_vary)
    will_vary = bool(payload.duration_will_vary)
    value = payload.value
    if value is None and not text:
        raise HTTPException(400, "Give a duration value and unit, or a text")
    if (value is None) != (unit_code is None):
        raise HTTPException(400, "A duration value needs a unit, and a unit a value")
    if will_vary and value is not None:
        raise HTTPException(
            400,
            "A duration that will vary cannot have a value: remove the value"
            " and unit, or untick duration will vary",
        )
    if not will_vary and value is None:
        raise HTTPException(
            400,
            "A fixed duration needs a value and unit; tick duration will vary"
            " to describe it with text only",
        )
    if reason and not will_vary:
        raise HTTPException(
            400, "A reason can only be given when the duration will vary"
        )
    unit_decode = unit_version = None
    if value is not None:
        if value <= 0:
            raise HTTPException(400, "The duration value must be greater than 0")
        terms, unit_version = _sdtm_terms(DURATION_UNIT_CODELIST)
        if unit_code not in terms:
            raise HTTPException(
                400,
                f"{unit_code} is not in the duration unit codelist"
                f" {DURATION_UNIT_CODELIST}",
            )
        unit_decode = terms[unit_code]
    return {
        "planned_duration_text": text,
        "planned_duration_value": value,
        "planned_duration_unit_code": unit_code,
        "planned_duration_unit_decode": unit_decode,
        "planned_duration_unit_version": unit_version,
        "planned_duration_will_vary": will_vary,
        "planned_duration_reason": reason,
    }


def _write_duration(soa_id: int, timeline: dict, values: dict) -> dict:
    conn = _connect()
    try:
        conn.execute(
            "UPDATE schedule_timelines SET "
            + ", ".join(f"{col}=?" for col in _DURATION_COLUMNS)
            + " WHERE soa_id=? AND id=?",
            (
                *(
                    int(values[c]) if c == "planned_duration_will_vary" else values[c]
                    for c in _DURATION_COLUMNS
                ),
                soa_id,
                timeline["id"],
            ),
        )
        conn.commit()
    finally:
        conn.close()
    before = {"schedule_timeline_uid": timeline["uid"], **timeline["duration"]}
    after = {"schedule_timeline_uid": timeline["uid"], **values}
    _record_schedule_timeline_audit(soa_id, "update", timeline["id"], before, after)
    return values


def _empty_duration() -> dict:
    values = {col: None for col in _DURATION_COLUMNS}
    values["planned_duration_will_vary"] = False
    return values


@router.put("/soa/{soa_id}/schedule_timelines/{timeline_id}/planned_duration")
def api_set_planned_duration(soa_id: int, timeline_id: int, payload: PlannedDurationIn):
    _require_soa(soa_id)
    timeline = _get_timeline(soa_id, timeline_id)
    return _write_duration(soa_id, timeline, _validate_duration(payload))


@router.delete(
    "/soa/{soa_id}/schedule_timelines/{timeline_id}/planned_duration",
    status_code=204,
)
def api_clear_planned_duration(soa_id: int, timeline_id: int):
    _require_soa(soa_id)
    timeline = _get_timeline(soa_id, timeline_id)
    _write_duration(soa_id, timeline, _empty_duration())
    return Response(status_code=204)


def _duration_partial(request: Request, soa_id: int, timeline_id: int, error=""):
    terms, _ = _sdtm_terms(DURATION_UNIT_CODELIST)
    return templates.TemplateResponse(
        request,
        "_planned_duration_partial.html",
        {
            "soa_id": soa_id,
            "timeline": _get_timeline(soa_id, timeline_id),
            "units": _term_options(terms),
            "error": error,
        },
    )


_UI_DURATION = "/ui/soa/{soa_id}/schedule_timelines/{timeline_id}/planned_duration"


@router.get(_UI_DURATION, response_class=HTMLResponse)
def ui_planned_duration(request: Request, soa_id: int, timeline_id: int):
    _require_soa(soa_id)
    return _duration_partial(request, soa_id, timeline_id)


@router.post(_UI_DURATION, response_class=HTMLResponse)
def ui_save_planned_duration(
    request: Request,
    soa_id: int,
    timeline_id: int,
    text: Optional[str] = Form(None),
    value: Optional[str] = Form(None),
    unit_code: Optional[str] = Form(None),
    duration_will_vary: Optional[str] = Form(None),
    reason_duration_will_vary: Optional[str] = Form(None),
):
    _require_soa(soa_id)
    timeline = _get_timeline(soa_id, timeline_id)
    error = ""
    try:
        payload = PlannedDurationIn(
            text=text,
            value=float(value) if _clean(value) else None,
            unit_code=unit_code,
            duration_will_vary=bool(duration_will_vary),
            reason_duration_will_vary=reason_duration_will_vary,
        )
        _write_duration(soa_id, timeline, _validate_duration(payload))
    except ValueError:
        error = "The duration value must be a number"
    except HTTPException as exc:
        error = str(exc.detail)
    except sqlite3.Error:
        logger.exception("Saving planned duration failed")
        error = "Could not save the planned duration"
    return _duration_partial(request, soa_id, timeline_id, error)


@router.post(_UI_DURATION + "/delete", response_class=HTMLResponse)
def ui_clear_planned_duration(request: Request, soa_id: int, timeline_id: int):
    _require_soa(soa_id)
    timeline = _get_timeline(soa_id, timeline_id)
    _write_duration(soa_id, timeline, _empty_duration())
    return _duration_partial(request, soa_id, timeline_id)


# --- Population: planned sex and planned age ---------------------------

_AGE_COLUMNS = (
    "planned_age_min",
    "planned_age_max",
    "planned_age_unit_code",
    "planned_age_unit_decode",
    "planned_age_unit_version",
    "planned_age_is_approximate",
)


def get_population(soa_id: int) -> dict:
    """Return the stored planned sex and planned age for an SoA."""
    conn = _connect()
    try:
        row = conn.execute(
            "SELECT planned_sex, " + ", ".join(_AGE_COLUMNS) + " FROM soa WHERE id=?",
            (soa_id,),
        ).fetchone()
    finally:
        conn.close()
    planned_sex = json.loads(row[0]) if row and row[0] else []
    age = dict(zip(_AGE_COLUMNS, row[1:])) if row else {}
    has_age = age.get("planned_age_min") is not None or (
        age.get("planned_age_max") is not None
    )
    planned_age = None
    if has_age:
        planned_age = {
            "min_value": age["planned_age_min"],
            "max_value": age["planned_age_max"],
            "unit_code": age["planned_age_unit_code"],
            "unit_decode": age["planned_age_unit_decode"],
            "code_system_version": age["planned_age_unit_version"],
            "is_approximate": bool(age["planned_age_is_approximate"]),
        }
    return {"planned_sex": planned_sex, "planned_age": planned_age}


def _update_soa(soa_id: int, values: dict) -> None:
    conn = _connect()
    try:
        conn.execute(
            "UPDATE soa SET " + ", ".join(f"{col}=?" for col in values) + " WHERE id=?",
            (*values.values(), soa_id),
        )
        conn.commit()
    finally:
        conn.close()


def set_planned_sex(soa_id: int, codes: List[str]) -> list:
    codes = [c.strip() for c in codes if c and c.strip()]
    if not codes or len(codes) > 2:
        raise HTTPException(400, "Choose one or two planned sex terms")
    if len(set(codes)) != len(codes):
        raise HTTPException(400, "Each planned sex term can be chosen once")
    terms, version = _sdtm_terms(PLANNED_SEX_CODELIST)
    unknown = [c for c in codes if c not in terms]
    if unknown:
        raise HTTPException(
            400,
            f"{', '.join(unknown)} not in the planned sex codelist"
            f" {PLANNED_SEX_CODELIST}",
        )
    planned_sex = [
        {"code": c, "decode": terms[c], "code_system_version": version} for c in codes
    ]
    _update_soa(soa_id, {"planned_sex": json.dumps(planned_sex)})
    return planned_sex


def set_planned_age(soa_id: int, payload: PlannedAgeIn) -> dict:
    """Store a planned age range; either bound may be n/a (None).

    USDM v4.0 requires both Range bounds. A one-sided range is kept
    because v4.1 is expected to allow a missing bound.
    """
    low, high = payload.min_value, payload.max_value
    unit_code = _clean(payload.unit_code)
    if low is None and high is None:
        raise HTTPException(400, "Give at least one of the minimum and maximum age")
    if any(v is not None and v < 0 for v in (low, high)):
        raise HTTPException(400, "Ages cannot be negative")
    if low is not None and high is not None and low > high:
        raise HTTPException(400, "The minimum age is greater than the maximum")
    terms, version = _sdtm_terms(AGE_UNIT_CODELIST)
    if unit_code not in terms:
        raise HTTPException(
            400, f"Choose an age unit from codelist {AGE_UNIT_CODELIST}"
        )
    _update_soa(
        soa_id,
        {
            "planned_age_min": low,
            "planned_age_max": high,
            "planned_age_unit_code": unit_code,
            "planned_age_unit_decode": terms[unit_code],
            "planned_age_unit_version": version,
            "planned_age_is_approximate": int(bool(payload.is_approximate)),
        },
    )
    return get_population(soa_id)["planned_age"]


def _clear_planned_age(soa_id: int) -> None:
    values = {col: None for col in _AGE_COLUMNS}
    values["planned_age_is_approximate"] = 0
    _update_soa(soa_id, values)


@router.get("/soa/{soa_id}/population")
def api_get_population(soa_id: int):
    _require_soa(soa_id)
    return get_population(soa_id)


@router.put("/soa/{soa_id}/population/planned_sex")
def api_set_planned_sex(soa_id: int, payload: PlannedSexIn):
    _require_soa(soa_id)
    return set_planned_sex(soa_id, payload.codes)


@router.delete("/soa/{soa_id}/population/planned_sex", status_code=204)
def api_clear_planned_sex(soa_id: int):
    _require_soa(soa_id)
    _update_soa(soa_id, {"planned_sex": None})
    return Response(status_code=204)


@router.put("/soa/{soa_id}/population/planned_age")
def api_set_planned_age(soa_id: int, payload: PlannedAgeIn):
    _require_soa(soa_id)
    return set_planned_age(soa_id, payload)


@router.delete("/soa/{soa_id}/population/planned_age", status_code=204)
def api_clear_planned_age(soa_id: int):
    _require_soa(soa_id)
    _clear_planned_age(soa_id)
    return Response(status_code=204)


def _population_partial(request: Request, soa_id: int, error: str = ""):
    sex_terms, _ = _sdtm_terms(PLANNED_SEX_CODELIST)
    unit_terms, _ = _sdtm_terms(AGE_UNIT_CODELIST)
    population = get_population(soa_id)
    return templates.TemplateResponse(
        request,
        "_population_partial.html",
        {
            "soa_id": soa_id,
            "planned_sex_codes": [s["code"] for s in population["planned_sex"]],
            "planned_age": population["planned_age"] or {},
            "sex_options": _term_options(sex_terms),
            "unit_options": _term_options(unit_terms),
            "error": error,
        },
    )


def _form_number(value: Optional[str]) -> Optional[float]:
    """Blank or 'n/a' means no value; anything else must be a number."""
    value = _clean(value)
    if value is None or value.lower() in ("n/a", "na"):
        return None
    return float(value)


@router.get("/ui/soa/{soa_id}/population", response_class=HTMLResponse)
def ui_population(request: Request, soa_id: int):
    _require_soa(soa_id)
    return _population_partial(request, soa_id)


@router.post("/ui/soa/{soa_id}/population/planned_sex", response_class=HTMLResponse)
def ui_save_planned_sex(
    request: Request, soa_id: int, codes: List[str] = Form(default_factory=list)
):
    _require_soa(soa_id)
    error = ""
    if codes:
        try:
            set_planned_sex(soa_id, codes)
        except HTTPException as exc:
            error = str(exc.detail)
    else:
        _update_soa(soa_id, {"planned_sex": None})
    return _population_partial(request, soa_id, error)


@router.post("/ui/soa/{soa_id}/population/planned_age", response_class=HTMLResponse)
def ui_save_planned_age(
    request: Request,
    soa_id: int,
    min_value: Optional[str] = Form(None),
    max_value: Optional[str] = Form(None),
    unit_code: Optional[str] = Form(None),
    is_approximate: Optional[str] = Form(None),
):
    _require_soa(soa_id)
    error = ""
    try:
        payload = PlannedAgeIn(
            min_value=_form_number(min_value),
            max_value=_form_number(max_value),
            unit_code=unit_code,
            is_approximate=bool(is_approximate),
        )
        set_planned_age(soa_id, payload)
    except ValueError:
        error = "Ages must be numbers, or blank / n/a"
    except HTTPException as exc:
        error = str(exc.detail)
    return _population_partial(request, soa_id, error)


@router.post(
    "/ui/soa/{soa_id}/population/planned_age/delete", response_class=HTMLResponse
)
def ui_clear_planned_age(request: Request, soa_id: int):
    _require_soa(soa_id)
    _clear_planned_age(soa_id)
    return _population_partial(request, soa_id)
