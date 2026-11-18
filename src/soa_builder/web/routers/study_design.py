"""Intervention model and schedule timeline planned duration.

Two USDM v4.0 attributes with CDISC-coded values:

- InterventionalStudyDesign.model: a term from the SDTM Intervention
  Model Response codelist (INTERVENTION_MODEL_CODELIST, C99076),
  stored on ``soa``.
- ScheduleTimeline.plannedDuration: a Duration with optional text and
  a Quantity whose unit is a term from DURATION_UNIT_CODELIST
  (C66781), stored on ``schedule_timelines``.

Codes are checked against the latest SDTM CT package, and that
package's date is stored as the code system version.
"""

import logging
import os
import sqlite3
from typing import Optional

from fastapi import APIRouter, Form, HTTPException, Request, Response
from fastapi.responses import HTMLResponse
from fastapi.templating import Jinja2Templates
from pydantic import BaseModel

from .. import utils as web_utils
from ..audit import _record_schedule_timeline_audit
from ..codelist_config import DURATION_UNIT_CODELIST, INTERVENTION_MODEL_CODELIST
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
    """Return the column values for a valid planned duration."""
    text = _clean(payload.text)
    unit_code = _clean(payload.unit_code)
    value = payload.value
    if value is None and not text:
        raise HTTPException(400, "Give a duration value and unit, or a text")
    if (value is None) != (unit_code is None):
        raise HTTPException(400, "A duration value needs a unit, and a unit a value")
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
        "planned_duration_will_vary": bool(payload.duration_will_vary),
        "planned_duration_reason": _clean(payload.reason_duration_will_vary),
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
