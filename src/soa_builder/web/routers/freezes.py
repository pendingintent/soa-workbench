import html
import json
import logging
import os

from fastapi import APIRouter, Form, HTTPException, Request
from fastapi.responses import HTMLResponse, JSONResponse
from fastapi.templating import Jinja2Templates

from ..db import _connect
from ..utils import soa_exists
from ._freeze_helpers import (
    _create_freeze,
    _delete_freeze,
    _diff_freezes_limited,
    _get_freeze,
    _list_freezes,
    _record_rollback_audit,
    _rollback_freeze,
    _rollback_preview,
)
from .amendments import (
    _insert_code,
    _next_amendment_uid,
    _next_reason_uid,
    _REASON_CODELIST,
)
from ..audit import _record_amendment_audit, _record_reason_audit
from ..schemas import StudyAmendmentCreate

TEMPLATES_DIR = os.path.join(os.path.dirname(os.path.dirname(__file__)), "templates")
templates = Jinja2Templates(directory=TEMPLATES_DIR)

router = APIRouter()
logger = logging.getLogger("soa_builder.web.routers.freezes")


@router.get(
    "/ui/soa/{soa_id}/freezes",
    response_class=HTMLResponse,
    name="ui_list_freezes",
)
def ui_list_freezes(request: Request, soa_id: int):
    if not soa_exists(soa_id):
        raise HTTPException(404, "SOA not found")
    freezes = _list_freezes(soa_id)
    return templates.TemplateResponse(
        request,
        "freezes.html",
        {"soa_id": soa_id, "freezes": freezes},
    )


@router.post("/ui/soa/{soa_id}/freeze", response_class=HTMLResponse)
def ui_freeze_soa(
    request: Request,
    soa_id: int,
    version_label: str = Form(""),
    is_amendment: str = Form(""),
    amendment_name: str = Form(""),
    amendment_number: str = Form(""),
    amendment_summary: str = Form(""),
    amendment_label: str = Form(""),
    amendment_description: str = Form(""),
    primary_reason_code: str = Form(""),
    primary_reason_other: str = Form(""),
):
    if not soa_exists(soa_id):
        raise HTTPException(404, "SOA not found")
    try:
        freeze_id, _ = _create_freeze(soa_id, version_label or None)
    except HTTPException as he:
        if request.headers.get("HX-Request") == "true":
            return HTMLResponse(
                f"<div class='error' style='color:#c62828;font-size:0.7em;'>"
                f"Error: {he.detail}</div>"
            )
        return HTMLResponse(
            f"<div class='error' style='color:#c62828;font-size:0.85em;'>"
            f"Error: {he.detail}</div>",
            headers={"Refresh": f"2; url=/ui/soa/{soa_id}/freezes"},
        )

    if is_amendment:
        try:
            body = StudyAmendmentCreate(
                name=amendment_name,
                number=amendment_number,
                summary=amendment_summary,
                label=amendment_label or None,
                description=amendment_description or None,
                primary_reason_code=primary_reason_code,
                primary_reason_other=primary_reason_other or None,
            )
        except Exception as exc:
            err = html.escape(str(exc))
            if request.headers.get("HX-Request") == "true":
                return HTMLResponse(
                    f"<div class='error' style='color:#c62828;"
                    f"font-size:0.7em;'>Amendment error: {err}</div>"
                )
            return HTMLResponse(
                f"<div class='error' style='color:#c62828;'>"
                f"Amendment error: {err}</div>",
                headers={"Refresh": f"2; url=/ui/soa/{soa_id}/freezes"},
            )
        try:
            conn = _connect()
            cur = conn.cursor()
            amendment_uid = _next_amendment_uid(cur, soa_id)
            reason_uid = _next_reason_uid(cur, soa_id)
            code_uid = _insert_code(
                cur, soa_id, body.primary_reason_code, _REASON_CODELIST
            )
            cur.execute(
                "INSERT INTO study_amendment "
                "(soa_id,freeze_id,amendment_uid,name,number,summary,"
                "label,description) VALUES (?,?,?,?,?,?,?,?)",
                (
                    soa_id,
                    freeze_id,
                    amendment_uid,
                    body.name,
                    body.number,
                    body.summary,
                    body.label,
                    body.description,
                ),
            )
            amendment_id = cur.lastrowid
            cur.execute(
                "INSERT INTO study_amendment_reason "
                "(soa_id,amendment_uid,reason_uid,role,code_uid,"
                "other_reason) VALUES (?,?,?,?,?,?)",
                (
                    soa_id,
                    amendment_uid,
                    reason_uid,
                    "primary",
                    code_uid,
                    body.primary_reason_other,
                ),
            )
            reason_id = cur.lastrowid
            conn.commit()
            conn.close()
            _record_amendment_audit(
                soa_id,
                "create",
                amendment_id,
                after={"amendment_uid": amendment_uid, "name": body.name},
            )
            _record_reason_audit(
                soa_id,
                "create",
                reason_id,
                after={
                    "reason_uid": reason_uid,
                    "role": "primary",
                    "code": body.primary_reason_code,
                },
            )
        except Exception as exc:
            logger.exception(
                "Amendment creation failed for freeze %s: %s", freeze_id, exc
            )

    if request.headers.get("HX-Request") == "true":
        return HTMLResponse("", headers={"HX-Redirect": f"/ui/soa/{soa_id}/freezes"})
    safe_soa_id = html.escape(str(soa_id))
    return HTMLResponse(
        f"<script>window.location='/ui/soa/{safe_soa_id}/freezes';</script>"
    )


@router.get("/soa/{soa_id}/freeze/{freeze_id}")
def get_freeze(soa_id: int, freeze_id: int):
    if not soa_exists(soa_id):
        raise HTTPException(404, "SOA not found")
    conn = _connect()
    cur = conn.cursor()
    cur.execute(
        "SELECT snapshot_json FROM soa_freeze WHERE id=? AND soa_id=?",
        (freeze_id, soa_id),
    )
    row = cur.fetchone()
    conn.close()
    if not row:
        raise HTTPException(404, "Freeze not found")
    try:
        data = json.loads(row[0])
    except Exception as e:
        logger.exception(
            "get_freeze JSON decode failed soa_id=%s freeze_id=%s: %s",
            soa_id,
            freeze_id,
            e,
        )
        data = {"error": "Corrupt snapshot"}
    return JSONResponse(data)


@router.get("/ui/soa/{soa_id}/freeze/{freeze_id}/view", response_class=HTMLResponse)
def ui_freeze_view(request: Request, soa_id: int, freeze_id: int):
    freeze = _get_freeze(soa_id, freeze_id)
    if not freeze:
        raise HTTPException(404, "Freeze not found")
    return templates.TemplateResponse(
        request,
        "freeze_modal.html",
        {"mode": "view", "freeze": freeze, "soa_id": soa_id},
    )


@router.get("/ui/soa/{soa_id}/freeze/diff", response_class=HTMLResponse)
def ui_freeze_diff(request: Request, soa_id: int, left: int, right: int, full: int = 0):
    limit = None if full == 1 else 50
    diff = _diff_freezes_limited(soa_id, left, right, limit=limit)
    return templates.TemplateResponse(
        request,
        "freeze_modal.html",
        {"mode": "diff", "diff": diff, "soa_id": soa_id},
    )


@router.post(
    "/ui/soa/{soa_id}/freeze/{freeze_id}/rollback", response_class=HTMLResponse
)
def ui_freeze_rollback(request: Request, soa_id: int, freeze_id: int):
    result = _rollback_freeze(soa_id, freeze_id)
    _record_rollback_audit(
        soa_id,
        freeze_id,
        {
            "visits_restored": result["visits_restored"],
            "activities_restored": result["activities_restored"],
            "cells_restored": result["cells_restored"],
            "concept_mappings_restored": result["concept_mappings_restored"],
            "elements_restored": result.get("elements_restored"),
        },
    )
    if request.headers.get("HX-Request") == "true":
        return HTMLResponse("", headers={"HX-Redirect": f"/ui/soa/{soa_id}/freezes"})
    safe_soa_id = html.escape(str(soa_id))
    return HTMLResponse(
        f"<script>window.location='/ui/soa/{safe_soa_id}/freezes';</script>"
    )


@router.get(
    "/ui/soa/{soa_id}/freeze/{freeze_id}/rollback_preview",
    response_class=HTMLResponse,
)
def ui_freeze_rollback_preview(request: Request, soa_id: int, freeze_id: int):
    preview = _rollback_preview(soa_id, freeze_id)
    freeze = _get_freeze(soa_id, freeze_id)
    return templates.TemplateResponse(
        request,
        "freeze_modal.html",
        {
            "mode": "rollback_preview",
            "preview": preview,
            "freeze": freeze,
            "soa_id": soa_id,
        },
    )


@router.post("/ui/soa/{soa_id}/freeze/{freeze_id}/delete", response_class=HTMLResponse)
def ui_freeze_delete(request: Request, soa_id: int, freeze_id: int):
    if not soa_exists(soa_id):
        raise HTTPException(404, "SOA not found")
    if not _delete_freeze(soa_id, freeze_id):
        raise HTTPException(404, "Freeze not found")
    if request.headers.get("HX-Request") == "true":
        return HTMLResponse("", headers={"HX-Redirect": f"/ui/soa/{soa_id}/freezes"})
    safe_soa_id = html.escape(str(soa_id))
    return HTMLResponse(
        f"<script>window.location='/ui/soa/{safe_soa_id}/freezes';</script>"
    )


@router.get("/soa/{soa_id}/freeze/diff.json")
def get_freeze_diff_json(soa_id: int, left: int, right: int, full: int = 0):
    limit = None if full == 1 else 1000
    diff = _diff_freezes_limited(soa_id, left, right, limit=limit)
    return JSONResponse(diff)
