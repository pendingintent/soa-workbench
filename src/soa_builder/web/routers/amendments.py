"""Amendment router: API + UI endpoints for USDM StudyAmendment entities."""

import html
import json
import logging
import os
from typing import Optional

from fastapi import APIRouter, Form, HTTPException, Request
from fastapi.responses import HTMLResponse, JSONResponse
from fastapi.templating import Jinja2Templates

from ..audit import (
    _record_amendment_audit,
    _record_change_audit,
    _record_impact_audit,
    _record_reason_audit,
    _record_ref_audit,
)
from ..db import _connect
from ..schemas import (
    DocumentContentReferenceCreate,
    StudyAmendmentCreate,
    StudyAmendmentImpactCreate,
    StudyAmendmentReasonCreate,
    StudyAmendmentUpdate,
    StudyChangeCreate,
)
from ..utils import (
    get_ddf_ct_codelist_map,
    get_latest_ddf_ct_href,
    get_next_code_uid,
    soa_exists,
)

router = APIRouter()
ui_router = APIRouter()
logger = logging.getLogger("soa_builder.web.routers.amendments")
templates = Jinja2Templates(
    directory=os.path.join(os.path.dirname(__file__), "..", "templates")
)

_REASON_CODELIST = "C207415"
_IMPACT_TYPE_CODELIST = "C215481"


# ---------------------------------------------------------------------------
# UID helpers
# ---------------------------------------------------------------------------


def _next_uid(cur, soa_id: int, table: str, col: str, prefix: str) -> str:
    """Return next UID for prefix, never reusing deleted UIDs."""
    max_n = 0
    cur.execute(
        f"SELECT {col} FROM {table} WHERE soa_id=? AND {col} LIKE '{prefix}%'",
        (soa_id,),
    )
    for (uid,) in cur.fetchall():
        if isinstance(uid, str) and uid.startswith(prefix):
            try:
                n = int(uid.split("_")[-1])
                if n > max_n:
                    max_n = n
            except (ValueError, IndexError):
                pass
    audit_table = f"{table}_audit"
    cur.execute(
        f"SELECT before_json, after_json FROM {audit_table} WHERE soa_id=?",
        (soa_id,),
    )
    for before_raw, after_raw in cur.fetchall():
        for raw in (before_raw, after_raw):
            if not raw:
                continue
            try:
                uid = json.loads(raw).get(col, "")
                if isinstance(uid, str) and uid.startswith(prefix):
                    n = int(uid.split("_")[-1])
                    if n > max_n:
                        max_n = n
            except Exception:
                pass
    return f"{prefix}{max_n + 1}"


def _next_amendment_uid(cur, soa_id: int) -> str:
    return _next_uid(cur, soa_id, "study_amendment", "amendment_uid", "StudyAmendment_")


def _next_reason_uid(cur, soa_id: int) -> str:
    return _next_uid(
        cur,
        soa_id,
        "study_amendment_reason",
        "reason_uid",
        "StudyAmendmentReason_",
    )


def _next_impact_uid(cur, soa_id: int) -> str:
    return _next_uid(
        cur,
        soa_id,
        "study_amendment_impact",
        "impact_uid",
        "StudyAmendmentImpact_",
    )


def _next_change_uid(cur, soa_id: int) -> str:
    return _next_uid(cur, soa_id, "study_change", "change_uid", "StudyChange_")


def _next_ref_uid(cur, soa_id: int) -> str:
    return _next_uid(
        cur,
        soa_id,
        "document_content_reference",
        "ref_uid",
        "DocumentContentReference_",
    )


# ---------------------------------------------------------------------------
# Code insertion helper
# ---------------------------------------------------------------------------


def _insert_code(cur, soa_id: int, submission_value: str, codelist_code: str) -> str:
    """Insert code_association row and return generated Code_N UID."""
    code_uid = get_next_code_uid(cur, soa_id)
    slug = get_latest_ddf_ct_href() or ""
    codelist_table = f"/mdr/ct/packages/{slug}" if slug else "/mdr/ct/packages"
    cur.execute(
        "INSERT INTO code_association "
        "(soa_id, code_uid, codelist_table, codelist_code, code) "
        "VALUES (?,?,?,?,?)",
        (soa_id, code_uid, codelist_table, codelist_code, submission_value),
    )
    return code_uid


# ---------------------------------------------------------------------------
# Row helpers
# ---------------------------------------------------------------------------


def _amendment_row(cur, soa_id: int, amendment_id: int) -> Optional[dict]:
    cur.execute(
        "SELECT id,soa_id,freeze_id,amendment_uid,name,number,summary,"
        "label,description FROM study_amendment WHERE id=? AND soa_id=?",
        (amendment_id, soa_id),
    )
    row = cur.fetchone()
    if not row:
        return None
    return {
        "id": row[0],
        "soa_id": row[1],
        "freeze_id": row[2],
        "amendment_uid": row[3],
        "name": row[4],
        "number": row[5],
        "summary": row[6],
        "label": row[7],
        "description": row[8],
    }


def _load_amendment_data(soa_id: int, amendment_id: int) -> dict:
    """DB-only context for HTMX partial swaps — no API calls."""
    conn = _connect()
    cur = conn.cursor()

    amendment = _amendment_row(cur, soa_id, amendment_id)
    if not amendment:
        conn.close()
        return {}

    cur.execute(
        "SELECT r.id, r.reason_uid, r.role, r.other_reason, ca.code "
        "FROM study_amendment_reason r "
        "LEFT JOIN code_association ca ON ca.code_uid=r.code_uid "
        "AND ca.soa_id=r.soa_id "
        "WHERE r.soa_id=? AND r.amendment_uid=? "
        "ORDER BY r.id",
        (soa_id, amendment["amendment_uid"]),
    )
    reasons = [
        {
            "id": r[0],
            "reason_uid": r[1],
            "role": r[2],
            "other_reason": r[3],
            "code": r[4] or "",
        }
        for r in cur.fetchall()
    ]

    cur.execute(
        "SELECT i.id, i.impact_uid, i.text, i.is_substantial, ca.code "
        "FROM study_amendment_impact i "
        "LEFT JOIN code_association ca ON ca.code_uid=i.type_code_uid "
        "AND ca.soa_id=i.soa_id "
        "WHERE i.soa_id=? AND i.amendment_uid=? "
        "ORDER BY i.id",
        (soa_id, amendment["amendment_uid"]),
    )
    impacts = [
        {
            "id": r[0],
            "impact_uid": r[1],
            "text": r[2],
            "is_substantial": bool(r[3]),
            "type_code": r[4] or "",
        }
        for r in cur.fetchall()
    ]

    cur.execute(
        "SELECT id,change_uid,name,label,description,summary,rationale "
        "FROM study_change "
        "WHERE soa_id=? AND amendment_uid=? ORDER BY id",
        (soa_id, amendment["amendment_uid"]),
    )
    changes_rows = cur.fetchall()
    changes = []
    for cr in changes_rows:
        change_uid = cr[1]
        cur.execute(
            "SELECT id,ref_uid,section_number,section_title,applies_to_id "
            "FROM document_content_reference "
            "WHERE soa_id=? AND change_uid=? ORDER BY id",
            (soa_id, change_uid),
        )
        sections = [
            {
                "id": s[0],
                "ref_uid": s[1],
                "section_number": s[2],
                "section_title": s[3],
                "applies_to_id": s[4],
            }
            for s in cur.fetchall()
        ]
        changes.append(
            {
                "id": cr[0],
                "change_uid": change_uid,
                "name": cr[2],
                "label": cr[3],
                "description": cr[4],
                "summary": cr[5],
                "rationale": cr[6],
                "sections": sections,
            }
        )

    conn.close()
    return {
        "amendment": amendment,
        "secondary_reasons": [r for r in reasons if r["role"] == "secondary"],
        "impacts": impacts,
        "changes": changes,
        "soa_id": soa_id,
    }


def _load_amendment_context(soa_id: int, amendment_id: int) -> dict:
    """Full context including codelists — used only for initial page load."""
    ctx = _load_amendment_data(soa_id, amendment_id)
    if not ctx:
        return {}
    ctx["reason_terms"] = get_ddf_ct_codelist_map(_REASON_CODELIST)
    ctx["impact_terms"] = get_ddf_ct_codelist_map(_IMPACT_TYPE_CODELIST)
    return ctx


# ---------------------------------------------------------------------------
# JSON API
# ---------------------------------------------------------------------------


@router.post(
    "/soa/{soa_id}/freeze/{freeze_id}/amendment",
    status_code=201,
)
def create_amendment(soa_id: int, freeze_id: int, body: StudyAmendmentCreate):
    if not soa_exists(soa_id):
        raise HTTPException(404, "SOA not found")
    conn = _connect()
    cur = conn.cursor()
    cur.execute(
        "SELECT id FROM soa_freeze WHERE id=? AND soa_id=?",
        (freeze_id, soa_id),
    )
    if not cur.fetchone():
        conn.close()
        raise HTTPException(404, "Freeze not found")
    cur.execute(
        "SELECT id FROM study_amendment WHERE freeze_id=? AND soa_id=?",
        (freeze_id, soa_id),
    )
    if cur.fetchone():
        conn.close()
        raise HTTPException(409, "Amendment already exists for this freeze")

    amendment_uid = _next_amendment_uid(cur, soa_id)
    reason_uid = _next_reason_uid(cur, soa_id)
    code_uid = _insert_code(cur, soa_id, body.primary_reason_code, _REASON_CODELIST)

    cur.execute(
        "INSERT INTO study_amendment "
        "(soa_id,freeze_id,amendment_uid,name,number,summary,label,"
        "description) VALUES (?,?,?,?,?,?,?,?)",
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
        "(soa_id,amendment_uid,reason_uid,role,code_uid,other_reason) "
        "VALUES (?,?,?,?,?,?)",
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

    after = {
        "amendment_uid": amendment_uid,
        "name": body.name,
        "number": body.number,
        "summary": body.summary,
    }
    _record_amendment_audit(soa_id, "create", amendment_id, after=after)
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
    return JSONResponse(
        {"id": amendment_id, "amendment_uid": amendment_uid},
        status_code=201,
    )


@router.get("/soa/{soa_id}/freeze/{freeze_id}/amendment")
def get_amendment(soa_id: int, freeze_id: int):
    if not soa_exists(soa_id):
        raise HTTPException(404, "SOA not found")
    conn = _connect()
    cur = conn.cursor()
    cur.execute(
        "SELECT id FROM study_amendment WHERE freeze_id=? AND soa_id=?",
        (freeze_id, soa_id),
    )
    row = cur.fetchone()
    conn.close()
    if not row:
        raise HTTPException(404, "Amendment not found")
    conn = _connect()
    cur = conn.cursor()
    data = _amendment_row(cur, soa_id, row[0])
    conn.close()
    return JSONResponse(data)


@router.patch("/soa/{soa_id}/amendment/{amendment_id}")
def update_amendment(soa_id: int, amendment_id: int, body: StudyAmendmentUpdate):
    if not soa_exists(soa_id):
        raise HTTPException(404, "SOA not found")
    conn = _connect()
    cur = conn.cursor()
    before = _amendment_row(cur, soa_id, amendment_id)
    if not before:
        conn.close()
        raise HTTPException(404, "Amendment not found")
    fields = body.model_dump(exclude_none=True)
    if not fields:
        conn.close()
        return JSONResponse(before)
    set_clause = ", ".join(f"{k}=?" for k in fields)
    cur.execute(
        f"UPDATE study_amendment SET {set_clause} WHERE id=? AND soa_id=?",
        (*fields.values(), amendment_id, soa_id),
    )
    conn.commit()
    after = _amendment_row(cur, soa_id, amendment_id)
    conn.close()
    _record_amendment_audit(soa_id, "update", amendment_id, before=before, after=after)
    return JSONResponse(after)


@router.delete("/soa/{soa_id}/amendment/{amendment_id}", status_code=204)
def delete_amendment(soa_id: int, amendment_id: int):
    if not soa_exists(soa_id):
        raise HTTPException(404, "SOA not found")
    conn = _connect()
    cur = conn.cursor()
    before = _amendment_row(cur, soa_id, amendment_id)
    if not before:
        conn.close()
        raise HTTPException(404, "Amendment not found")
    amendment_uid = before["amendment_uid"]

    # 1. Collect impact code_uids BEFORE deleting impacts
    cur.execute(
        "SELECT type_code_uid FROM study_amendment_impact "
        "WHERE soa_id=? AND amendment_uid=?",
        (soa_id, amendment_uid),
    )
    impact_code_uids = [row[0] for row in cur.fetchall()]

    # 2. Collect reason code_uids BEFORE deleting reasons
    cur.execute(
        "SELECT code_uid FROM study_amendment_reason "
        "WHERE soa_id=? AND amendment_uid=?",
        (soa_id, amendment_uid),
    )
    reason_code_uids = [row[0] for row in cur.fetchall()]

    # 3. Delete document_content_reference rows for all changes
    cur.execute(
        "SELECT change_uid FROM study_change WHERE soa_id=? AND amendment_uid=?",
        (soa_id, amendment_uid),
    )
    for (cu,) in cur.fetchall():
        cur.execute(
            "DELETE FROM document_content_reference WHERE soa_id=? AND change_uid=?",
            (soa_id, cu),
        )

    # 4. Delete changes
    cur.execute(
        "DELETE FROM study_change WHERE soa_id=? AND amendment_uid=?",
        (soa_id, amendment_uid),
    )

    # 5. Delete impacts
    cur.execute(
        "DELETE FROM study_amendment_impact WHERE soa_id=? AND amendment_uid=?",
        (soa_id, amendment_uid),
    )

    # 6. Delete impact code_association rows
    for cu in impact_code_uids:
        cur.execute(
            "DELETE FROM code_association WHERE soa_id=? AND code_uid=?",
            (soa_id, cu),
        )

    # 7. Delete reasons
    cur.execute(
        "DELETE FROM study_amendment_reason WHERE soa_id=? AND amendment_uid=?",
        (soa_id, amendment_uid),
    )

    # 8. Delete reason code_association rows
    for cu in reason_code_uids:
        cur.execute(
            "DELETE FROM code_association WHERE soa_id=? AND code_uid=?",
            (soa_id, cu),
        )

    # 9. Delete the amendment itself
    cur.execute(
        "DELETE FROM study_amendment WHERE id=? AND soa_id=?",
        (amendment_id, soa_id),
    )
    conn.commit()
    conn.close()
    _record_amendment_audit(soa_id, "delete", amendment_id, before=before)


@router.post(
    "/soa/{soa_id}/amendment/{amendment_id}/reasons",
    status_code=201,
)
def add_secondary_reason(
    soa_id: int, amendment_id: int, body: StudyAmendmentReasonCreate
):
    if not soa_exists(soa_id):
        raise HTTPException(404, "SOA not found")
    conn = _connect()
    cur = conn.cursor()
    am = _amendment_row(cur, soa_id, amendment_id)
    if not am:
        conn.close()
        raise HTTPException(404, "Amendment not found")
    reason_uid = _next_reason_uid(cur, soa_id)
    code_uid = _insert_code(cur, soa_id, body.code, _REASON_CODELIST)
    cur.execute(
        "INSERT INTO study_amendment_reason "
        "(soa_id,amendment_uid,reason_uid,role,code_uid,other_reason) "
        "VALUES (?,?,?,?,?,?)",
        (
            soa_id,
            am["amendment_uid"],
            reason_uid,
            "secondary",
            code_uid,
            body.other_reason,
        ),
    )
    reason_id = cur.lastrowid
    conn.commit()
    conn.close()
    _record_reason_audit(
        soa_id,
        "create",
        reason_id,
        after={
            "reason_uid": reason_uid,
            "role": "secondary",
            "code": body.code,
        },
    )
    return JSONResponse({"id": reason_id, "reason_uid": reason_uid}, status_code=201)


@router.delete(
    "/soa/{soa_id}/amendment/{amendment_id}/reason/{reason_id}",
    status_code=204,
)
def delete_secondary_reason(soa_id: int, amendment_id: int, reason_id: int):
    if not soa_exists(soa_id):
        raise HTTPException(404, "SOA not found")
    conn = _connect()
    cur = conn.cursor()
    cur.execute(
        "SELECT id,reason_uid,role,code_uid,other_reason "
        "FROM study_amendment_reason WHERE id=? AND soa_id=?",
        (reason_id, soa_id),
    )
    row = cur.fetchone()
    if not row:
        conn.close()
        raise HTTPException(404, "Reason not found")
    if row[2] == "primary":
        conn.close()
        raise HTTPException(400, "Cannot delete primary reason")
    before = {
        "reason_uid": row[1],
        "role": row[2],
        "code_uid": row[3],
    }
    cur.execute(
        "DELETE FROM code_association WHERE soa_id=? AND code_uid=?",
        (soa_id, row[3]),
    )
    cur.execute(
        "DELETE FROM study_amendment_reason WHERE id=? AND soa_id=?",
        (reason_id, soa_id),
    )
    conn.commit()
    conn.close()
    _record_reason_audit(soa_id, "delete", reason_id, before=before)


@router.post(
    "/soa/{soa_id}/amendment/{amendment_id}/impacts",
    status_code=201,
)
def add_impact(soa_id: int, amendment_id: int, body: StudyAmendmentImpactCreate):
    if not soa_exists(soa_id):
        raise HTTPException(404, "SOA not found")
    conn = _connect()
    cur = conn.cursor()
    am = _amendment_row(cur, soa_id, amendment_id)
    if not am:
        conn.close()
        raise HTTPException(404, "Amendment not found")
    impact_uid = _next_impact_uid(cur, soa_id)
    type_code_uid = _insert_code(cur, soa_id, body.type_code, _IMPACT_TYPE_CODELIST)
    cur.execute(
        "INSERT INTO study_amendment_impact "
        "(soa_id,amendment_uid,impact_uid,type_code_uid,text,is_substantial)"
        " VALUES (?,?,?,?,?,?)",
        (
            soa_id,
            am["amendment_uid"],
            impact_uid,
            type_code_uid,
            body.text,
            int(body.is_substantial),
        ),
    )
    impact_id = cur.lastrowid
    conn.commit()
    conn.close()
    _record_impact_audit(
        soa_id,
        "create",
        impact_id,
        after={"impact_uid": impact_uid, "type_code": body.type_code},
    )
    return JSONResponse({"id": impact_id, "impact_uid": impact_uid}, status_code=201)


@router.delete(
    "/soa/{soa_id}/amendment/{amendment_id}/impact/{impact_id}",
    status_code=204,
)
def delete_impact(soa_id: int, amendment_id: int, impact_id: int):
    if not soa_exists(soa_id):
        raise HTTPException(404, "SOA not found")
    conn = _connect()
    cur = conn.cursor()
    cur.execute(
        "SELECT id,impact_uid,type_code_uid FROM study_amendment_impact "
        "WHERE id=? AND soa_id=?",
        (impact_id, soa_id),
    )
    row = cur.fetchone()
    if not row:
        conn.close()
        raise HTTPException(404, "Impact not found")
    before = {"impact_uid": row[1]}
    cur.execute(
        "DELETE FROM code_association WHERE soa_id=? AND code_uid=?",
        (soa_id, row[2]),
    )
    cur.execute(
        "DELETE FROM study_amendment_impact WHERE id=? AND soa_id=?",
        (impact_id, soa_id),
    )
    conn.commit()
    conn.close()
    _record_impact_audit(soa_id, "delete", impact_id, before=before)


@router.post(
    "/soa/{soa_id}/amendment/{amendment_id}/changes",
    status_code=201,
)
def add_change(soa_id: int, amendment_id: int, body: StudyChangeCreate):
    if not soa_exists(soa_id):
        raise HTTPException(404, "SOA not found")
    conn = _connect()
    cur = conn.cursor()
    am = _amendment_row(cur, soa_id, amendment_id)
    if not am:
        conn.close()
        raise HTTPException(404, "Amendment not found")
    change_uid = _next_change_uid(cur, soa_id)
    cur.execute(
        "INSERT INTO study_change "
        "(soa_id,amendment_uid,change_uid,name,label,description,"
        "summary,rationale) VALUES (?,?,?,?,?,?,?,?)",
        (
            soa_id,
            am["amendment_uid"],
            change_uid,
            body.name,
            body.label,
            body.description,
            body.summary,
            body.rationale,
        ),
    )
    change_id = cur.lastrowid
    conn.commit()
    conn.close()
    _record_change_audit(
        soa_id,
        "create",
        change_id,
        after={"change_uid": change_uid, "name": body.name},
    )
    return JSONResponse({"id": change_id, "change_uid": change_uid}, status_code=201)


@router.delete(
    "/soa/{soa_id}/amendment/{amendment_id}/change/{change_id}",
    status_code=204,
)
def delete_change(soa_id: int, amendment_id: int, change_id: int):
    if not soa_exists(soa_id):
        raise HTTPException(404, "SOA not found")
    conn = _connect()
    cur = conn.cursor()
    cur.execute(
        "SELECT id,change_uid FROM study_change WHERE id=? AND soa_id=?",
        (change_id, soa_id),
    )
    row = cur.fetchone()
    if not row:
        conn.close()
        raise HTTPException(404, "Change not found")
    before = {"change_uid": row[1]}
    cur.execute(
        "DELETE FROM document_content_reference WHERE soa_id=? AND change_uid=?",
        (soa_id, row[1]),
    )
    cur.execute(
        "DELETE FROM study_change WHERE id=? AND soa_id=?",
        (change_id, soa_id),
    )
    conn.commit()
    conn.close()
    _record_change_audit(soa_id, "delete", change_id, before=before)


@router.post(
    "/soa/{soa_id}/change/{change_id}/sections",
    status_code=201,
)
def add_section(soa_id: int, change_id: int, body: DocumentContentReferenceCreate):
    if not soa_exists(soa_id):
        raise HTTPException(404, "SOA not found")
    conn = _connect()
    cur = conn.cursor()
    cur.execute(
        "SELECT change_uid FROM study_change WHERE id=? AND soa_id=?",
        (change_id, soa_id),
    )
    row = cur.fetchone()
    if not row:
        conn.close()
        raise HTTPException(404, "Change not found")
    change_uid = row[0]
    ref_uid = _next_ref_uid(cur, soa_id)
    cur.execute(
        "INSERT INTO document_content_reference "
        "(soa_id,change_uid,ref_uid,section_number,section_title,"
        "applies_to_id) VALUES (?,?,?,?,?,?)",
        (
            soa_id,
            change_uid,
            ref_uid,
            body.section_number,
            body.section_title,
            body.applies_to_id,
        ),
    )
    ref_id = cur.lastrowid
    conn.commit()
    conn.close()
    _record_ref_audit(
        soa_id,
        "create",
        ref_id,
        after={"ref_uid": ref_uid, "section_number": body.section_number},
    )
    return JSONResponse({"id": ref_id, "ref_uid": ref_uid}, status_code=201)


@router.delete(
    "/soa/{soa_id}/change/{change_id}/section/{ref_id}",
    status_code=204,
)
def delete_section(soa_id: int, change_id: int, ref_id: int):
    if not soa_exists(soa_id):
        raise HTTPException(404, "SOA not found")
    conn = _connect()
    cur = conn.cursor()
    cur.execute(
        "SELECT id,ref_uid FROM document_content_reference WHERE id=? AND soa_id=?",
        (ref_id, soa_id),
    )
    row = cur.fetchone()
    if not row:
        conn.close()
        raise HTTPException(404, "Section not found")
    before = {"ref_uid": row[1]}
    cur.execute(
        "DELETE FROM document_content_reference WHERE id=? AND soa_id=?",
        (ref_id, soa_id),
    )
    conn.commit()
    conn.close()
    _record_ref_audit(soa_id, "delete", ref_id, before=before)


# ---------------------------------------------------------------------------
# UI endpoints
# ---------------------------------------------------------------------------


@ui_router.get(
    "/ui/soa/{soa_id}/freeze/{freeze_id}/amendment",
    response_class=HTMLResponse,
)
def ui_amendment_edit(request: Request, soa_id: int, freeze_id: int):
    if not soa_exists(soa_id):
        raise HTTPException(404, "SOA not found")
    conn = _connect()
    cur = conn.cursor()
    cur.execute(
        "SELECT id FROM study_amendment WHERE freeze_id=? AND soa_id=?",
        (freeze_id, soa_id),
    )
    row = cur.fetchone()
    conn.close()
    if not row:
        raise HTTPException(404, "Amendment not found for this freeze")
    ctx = _load_amendment_context(soa_id, row[0])
    if not ctx:
        raise HTTPException(404, "Amendment not found")
    ctx["freeze_id"] = freeze_id
    return templates.TemplateResponse(request, "amendment_edit.html", ctx)


@ui_router.get(
    "/ui/soa/{soa_id}/amendment/form_fields",
    response_class=HTMLResponse,
)
def ui_amendment_form_fields(request: Request, soa_id: int):
    reason_terms = get_ddf_ct_codelist_map(_REASON_CODELIST)
    return templates.TemplateResponse(
        request,
        "amendment_form_fields.html",
        {"reason_terms": reason_terms, "soa_id": soa_id},
    )


@ui_router.post(
    "/ui/soa/{soa_id}/freeze/{freeze_id}/amendment/create",
    response_class=HTMLResponse,
)
def ui_create_amendment(
    request: Request,
    soa_id: int,
    freeze_id: int,
    name: str = Form(...),
    number: str = Form(...),
    summary: str = Form(...),
    label: str = Form(""),
    description: str = Form(""),
    primary_reason_code: str = Form(...),
    primary_reason_other: str = Form(""),
):
    if not soa_exists(soa_id):
        raise HTTPException(404, "SOA not found")
    try:
        body = StudyAmendmentCreate(
            name=name,
            number=number,
            summary=summary,
            label=label or None,
            description=description or None,
            primary_reason_code=primary_reason_code,
            primary_reason_other=primary_reason_other or None,
        )
    except Exception as exc:
        msg = str(exc)
        if request.headers.get("HX-Request") == "true":
            return HTMLResponse(
                f"<div class='error' style='color:#c62828;font-size:0.7em;'>"
                f"Error: {html.escape(msg)}</div>"
            )
        return HTMLResponse(
            f"<div class='error' style='color:#c62828;'>{html.escape(msg)}</div>"
        )
    conn = _connect()
    cur = conn.cursor()
    cur.execute(
        "SELECT id FROM soa_freeze WHERE id=? AND soa_id=?",
        (freeze_id, soa_id),
    )
    if not cur.fetchone():
        conn.close()
        raise HTTPException(404, "Freeze not found")
    cur.execute(
        "SELECT id FROM study_amendment WHERE freeze_id=? AND soa_id=?",
        (freeze_id, soa_id),
    )
    if cur.fetchone():
        conn.close()
        msg = "Amendment already exists for this freeze"
        if request.headers.get("HX-Request") == "true":
            return HTMLResponse(
                f"<div class='error' style='color:#c62828;font-size:0.7em;'>"
                f"Error: {html.escape(msg)}</div>"
            )
        return HTMLResponse(
            f"<div class='error' style='color:#c62828;'>{html.escape(msg)}</div>"
        )

    amendment_uid = _next_amendment_uid(cur, soa_id)
    reason_uid = _next_reason_uid(cur, soa_id)
    code_uid = _insert_code(cur, soa_id, body.primary_reason_code, _REASON_CODELIST)
    cur.execute(
        "INSERT INTO study_amendment "
        "(soa_id,freeze_id,amendment_uid,name,number,summary,label,"
        "description) VALUES (?,?,?,?,?,?,?,?)",
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
        "(soa_id,amendment_uid,reason_uid,role,code_uid,other_reason) "
        "VALUES (?,?,?,?,?,?)",
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
    safe_soa = html.escape(str(soa_id))
    if request.headers.get("HX-Request") == "true":
        return HTMLResponse(
            "",
            headers={"HX-Redirect": f"/ui/soa/{safe_soa}/freezes"},
        )
    return HTMLResponse(
        f"<script>window.location='/ui/soa/{safe_soa}/freezes';</script>"
    )


def _ui_reasons_partial(
    request: Request, soa_id: int, amendment_id: int
) -> HTMLResponse:
    ctx = _load_amendment_data(soa_id, amendment_id)
    if not ctx:
        raise HTTPException(404, "Amendment not found")
    ctx["reason_terms"] = get_ddf_ct_codelist_map(_REASON_CODELIST)
    return templates.TemplateResponse(
        request,
        "amendment_reasons_partial.html",
        ctx,
    )


def _ui_impacts_partial(
    request: Request, soa_id: int, amendment_id: int
) -> HTMLResponse:
    ctx = _load_amendment_data(soa_id, amendment_id)
    if not ctx:
        raise HTTPException(404, "Amendment not found")
    ctx["impact_terms"] = get_ddf_ct_codelist_map(_IMPACT_TYPE_CODELIST)
    return templates.TemplateResponse(
        request,
        "amendment_impacts_partial.html",
        ctx,
    )


def _ui_changes_partial(
    request: Request, soa_id: int, amendment_id: int
) -> HTMLResponse:
    ctx = _load_amendment_data(soa_id, amendment_id)
    if not ctx:
        raise HTTPException(404, "Amendment not found")
    return templates.TemplateResponse(
        request,
        "amendment_changes_partial.html",
        ctx,
    )


@ui_router.post(
    "/ui/soa/{soa_id}/amendment/{amendment_id}/reasons/add",
    response_class=HTMLResponse,
)
def ui_add_secondary_reason(
    request: Request,
    soa_id: int,
    amendment_id: int,
    code: str = Form(...),
    other_reason: str = Form(""),
):
    if not soa_exists(soa_id):
        raise HTTPException(404, "SOA not found")
    try:
        body = StudyAmendmentReasonCreate(code=code, other_reason=other_reason or None)
    except Exception as exc:
        return HTMLResponse(
            f"<div style='color:#c62828;font-size:0.8em;'>"
            f"Error: {html.escape(str(exc))}</div>"
        )
    conn = _connect()
    cur = conn.cursor()
    am = _amendment_row(cur, soa_id, amendment_id)
    if not am:
        conn.close()
        raise HTTPException(404, "Amendment not found")
    reason_uid = _next_reason_uid(cur, soa_id)
    code_uid = _insert_code(cur, soa_id, body.code, _REASON_CODELIST)
    cur.execute(
        "INSERT INTO study_amendment_reason "
        "(soa_id,amendment_uid,reason_uid,role,code_uid,other_reason) "
        "VALUES (?,?,?,?,?,?)",
        (
            soa_id,
            am["amendment_uid"],
            reason_uid,
            "secondary",
            code_uid,
            body.other_reason,
        ),
    )
    reason_id = cur.lastrowid
    conn.commit()
    conn.close()
    _record_reason_audit(
        soa_id,
        "create",
        reason_id,
        after={"reason_uid": reason_uid, "role": "secondary"},
    )
    return _ui_reasons_partial(request, soa_id, amendment_id)


@ui_router.post(
    "/ui/soa/{soa_id}/amendment/{amendment_id}/reason/{reason_id}/delete",
    response_class=HTMLResponse,
)
def ui_delete_secondary_reason(
    request: Request, soa_id: int, amendment_id: int, reason_id: int
):
    if not soa_exists(soa_id):
        raise HTTPException(404, "SOA not found")
    conn = _connect()
    cur = conn.cursor()
    cur.execute(
        "SELECT id,reason_uid,role,code_uid FROM study_amendment_reason "
        "WHERE id=? AND soa_id=?",
        (reason_id, soa_id),
    )
    row = cur.fetchone()
    if not row or row[2] == "primary":
        conn.close()
        raise HTTPException(400, "Cannot delete primary or missing reason")
    before = {"reason_uid": row[1]}
    cur.execute(
        "DELETE FROM code_association WHERE soa_id=? AND code_uid=?",
        (soa_id, row[3]),
    )
    cur.execute(
        "DELETE FROM study_amendment_reason WHERE id=? AND soa_id=?",
        (reason_id, soa_id),
    )
    conn.commit()
    conn.close()
    _record_reason_audit(soa_id, "delete", reason_id, before=before)
    return _ui_reasons_partial(request, soa_id, amendment_id)


@ui_router.post(
    "/ui/soa/{soa_id}/amendment/{amendment_id}/impacts/add",
    response_class=HTMLResponse,
)
def ui_add_impact(
    request: Request,
    soa_id: int,
    amendment_id: int,
    type_code: str = Form(...),
    text: str = Form(...),
    is_substantial: str = Form(""),
):
    if not soa_exists(soa_id):
        raise HTTPException(404, "SOA not found")
    try:
        body = StudyAmendmentImpactCreate(
            type_code=type_code,
            text=text,
            is_substantial=bool(is_substantial),
        )
    except Exception as exc:
        return HTMLResponse(
            f"<div style='color:#c62828;font-size:0.8em;'>"
            f"Error: {html.escape(str(exc))}</div>"
        )
    conn = _connect()
    cur = conn.cursor()
    am = _amendment_row(cur, soa_id, amendment_id)
    if not am:
        conn.close()
        raise HTTPException(404, "Amendment not found")
    impact_uid = _next_impact_uid(cur, soa_id)
    type_code_uid = _insert_code(cur, soa_id, body.type_code, _IMPACT_TYPE_CODELIST)
    cur.execute(
        "INSERT INTO study_amendment_impact "
        "(soa_id,amendment_uid,impact_uid,type_code_uid,text,is_substantial)"
        " VALUES (?,?,?,?,?,?)",
        (
            soa_id,
            am["amendment_uid"],
            impact_uid,
            type_code_uid,
            body.text,
            int(body.is_substantial),
        ),
    )
    impact_id = cur.lastrowid
    conn.commit()
    conn.close()
    _record_impact_audit(
        soa_id,
        "create",
        impact_id,
        after={"impact_uid": impact_uid},
    )
    return _ui_impacts_partial(request, soa_id, amendment_id)


@ui_router.post(
    "/ui/soa/{soa_id}/amendment/{amendment_id}/impact/{impact_id}/delete",
    response_class=HTMLResponse,
)
def ui_delete_impact(request: Request, soa_id: int, amendment_id: int, impact_id: int):
    if not soa_exists(soa_id):
        raise HTTPException(404, "SOA not found")
    conn = _connect()
    cur = conn.cursor()
    cur.execute(
        "SELECT id,impact_uid,type_code_uid FROM study_amendment_impact "
        "WHERE id=? AND soa_id=?",
        (impact_id, soa_id),
    )
    row = cur.fetchone()
    if not row:
        conn.close()
        raise HTTPException(404, "Impact not found")
    before = {"impact_uid": row[1]}
    cur.execute(
        "DELETE FROM code_association WHERE soa_id=? AND code_uid=?",
        (soa_id, row[2]),
    )
    cur.execute(
        "DELETE FROM study_amendment_impact WHERE id=? AND soa_id=?",
        (impact_id, soa_id),
    )
    conn.commit()
    conn.close()
    _record_impact_audit(soa_id, "delete", impact_id, before=before)
    return _ui_impacts_partial(request, soa_id, amendment_id)


@ui_router.post(
    "/ui/soa/{soa_id}/amendment/{amendment_id}/changes/add",
    response_class=HTMLResponse,
)
def ui_add_change(
    request: Request,
    soa_id: int,
    amendment_id: int,
    name: str = Form(...),
    summary: str = Form(...),
    rationale: str = Form(...),
    label: str = Form(""),
    description: str = Form(""),
):
    if not soa_exists(soa_id):
        raise HTTPException(404, "SOA not found")
    try:
        body = StudyChangeCreate(
            name=name,
            summary=summary,
            rationale=rationale,
            label=label or None,
            description=description or None,
        )
    except Exception as exc:
        return HTMLResponse(
            f"<div style='color:#c62828;font-size:0.8em;'>"
            f"Error: {html.escape(str(exc))}</div>"
        )
    conn = _connect()
    cur = conn.cursor()
    am = _amendment_row(cur, soa_id, amendment_id)
    if not am:
        conn.close()
        raise HTTPException(404, "Amendment not found")
    change_uid = _next_change_uid(cur, soa_id)
    cur.execute(
        "INSERT INTO study_change "
        "(soa_id,amendment_uid,change_uid,name,label,description,"
        "summary,rationale) VALUES (?,?,?,?,?,?,?,?)",
        (
            soa_id,
            am["amendment_uid"],
            change_uid,
            body.name,
            body.label,
            body.description,
            body.summary,
            body.rationale,
        ),
    )
    change_id = cur.lastrowid
    conn.commit()
    conn.close()
    _record_change_audit(
        soa_id,
        "create",
        change_id,
        after={"change_uid": change_uid, "name": body.name},
    )
    return _ui_changes_partial(request, soa_id, amendment_id)


@ui_router.post(
    "/ui/soa/{soa_id}/amendment/{amendment_id}/change/{change_id}/delete",
    response_class=HTMLResponse,
)
def ui_delete_change(request: Request, soa_id: int, amendment_id: int, change_id: int):
    if not soa_exists(soa_id):
        raise HTTPException(404, "SOA not found")
    conn = _connect()
    cur = conn.cursor()
    cur.execute(
        "SELECT id,change_uid FROM study_change WHERE id=? AND soa_id=?",
        (change_id, soa_id),
    )
    row = cur.fetchone()
    if not row:
        conn.close()
        raise HTTPException(404, "Change not found")
    before = {"change_uid": row[1]}
    cur.execute(
        "DELETE FROM document_content_reference WHERE soa_id=? AND change_uid=?",
        (soa_id, row[1]),
    )
    cur.execute(
        "DELETE FROM study_change WHERE id=? AND soa_id=?",
        (change_id, soa_id),
    )
    conn.commit()
    conn.close()
    _record_change_audit(soa_id, "delete", change_id, before=before)
    return _ui_changes_partial(request, soa_id, amendment_id)


@ui_router.post(
    "/ui/soa/{soa_id}/change/{change_id}/sections/add",
    response_class=HTMLResponse,
)
def ui_add_section(
    request: Request,
    soa_id: int,
    change_id: int,
    section_number: str = Form(...),
    section_title: str = Form(...),
    applies_to_id: str = Form(...),
    amendment_id: int = Form(...),
):
    if not soa_exists(soa_id):
        raise HTTPException(404, "SOA not found")
    try:
        body = DocumentContentReferenceCreate(
            section_number=section_number,
            section_title=section_title,
            applies_to_id=applies_to_id,
        )
    except Exception as exc:
        return HTMLResponse(
            f"<div style='color:#c62828;font-size:0.8em;'>"
            f"Error: {html.escape(str(exc))}</div>"
        )
    conn = _connect()
    cur = conn.cursor()
    cur.execute(
        "SELECT change_uid FROM study_change WHERE id=? AND soa_id=?",
        (change_id, soa_id),
    )
    row = cur.fetchone()
    if not row:
        conn.close()
        raise HTTPException(404, "Change not found")
    ref_uid = _next_ref_uid(cur, soa_id)
    cur.execute(
        "INSERT INTO document_content_reference "
        "(soa_id,change_uid,ref_uid,section_number,section_title,"
        "applies_to_id) VALUES (?,?,?,?,?,?)",
        (
            soa_id,
            row[0],
            ref_uid,
            body.section_number,
            body.section_title,
            body.applies_to_id,
        ),
    )
    ref_id = cur.lastrowid
    conn.commit()
    conn.close()
    _record_ref_audit(
        soa_id,
        "create",
        ref_id,
        after={"ref_uid": ref_uid},
    )
    return _ui_changes_partial(request, soa_id, amendment_id)


@ui_router.post(
    "/ui/soa/{soa_id}/change/{change_id}/section/{ref_id}/delete",
    response_class=HTMLResponse,
)
def ui_delete_section(
    request: Request,
    soa_id: int,
    change_id: int,
    ref_id: int,
    amendment_id: int = Form(...),
):
    if not soa_exists(soa_id):
        raise HTTPException(404, "SOA not found")
    conn = _connect()
    cur = conn.cursor()
    cur.execute(
        "SELECT id,ref_uid FROM document_content_reference WHERE id=? AND soa_id=?",
        (ref_id, soa_id),
    )
    row = cur.fetchone()
    if not row:
        conn.close()
        raise HTTPException(404, "Section not found")
    before = {"ref_uid": row[1]}
    cur.execute(
        "DELETE FROM document_content_reference WHERE id=? AND soa_id=?",
        (ref_id, soa_id),
    )
    conn.commit()
    conn.close()
    _record_ref_audit(soa_id, "delete", ref_id, before=before)
    return _ui_changes_partial(request, soa_id, amendment_id)
