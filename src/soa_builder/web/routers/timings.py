import logging
import json
import os
from typing import Optional

from fastapi import APIRouter, HTTPException, Request, Form
from fastapi.responses import JSONResponse, HTMLResponse, RedirectResponse
from fastapi.templating import Jinja2Templates

from ..audit import _record_timing_audit
from ..db import _connect
from ..schemas import TimingCreate, TimingUpdate
from ..utils import soa_exists
from ..utils import get_study_timing_type
from ..utils import get_next_code_uid as _get_next_code_uid

router = APIRouter()
logger = logging.getLogger("soa_builder.web.routers.timings")
templates = Jinja2Templates(
    directory=os.path.join(os.path.dirname(__file__), "..", "templates")
)


def _nz(s: Optional[str]) -> Optional[str]:
    s = (s or "").strip()
    return s or None


# UI code to list timings in an SOA
@router.get("/ui/soa/{soa_id}/timings", response_class=HTMLResponse)
def ui_list_timings(request: Request, soa_id: int):
    if not soa_exists(soa_id):
        raise HTTPException(404, "SOA not found")
    timings = list_timings(soa_id)
    # Build mapping: submissionValue -> code and reverse
    try:
        sv_to_code = get_study_timing_type("C201264")
    except Exception:
        sv_to_code = {}
    # Mapping for Relative To/From (C201265)
    try:
        sv_to_code_rtf = get_study_timing_type("C201265")
    except Exception:
        sv_to_code_rtf = {}
    code_to_sv = {v: k for k, v in (sv_to_code or {}).items()}
    code_to_sv_rtf = {v: k for k, v in (sv_to_code_rtf or {}).items()}
    # Map timing.type (code_uid) -> code via code table, then to submissionValue
    conn = _connect()
    cur = conn.cursor()
    cur.execute("SELECT code_uid, code FROM code WHERE soa_id=?", (soa_id,))
    code_uid_to_code = {r[0]: r[1] for r in cur.fetchall() if r[0]}
    conn.close()
    for t in timings:
        sv = None
        cu = t.get("type")
        if cu and cu in code_uid_to_code:
            code_val = code_uid_to_code.get(cu)
            sv = code_to_sv.get(str(code_val))
        t["type_submission_value"] = sv
        # Decode relative_to_from (stores code_uid) -> submission value
        rtf_sv = None
        rtf_cu = t.get("relative_to_from")
        if rtf_cu and rtf_cu in code_uid_to_code:
            rtf_code_val = code_uid_to_code.get(rtf_cu)
            rtf_sv = code_to_sv_rtf.get(str(rtf_code_val))
        t["relative_to_from_submission_value"] = rtf_sv
    return templates.TemplateResponse(
        request,
        "timings.html",
        {
            "request": request,
            "soa_id": soa_id,
            "timings": timings,
            "timing_type_options": sorted(list(sv_to_code.keys())),
            "relative_to_from_options": sorted(list(sv_to_code_rtf.keys())),
        },
    )


# UI code to create a timing for an SOA
@router.post("/ui/soa/{soa_id}/timings/create")
def ui_create_timing(
    request: Request,
    soa_id: int,
    name: str = Form(...),
    label: Optional[str] = Form(None),
    description: Optional[str] = Form(None),
    type_submission_value: Optional[str] = Form(None),
    value: Optional[str] = Form(None),
    value_label: Optional[str] = Form(None),
    relative_to_from_submission_value: Optional[str] = Form(None),
    relative_from_schedule_instance: Optional[str] = Form(None),
    relative_to_schedule_instance: Optional[str] = Form(None),
    window_label: Optional[str] = Form(None),
    window_upper: Optional[str] = Form(None),
    window_lower: Optional[str] = Form(None),
):
    # Map selected submission value to code_uid stored in code table
    code_uid: Optional[str] = None
    sv = (type_submission_value or "").strip()
    if sv:
        try:
            sv_to_code = get_study_timing_type("C201264")
            code_val = sv_to_code.get(sv)
            if code_val:
                conn_c = _connect()
                cur_c = conn_c.cursor()
                code_uid = _get_next_code_uid(cur_c, soa_id)
                cur_c.execute(
                    "INSERT INTO code (soa_id, code_uid, codelist_table, codelist_code, code) VALUES (?,?,?,?,?)",
                    (
                        soa_id,
                        code_uid,
                        "ddf_terminology",
                        "C201264",
                        str(code_val),
                    ),
                )
                conn_c.commit()
                conn_c.close()
        except Exception:
            code_uid = None
    # Map Relative To/From submission value to code_uid
    rtf_code_uid: Optional[str] = None
    rsv = (relative_to_from_submission_value or "").strip()
    if rsv:
        try:
            sv_to_code_rtf = get_study_timing_type("C201265")
            rtf_code_val = sv_to_code_rtf.get(rsv)
            if rtf_code_val:
                conn_c2 = _connect()
                cur_c2 = conn_c2.cursor()
                rtf_code_uid = _get_next_code_uid(cur_c2, soa_id)
                cur_c2.execute(
                    "INSERT INTO code (soa_id, code_uid, codelist_table, codelist_code, code) VALUES (?,?,?,?,?)",
                    (
                        soa_id,
                        rtf_code_uid,
                        "ddf_terminology",
                        "C201265",
                        str(rtf_code_val),
                    ),
                )
                conn_c2.commit()
                conn_c2.close()
        except Exception:
            rtf_code_uid = None
    payload = TimingCreate(
        name=name,
        label=label,
        description=description,
        type=code_uid,
        value=value,
        value_label=value_label,
        relative_to_from=rtf_code_uid,
        relative_from_schedule_instance=relative_from_schedule_instance,
        relative_to_schedule_instance=relative_to_schedule_instance,
        window_label=window_label,
        window_upper=window_upper,
        window_lower=window_lower,
    )
    create_timing(soa_id, payload)
    return RedirectResponse(url=f"/ui/soa/{soa_id}/timings", status_code=303)


# API endpoint to list timings for SOA
@router.get("/soa/{soa_id}/timings", response_class=JSONResponse, response_model=None)
def list_timings(soa_id: int):
    if not soa_exists(soa_id):
        raise HTTPException(404, "SOA not found")

    conn = _connect()
    cur = conn.cursor()
    cur.execute(
        "SELECT id,timing_uid,name,label,description,type, "
        "value,value_label,relative_to_from,relative_from_schedule_instance, "
        "relative_to_schedule_instance,window_label,window_upper,window_lower,order_index "
        "FROM timing WHERE soa_id=? order by order_index, id",
        (soa_id,),
    )
    rows = [
        {
            "id": r[0],
            "timing_uid": r[1],
            "name": r[2],
            "label": r[3],
            "description": r[4],
            "type": r[5],
            "value": r[6],
            "value_label": r[7],
            "relative_to_from": r[8],
            "relative_from_schedule_instance": r[9],
            "relative_to_schedule_instance": r[10],
            "window_label": r[11],
            "window_upper": r[12],
            "window_lower": r[13],
            "order_index": r[14],
        }
        for r in cur.fetchall()
    ]
    conn.close()
    return rows


@router.get("/soa/{soa_id}/timing_audit", response_class=JSONResponse)
def list_timing_audit(soa_id: int):
    if not soa_exists(soa_id):
        raise HTTPException(404, "SOA not found")
    conn = _connect()
    cur = conn.cursor()
    try:
        cur.execute(
            "SELECT id, timing_id, action, before_json, after_json, performed_at FROM timing_audit WHERE soa_id=? ORDER BY id DESC",
            (soa_id,),
        )
    except Exception:
        # If table does not exist yet, return empty list for backward compatibility
        conn.close()
        return JSONResponse([])
    rows = []
    for r in cur.fetchall():
        try:
            before = json.loads(r[3]) if r[3] else None
        except Exception:
            before = None
        try:
            after = json.loads(r[4]) if r[4] else None
        except Exception:
            after = None
        rows.append(
            {
                "id": r[0],
                "timing_id": r[1],
                "action": r[2],
                "before": before,
                "after": after,
                "performed_at": r[5],
            }
        )
    conn.close()
    return JSONResponse(rows)


# API endpoint for creating a timing in an SOA
@router.post(
    "/soa/{soa_id}/timings",
    response_class=JSONResponse,
    status_code=201,
    response_model=None,
)
def create_timing(soa_id: int, payload: TimingCreate):
    if not soa_exists(soa_id):
        raise HTTPException(404, "SOA not found")

    name = (payload.name or "").strip()
    if not name:
        raise HTTPException(400, "Timing name required")

    conn = _connect()
    cur = conn.cursor()
    cur.execute(
        "SELECT COALESCE(MAX(order_index),0) FROM timing WHERE soa_id=?",
        (soa_id,),
    )
    next_ord = (cur.fetchone() or [0])[0] + 1
    cur.execute(
        "SELECT timing_uid FROM timing WHERE soa_id=? AND timing_uid LIKE 'Timing_%'",
        (soa_id,),
    )
    existing_uids = [r[0] for r in cur.fetchall() if r[0]]
    used_nums = set()
    for uid in existing_uids:
        if uid.startswith("Timing_"):
            tail = uid[len("Timing_") :]
            if tail.isdigit():
                used_nums.add(int(tail))
            else:
                logger.warning(
                    "Invalid timing_uid format encountered (ignored): %s",
                    uid,
                )
    """next_n = 1
    while next_n in used_nums:
        next_n += 1
    """
    # Always pick max(existing) + 1, do not fill gaps
    next_n = (max(used_nums) if used_nums else 0) + 1
    new_uid = f"Timing_{next_n}"
    cur.execute(
        """INSERT INTO timing (soa_id,timing_uid,name,label,description,type,value,value_label,
        relative_to_from,relative_from_schedule_instance,relative_to_schedule_instance,window_label,
        window_upper,window_lower,order_index) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
        (
            soa_id,
            new_uid,
            name,
            _nz(payload.label),
            _nz(payload.description),
            _nz(payload.type),
            _nz(payload.value),
            _nz(payload.value_label),
            _nz(payload.relative_to_from),
            _nz(payload.relative_from_schedule_instance),
            _nz(payload.relative_to_schedule_instance),
            _nz(payload.window_label),
            _nz(payload.window_upper),
            _nz(payload.window_lower),
            next_ord,
        ),
    )
    timing_id = cur.lastrowid
    conn.commit()
    conn.close()
    row = {
        "id": timing_id,
        "timing_uid": new_uid,
        "name": name,
        "label": (payload.label or "").strip() or None,
        "description": (payload.description or "").strip() or None,
        "order_index": next_ord,
    }
    _record_timing_audit(soa_id, "create", timing_id, before=None, after=row)
    return row


# API endpoint to update a timing in an SOA
@router.patch(
    "/soa/{soa_id}/timings/{timing_id}",
    response_class=JSONResponse,
    response_model=None,
)
def update_timing(soa_id: int, timing_id: int, payload: TimingUpdate):
    if not soa_exists(soa_id):
        raise HTTPException(404, "SOA not found")

    conn = _connect()
    cur = conn.cursor()
    cur.execute(
        "SELECT id,timing_uid,name,label,description,type,value,value_label,relative_to_from,"
        "relative_from_schedule_instance,relative_to_schedule_instance,window_label,window_upper,"
        "window_lower,order_index FROM timing WHERE soa_id=? AND id=?",
        (
            soa_id,
            timing_id,
        ),
    )
    row = cur.fetchone()
    if not row:
        conn.close()
        raise HTTPException(404, f"Timing id={int(timing_id)} not found")

    before = {
        "id": row[0],
        "timing_uid": row[1],
        "name": row[2],
        "label": row[3],
        "description": row[4],
        "type": row[5],
        "value": row[6],
        "value_label": row[7],
        "relative_to_from": row[8],
        "relative_from_schedule_instance": row[9],
        "relative_to_schedule_instance": row[10],
        "window_label": row[11],
        "window_upper": row[12],
        "window_lower": row[13],
        "order_index": row[14],
    }
    new_name = (payload.name if payload.name is not None else before["name"]) or ""
    new_label = payload.label if payload.label is not None else before["label"]
    new_description = (
        payload.description
        if payload.description is not None
        else before["description"]
    )
    new_type = payload.type if payload.type is not None else before["type"]
    new_value = payload.value if payload.value is not None else before["value"]
    new_value_label = (
        payload.value_label
        if payload.value_label is not None
        else before["value_label"]
    )
    new_relative_to_from = (
        payload.relative_to_from
        if payload.relative_to_from is not None
        else before["relative_to_from"]
    )
    new_relative_from_schedule_instance = (
        payload.relative_from_schedule_instance
        if payload.relative_from_schedule_instance is not None
        else before["relative_from_schedule_instance"]
    )
    new_relative_to_schedule_instance = (
        payload.relative_to_schedule_instance
        if payload.relative_to_schedule_instance is not None
        else before["relative_to_schedule_instance"]
    )
    new_window_label = (
        payload.window_label
        if payload.window_label is not None
        else before["window_label"]
    )
    new_window_upper = (
        payload.window_upper
        if payload.window_upper is not None
        else before["window_upper"]
    )
    new_window_lower = (
        payload.window_lower
        if payload.window_lower is not None
        else before["window_lower"]
    )

    cur.execute(
        "UPDATE timing SET name=?, label=?, description=?, type=?, value=?, value_label=?, "
        "relative_to_from=?, relative_from_schedule_instance=?, relative_to_schedule_instance=?, "
        "window_label=?, window_upper=?, window_lower=? WHERE id=? AND soa_id=?",
        (
            _nz(new_name),
            _nz(new_label),
            _nz(new_description),
            _nz(new_type),
            _nz(new_value),
            _nz(new_value_label),
            _nz(new_relative_to_from),
            _nz(new_relative_from_schedule_instance),
            _nz(new_relative_to_schedule_instance),
            _nz(new_window_label),
            _nz(new_window_upper),
            _nz(new_window_lower),
            timing_id,
            soa_id,
        ),
    )
    conn.commit()
    cur.execute(
        "SELECT id,timing_uid,name,label,description,type,value,value_label,relative_to_from,"
        "relative_from_schedule_instance,relative_to_schedule_instance,window_label,window_upper,"
        "window_lower,order_index FROM timing WHERE soa_id=? AND id=?",
        (soa_id, timing_id),
    )
    r = cur.fetchone()
    conn.close()
    after = {
        "id": r[0],
        "timing_uid": r[1],
        "name": r[2],
        "label": r[3],
        "description": r[4],
        "type": r[5],
        "value": r[6],
        "value_label": r[7],
        "relative_to_from": r[8],
        "relative_from_schedule_instance": r[9],
        "relative_to_schedule_instance": r[10],
        "window_label": r[11],
        "window_upper": r[12],
        "window_lower": r[13],
        "order_index": r[14],
    }
    mutable = [
        "name",
        "label",
        "description",
        "type",
        "value",
        "value_label",
        "relative_to_from",
        "relative_from_schedule_instance",
        "relative_to_schedule_instance",
        "window_label",
        "window_upper",
        "window_lower",
    ]
    update_fields = [
        f for f in mutable if (before.get(f) or None) != (after.get(f) or None)
    ]
    _record_timing_audit(
        soa_id,
        "update",
        timing_id,
        before=before,
        after={**after, "updated_fields": update_fields},
    )
    return {**after, "updated_fields": update_fields}


# UI code to update a timing in an SOA
@router.post("/ui/soa/{soa_id}/timings/{timing_id}/update")
def ui_update_timing(
    request: Request,
    soa_id: int,
    timing_id: int,
    name: Optional[str] = Form(None),
    label: Optional[str] = Form(None),
    description: Optional[str] = Form(None),
    type_submission_value: Optional[str] = Form(None),
    value: Optional[str] = Form(None),
    value_label: Optional[str] = Form(None),
    relative_to_from_submission_value: Optional[str] = Form(None),
    relative_from_schedule_instance: Optional[str] = Form(None),
    relative_to_schedule_instance: Optional[str] = Form(None),
    window_label: Optional[str] = Form(None),
    window_upper: Optional[str] = Form(None),
    window_lower: Optional[str] = Form(None),
):
    # Determine existing code values for type and relative_to_from
    mapped_type: Optional[str] = None
    mapped_rtf: Optional[str] = None
    try:
        conn_chk = _connect()
        cur_chk = conn_chk.cursor()
        cur_chk.execute(
            "SELECT type, relative_to_from FROM timing WHERE soa_id=? AND id=?",
            (soa_id, timing_id),
        )
        row_chk = cur_chk.fetchone()
        existing_type_uid = row_chk[0] if row_chk else None
        existing_rtf_uid = row_chk[1] if row_chk else None
        # Map existing code_uids -> code values
        cur_chk.execute(
            "SELECT code_uid, code FROM code WHERE soa_id=?",
            (soa_id,),
        )
        code_rows = cur_chk.fetchall() or []
        code_uid_to_code = {r[0]: str(r[1]) for r in code_rows if r and r[0]}
        # Handle type mapping only when changed
        if type_submission_value is not None:
            sv = (type_submission_value or "").strip()
            if sv == "":
                mapped_type = ""  # clear field
            else:
                sv_to_code = get_study_timing_type("C201264")
                new_code_val = sv_to_code.get(sv)
                current_code_val = (
                    code_uid_to_code.get(existing_type_uid)
                    if existing_type_uid
                    else None
                )
                if new_code_val is None:
                    mapped_type = None  # invalid; ignore
                elif str(current_code_val) == str(new_code_val):
                    mapped_type = None  # unchanged; do not update
                else:
                    # Always create a fresh code_uid; do not reuse across timings
                    mapped_type = _get_next_code_uid(cur_chk, soa_id)
                    cur_chk.execute(
                        "INSERT INTO code (soa_id, code_uid, codelist_table, codelist_code, code) VALUES (?,?,?,?,?)",
                        (
                            soa_id,
                            mapped_type,
                            "ddf_terminology",
                            "C201264",
                            str(new_code_val),
                        ),
                    )
        # Handle relative_to_from mapping only when changed
        if relative_to_from_submission_value is not None:
            rsv = (relative_to_from_submission_value or "").strip()
            if rsv == "":
                mapped_rtf = ""  # clear field
            else:
                sv_to_code_rtf = get_study_timing_type("C201265")
                new_rtf_code_val = sv_to_code_rtf.get(rsv)
                current_rtf_code_val = (
                    code_uid_to_code.get(existing_rtf_uid) if existing_rtf_uid else None
                )
                if new_rtf_code_val is None:
                    mapped_rtf = None  # invalid; ignore
                elif str(current_rtf_code_val) == str(new_rtf_code_val):
                    mapped_rtf = None  # unchanged; do not update
                else:
                    mapped_rtf = _get_next_code_uid(cur_chk, soa_id)
                    cur_chk.execute(
                        "INSERT INTO code (soa_id, code_uid, codelist_table, codelist_code, code) VALUES (?,?,?,?,?)",
                        (
                            soa_id,
                            mapped_rtf,
                            "ddf_terminology",
                            "C201265",
                            str(new_rtf_code_val),
                        ),
                    )
        conn_chk.commit()
        conn_chk.close()
    except Exception:
        # On any error, fall back to previous behavior (leave fields unchanged)
        mapped_type = None if type_submission_value not in ("",) else ""
        mapped_rtf = None if relative_to_from_submission_value not in ("",) else ""
    payload = TimingUpdate(
        name=name,
        label=label,
        description=description,
        type=mapped_type,
        value=value,
        value_label=value_label,
        relative_to_from=mapped_rtf,
        relative_from_schedule_instance=relative_from_schedule_instance,
        relative_to_schedule_instance=relative_to_schedule_instance,
        window_label=window_label,
        window_upper=window_upper,
        window_lower=window_lower,
    )
    update_timing(soa_id, timing_id, payload)
    return RedirectResponse(url=f"/ui/soa/{soa_id}/timings", status_code=303)


# API endpoint to delete a timing
@router.delete(
    "/soa/{soa_id}/timings/{timing_id}",
    response_class=JSONResponse,
    response_model=None,
)
def delete_timing(soa_id: int, timing_id: int):
    if not soa_exists(soa_id):
        raise HTTPException(404, "SOA not found")

    conn = _connect()
    cur = conn.cursor()
    cur.execute(
        "SELECT id,timing_uid,name,label,description FROM timing WHERE soa_id=? AND id=?",
        (
            soa_id,
            timing_id,
        ),
    )
    row = cur.fetchone()
    if not row:
        conn.close()
        raise HTTPException(404, f"Timing id={timing_id} not found")
    before = {
        "id": row[0],
        "timing_uid": row[1],
        "name": row[2],
        "label": row[3],
        "description": row[4],
    }
    cur.execute(
        "DELETE FROM timing WHERE id=? AND soa_id=?",
        (
            timing_id,
            soa_id,
        ),
    )
    conn.commit()
    conn.close()

    _record_timing_audit(soa_id, "delete", timing_id, before=before, after=None)
    return {"deleted": True, "id": timing_id}


# UI Code to delete timing
@router.post("/ui/soa/{soa_id}/timings/{timing_id}/delete")
def ui_delete_timing(request: Request, soa_id: int, timing_id: int):
    delete_timing(soa_id, timing_id)
    return RedirectResponse(url=f"/ui/soa/{int(soa_id)}/timings", status_code=303)
