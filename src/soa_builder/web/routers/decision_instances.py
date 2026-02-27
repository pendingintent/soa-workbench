import logging
import os
from typing import Optional, List

from fastapi import APIRouter, HTTPException, Request, Form, Body
from fastapi.responses import JSONResponse, HTMLResponse, RedirectResponse
from fastapi.templating import Jinja2Templates

from ..audit import _record_decision_instance_audit
from ..db import _connect
from ..schemas import DecisionInstanceCreate, DecisionInstanceUpdate
from ..utils import (
    soa_exists,
    get_epoch_uid,
    get_schedule_timeline,
    redirect_url_from_referer as _redirect_url,
    _nz as _nz,
)

router = APIRouter()
logger = logging.getLogger("soa_builder.web.routers.decision_instances")
templates = Jinja2Templates(
    directory=os.path.join(os.path.dirname(__file__), "..", "templates")
)


@router.get(
    "/soa/{soa_id}/decision_instances",
    response_class=JSONResponse,
    response_model=None,
)
def list_decision_instances(soa_id: int):
    if not soa_exists(soa_id):
        raise HTTPException(404, "SOA not found")

    conn = _connect()
    cur = conn.cursor()
    cur.execute(
        "SELECT id, instance_uid, name, label, description, default_condition_uid, "
        "epoch_uid, member_of_timeline, order_index FROM decision_instances "
        "WHERE soa_id=? ORDER BY order_index, id",
        (soa_id,),
    )
    rows = [
        {
            "id": r[0],
            "instance_uid": r[1],
            "name": r[2],
            "label": r[3],
            "description": r[4],
            "default_condition_uid": r[5],
            "epoch_uid": r[6],
            "member_of_timeline": r[7],
            "order_index": r[8],
        }
        for r in cur.fetchall()
    ]
    conn.close()
    return rows


@router.get("/ui/soa/{soa_id}/decision_instances", response_class=HTMLResponse)
def ui_list_decision_instances(request: Request, soa_id: int):
    if not soa_exists(soa_id):
        raise HTTPException(404, "SOA not found")

    decision_instances = list_decision_instances(soa_id)
    schedule_timelines_options = get_schedule_timeline(soa_id)
    epoch_options = get_epoch_uid(soa_id)

    conn = _connect()
    cur = conn.cursor()
    cur.execute(
        "SELECT study_id, study_label, study_description, name, created_at FROM soa WHERE id=?",
        (soa_id,),
    )
    meta_row = cur.fetchone()
    conn.close()

    study_id, study_label, study_description, study_name, study_created_at = meta_row
    return templates.TemplateResponse(
        request,
        "decision_instances.html",
        {
            "request": request,
            "soa_id": soa_id,
            "decision_instances": decision_instances,
            "schedule_timelines_options": schedule_timelines_options,
            "epoch_options": epoch_options,
            "study_id": study_id,
            "study_label": study_label,
            "study_description": study_description,
            "study_name": study_name,
            "study_created_at": study_created_at,
        },
    )


@router.post(
    "/soa/{soa_id}/decision_instances",
    response_class=JSONResponse,
    status_code=201,
    response_model=None,
)
def create_decision_instance(soa_id: int, payload: DecisionInstanceCreate):
    if not soa_exists(soa_id):
        raise HTTPException(404, "SOA not found")

    name = (payload.name or "").strip()
    if not name:
        raise HTTPException(400, "Decision instance name required")

    conn = _connect()
    cur = conn.cursor()
    cur.execute(
        "SELECT COALESCE(MAX(order_index), 0) FROM decision_instances WHERE soa_id=?",
        (soa_id,),
    )
    next_ord = (cur.fetchone() or [0])[0] + 1

    cur.execute(
        "SELECT instance_uid FROM decision_instances WHERE soa_id=? "
        "AND instance_uid LIKE 'ScheduledDecisionInstance_%'",
        (soa_id,),
    )
    existing_uids = [r[0] for r in cur.fetchall() if r[0]]
    used_nums = set()
    for uid in existing_uids:
        if uid.startswith("ScheduledDecisionInstance_"):
            tail = uid[len("ScheduledDecisionInstance_") :]
            if tail.isdigit():
                used_nums.add(int(tail))
            else:
                logger.warning(
                    "Invalid decision instance uid format (ignored): %s", uid
                )
    next_n = (max(used_nums) if used_nums else 0) + 1
    new_uid = f"ScheduledDecisionInstance_{next_n}"

    cur.execute(
        "INSERT INTO decision_instances (soa_id, instance_uid, name, label, description, "
        "default_condition_uid, epoch_uid, member_of_timeline, order_index) "
        "VALUES (?,?,?,?,?,?,?,?,?)",
        (
            soa_id,
            new_uid,
            name,
            _nz(payload.label),
            _nz(payload.description),
            _nz(payload.default_condition_uid),
            _nz(payload.epoch_uid),
            _nz(payload.member_of_timeline),
            next_ord,
        ),
    )
    decision_instance_id = cur.lastrowid
    conn.commit()
    conn.close()

    after = {
        "id": decision_instance_id,
        "instance_uid": new_uid,
        "name": name,
        "label": _nz(payload.label),
        "description": _nz(payload.description),
        "default_condition_uid": _nz(payload.default_condition_uid),
        "epoch_uid": _nz(payload.epoch_uid),
        "member_of_timeline": _nz(payload.member_of_timeline),
        "order_index": next_ord,
    }
    _record_decision_instance_audit(
        soa_id, "create", decision_instance_id, before=None, after=after
    )
    return after


@router.post("/ui/soa/{soa_id}/decision_instances/create")
def ui_create_decision_instance(
    request: Request,
    soa_id: int,
    name: str = Form(...),
    label: Optional[str] = Form(None),
    description: Optional[str] = Form(None),
    default_condition_uid: Optional[str] = Form(None),
    epoch_uid: Optional[str] = Form(None),
    member_of_timeline: Optional[str] = Form(None),
):
    payload = DecisionInstanceCreate(
        name=name,
        label=label,
        description=description,
        default_condition_uid=default_condition_uid,
        epoch_uid=epoch_uid,
        member_of_timeline=member_of_timeline,
    )
    create_decision_instance(soa_id, payload)
    return RedirectResponse(
        url=_redirect_url(request, f"/ui/soa/{int(soa_id)}/decision_instances"),
        status_code=303,
    )


@router.patch(
    "/soa/{soa_id}/decision_instances/{decision_instance_id}",
    response_class=JSONResponse,
    response_model=None,
)
def update_decision_instance(
    soa_id: int, decision_instance_id: int, payload: DecisionInstanceUpdate
):
    if not soa_exists(soa_id):
        raise HTTPException(404, "SOA not found")

    conn = _connect()
    cur = conn.cursor()
    cur.execute(
        "SELECT id, instance_uid, name, label, description, default_condition_uid, "
        "epoch_uid, member_of_timeline, order_index FROM decision_instances "
        "WHERE soa_id=? AND id=?",
        (soa_id, decision_instance_id),
    )
    row = cur.fetchone()
    if not row:
        conn.close()
        raise HTTPException(
            404, f"Decision instance id={int(decision_instance_id)} not found"
        )

    before = {
        "id": row[0],
        "instance_uid": row[1],
        "name": row[2],
        "label": row[3],
        "description": row[4],
        "default_condition_uid": row[5],
        "epoch_uid": row[6],
        "member_of_timeline": row[7],
        "order_index": row[8],
    }

    new_name = (payload.name if payload.name is not None else before["name"]) or ""
    new_label = payload.label if payload.label is not None else before["label"]
    new_description = (
        payload.description
        if payload.description is not None
        else before["description"]
    )
    new_default_condition_uid = (
        payload.default_condition_uid
        if payload.default_condition_uid is not None
        else before["default_condition_uid"]
    )
    new_epoch_uid = (
        payload.epoch_uid if payload.epoch_uid is not None else before["epoch_uid"]
    )
    new_member_of_timeline = (
        payload.member_of_timeline
        if payload.member_of_timeline is not None
        else before["member_of_timeline"]
    )

    cur.execute(
        "UPDATE decision_instances SET name=?, label=?, description=?, "
        "default_condition_uid=?, epoch_uid=?, member_of_timeline=? "
        "WHERE id=? AND soa_id=?",
        (
            _nz(new_name),
            _nz(new_label),
            _nz(new_description),
            _nz(new_default_condition_uid),
            _nz(new_epoch_uid),
            _nz(new_member_of_timeline),
            decision_instance_id,
            soa_id,
        ),
    )
    conn.commit()
    cur.execute(
        "SELECT id, instance_uid, name, label, description, default_condition_uid, "
        "epoch_uid, member_of_timeline, order_index FROM decision_instances "
        "WHERE soa_id=? AND id=?",
        (soa_id, decision_instance_id),
    )
    r = cur.fetchone()
    conn.close()

    after = {
        "id": r[0],
        "instance_uid": r[1],
        "name": r[2],
        "label": r[3],
        "description": r[4],
        "default_condition_uid": r[5],
        "epoch_uid": r[6],
        "member_of_timeline": r[7],
        "order_index": r[8],
    }
    mutable = [
        "name",
        "label",
        "description",
        "default_condition_uid",
        "epoch_uid",
        "member_of_timeline",
    ]
    updated_fields = [
        f for f in mutable if (before.get(f) or None) != (after.get(f) or None)
    ]
    _record_decision_instance_audit(
        soa_id,
        "update",
        decision_instance_id,
        before=before,
        after={**after, "updated_fields": updated_fields},
    )
    return {**after, "updated_fields": updated_fields}


@router.post("/ui/soa/{soa_id}/decision_instances/{decision_instance_id}/update")
def ui_update_decision_instance(
    request: Request,
    soa_id: int,
    decision_instance_id: int,
    name: Optional[str] = Form(None),
    label: Optional[str] = Form(None),
    description: Optional[str] = Form(None),
    default_condition_uid: Optional[str] = Form(None),
    epoch_uid: Optional[str] = Form(None),
    member_of_timeline: Optional[str] = Form(None),
):
    payload = DecisionInstanceUpdate(
        name=name,
        label=label,
        description=description,
        default_condition_uid=default_condition_uid,
        epoch_uid=epoch_uid,
        member_of_timeline=member_of_timeline,
    )
    update_decision_instance(soa_id, decision_instance_id, payload)
    return RedirectResponse(
        url=_redirect_url(request, f"/ui/soa/{int(soa_id)}/decision_instances"),
        status_code=303,
    )


@router.delete(
    "/soa/{soa_id}/decision_instances/{decision_instance_id}",
    response_class=JSONResponse,
    response_model=None,
)
def delete_decision_instance(soa_id: int, decision_instance_id: int):
    if not soa_exists(soa_id):
        raise HTTPException(404, "SOA not found")

    conn = _connect()
    cur = conn.cursor()
    cur.execute(
        "SELECT id, instance_uid, name FROM decision_instances WHERE soa_id=? AND id=?",
        (soa_id, decision_instance_id),
    )
    row = cur.fetchone()
    if not row:
        conn.close()
        raise HTTPException(
            404, f"Decision instance id={int(decision_instance_id)} not found"
        )

    before = {"id": row[0], "instance_uid": row[1], "name": row[2]}
    cur.execute(
        "DELETE FROM decision_instances WHERE id=? AND soa_id=?",
        (decision_instance_id, soa_id),
    )
    conn.commit()
    conn.close()
    _record_decision_instance_audit(
        soa_id, "delete", decision_instance_id, before=before, after=None
    )
    return {"deleted": True, "id": decision_instance_id}


@router.post("/ui/soa/{soa_id}/decision_instances/{decision_instance_id}/delete")
def ui_delete_decision_instance(
    request: Request, soa_id: int, decision_instance_id: int
):
    delete_decision_instance(soa_id, decision_instance_id)
    return RedirectResponse(
        url=_redirect_url(request, f"/ui/soa/{int(soa_id)}/decision_instances"),
        status_code=303,
    )


@router.post("/soa/{soa_id}/decision_instances/reorder", response_class=JSONResponse)
def reorder_decision_instances(
    soa_id: int,
    order: List[int] = Body(..., embed=True),
):
    if not soa_exists(soa_id):
        raise HTTPException(404, "SOA not found")
    if not order:
        raise HTTPException(400, "Order list required")

    conn = _connect()
    cur = conn.cursor()
    cur.execute("SELECT id FROM decision_instances WHERE soa_id=?", (soa_id,))
    existing = {r[0] for r in cur.fetchall()}
    if set(order) - existing:
        conn.close()
        raise HTTPException(400, "Order contains invalid decision instance id")

    for idx, did in enumerate(order, start=1):
        cur.execute(
            "UPDATE decision_instances SET order_index=? WHERE id=?", (idx, did)
        )
    conn.commit()
    conn.close()
    return JSONResponse({"ok": True, "new_order": order})
