import logging
import os
from typing import Optional, List

from fastapi import APIRouter, HTTPException, Request, Form, Body
from fastapi.responses import JSONResponse, HTMLResponse, RedirectResponse
from fastapi.templating import Jinja2Templates

from ..audit import _record_instance_audit
from ..db import _connect
from ..schemas import InstanceCreate, InstanceUpdate
from ..utils import (
    soa_exists,
    get_encounter_id,
    get_epoch_uid,
    get_schedule_timeline,
    get_scheduled_activity_instance,
)

router = APIRouter()
logger = logging.getLogger("soa_builder.web.routers.instances")
templates = Jinja2Templates(
    directory=os.path.join(os.path.dirname(__file__), "..", "templates")
)


def _nz(s: Optional[str]) -> Optional[str]:
    s = (s or "").strip()
    return s or None


# API endpoint to list timeline instances for SOA
@router.get("/soa/{soa_id}/instances", response_class=JSONResponse, response_model=None)
def list_instances(soa_id: int):
    if not soa_exists(soa_id):
        raise HTTPException(404, "SOA not found")

    conn = _connect()
    cur = conn.cursor()
    cur.execute(
        "SELECT id,instance_uid,name,label,description,default_condition_uid,epoch_uid,timeline_id,"
        "timeline_exit_id,order_index,encounter_uid,member_of_timeline FROM instances WHERE soa_id=? ORDER BY order_index,id",
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
            "timeline_id": r[7],
            "timeline_exit_id": r[8],
            "order_index": r[9],
            "encounter_uid": r[10],
            "member_of_timeline": r[11],
        }
        for r in cur.fetchall()
    ]
    conn.close()
    return rows


# UI code to list instances in an SOA
@router.get("/ui/soa/{soa_id}/instances", response_class=HTMLResponse)
def ui_list_instances(request: Request, soa_id: int):
    if not soa_exists(soa_id):
        raise HTTPException(404, "SOA not found")

    instances = list_instances(soa_id)
    encounter_options = get_encounter_id(soa_id)
    epoch_options = get_epoch_uid(soa_id)
    schedule_timelines_options = get_schedule_timeline(soa_id)
    instance_options = get_scheduled_activity_instance(soa_id)

    return templates.TemplateResponse(
        request,
        "instances.html",
        {
            "request": request,
            "soa_id": soa_id,
            "instances": instances,
            "encounter_options": encounter_options,
            "epoch_options": epoch_options,
            "schedule_timelines_options": schedule_timelines_options,
            "instance_options": instance_options,
        },
    )


# API endpoint for creating a timeline instance in an SOA
@router.post(
    "/soa/{soa_id}/instances",
    response_class=JSONResponse,
    status_code=201,
    response_model=None,
)
def create_instance(soa_id: int, payload: InstanceCreate):
    if not soa_exists(soa_id):
        raise HTTPException(404, "SOA not found")

    name = (payload.name or "").strip()
    if not name:
        raise HTTPException(400, "Instance name required")

    conn = _connect()
    cur = conn.cursor()
    cur.execute(
        "SELECT COALESCE(MAX(order_index),0) FROM instances WHERE soa_id=?",
        (soa_id,),
    )
    next_ord = (cur.fetchone() or [0])[0] + 1
    cur.execute(
        "SELECT instance_uid FROM instances WHERE soa_id=? AND instance_uid LIKE 'ScheduledActivityInstance_%'",
        (soa_id,),
    )
    existing_uids = [r[0] for r in cur.fetchall() if r[0]]
    used_nums = set()
    for uid in existing_uids:
        if uid.startswith("ScheduledActivityInstance_"):
            tail = uid[len("ScheduledActivityInstance_") :]
            if tail.isdigit():
                used_nums.add(int(tail))
            else:
                logger.warning(
                    "Invalid instance_uid format encountered (ignored): %s",
                    uid,
                )
    # Always pick max(existing) + 1, do not fill gaps
    next_n = (max(used_nums) if used_nums else 0) + 1
    new_uid = f"ScheduledActivityInstance_{next_n}"
    cur.execute(
        "INSERT INTO instances (soa_id,instance_uid,name,label,description,default_condition_uid,epoch_uid,"
        "timeline_id,timeline_exit_id,order_index,encounter_uid,member_of_timeline) VALUES (?,?,?,?,?,?,?,?,?,?,?,?)",
        (
            soa_id,
            new_uid,
            name,
            _nz(payload.label),
            _nz(payload.description),
            _nz(payload.default_condition_uid),
            _nz(payload.epoch_uid),
            _nz(payload.timeline_id),
            _nz(payload.timeline_exit_id),
            next_ord,
            _nz(payload.encounter_uid),
            _nz(payload.member_of_timeline),
        ),
    )
    instance_id = cur.lastrowid
    conn.commit()
    conn.close()
    after = {
        "id": instance_id,
        "instance_uid": new_uid,
        "name": name,
        "label": (payload.label or "").strip() or None,
        "description": (payload.description or "").strip() or None,
        "default_condition_uid": (payload.default_condition_uid or "").strip() or None,
        "epoch_uid": (payload.epoch_uid or "").strip() or None,
        "timeline_id": (payload.timeline_id or "").strip() or None,
        "timeline_exit_id": (payload.timeline_exit_id or "").strip() or None,
        "encounter_uid": (payload.encounter_uid or "").strip() or None,
        "member_of_timeline": (payload.member_of_timeline or "").strip() or None,
    }

    _record_instance_audit(soa_id, "create", instance_id, before=None, after=after)
    return after


# UI code to create new instance in an SOA
@router.post("/ui/soa/{soa_id}/instances/create")
def ui_create_instance(
    request: Request,
    soa_id: int,
    name: str = Form(...),
    label: Optional[str] = Form(None),
    description: Optional[str] = Form(None),
    default_condition_uid: Optional[str] = Form(None),
    epoch_uid: Optional[str] = Form(None),
    timeline_id: Optional[str] = Form(None),
    timeline_exit_id: Optional[str] = Form(None),
    encounter_uid: Optional[str] = Form(None),
    member_of_timeline: Optional[str] = Form(None),
):
    payload = InstanceCreate(
        name=name,
        label=label,
        description=description,
        default_condition_uid=default_condition_uid,
        epoch_uid=epoch_uid,
        timeline_id=timeline_id,
        timeline_exit_id=timeline_exit_id,
        encounter_uid=encounter_uid,
        member_of_timeline=member_of_timeline,
    )
    create_instance(soa_id, payload)
    return RedirectResponse(url=f"/ui/soa/{int(soa_id)}/instances", status_code=303)


# API endpoint to update a timeline instance in an SOA
@router.patch(
    "/soa/{soa_id}/instances/{instance_id}",
    response_class=JSONResponse,
    response_model=None,
)
def update_instance(soa_id: int, instance_id: int, payload: InstanceUpdate):
    if not soa_exists(soa_id):
        raise HTTPException(404, "SOA not found")

    conn = _connect()
    cur = conn.cursor()
    cur.execute(
        "SELECT id,instance_uid,name,label,description,default_condition_uid, epoch_uid,"
        "timeline_id,timeline_exit_id,order_index,encounter_uid,member_of_timeline from instances WHERE soa_id=? and id=?",
        (
            soa_id,
            instance_id,
        ),
    )
    row = cur.fetchone()
    if not row:
        conn.close()
        raise HTTPException(404, f"Instance id={int(instance_id)} not found")

    before = {
        "id": row[0],
        "instance_uid": row[1],
        "name": row[2],
        "label": row[3],
        "description": row[4],
        "default_condition_uid": row[5],
        "epoch_uid": row[6],
        "timeline_id": row[7],
        "timeline_exit_id": row[8],
        "order_index": row[9],
        "encounter_uid": row[10],
        "member_of_timeline": row[11],
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
    new_timeline_id = (
        payload.timeline_id
        if payload.timeline_id is not None
        else before["timeline_id"]
    )
    new_timeline_exit_id = (
        payload.timeline_exit_id
        if payload.timeline_exit_id is not None
        else before["timeline_exit_id"]
    )
    new_encounter_uid = (
        payload.encounter_uid
        if payload.encounter_uid is not None
        else before["encounter_uid"]
    )
    new_member_of_timeline = (
        payload.member_of_timeline
        if payload.member_of_timeline is not None
        else before["member_of_timeline"]
    )

    cur.execute(
        "UPDATE instances SET name=?, label=?, description=?, default_condition_uid=?, epoch_uid=?, "
        "timeline_id=?, timeline_exit_id=?, encounter_uid=?, member_of_timeline=? WHERE id=? and soa_id=?",
        (
            _nz(new_name),
            _nz(new_label),
            _nz(new_description),
            _nz(new_default_condition_uid),
            _nz(new_epoch_uid),
            _nz(new_timeline_id),
            _nz(new_timeline_exit_id),
            _nz(new_encounter_uid),
            _nz(new_member_of_timeline),
            instance_id,
            soa_id,
        ),
    )
    conn.commit()
    cur.execute(
        "SELECT id,instance_uid,name,label,description,default_condition_uid,epoch_uid,timeline_id,"
        "timeline_exit_id,order_index,encounter_uid,member_of_timeline FROM instances WHERE soa_id=? and id=?",
        (
            soa_id,
            instance_id,
        ),
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
        "timeline_id": r[7],
        "timeline_exit_id": r[8],
        "order_index": r[9],
        "encounter_uid": r[10],
        "member_of_timeline": r[11],
    }
    mutable = [
        "name",
        "label",
        "description",
        "default_condition_uid",
        "epoch_uid",
        "timeline_id",
        "timeline_exit_id",
        "encounter_uid",
        "member_of_timeline",
    ]
    update_fields = [
        f for f in mutable if (before.get(f) or None) != (after.get(f) or None)
    ]
    _record_instance_audit(
        soa_id,
        "update",
        instance_id,
        before=before,
        after={**after, "updated_fields": update_fields},
    )
    return {**after, "updated_fields": update_fields}


# UI code to update an instance in an SOA
@router.post("/ui/soa/{soa_id}/instances/{instance_id}/update")
def ui_update_instance(
    request: Request,
    soa_id: int,
    instance_id: int,
    name: Optional[str] = Form(None),
    label: Optional[str] = Form(None),
    description: Optional[str] = Form(None),
    default_condition_uid: Optional[str] = Form(None),
    epoch_uid: Optional[str] = Form(None),
    timeline_id: Optional[str] = Form(None),
    timeline_exit_id: Optional[str] = Form(None),
    encounter_uid: Optional[str] = Form(None),
    member_of_timeline: Optional[str] = Form(None),
):
    payload = InstanceUpdate(
        name=name,
        label=label,
        description=description,
        default_condition_uid=default_condition_uid,
        epoch_uid=epoch_uid,
        timeline_id=timeline_id,
        timeline_exit_id=timeline_exit_id,
        encounter_uid=encounter_uid,
        member_of_timeline=member_of_timeline,
    )
    update_instance(soa_id, instance_id, payload)
    return RedirectResponse(url=f"/ui/soa/{int(soa_id)}/instances", status_code=303)


# API endpoint to delete a timeline instance
@router.delete(
    "/soa/{soa_id}/instances/{instance_id}",
    response_class=JSONResponse,
    response_model=None,
)
def delete_instance(soa_id: int, instance_id: int):
    if not soa_exists(soa_id):
        raise HTTPException(404, "SOA not found")

    conn = _connect()
    cur = conn.cursor()
    cur.execute(
        "SELECT id,instance_uid,name,label,description FROM instances WHERE soa_id=? and id=?",
        (
            soa_id,
            instance_id,
        ),
    )
    row = cur.fetchone()
    if not row:
        conn.close()
        raise HTTPException(404, f"Instance id={int(instance_id)} not found")
    before = {
        "id": row[0],
        "instance_uid": row[1],
        "name": row[2],
        "label": row[3],
        "description": row[4],
    }
    cur.execute(
        "DELETE FROM instances WHERE id=? and soa_id=?",
        (
            instance_id,
            soa_id,
        ),
    )
    conn.commit()
    conn.close()
    _record_instance_audit(soa_id, "delete", instance_id, before, after=None)
    return {"deleted": True, "id": instance_id}


# UI code to delete timeline instance
@router.post("/ui/soa/{soa_id}/instances/{instance_id}/delete")
def ui_del_instance(request: Request, soa_id: int, instance_id: int):
    delete_instance(soa_id, instance_id)
    return RedirectResponse(url=f"/ui/soa/{int(soa_id)}/instances", status_code=303)


# API endpoint to reorder instances
@router.post("/soa/{soa_id}/instances/reorder", response_class=JSONResponse)
def reorder_instances_api(
    soa_id: int,
    order: List[int] = Body(..., embed=True),  # JSON body: {"order":[...]}
):
    if not soa_exists(soa_id):
        raise HTTPException(404, "SOA not found")
    if not order:
        raise HTTPException(400, "Order list required")

    conn = _connect()
    cur = conn.cursor()
    cur.execute(
        "SELECT id,name FROM instances WHERE soa_id=? ORDER BY order_index", (soa_id,)
    )
    rows = cur.fetchall()
    old_order = [r[0] for r in rows]  # IDs for API response
    id_to_name = {r[0]: r[1] for r in rows}
    old_order_names = [r[1] for r in rows]  # Names for audit

    cur.execute("SELECT id,name FROM instances WHERE soa_id=?", (soa_id,))
    existing = {r[0] for r in cur.fetchall()}
    if set(order) - existing:
        conn.close()
        raise HTTPException(400, "Order contains invalid instance id")

    for idx, instance_id in enumerate(order, start=1):
        cur.execute("UPDATE instances SET order_index=? WHERE id=?", (idx, instance_id))
    conn.commit()
    conn.close()

    new_order_names = [id_to_name.get(iid, str(iid)) for iid in order]

    _record_instance_audit(
        soa_id,
        "reorder",
        instance_id=None,
        before={"old_order": old_order_names},
        after={"new_order": new_order_names},
    )
    return JSONResponse({"ok": True, "old_order": old_order, "new_order": order})
