import logging
import os
from typing import Optional

from fastapi import APIRouter, HTTPException, Request, Form
from fastapi.responses import JSONResponse, HTMLResponse, RedirectResponse
from fastapi.templating import Jinja2Templates

from ..audit import _record_instance_audit
from ..db import _connect
from ..schemas import InstanceCreate, InstanceUpdate
from ..utils import soa_exists

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
        "timeline_exit_id,order_index,encounter_uid FROM instances WHERE soa_id=? ORDER BY order_index,id",
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
    return templates.TemplateResponse(
        "instances.html",
        {
            "request": request,
            "soa_id": soa_id,
            "instances": instances,
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
    next_n = 1
    while next_n in used_nums:
        next_n += 1
    new_uid = f"ScheduledActivityInstance_{next_n}"
    cur.execute(
        "INSERT INTO instances (soa_id,instance_uid,name,label,description,default_condition_uid,epoch_uid,"
        "timeline_id,timeline_exit_id,order_index,encounter_uid) VALUES (?,?,?,?,?,?,?,?,?,?,?)",
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
        ),
    )
    instance_id = cur.lastrowid
    conn.commit()
    conn.close()
    row = {
        "id": instance_id,
        "instance_uid": new_uid,
        "name": name,
        "label": (payload.label or "").strip() or None,
        "description": (payload.description or "").strip() or None,
    }

    _record_instance_audit(soa_id, "create", instance_id, before=None, after=row)
    return row


# UI code to create new intsance in an SOA
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
        "timeline_id,timeline_exit_id,order_index,encounter_uid from instances WHERE soa_id=? and id=?",
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

    cur.execute(
        "UPDATE instances SET name=?, label=?, description=?, default_condition_uid=?, epoch_uid=?, "
        "timeline_id=?, timeline_exit_id=?, encounter_uid=? WHERE id=? and soa_id=?",
        (
            _nz(new_name),
            _nz(new_label),
            _nz(new_description),
            _nz(new_default_condition_uid),
            _nz(new_epoch_uid),
            _nz(new_timeline_id),
            _nz(new_timeline_exit_id),
            _nz(new_encounter_uid),
            instance_id,
            soa_id,
        ),
    )
    conn.commit()
    cur.execute(
        "SELECT id,instance_uid,name,label,description,default_condition_uid,epoch_uid,timeline_id,"
        "timeline_exit_id,order_index,encounter_uid FROM instances WHERE soa_id=? and id=?",
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
