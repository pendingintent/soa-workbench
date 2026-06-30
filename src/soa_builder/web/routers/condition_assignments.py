import logging
import os
from typing import Optional

from fastapi import APIRouter, HTTPException, Request, Form
from fastapi.responses import JSONResponse, HTMLResponse, RedirectResponse
from fastapi.templating import Jinja2Templates

from ..audit import _record_condition_assignment_audit
from ..db import _connect
from ..schemas import ConditionAssignmentCreate, ConditionAssignmentUpdate
from ..utils import (
    soa_exists,
    _nz as _nz,
    get_scheduled_activity_instance,
    redirect_url_from_referer as _redirect_url,
)

router = APIRouter()
logger = logging.getLogger("soa_builder.web.routers.condition_assignments")
templates = Jinja2Templates(
    directory=os.path.join(os.path.dirname(__file__), "..", "templates")
)


# API endpoint to list conditions
@router.get(
    "/soa/{soa_id}/condition_assignments",
    response_class=JSONResponse,
    response_model=None,
)
def list_condition_assignments(soa_id: int):
    if not soa_exists(soa_id):
        raise HTTPException(404, "SOA not found")

    conn = _connect()
    cur = conn.cursor()
    cur.execute(
        """
        SELECT id, condition_assignment_uid, name, label, description, condition,
        decision_instance_uid, condition_target_uid, order_index FROM condition_assignment
        WHERE soa_id=? ORDER BY name, id
        """,
        (soa_id,),
    )
    rows = [
        {
            "id": r[0],
            "condition_assignment_uid": r[1],
            "name": r[2],
            "label": r[3],
            "description": r[4],
            "condition": r[5],
            "decision_instance_uid": r[6],
            "condition_target_uid": r[7],
            "order_index": r[8],
        }
        for r in cur.fetchall()
    ]
    conn.close()
    return rows


# UI code to list conditions
@router.get("/ui/soa/{soa_id}/condition_assignments", response_class=HTMLResponse)
def ui_list_condition_assignments(request: Request, soa_id: int):
    if not soa_exists(soa_id):
        raise HTTPException(404, "SOA not found")

    conditions = list_condition_assignments(soa_id)
    instance_options = get_scheduled_activity_instance(soa_id)

    # Study metadata
    conn = _connect()
    cur = conn.cursor()
    cur.execute(
        "SELECT study_id, study_label, study_description, name, created_at FROM soa WHERE id=?",
        (soa_id,),
    )
    meta_row = cur.fetchone()
    conn.close()
    study_id, study_label, study_description, study_name, study_created_at = meta_row
    study_meta = {
        "study_id": study_id,
        "study_label": study_label,
        "study_description": study_description,
        "study_name": study_name,
        "study_created_at": study_created_at,
    }

    return templates.TemplateResponse(
        request,
        "condition_assignments.html",
        {
            "request": request,
            "soa_id": soa_id,
            "conditions": conditions,
            "instance_options": instance_options,
            **study_meta,
        },
    )


# API endpoint to create a new condition
@router.post(
    "/soa/{soa_id}/condition_assignments",
    response_class=JSONResponse,
    status_code=201,
    response_model=None,
)
def create_condition_assignment(soa_id: int, payload: ConditionAssignmentCreate):
    if not soa_exists(soa_id):
        raise HTTPException(404, "SOA not found")

    name = (payload.name or "").strip()
    if not name:
        raise HTTPException(400, "Condition name required")

    # Calculate next order_index and condition_assignment_uid
    conn = _connect()
    cur = conn.cursor()
    cur.execute(
        "SELECT COALESCE(MAX(order_index),0) FROM condition_assignment WHERE soa_id=?",
        (soa_id,),
    )
    next_ord = (cur.fetchone() or [0])[0] + 1
    cur.execute(
        "SELECT condition_assignment_uid FROM condition_assignment WHERE soa_id=? and condition_assignment_uid LIKE 'Condition_%'",
        (soa_id,),
    )
    existing_uids = [r[0] for r in cur.fetchall() if r[0]]
    used_nums = set()
    for uid in existing_uids:
        if uid.startswith("Condition_"):
            tail = uid[len("Condition_") :]
            if tail.isdigit():
                used_nums.add(int(tail))
            else:
                logger.warning(
                    "Invalid condition_assignment_uid format encountered (ignored): %s",
                    uid,
                )
    # Always pick max(existing) + 1, do not fill gaps
    next_n = (max(used_nums) if used_nums else 0) + 1
    new_uid = f"Condition_{next_n}"
    # Insert values for new condition into the condition_assignment table
    cur.execute(
        """
        INSERT INTO condition_assignment (soa_id,condition_assignment_uid,name,label,description,condition,
        decision_instance_uid,condition_target_uid,order_index) VALUES (?,?,?,?,?,?,?,?,?)
        """,
        (
            soa_id,
            new_uid,
            name,
            _nz(payload.label),
            _nz(payload.description),
            _nz(payload.condition),
            _nz(payload.decision_instance_uid),
            _nz(payload.condition_target_uid),
            next_ord,
        ),
    )
    condition_id = cur.lastrowid
    conn.commit()
    conn.close()
    after = {
        "id": condition_id,
        "condition_assignment_uid": new_uid,
        "name": name,
        "label": (payload.label or "").strip() or None,
        "description": (payload.description or "").strip() or None,
        "condition": (payload.condition or "").strip() or None,
        "decision_instance_uid": (payload.decision_instance_uid or "").strip() or None,
        "condition_target_uid": (payload.condition_target_uid or "").strip() or None,
    }
    _record_condition_assignment_audit(
        soa_id, "create", condition_id, before=None, after=after
    )
    return after


# UI endpoint for creating new condition
@router.post("/ui/soa/{soa_id}/condition_assignments/create")
def ui_create_condition_assignment(
    request: Request,
    soa_id: int,
    name: str = Form(...),
    label: Optional[str] = Form(None),
    description: Optional[str] = Form(None),
    condition: Optional[str] = Form(None),
    decision_instance_uid: Optional[str] = Form(None),
    condition_target_uid: Optional[str] = Form(None),
):
    payload = ConditionAssignmentCreate(
        name=name,
        label=label,
        description=description,
        condition=condition,
        decision_instance_uid=decision_instance_uid,
        condition_target_uid=condition_target_uid,
    )
    create_condition_assignment(soa_id, payload)
    return RedirectResponse(
        url=_redirect_url(request, f"/ui/soa/{int(soa_id)}/condition_assignments"),
        status_code=303,
    )


# API endpoint to update a condition
@router.patch(
    "/soa/{soa_id}/condition_assignments/{condition_id}",
    response_class=JSONResponse,
    response_model=None,
)
def update_condition_assignment(
    soa_id: int, condition_id: int, payload: ConditionAssignmentUpdate
):
    if not soa_exists(soa_id):
        raise HTTPException(404, "SOA not found")

    conn = _connect()
    cur = conn.cursor()
    cur.execute(
        """
        SELECT id,condition_assignment_uid,name,label,description,condition,decision_instance_uid,
        condition_target_uid FROM condition_assignment WHERE soa_id=? AND id=?
        """,
        (
            soa_id,
            condition_id,
        ),
    )
    row = cur.fetchone()
    if not row:
        raise HTTPException(404, f"Condition id={int(condition_id)} not found")

    before = {
        "id": row[0],
        "condition_assignment_uid": row[1],
        "name": row[2],
        "label": row[3],
        "description": row[4],
        "condition": row[5],
        "decision_instance_uid": row[6],
        "condition_target_uid": row[7],
    }

    new_name = (payload.name if payload.name is not None else before["name"]) or ""
    new_label = payload.label if payload.label is not None else before["label"]
    new_description = (
        payload.description
        if payload.description is not None
        else before["description"]
    )
    new_condition = (
        payload.condition if payload.condition is not None else before["condition"]
    )
    new_decision_instance_uid = (
        payload.decision_instance_uid
        if payload.decision_instance_uid is not None
        else before["decision_instance_uid"]
    )
    new_condition_target_uid = (
        payload.condition_target_uid
        if payload.condition_target_uid is not None
        else before["condition_target_uid"]
    )

    cur.execute(
        """
        UPDATE condition_assignment SET name=?,label=?,description=?,condition=?,decision_instance_uid=?,condition_target_uid=?
        WHERE id=? AND soa_id=?
        """,
        (
            _nz(new_name),
            _nz(new_label),
            _nz(new_description),
            _nz(new_condition),
            _nz(new_decision_instance_uid),
            _nz(new_condition_target_uid),
            condition_id,
            soa_id,
        ),
    )
    conn.commit()
    cur.execute(
        """
        SELECT id,condition_assignment_uid,name,label,description,condition,decision_instance_uid,
        condition_target_uid FROM condition_assignment WHERE soa_id=? AND id=?
        """,
        (
            soa_id,
            condition_id,
        ),
    )
    r = cur.fetchone()
    conn.close()
    after = {
        "id": r[0],
        "condition_assignment_uid": r[1],
        "name": r[2],
        "label": r[3],
        "description": r[4],
        "condition": r[5],
        "decision_instance_uid": r[6],
        "condition_target_uid": r[7],
    }
    mutable = [
        "name",
        "label",
        "description",
        "condition",
        "decision_instance_uid",
        "condition_target_uid",
    ]
    update_fields = [
        f for f in mutable if (before.get(f) or None) != (after.get(f) or None)
    ]
    _record_condition_assignment_audit(
        soa_id,
        "update",
        condition_id,
        before=before,
        after={**after, "updated_fields": update_fields},
    )
    return {**after, "updated_fields": update_fields}


# UI endpoint for updating a condition
@router.post("/ui/soa/{soa_id}/condition_assignments/{condition_id}/update")
def ui_update_condition_assignment(
    request: Request,
    soa_id: int,
    condition_id: int,
    name: Optional[str] = Form(None),
    label: Optional[str] = Form(None),
    description: Optional[str] = Form(None),
    condition: Optional[str] = Form(None),
    decision_instance_uid: Optional[str] = Form(None),
    condition_target_uid: Optional[str] = Form(None),
):
    payload = ConditionAssignmentUpdate(
        name=name,
        label=label,
        description=description,
        condition=condition,
        decision_instance_uid=decision_instance_uid,
        condition_target_uid=condition_target_uid,
    )
    update_condition_assignment(soa_id, condition_id, payload)
    return RedirectResponse(
        url=_redirect_url(request, f"/ui/soa/{int(soa_id)}/condition_assignments"),
        status_code=303,
    )


# API endpoint for deleting a condition
@router.delete(
    "/soa/{soa_id}/condition_assignments/{condition_id}",
    response_class=JSONResponse,
    response_model=None,
)
def delete_condition_assignment(soa_id: int, condition_id: int):
    if not soa_exists(soa_id):
        raise HTTPException(404, "SOA not found")

    conn = _connect()
    cur = conn.cursor()
    cur.execute(
        """
        SELECT id,condition_assignment_uid,name,label,description,condition,decision_instance_uid,condition_target_uid
        FROM condition_assignment WHERE soa_id=? and id=?
        """,
        (
            soa_id,
            condition_id,
        ),
    )
    row = cur.fetchone()
    if not row:
        raise HTTPException(404, f"Condition id={int(condition_id)} not found")

    before = {
        "id": row[0],
        "condition_assignment_uid": row[1],
        "name": row[2],
        "label": row[3],
        "description": row[4],
        "condition": row[5],
        "decision_intance_uid": row[6],
        "condition_target_uid": row[7],
    }
    cur.execute(
        "DELETE FROM condition_assignment WHERE soa_id=? AND id=?",
        (
            soa_id,
            condition_id,
        ),
    )
    conn.commit()
    conn.close()
    _record_condition_assignment_audit(
        soa_id, "delete", condition_id, before, after=None
    )
    return {"deleted": True, "id": condition_id}


# UI endpoint to delete a condition
@router.post("/ui/soa/{soa_id}/condition_assignments/{condition_id}/delete")
def ui_delete_condition_assignment(request: Request, soa_id: int, condition_id: int):
    delete_condition_assignment(soa_id, condition_id)
    return RedirectResponse(
        url=_redirect_url(
            request,
            f"/ui/soa/{int(soa_id)}/condition_assignments",
        ),
        status_code=303,
    )
