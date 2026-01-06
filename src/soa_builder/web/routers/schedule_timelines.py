import logging
import os
from typing import Optional

from fastapi import APIRouter, HTTPException, Request, Form
from fastapi.responses import JSONResponse, HTMLResponse, RedirectResponse
from fastapi.templating import Jinja2Templates

from ..audit import _record_schedule_timeline_audit
from ..db import _connect
from ..schemas import ScheduleTimelineUpdate, ScheduleTimelineCreate
from ..utils import soa_exists, get_scheduled_activity_instance


router = APIRouter()
logger = logging.getLogger("soa_builder.web.routers.schedule_timelines")
templates = Jinja2Templates(
    directory=os.path.join(os.path.dirname(__file__), "..", "templates")
)


def _nz(s: Optional[str]) -> Optional[str]:
    s = (s or "").strip()
    return s or None


def _to_bool(v: Optional[str]) -> bool:
    if v is None:
        return False
    return v.strip().lower() in {"1", "true", "on", "yes"}


def _assert_main_unique(soa_id: int, exclude_id: Optional[int] = None) -> None:
    """Ensure only one schedule timeline is marked as main for an SOA"""
    conn = _connect()
    cur = conn.cursor()
    if exclude_id is None:
        cur.execute(
            "SELECT id FROM schedule_timelines WHERE soa_id=? AND main_timeline=1 LIMIT 1",
            (soa_id,),
        )
    else:
        cur.execute(
            "SELECT id FROM schedule_timelines WHERE soa_id=? AND main_timeline=1 AND id!=? LIMIT 1",
            (soa_id, exclude_id),
        )
    row = cur.fetchone()
    conn.close()
    if row:
        raise HTTPException(400, "Only one main_timeline can exist in a SOA")


# API endpoint to list schedule timelines for an SOA
def list_schedule_timelines(soa_id: int):
    if not soa_exists(soa_id):
        raise HTTPException(404, "SOA not found")

    conn = _connect()
    cur = conn.cursor()
    cur.execute(
        """SELECT id,schedule_timeline_uid,name,label,description,main_timeline,entry_condition,
        entry_id,exit_id,order_index FROM schedule_timelines WHERE soa_id=? ORDER BY order_index,id""",
        (soa_id,),
    )
    rows = [
        {
            "id": r[0],
            "schedule_timeline_uid": r[1],
            "name": r[2],
            "label": r[3],
            "description": r[4],
            "main_timeline": bool(r[5]) if r[5] is not None else False,
            "entry_condition": r[6],
            "entry_id": r[7],
            "exit_id": r[8],
            "order_index": r[9],
        }
        for r in cur.fetchall()
    ]
    conn.close()
    return rows


# UI code to list schedule timelines in an SOA
@router.get("/ui/soa/{soa_id}/schedule_timelines", response_class=HTMLResponse)
def ui_list_schedule_timelines(request: Request, soa_id: int):
    if not soa_exists(soa_id):
        raise HTTPException(404, "SOA not found")

    schedule_timelines = list_schedule_timelines(soa_id)
    instance_options = get_scheduled_activity_instance(soa_id)

    return templates.TemplateResponse(
        request,
        "schedule_timelines.html",
        {
            "request": request,
            "soa_id": soa_id,
            "schedule_timelines": schedule_timelines,
            "instance_options": instance_options,
        },
    )


# API endpoint for creating a schedule timeline in an SOA
@router.post(
    "/soa/{soa_id}/schedule_timelines",
    response_class=JSONResponse,
    status_code=201,
    response_model=None,
)
def create_schedule_timeline(soa_id: int, payload: ScheduleTimelineCreate):
    if not soa_exists(soa_id):
        raise HTTPException(404, "SOA not found")

    name = (payload.name).strip()
    if not name:
        raise HTTPException(400, "Schedule Timeline name is required")

    if payload.main_timeline:
        _assert_main_unique(soa_id)

    conn = _connect()
    cur = conn.cursor()
    cur.execute(
        "SELECT COALESCE(MAX(order_index),0) FROM schedule_timelines WHERE soa_id=?",
        (soa_id,),
    )
    next_ord = (cur.fetchone() or [0])[0] + 1
    cur.execute(
        "SELECT schedule_timeline_uid FROM schedule_timelines WHERE soa_id=? AND schedule_timeline_uid LIKE 'ScheduleTimeline_%'",
        (soa_id,),
    )
    existing_uids = [r[0] for r in cur.fetchall() if r[0]]
    used_nums = set()
    for uid in existing_uids:
        if uid.startswith("ScheduleTimeline_"):
            tail = uid[len("ScheduleTimeline_") :]
            if tail.isdigit():
                used_nums.add(int(tail))
            else:
                logger.warning(
                    "Invalid schedule_timeline_uid format encountered (ignored): %s",
                    uid,
                )

    # Always pick max(existing) + 1; do not fill gaps
    next_n = (max(used_nums) if used_nums else 0) + 1
    new_uid = f"ScheduleTimeline_{next_n}"
    cur.execute(
        """INSERT INTO schedule_timelines (soa_id,schedule_timeline_uid,name,label,description,main_timeline,
        entry_condition,entry_id,exit_id,order_index) VALUES (?,?,?,?,?,?,?,?,?,?)""",
        (
            soa_id,
            new_uid,
            name,
            _nz(payload.label),
            _nz(payload.description),
            1 if payload.main_timeline else 0,
            _nz(payload.entry_condition),
            _nz(payload.entry_id),
            _nz(payload.exit_id),
            next_ord,
        ),
    )
    schedule_timeline_id = cur.lastrowid
    conn.commit()
    conn.close()
    after = {
        "id": schedule_timeline_id,
        "schedule_timeline_uid": new_uid,
        "name": name,
        "label": (payload.label or "").strip() or None,
        "description": (payload.description or "").strip() or None,
        "main_timeline": bool(payload.main_timeline),
        "entry_condition": (payload.entry_condition or "").strip() or None,
        "entry_id": (payload.entry_id or "").strip() or None,
        "exit_id": (payload.exit_id or "").strip() or None,
    }
    _record_schedule_timeline_audit(
        soa_id, "create", schedule_timeline_id, before=None, after=after
    )
    return after


# UI code to create a schedule timeline for an SOA
@router.post("/ui/soa/{soa_id}/schedule_timelines/create")
def ui_create_schedule_timeline(
    request: Request,
    soa_id: int,
    name: str = Form(...),
    label: Optional[str] = Form(None),
    description: Optional[str] = Form(None),
    main_timeline: Optional[str] = Form(None),
    entry_condition: Optional[str] = Form(None),
    entry_id: Optional[str] = Form(None),
    exit_id: Optional[str] = Form(None),
):
    if not soa_exists(soa_id):
        raise HTTPException(404, "SOA not found")

    payload = ScheduleTimelineCreate(
        name=name,
        label=label,
        description=description,
        main_timeline=_to_bool(main_timeline),
        entry_condition=entry_condition,
        entry_id=entry_id,
        exit_id=exit_id,
    )
    create_schedule_timeline(soa_id, payload)
    return RedirectResponse(
        url=f"/ui/soa/{int(soa_id)}/schedule_timelines", status_code=303
    )


# API endpoint to update a schedule timeline for an SOA
@router.patch(
    "/soa/{soa_id}/schedule_timelines/{schedule_timeline_id}",
    response_class=JSONResponse,
    response_model=None,
)
def update_schedule_timeline(
    soa_id: int, schedule_timeline_id: int, payload: ScheduleTimelineUpdate
):
    if not soa_exists(soa_id):
        raise HTTPException(404, "SOA not found")

    conn = _connect()
    cur = conn.cursor()
    cur.execute(
        """SELECT id,schedule_timeline_uid,name,label,description,main_timeline,entry_condition,
        entry_id,exit_id,order_index FROM schedule_timelines WHERE soa_id=? AND id=?""",
        (
            soa_id,
            schedule_timeline_id,
        ),
    )
    row = cur.fetchone()
    if not row:
        conn.close()
        raise HTTPException(404, f"Schedule Timeline={schedule_timeline_id} not found")

    before = {
        "id": row[0],
        "schedule_timeline_uid": row[1],
        "name": row[2],
        "label": row[3],
        "description": row[4],
        "main_timeline": bool(row[5]),
        "entry_condition": row[6],
        "entry_id": row[7],
        "exit_id": row[8],
        "order_index": row[9],
    }
    new_name = payload.name if payload.name is not None else before["name"]
    new_label = payload.label if payload.label is not None else before["label"]
    new_description = (
        payload.description
        if payload.description is not None
        else before["description"]
    )
    new_main_timeline = (
        payload.main_timeline
        if payload.main_timeline is not None
        else before["main_timeline"]
    )

    # Enforce uniqueness for main timeline (exclude the current row)
    if bool(new_main_timeline):
        _assert_main_unique(soa_id, exclude_id=schedule_timeline_id)

    new_entry_condition = (
        payload.entry_condition
        if payload.entry_condition is not None
        else before["entry_condition"]
    )
    if payload.entry_id is not None:
        new_entry_id = _nz(payload.entry_id)
    else:
        new_entry_id = before["entry_id"]

    new_exit_id = payload.exit_id if payload.exit_id is not None else before["exit_id"]

    cur.execute(
        """
        UPDATE schedule_timelines SET name=?, label=?, description=?, main_timeline=?, entry_condition=?,
        entry_id=?, exit_id=? WHERE id=? AND soa_id=?
        """,
        (
            _nz(new_name),
            _nz(new_label),
            _nz(new_description),
            1 if new_main_timeline else 0,
            _nz(new_entry_condition),
            new_entry_id,
            new_exit_id,
            schedule_timeline_id,
            soa_id,
        ),
    )
    conn.commit()
    cur.execute(
        """SELECT id,schedule_timeline_uid,name,label,description,main_timeline,entry_condition,
        entry_id,exit_id,order_index FROM schedule_timelines WHERE soa_id=? AND id=?""",
        (
            soa_id,
            schedule_timeline_id,
        ),
    )
    r = cur.fetchone()
    conn.close()
    after = {
        "id": r[0],
        "schedule_timeline_uid": r[1],
        "name": r[2],
        "label": r[3],
        "description": r[4],
        "main_timeline": bool(r[5]),
        "entry_condition": r[6],
        "entry_id": r[7],
        "exit_id": r[8],
    }
    mutable = [
        "name",
        "label",
        "description",
        "main_timeline",
        "entry_condition",
        "entry_id",
        "exit_id",
    ]
    updated_fields = [
        f for f in mutable if (before.get(f) or None) != (after.get(f) or None)
    ]
    _record_schedule_timeline_audit(
        soa_id,
        "update",
        schedule_timeline_id,
        before=before,
        after={**after, "updated_fields": updated_fields},
    )
    return {**after, "updated_fields": updated_fields}


# UI code to update a Schedule Timeline for an SOA
@router.post("/ui/soa/{soa_id}/schedule_timelines/{schedule_timeline_id}/update")
def ui_update_schedule_timeline(
    request: Request,
    soa_id: int,
    schedule_timeline_id: int,
    name: Optional[str] = Form(None),
    label: Optional[str] = Form(None),
    description: Optional[str] = Form(None),
    main_timeline: Optional[str] = Form(None),
    entry_condition: Optional[str] = Form(None),
    entry_id: Optional[str] = Form(None),
    exit_id: Optional[str] = Form(None),
):
    payload = ScheduleTimelineUpdate(
        name=name,
        label=label,
        description=description,
        main_timeline=_to_bool(main_timeline),
        entry_condition=entry_condition,
        entry_id=_nz(entry_id),
        exit_id=_nz(exit_id),
    )
    update_schedule_timeline(soa_id, schedule_timeline_id, payload)
    return RedirectResponse(
        url=f"/ui/soa/{int(soa_id)}/schedule_timelines", status_code=303
    )


# API endpoint to delete a Schedule Timeline for an SOA
@router.delete(
    "/soa/{soa_id}/schedule_timelines/{schedule_timeline_id}",
    response_class=JSONResponse,
    response_model=None,
)
def delete_schedule_timeline(soa_id: int, schedule_timeline_id: int):
    if not soa_exists(soa_id):
        raise HTTPException(404, "SOA not found")

    conn = _connect()
    cur = conn.cursor()
    cur.execute(
        """SELECT id, schedule_timeline_uid,name,label,description,main_timeline,
        entry_condition,entry_id,exit_id FROM schedule_timelines WHERE id=? AND soa_id=?""",
        (
            schedule_timeline_id,
            soa_id,
        ),
    )
    row = cur.fetchone()
    if not row:
        conn.close()
        raise HTTPException(
            404, f"Schedule Timeline id={int(schedule_timeline_id)} not found"
        )

    before = {
        "id": row[0],
        "schedule_timeline_uid": row[1],
        "name": row[2],
        "label": row[3],
        "description": row[4],
        "main_timeline": bool(row[5]),
        "entry_condition": row[6],
        "entry_id": row[7],
        "exit_id": row[8],
    }
    cur.execute(
        "DELETE FROM schedule_timelines WHERE id=? AND soa_id=?",
        (
            schedule_timeline_id,
            soa_id,
        ),
    )
    conn.commit()
    conn.close()
    _record_schedule_timeline_audit(
        soa_id, "delete", schedule_timeline_id, before=before, after=None
    )
    return {"deleted": True, "id": schedule_timeline_id}


# UI code to delete a Schedule Timeline for an SOA
@router.post("/ui/soa/{soa_id}/schedule_timelines/{schedule_timeline_id}/delete")
def ui_delete_schedule_timeline(
    request: Request, soa_id: int, schedule_timeline_id: int
):
    delete_schedule_timeline(soa_id, schedule_timeline_id)
    return RedirectResponse(
        url=f"/ui/soa/{int(soa_id)}/schedule_timelines", status_code=303
    )
