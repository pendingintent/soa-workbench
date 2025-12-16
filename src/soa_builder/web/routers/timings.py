import logging
from typing import Optional

from fastapi import APIRouter, HTTPException
from fastapi.responses import JSONResponse

from ..audit import _record_timing_audit
from ..db import _connect
from ..schemas import TimingCreate, TimingUpdate
from ..utils import soa_exists

router = APIRouter(prefix="/soa/{soa_id}")
logger = logging.getLogger("soa_builder.web.routers.timings")


def _nz(s: Optional[str]) -> Optional[str]:
    s = (s or "").strip()
    return s or None


@router.get("/timings", response_class=JSONResponse, response_model=None)
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


@router.post(
    "/timings", response_class=JSONResponse, status_code=201, response_model=None
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
    next_n = 1
    while next_n in used_nums:
        next_n += 1
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


@router.patch("/timings/{timing_id}", response_class=JSONResponse, response_model=None)
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
        raise HTTPException(404, f"Timing id={timing_id} not found")

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


@router.delete("/timings/{timing_id}", response_class=JSONResponse, response_model=None)
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
