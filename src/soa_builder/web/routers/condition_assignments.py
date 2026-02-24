import logging
from typing import Optional

from fastapi import APIRouter, HTTPException, Query
from fastapi.responses import JSONResponse

from ..audit import _record_condition_assignment_audit
from ..db import _connect
from ..schemas import ConditionAssignmentCreate, ConditionAssignmentUpdate
from ..utils import soa_exists

router = APIRouter()
logger = logging.getLogger("soa_builder.web.routers.condition_assignments")


def _nz(s: Optional[str]) -> Optional[str]:
    s = (s or "").strip()
    return s or None


@router.get(
    "/soa/{soa_id}/condition_assignments",
    response_class=JSONResponse,
    response_model=None,
)
def list_condition_assignments(
    soa_id: int,
    decision_instance_uid: Optional[str] = Query(default=None),
):
    if not soa_exists(soa_id):
        raise HTTPException(404, "SOA not found")

    conn = _connect()
    cur = conn.cursor()
    if decision_instance_uid:
        cur.execute(
            "SELECT id, condition_assignment_uid, decision_instance_uid, name, label, description, condition, "
            "condition_target_uid, order_index FROM condition_assignment "
            "WHERE soa_id=? AND decision_instance_uid=? ORDER BY order_index, id",
            (soa_id, decision_instance_uid),
        )
    else:
        cur.execute(
            "SELECT id, condition_assignment_uid, decision_instance_uid, name, label, description, condition, "
            "condition_target_uid, order_index FROM condition_assignment "
            "WHERE soa_id=? ORDER BY order_index, id",
            (soa_id,),
        )
    rows = [
        {
            "id": r[0],
            "condition_assignment_uid": r[1],
            "decision_instance_uid": r[2],
            "name": r[3],
            "label": r[4],
            "description": r[5],
            "condition": r[6],
            "condition_target_uid": r[7],
            "order_index": r[8],
        }
        for r in cur.fetchall()
    ]
    conn.close()
    return rows


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
    decision_instance_uid = (payload.decision_instance_uid or "").strip()
    condition = (payload.condition or "").strip()
    condition_target_uid = (payload.condition_target_uid or "").strip()

    if not name:
        raise HTTPException(400, "name required")
    if not decision_instance_uid:
        raise HTTPException(400, "decision_instance_uid required")
    if not condition:
        raise HTTPException(400, "condition required")
    if not condition_target_uid:
        raise HTTPException(400, "condition_target_uid required")

    conn = _connect()
    cur = conn.cursor()
    # Verify the decision instance exists for this SOA
    cur.execute(
        "SELECT id FROM decision_instances WHERE soa_id=? AND instance_uid=?",
        (soa_id, decision_instance_uid),
    )
    if not cur.fetchone():
        conn.close()
        raise HTTPException(
            404, f"Decision instance '{decision_instance_uid}' not found"
        )

    cur.execute(
        "SELECT COALESCE(MAX(order_index), 0) FROM condition_assignment WHERE soa_id=?",
        (soa_id,),
    )
    next_ord = (cur.fetchone() or [0])[0] + 1

    cur.execute(
        "SELECT condition_assignment_uid FROM condition_assignment WHERE soa_id=? "
        "AND condition_assignment_uid LIKE 'ConditionAssignment_%'",
        (soa_id,),
    )
    existing_uids = [r[0] for r in cur.fetchall() if r[0]]
    used_nums = set()
    for uid in existing_uids:
        if uid.startswith("ConditionAssignment_"):
            tail = uid[len("ConditionAssignment_") :]
            if tail.isdigit():
                used_nums.add(int(tail))
            else:
                logger.warning(
                    "Invalid condition assignment uid format (ignored): %s", uid
                )
    next_n = (max(used_nums) if used_nums else 0) + 1
    new_uid = f"ConditionAssignment_{next_n}"

    cur.execute(
        "INSERT INTO condition_assignment (soa_id, condition_assignment_uid, "
        "decision_instance_uid, name, label, description, condition, condition_target_uid, order_index) "
        "VALUES (?,?,?,?,?,?,?,?,?)",
        (
            soa_id,
            new_uid,
            decision_instance_uid,
            name,
            _nz(payload.label),
            _nz(payload.description),
            condition,
            condition_target_uid,
            next_ord,
        ),
    )
    ca_id = cur.lastrowid
    conn.commit()
    conn.close()

    after = {
        "id": ca_id,
        "condition_assignment_uid": new_uid,
        "decision_instance_uid": decision_instance_uid,
        "name": name,
        "label": _nz(payload.label),
        "description": _nz(payload.description),
        "condition": condition,
        "condition_target_uid": condition_target_uid,
        "order_index": next_ord,
    }
    _record_condition_assignment_audit(
        soa_id, "create", ca_id, before=None, after=after
    )
    return after


@router.patch(
    "/soa/{soa_id}/condition_assignments/{ca_id}",
    response_class=JSONResponse,
    response_model=None,
)
def update_condition_assignment(
    soa_id: int, ca_id: int, payload: ConditionAssignmentUpdate
):
    if not soa_exists(soa_id):
        raise HTTPException(404, "SOA not found")

    conn = _connect()
    cur = conn.cursor()
    cur.execute(
        "SELECT id, condition_assignment_uid, decision_instance_uid, name, label, description, condition, "
        "condition_target_uid, order_index FROM condition_assignment "
        "WHERE soa_id=? AND id=?",
        (soa_id, ca_id),
    )
    row = cur.fetchone()
    if not row:
        conn.close()
        raise HTTPException(404, f"Condition assignment id={int(ca_id)} not found")

    before = {
        "id": row[0],
        "condition_assignment_uid": row[1],
        "decision_instance_uid": row[2],
        "name": row[3],
        "label": row[4],
        "description": row[5],
        "condition": row[6],
        "condition_target_uid": row[7],
        "order_index": row[8],
    }

    new_name = payload.name if payload.name is not None else before["name"]
    new_label = payload.label if payload.label is not None else before["label"]
    new_description = (
        payload.description
        if payload.description is not None
        else before["description"]
    )
    new_decision_instance_uid = (
        payload.decision_instance_uid
        if payload.decision_instance_uid is not None
        else before["decision_instance_uid"]
    )
    new_condition = (
        payload.condition if payload.condition is not None else before["condition"]
    )
    new_target = (
        payload.condition_target_uid
        if payload.condition_target_uid is not None
        else before["condition_target_uid"]
    )

    cur.execute(
        "UPDATE condition_assignment SET name=?, label=?, description=?, "
        "decision_instance_uid=?, condition=?, condition_target_uid=? WHERE id=? AND soa_id=?",
        (
            (new_name or "").strip(),
            _nz(new_label),
            _nz(new_description),
            _nz(new_decision_instance_uid),
            (new_condition or "").strip(),
            (new_target or "").strip(),
            ca_id,
            soa_id,
        ),
    )
    conn.commit()
    cur.execute(
        "SELECT id, condition_assignment_uid, decision_instance_uid, name, label, description, condition, "
        "condition_target_uid, order_index FROM condition_assignment "
        "WHERE soa_id=? AND id=?",
        (soa_id, ca_id),
    )
    r = cur.fetchone()
    conn.close()

    after = {
        "id": r[0],
        "condition_assignment_uid": r[1],
        "decision_instance_uid": r[2],
        "name": r[3],
        "label": r[4],
        "description": r[5],
        "condition": r[6],
        "condition_target_uid": r[7],
        "order_index": r[8],
    }
    mutable = [
        "name",
        "label",
        "description",
        "decision_instance_uid",
        "condition",
        "condition_target_uid",
    ]
    updated_fields = [
        f for f in mutable if (before.get(f) or None) != (after.get(f) or None)
    ]
    _record_condition_assignment_audit(
        soa_id,
        "update",
        ca_id,
        before=before,
        after={**after, "updated_fields": updated_fields},
    )
    return {**after, "updated_fields": updated_fields}


@router.delete(
    "/soa/{soa_id}/condition_assignments/{ca_id}",
    response_class=JSONResponse,
    response_model=None,
)
def delete_condition_assignment(soa_id: int, ca_id: int):
    if not soa_exists(soa_id):
        raise HTTPException(404, "SOA not found")

    conn = _connect()
    cur = conn.cursor()
    cur.execute(
        "SELECT id, condition_assignment_uid, condition FROM condition_assignment "
        "WHERE soa_id=? AND id=?",
        (soa_id, ca_id),
    )
    row = cur.fetchone()
    if not row:
        conn.close()
        raise HTTPException(404, f"Condition assignment id={int(ca_id)} not found")

    before = {"id": row[0], "condition_assignment_uid": row[1], "condition": row[2]}
    cur.execute(
        "DELETE FROM condition_assignment WHERE id=? AND soa_id=?", (ca_id, soa_id)
    )
    conn.commit()
    conn.close()
    _record_condition_assignment_audit(
        soa_id, "delete", ca_id, before=before, after=None
    )
    return {"deleted": True, "id": ca_id}
