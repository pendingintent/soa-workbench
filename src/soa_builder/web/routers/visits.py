from typing import List, Optional

from fastapi import APIRouter, HTTPException
import logging
from fastapi.responses import JSONResponse

from ..audit import _record_reorder_audit, _record_visit_audit
from ..db import _connect
from ..utils import soa_exists
from ..schemas import VisitCreate, VisitUpdate

router = APIRouter(prefix="/soa/{soa_id}")
logger = logging.getLogger("soa_builder.web.routers.visits")


def _nz(s: Optional[str]) -> Optional[str]:
    s = (s or "").strip()
    return s or None


# API endpoint to list encounters for an SOA
@router.get("/visits", response_class=JSONResponse)
def list_visits(soa_id: int):
    if not soa_exists(soa_id):
        raise HTTPException(404, "SOA not found")
    conn = _connect()
    cur = conn.cursor()
    cur.execute(
        "SELECT id,name,label,order_index,epoch_id,encounter_uid,description FROM visit WHERE soa_id=? ORDER BY order_index",
        (soa_id,),
    )
    rows = [
        {
            "id": r[0],
            "name": r[1],
            "label": r[2],
            "order_index": r[3],
            "epoch_id": r[4],
            "encounter_uid": r[5],
            "description": r[6],
        }
        for r in cur.fetchall()
    ]
    conn.close()
    return JSONResponse(rows)


# API endpoint to return a visit
@router.get("/visits/{visit_id}", response_class=JSONResponse)
def get_visit(soa_id: int, visit_id: int):
    if not soa_exists(soa_id):
        raise HTTPException(404, "SOA not found")
    conn = _connect()
    cur = conn.cursor()
    cur.execute(
        "SELECT id,name,label,order_index,epoch_id,encounter_uid,description FROM visit WHERE id=? AND soa_id=?",
        (visit_id, soa_id),
    )
    row = cur.fetchone()
    conn.close()
    if not row:
        raise HTTPException(404, "Visit not found")
    return {
        "id": row[0],
        "soa_id": soa_id,
        "name": row[1],
        "label": row[2],
        "order_index": row[3],
        "epoch_id": row[4],
        "encounter_uid": row[5],
        "description": row[6],
    }


# API endpoint to add a visit
@router.post("/visits", response_class=JSONResponse)
def add_visit(soa_id: int, payload: VisitCreate):
    if not soa_exists(soa_id):
        raise HTTPException(404, "SOA not found")

    name = (payload.name or "").strip()
    if not name:
        raise HTTPException(400, "Encounter name required")

    conn = _connect()
    cur = conn.cursor()
    # Replace existing block with new block to create new encounter_uid and increment order_index
    # cur.execute("SELECT COUNT(*) FROM visit WHERE soa_id=?", (soa_id,))
    # order_index = cur.fetchone()[0] + 1

    # New code to calculate order_index
    cur.execute(
        "SELECT COALESCE(MAX(order_index),0) FROM visit WHERE soa_id=?",
        (soa_id,),
    )
    next_ord = (cur.fetchone() or [0])[0] + 1

    # New code to create encounter_uid and increment order_index
    cur.execute(
        "SELECT encounter_uid FROM visit WHERE soa_id=? AND encounter_uid LIKE 'Encounter_%'",
        (soa_id,),
    )
    existing_uids = [r[0] for r in cur.fetchall() if r[0]]
    used_nums = set()
    for uid in existing_uids:
        if uid.startswith("Encounter_"):
            tail = uid[len("Encounter_") :]
            if tail.isdigit():
                used_nums.add(int(tail))
            else:
                logger.warning(
                    "Invalid encounter_uid format encountered (ignored): %s", uid
                )
    # Always pick max(existing) + 1, do not fill gaps
    next_n = (max(used_nums) if used_nums else 0) + 1
    new_uid = f"Encounter_{next_n}"

    if payload.epoch_id is not None:
        cur.execute(
            "SELECT 1 FROM epoch WHERE id=? AND soa_id=?", (payload.epoch_id, soa_id)
        )
        if not cur.fetchone():
            conn.close()
            raise HTTPException(400, "Invalid epoch_id for this SOA")

    cur.execute(
        "INSERT INTO visit (soa_id,name,label,order_index,epoch_id,encounter_uid,description) VALUES (?,?,?,?,?,?,?)",
        (
            soa_id,
            name,
            _nz(payload.label),
            next_ord,
            payload.epoch_id,
            new_uid,
            _nz(payload.description),
        ),
    )
    encounter_id = cur.lastrowid
    conn.commit()
    conn.close()
    after = {
        "id": encounter_id,
        "name": payload.name,
        "label": (payload.label or "").strip() or None,
        "epoch_id": payload.epoch_id,
        "description": (payload.description or "").strip() or None,
    }
    _record_visit_audit(soa_id, "create", encounter_id, before=None, after=after)
    # Backwards-compatible field expected in tests
    return {**after, "visit_id": encounter_id}


# API endpoint to update a visit
@router.patch("/visits/{visit_id}", response_class=JSONResponse)
def update_visit(soa_id: int, visit_id: int, payload: VisitUpdate):
    if not soa_exists(soa_id):
        raise HTTPException(404, "SOA not found")
    conn = _connect()
    cur = conn.cursor()
    cur.execute(
        "SELECT id,name,label,order_index,epoch_id,encounter_uid,description FROM visit WHERE id=? AND soa_id=?",
        (visit_id, soa_id),
    )
    row = cur.fetchone()
    if not row:
        conn.close()
        raise HTTPException(404, "Visit not found")
    before = {
        "id": row[0],
        "name": row[1],
        "label": row[2],
        "order_index": row[3],
        "epoch_id": row[4],
        "encounter_uid": row[5],
        "description": row[6],
    }

    new_name = (payload.name if payload.name is not None else before["name"]) or ""
    if payload.epoch_id is not None:
        cur.execute(
            "SELECT 1 FROM epoch WHERE id=? AND soa_id=?", (payload.epoch_id, soa_id)
        )
        if not cur.fetchone():
            conn.close()
            raise HTTPException(400, "Invalid epoch_id for this SOA")

    new_label = payload.label if payload.label is not None else before["label"]
    new_epoch_id = (
        payload.epoch_id if payload.epoch_id is not None else before["epoch_id"]
    )
    new_description = (
        payload.description
        if payload.description is not None
        else before["description"]
    )

    cur.execute(
        "UPDATE visit SET name=?, label=?, epoch_id=?, description=? WHERE id=?",
        (
            _nz(new_name),
            _nz(new_label),
            new_epoch_id,
            _nz(new_description),
            visit_id,
        ),
    )
    conn.commit()
    cur.execute(
        "SELECT id,name,label,order_index,epoch_id,encounter_uid,description FROM visit WHERE soa_id=? AND id=?",
        (
            soa_id,
            visit_id,
        ),
    )
    r = cur.fetchone()
    conn.close()
    after = {
        "id": r[0],
        "name": r[1],
        "label": r[2],
        "order_index": r[3],
        "epoch_id": r[4],
        "encounter_uid": r[5],
        "description": r[6],
    }

    mutable = [
        "name",
        "label",
        "epoch_id",
        "description",
    ]

    updated_fields = [
        f for f in mutable if (before.get(f) or None) != (after.get(f) or None)
    ]

    _record_visit_audit(
        soa_id,
        "update",
        visit_id,
        before=before,
        after={**after, "updated_fields": updated_fields},
    )
    return JSONResponse({**after, "updated_fields": updated_fields})


# API endpoint to delete a visit from an SOA
@router.delete(
    "/visits/{visit_id}",
    response_class=JSONResponse,
)
def delete_visit(soa_id: int, visit_id: int):
    if not soa_exists(soa_id):
        raise HTTPException(404, "SOA not found")

    conn = _connect()
    cur = conn.cursor()
    cur.execute(
        "SELECT id,name,label,encounter_uid FROM visit WHERE soa_id=? AND id=?",
        (
            soa_id,
            visit_id,
        ),
    )
    row = cur.fetchone()
    if not row:
        raise HTTPException(404, f"Encounter id={int(visit_id)} not found")

    before = {
        "id": row[0],
        "name": row[1],
        "label": row[2],
        "encounter_uid": row[3],
    }
    # Delete target visit and its matrix cells
    cur.execute(
        "DELETE FROM matrix_cells WHERE soa_id=? AND visit_id=?", (soa_id, visit_id)
    )
    cur.execute("DELETE FROM visit WHERE id=? AND soa_id=?", (visit_id, soa_id))
    conn.commit()
    # Reindex remaining visits' order_index sequentially
    cur.execute("SELECT id FROM visit WHERE soa_id=? ORDER BY order_index", (soa_id,))
    remaining = [r[0] for r in cur.fetchall()]
    for idx, vid in enumerate(remaining, start=1):
        cur.execute("UPDATE visit SET order_index=? WHERE id=?", (idx, vid))
    conn.commit()
    conn.close()
    _record_visit_audit(soa_id, "delete", visit_id, before, after=None)
    return {"deleted": True, "id": visit_id}


# API endpoint to reorder a visit
@router.post("/visits/reorder", response_class=JSONResponse)
def reorder_visits_api(soa_id: int, order: List[int]):
    if not soa_exists(soa_id):
        raise HTTPException(404, "SOA not found")
    if not order:
        raise HTTPException(400, "Order list required")
    conn = _connect()
    cur = conn.cursor()
    cur.execute("SELECT id FROM visit WHERE soa_id=? ORDER BY order_index", (soa_id,))
    old_order = [r[0] for r in cur.fetchall()]
    cur.execute("SELECT id FROM visit WHERE soa_id=?", (soa_id,))
    existing = {r[0] for r in cur.fetchall()}
    if set(order) - existing:
        conn.close()
        raise HTTPException(400, "Order contains invalid visit id")
    for idx, vid in enumerate(order, start=1):
        cur.execute("UPDATE visit SET order_index=? WHERE id=?", (idx, vid))
    conn.commit()
    conn.close()
    _record_reorder_audit(soa_id, "visit", old_order, order)
    _record_visit_audit(
        soa_id,
        "reorder",
        visit_id=None,
        before={"old_order": old_order},
        after={"new_order": order},
    )
    return JSONResponse({"ok": True, "old_order": old_order, "new_order": order})
