import json
import logging
from datetime import datetime, timezone
from typing import List, Optional

from fastapi import APIRouter, HTTPException
from fastapi.responses import JSONResponse

from ..audit import _record_element_audit
from ..db import _connect
from ..utils import soa_exists
from ..schemas import ElementCreate, ElementUpdate

router = APIRouter(prefix="/soa/{soa_id}")
logger = logging.getLogger("soa_builder.web.routers.elements")


def _nz(s: Optional[str]) -> Optional[str]:
    s = (s or "").strip()
    return s or None


def _next_element_identifier(soa_id: int) -> str:
    """Compute next monotonically increasing StudyElement_N for an SoA.
    Scans current element rows and element_audit snapshots to avoid reusing numbers after deletes.
    """
    conn = _connect()
    cur = conn.cursor()
    max_n = 0
    try:
        cur.execute("SELECT element_id FROM element WHERE soa_id=?", (soa_id,))
        for (eid,) in cur.fetchall():
            if isinstance(eid, str) and eid.startswith("StudyElement_"):
                tail = eid.split("StudyElement_")[-1]
                if tail.isdigit():
                    max_n = max(max_n, int(tail))
    except Exception as e:
        logger.exception(
            "_next_element_identifier scan elements failed for soa_id=%s: %s",
            soa_id,
            e,
        )
    try:
        cur.execute(
            "SELECT before_json, after_json FROM element_audit WHERE soa_id=?",
            (soa_id,),
        )
        for bjson, ajson in cur.fetchall():
            for js in (bjson, ajson):
                if not js:
                    continue
                try:
                    obj = json.loads(js)
                except Exception as e:
                    logger.debug(
                        "_next_element_identifier JSON parse failed soa_id=%s: %s",
                        soa_id,
                        e,
                    )
                    obj = None
                if isinstance(obj, dict):
                    val = obj.get("element_id")
                    if isinstance(val, str) and val.startswith("StudyElement_"):
                        tail = val.split("StudyElement_")[-1]
                        if tail.isdigit():
                            max_n = max(max_n, int(tail))
    except Exception as e:
        logger.exception(
            "_next_element_identifier scan element_audit failed for soa_id=%s: %s",
            soa_id,
            e,
        )
    conn.close()
    return f"StudyElement_{max_n + 1}"


def _get_element_uid(soa_id: int, row_id: int) -> Optional[str]:
    """Return element.element_id (StudyElement_N) for row id if column exists, else None."""
    try:
        conn = _connect()
        cur = conn.cursor()
        cur.execute("PRAGMA table_info(element)")
        cols = {r[1] for r in cur.fetchall()}
        if "element_id" not in cols:
            conn.close()
            return None
        cur.execute(
            "SELECT element_id FROM element WHERE id=? AND soa_id=?",
            (row_id, soa_id),
        )
        r = cur.fetchone()
        conn.close()
        return r[0] if r else None
    except Exception as e:
        logger.exception(
            "_get_element_uid failed for soa_id=%s row_id=%s: %s", soa_id, row_id, e
        )
        return None


@router.get("/elements", response_class=JSONResponse)
def list_elements(soa_id: int):
    if not soa_exists(soa_id):
        raise HTTPException(404, "SOA not found")
    conn = _connect()
    cur = conn.cursor()
    cur.execute(
        "SELECT id,name,label,description,order_index,created_at,element_id FROM element WHERE soa_id=? ORDER BY order_index",
        (soa_id,),
    )
    rows = [
        {
            "id": r[0],
            "name": r[1],
            "label": r[2],
            "description": r[3],
            "order_index": r[4],
            "created_at": r[5],
            "element_id": r[6],
        }
        for r in cur.fetchall()
    ]
    conn.close()
    return JSONResponse(rows)


@router.get("/elements/{element_id}", response_class=JSONResponse)
def get_element(soa_id: int, element_id: int):
    if not soa_exists(soa_id):
        raise HTTPException(404, "SOA not found")
    conn = _connect()
    cur = conn.cursor()
    cur.execute(
        "SELECT id,name,label,description,order_index,created_at,element_id FROM element WHERE id=? AND soa_id=?",
        (element_id, soa_id),
    )
    r = cur.fetchone()
    conn.close()
    if not r:
        raise HTTPException(404, "Element not found")
    return {
        "id": r[0],
        "soa_id": soa_id,
        "name": r[1],
        "label": r[2],
        "description": r[3],
        "order_index": r[4],
        "created_at": r[5],
        "element_id": r[6],
    }


@router.get("/element_audit", response_class=JSONResponse)
def list_element_audit(soa_id: int):
    if not soa_exists(soa_id):
        raise HTTPException(404, "SOA not found")
    conn = _connect()
    cur = conn.cursor()
    cur.execute(
        "SELECT id, element_id, action, before_json, after_json, performed_at FROM element_audit WHERE soa_id=? ORDER BY id DESC",
        (soa_id,),
    )
    rows = []
    for r in cur.fetchall():
        try:
            before = json.loads(r[3]) if r[3] else None
        except Exception as e:
            logger.debug(
                "list_element_audit before JSON parse failed soa_id=%s: %s", soa_id, e
            )
            before = None
        try:
            after = json.loads(r[4]) if r[4] else None
        except Exception as e:
            logger.debug(
                "list_element_audit after JSON parse failed soa_id=%s: %s", soa_id, e
            )
            after = None
        rows.append(
            {
                "id": r[0],
                "element_id": r[1],
                "action": r[2],
                "before": before,
                "after": after,
                "performed_at": r[5],
            }
        )
    conn.close()
    return JSONResponse(rows)


@router.post("/elements", response_class=JSONResponse, status_code=201)
def create_element(soa_id: int, payload: ElementCreate):
    if not soa_exists(soa_id):
        raise HTTPException(404, "SOA not found")
    name = (payload.name or "").strip()
    if not name:
        raise HTTPException(400, "Name required")
    conn = _connect()
    cur = conn.cursor()

    # order_index
    cur.execute(
        "SELECT COALESCE(MAX(order_index),0) FROM element WHERE soa_id=?", (soa_id,)
    )
    next_ord = (cur.fetchone() or [0])[0] + 1

    # Create element_id and increment order_index
    cur.execute(
        "SELECT element_id FROM element WHERE soa_id=? AND element_id LIKE 'StudyElement_%'",
        (soa_id,),
    )
    existing_uids = [r[0] for r in cur.fetchall() if r[0]]
    used_nums = set()
    for uid in existing_uids:
        if uid.startswith("StudyElement_"):
            tail = uid[len("StudyElement_") :]
            if tail.isdigit():
                used_nums.add(int(tail))
            else:
                logger.warning(
                    "Invalid element_id format encountered (ignored): %s", uid
                )

    # Always pick max(existing) +1, do not fill gaps
    next_n = (max(used_nums) if used_nums else 0) + 1
    new_uid = f"StudyElement_{next_n}"

    if payload.testrl is not None:
        cur.execute(
            "SELECT 1 FROM transition_rule WHERE id=? AND soa_id=?",
            (
                payload.testrl,
                soa_id,
            ),
        )
        if not cur.fetchone():
            conn.close()
            raise HTTPException(400, "Invalid transition_rule id for this SOA")

    if payload.teenrl is not None:
        cur.execute(
            "SELECT 1 FROM transition_rule WHERE id=? AND soa_id=?",
            (
                payload.teenrl,
                soa_id,
            ),
        )
        if not cur.fetchone():
            conn.close()
            raise HTTPException(400, "Invalid transition_rule id for this SOA")

    now = datetime.now(timezone.utc).isoformat()

    cur.execute(
        "INSERT INTO element (soa_id,name,label,description,testrl,teenrl,order_index,created_at,element_id) VALUES (?,?,?,?,?,?,?,?,?)",
        (
            soa_id,
            name,
            _nz(payload.label),
            _nz(payload.description),
            payload.testrl,
            payload.teenrl,
            next_ord,
            now,
            new_uid,
        ),
    )

    eid = cur.lastrowid
    conn.commit()
    conn.close()
    after = {
        "id": eid,
        "name": name,
        "label": (payload.label or "").strip() or None,
        "description": (payload.description or "").strip() or None,
        "testrl": (payload.testrl or "").strip() or None,
        "teenrl": (payload.teenrl or "").strip() or None,
        "order_index": next_ord,
        "created_at": now,
        "element_id": new_uid,
    }
    # Audit with logical StudyElement_N when available
    _record_element_audit(soa_id, "create", eid, before=None, after=after)
    return {**after, "element_id": eid}


@router.patch("/elements/{element_id}", response_class=JSONResponse)
def update_element(soa_id: int, element_id: int, payload: ElementUpdate):
    if not soa_exists(soa_id):
        raise HTTPException(404, "SOA not found")
    conn = _connect()
    cur = conn.cursor()
    cur.execute(
        "SELECT id,name,label,description,testrl,teenrl,order_index,created_at,element_id FROM element WHERE id=? AND soa_id=?",
        (element_id, soa_id),
    )
    row = cur.fetchone()
    if not row:
        conn.close()
        raise HTTPException(404, "Element not found")
    before = {
        "id": row[0],
        "name": row[1],
        "label": row[2],
        "description": row[3],
        "testrl": row[4],
        "teenrl": row[5],
        "order_index": row[6],
        "created_at": row[7],
        "element_id": row[8],
    }
    new_name = (payload.name if payload.name is not None else before["name"]) or ""

    if payload.testrl is not None:
        cur.execute(
            "SELECT 1 FROM transition_rule WHERE id=? AND soa_id=?",
            (
                payload.testrl,
                soa_id,
            ),
        )
        if not cur.fetchone():
            conn.close()
            raise HTTPException(400, "Invalid transition_rule id for this SOA")

    if payload.teenrl is not None:
        cur.execute(
            "SELECT 1 from transition_rule WHERE id=? AND soa_id=?",
            (
                payload.teenrl,
                soa_id,
            ),
        )
        if not cur.fetchone():
            conn.close()
            raise HTTPException(400, "Invalid transition_rule id for this SOA")

    new_label = payload.label if payload.label is not None else before["label"]
    new_description = (
        payload.description
        if payload.description is not None
        else before["description"]
    )
    new_testrl = payload.testrl if payload.testrl is not None else before["testrl"]
    new_teenrl = payload.teenrl if payload.teenrl is not None else before["teenrl"]

    cur.execute(
        "UPDATE element SET name=?, label=?, description=?, testrl=?, teenrl=? WHERE id=?",
        (
            _nz(new_name),
            _nz(new_label),
            _nz(new_description),
            new_testrl,
            new_teenrl,
            element_id,
        ),
    )
    conn.commit()
    cur.execute(
        "SELECT id,name,label,description,testrl,teenrl,order_index,created_at,element_id FROM element WHERE id=?",
        (element_id,),
    )
    r = cur.fetchone()
    conn.close()
    after = {
        "id": r[0],
        "name": r[1],
        "label": r[2],
        "description": r[3],
        "testrl": r[4],
        "teenrl": r[5],
        "order_index": r[6],
        "created_at": r[7],
        "element_id": r[8],
    }
    mutable_fields = ["name", "label", "description", "testrl", "teenrl"]
    updated_fields = [
        f for f in mutable_fields if (before.get(f) or None) != (after.get(f) or None)
    ]
    # Audit with logical StudyElement_N key
    element_uid_for_audit = after.get("element_id") or _get_element_uid(
        soa_id, element_id
    )
    _record_element_audit(
        soa_id,
        "update",
        element_uid_for_audit,
        before=before,
        after={**after, "updated_fields": updated_fields},
    )
    return JSONResponse({**after, "updated_fields": updated_fields})


@router.delete("/elements/{element_id}", response_class=JSONResponse)
def delete_element(soa_id: int, element_id: int):
    if not soa_exists(soa_id):
        raise HTTPException(404, "SOA not found")
    conn = _connect()
    cur = conn.cursor()
    cur.execute(
        "SELECT id,name,label,description,testrl,teenrl,order_index,created_at,element_id FROM element WHERE id=? AND soa_id=?",
        (element_id, soa_id),
    )
    row = cur.fetchone()
    if not row:
        conn.close()
        raise HTTPException(404, "Element not found")
    before = {
        "id": row[0],
        "name": row[1],
        "label": row[2],
        "description": row[3],
        "testrl": row[4],
        "teenrl": row[5],
        "order_index": row[6],
        "created_at": row[7],
        "element_id": row[8],
    }
    cur.execute("DELETE FROM element WHERE id=?", (element_id,))
    conn.commit()
    # Reindex remaining elements' order_index sequentially
    cur.execute(
        "SELECT id FROM element WHERE soa_id=? ORDER BY order_index",
        (soa_id,),
    )
    remaining = [r[0] for r in cur.fetchall()]
    for idx, eid in enumerate(remaining, start=1):
        cur.execute(
            "UPDATE element SET order_index=? WHERE id=?",
            (
                idx,
                eid,
            ),
        )
    conn.commit()
    conn.close()
    _record_element_audit(
        soa_id,
        "delete",
        before.get("element_id"),
        before=before,
        after=None,
    )
    return JSONResponse({"deleted": True, "id": element_id})


# Deprecated - no need to reorder elements
@router.post("/elements/reorder", response_class=JSONResponse)
def reorder_elements_api(soa_id: int, order: List[int]):
    if not soa_exists(soa_id):
        raise HTTPException(404, "SOA not found")
    if not order:
        raise HTTPException(400, "Order list required")
    conn = _connect()
    cur = conn.cursor()
    cur.execute("SELECT id FROM element WHERE soa_id=? ORDER BY order_index", (soa_id,))
    old_order = [r[0] for r in cur.fetchall()]
    cur.execute("SELECT id FROM element WHERE soa_id=?", (soa_id,))
    existing = {r[0] for r in cur.fetchall()}
    if set(order) - existing:
        conn.close()
        raise HTTPException(400, "Order contains invalid element id")
    for idx, eid in enumerate(order, start=1):
        cur.execute("UPDATE element SET order_index=? WHERE id=?", (idx, eid))
    conn.commit()
    conn.close()
    _record_element_audit(
        soa_id,
        "reorder",
        element_id=None,
        before={"old_order": old_order},
        after={"new_order": order},
    )
    return JSONResponse({"ok": True, "old_order": old_order, "new_order": order})
