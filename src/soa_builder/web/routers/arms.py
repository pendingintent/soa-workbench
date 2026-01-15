import logging
import os
from typing import List, Optional

from fastapi import APIRouter, HTTPException, Request, Form
from fastapi.responses import JSONResponse, HTMLResponse, RedirectResponse
from fastapi.templating import Jinja2Templates

from ..audit import _record_arm_audit, _record_reorder_audit
from ..db import _connect
from ..schemas import ArmCreate, ArmUpdate
from ..utils import (
    get_next_code_uid as _get_next_code_uid,
    soa_exists,
    load_arm_type_map,
    load_arm_data_origin_type_map,
)

router = APIRouter()
logger = logging.getLogger("soa_builder.web.routers.arms")
templates = Jinja2Templates(
    directory=os.path.join(os.path.dirname(__file__), "..", "templates")
)


def _nz(s: Optional[str]) -> Optional[str]:
    s = (s or "").strip()
    return s or None


# API endpoint for listing Arms
@router.get("/soa/{soa_id}/arms", response_class=JSONResponse, response_model=None)
def list_arms(soa_id: int):
    if not soa_exists(soa_id):
        raise HTTPException(404, "SOA not found")

    conn = _connect()
    cur = conn.cursor()
    cur.execute(
        "SELECT id,name,label,description,type,data_origin_type,order_index,arm_uid FROM arm WHERE soa_id=? ORDER BY order_index",
        (soa_id,),
    )
    rows = [
        {
            "id": r[0],
            "name": r[1],
            "label": r[2],
            "description": r[3],
            "type": r[4],
            "data_origin_type": r[5],
            "order_index": r[6],
            "arm_uid": r[7],
        }
        for r in cur.fetchall()
    ]
    conn.close()
    return rows


# UI code for listing arms
@router.get("/ui/soa/{soa_id}/arms", response_class=HTMLResponse)
def ui_list_arms(request: Request, soa_id: int):
    if not soa_exists(soa_id):
        raise HTTPException(404, "SOA not found")

    arms = list_arms(soa_id)

    conn = _connect()
    cur = conn.cursor()
    # Map arm.type (code_uid) -> conceptId for Arm type (C174222)
    cur.execute(
        "SELECT code_uid,code FROM code WHERE soa_id=? AND codelist_code='C174222'",
        (soa_id,),
    )
    type_rows = cur.fetchall()
    # Map arm.data_origin_type (code_uid) -> conceptId for Arm Data Origin Type (C188727)
    cur.execute(
        "SELECT code_uid,code FROM code WHERE soa_id=? AND codelist_code='C188727'",
        (soa_id,),
    )
    data_origin_rows = cur.fetchall()
    conn.close()

    type_code_map = {row[0]: row[1] for row in type_rows if row[0]}
    data_origin_code_map = {row[0]: row[1] for row in data_origin_rows if row[0]}

    for a in arms:
        # Resolve Arm Type
        type_uid = a.get("type")
        type_concept_id = type_code_map.get(type_uid, "")
        if not type_concept_id and type_uid:
            type_concept_id = type_uid
        a["type_concept_id"] = type_concept_id

        # Resolve Arm Data Origin Type
        do_uid = a.get("data_origin_type")
        data_origin_concept_id = data_origin_code_map.get(do_uid, "")
        if not data_origin_concept_id and do_uid:
            data_origin_concept_id = do_uid
        a["data_origin_type_concept_id"] = data_origin_concept_id

    arm_type_options = load_arm_type_map()
    arm_data_origin_type_options = load_arm_data_origin_type_map()

    return templates.TemplateResponse(
        request,
        "arms.html",
        {
            "request": request,
            "soa_id": soa_id,
            "arms": arms,
            "arm_type_options": arm_type_options,
            "arm_data_origin_type_options": arm_data_origin_type_options,
        },
    )


# API endpoint for creating an Arm
@router.post("/soa/{soa_id}/arms", response_class=JSONResponse, status_code=201)
def create_arm(soa_id: int, payload: ArmCreate):
    if not soa_exists(soa_id):
        raise HTTPException(404, "SOA not found")

    name = (payload.name or "").strip()
    if not name:
        raise HTTPException(400, "Arm name required")

    conn = _connect()
    cur = conn.cursor()
    cur.execute(
        "SELECT COALESCE(MAX(order_index),0) FROM arm WHERE soa_id=?", (soa_id,)
    )
    next_ord = (cur.fetchone() or [0])[0] + 1

    # Code to create arm_uid and increment order_index
    cur.execute(
        "SELECT arm_uid FROM arm WHERE soa_id=? AND arm_uid LIKE 'StudyArm_%'",
        (soa_id,),
    )
    existing_uids = [r[0] for r in cur.fetchall() if r[0]]
    used_nums = set()
    for uid in existing_uids:
        if uid.startswith("StudyArm_"):
            tail = uid[len("StudyArm_") :]
            if tail.isdigit():
                used_nums.add(int(tail))
            else:
                logger.warning(
                    "Invalid arm_uid format encountered (ignored for numbering): %s",
                    uid,
                )

    # Always pick max(existing) + 1, do not fill gaps
    next_n = (max(used_nums) if used_nums else 0) + 1
    new_uid = f"StudyArm_{next_n}"

    # Generate Code_{N} for type only if value selected
    arm_type_value = (payload.type or "").strip()
    arm_type = None
    if arm_type_value:
        arm_type = _get_next_code_uid(cur, soa_id)
        logger.info("arm type: %s", arm_type)
        arm_type_codelist_table = "db://protocol_terminology"
        cur.execute(
            "INSERT INTO code (soa_id, code_uid, codelist_table, codelist_code, code) VALUES (?,?,?,?,?)",
            (
                soa_id,
                arm_type,
                arm_type_codelist_table,
                "C174222",
                arm_type_value,
            ),
        )

    arm_data_origin_type_value = (payload.data_origin_type or "").strip()
    arm_data_origin_type = None
    if arm_data_origin_type_value:
        arm_data_origin_type = _get_next_code_uid(cur, soa_id)
        logger.info("arm dataOriginType: %s", arm_data_origin_type)
        arm_data_origin_type_codelist_table = "db://ddf_terminology"
        cur.execute(
            "INSERT INTO code (soa_id, code_uid, codelist_table, codelist_code, code) VALUES (?,?,?,?,?)",
            (
                soa_id,
                arm_data_origin_type,
                arm_data_origin_type_codelist_table,
                "C188727",
                arm_data_origin_type_value,
            ),
        )

    cur.execute(
        """INSERT INTO arm (soa_id,name,label,description,type,data_origin_type,order_index,arm_uid)
            VALUES (?,?,?,?,?,?,?,?)""",
        (
            soa_id,
            name,
            (payload.label or "").strip() or None,
            (payload.description or "").strip() or None,
            arm_type,
            arm_data_origin_type,
            next_ord,
            new_uid,
        ),
    )
    arm_id = cur.lastrowid
    conn.commit()
    conn.close()
    after = {
        "id": arm_id,
        "name": name,
        "label": (payload.label or "").strip() or None,
        "description": (payload.description or "").strip() or None,
        "type": (payload.type or "").strip() or None,
        "data_origin_type": (payload.data_origin_type or "").strip() or None,
        "order_index": next_ord,
        "arm_uid": new_uid,
    }
    _record_arm_audit(soa_id, "create", arm_id, before=None, after=after)
    return after


# UI code for creating Arm
@router.post("/ui/soa/{soa_id}/arms/create")
def ui_create_arm(
    request: Request,
    soa_id: int,
    name: str = Form(...),
    label: Optional[str] = Form(None),
    description: Optional[str] = Form(None),
    type: Optional[str] = Form(None),
    data_origin_type: Optional[str] = Form(None),
):
    if not soa_exists(soa_id):
        raise HTTPException(404, "SOA not found")

    payload = ArmCreate(
        name=name,
        label=label,
        description=description,
        type=type,
        data_origin_type=data_origin_type,
    )
    create_arm(soa_id, payload)
    return RedirectResponse(url=f"/ui/soa/{int(soa_id)}/arms", status_code=303)


# API endpoint for updating an Arm
@router.patch("/soa/{soa_id}/arms/{arm_id}", response_class=JSONResponse)
def update_arm(soa_id: int, arm_id: int, payload: ArmUpdate):
    if not soa_exists(soa_id):
        raise HTTPException(404, "SOA not found")

    conn = _connect()
    cur = conn.cursor()
    cur.execute(
        "SELECT id,name,label,description,type,data_origin_type,order_index,arm_uid FROM arm WHERE id=? AND soa_id=?",
        (arm_id, soa_id),
    )
    row = cur.fetchone()
    if not row:
        conn.close()
        raise HTTPException(404, "Arm not found")

    before = {
        "id": row[0],
        "name": row[1],
        "label": row[2],
        "description": row[3],
        "type": row[4],
        "data_origin_type": row[5],
        "order_index": row[6],
        "arm_uid": row[7],
    }
    new_name = (payload.name if payload.name is not None else before["name"]) or ""
    new_label = payload.label if payload.label is not None else before["label"]
    new_desc = (
        payload.description
        if payload.description is not None
        else before["description"]
    )

    cur.execute(
        "UPDATE arm SET name=?, label=?, description=? WHERE id=? and soa_id=?",
        (
            _nz(new_name),
            _nz(new_label),
            _nz(new_desc),
            arm_id,
            soa_id,
        ),
    )
    conn.commit()

    new_type = (payload.type or "").strip()
    type_uid = before["type"]
    type_codelist_table = "db://protocol_terminology"
    if new_type:
        if not type_uid:
            # Create new Code_{N} and attach to arm.type
            type_uid = _get_next_code_uid(cur, soa_id)
            cur.execute(
                "INSERT INTO code (soa_id, code_uid, codelist_table, codelist_code, code) VALUES (?,?,?,?,?)",
                (
                    soa_id,
                    type_uid,
                    type_codelist_table,
                    "C174222",
                    new_type,
                ),
            )
            cur.execute(
                "UPDATE arm SET type=? WHERE id=? AND soa_id=?",
                (type_uid, arm_id, soa_id),
            )
        else:
            cur.execute(
                "UPDATE code SET code=? WHERE soa_id=? AND code_uid=?",
                (new_type, soa_id, type_uid),
            )
            if cur.rowcount == 0:
                # Fallback if code row is missing
                type_uid = _get_next_code_uid(cur, soa_id)
                cur.execute(
                    "INSERT INTO code (soa_id, code_uid, codelist_table, codelist_code, code) VALUES (?,?,?,?,?)",
                    (
                        soa_id,
                        type_uid,
                        type_codelist_table,
                        "C174222",
                        new_type,
                    ),
                )
                cur.execute(
                    "UPDATE arm SET type=? WHERE id=? AND soa_id=?",
                    (type_uid, arm_id, soa_id),
                )
        conn.commit()

    new_data_origin_type = (payload.data_origin_type or "").strip()
    data_origin_type_uid = before["data_origin_type"]
    data_origin_type_codelist_table = "db://ddf_terminology"
    if new_data_origin_type:
        if not data_origin_type_uid:
            data_origin_type_uid = _get_next_code_uid(cur, soa_id)
            cur.execute(
                "INSERT INTO code (soa_id, code_uid, codelist_table, codelist_code, code) VALUES (?,?,?,?,?)",
                (
                    soa_id,
                    data_origin_type_uid,
                    data_origin_type_codelist_table,
                    "C188727",
                    new_data_origin_type,
                ),
            )
            cur.execute(
                "UPDATE arm SET data_origin_type=? WHERE id=? AND soa_id=?",
                (data_origin_type_uid, arm_id, soa_id),
            )
        else:
            cur.execute(
                "UPDATE code SET code=? WHERE soa_id=? AND code_uid=?",
                (new_data_origin_type, soa_id, data_origin_type_uid),
            )
            if cur.rowcount == 0:
                data_origin_type_uid = _get_next_code_uid(cur, soa_id)
                cur.execute(
                    "INSERT INTO code (soa_id, code_uid, codelist_table, codelist_code, code) VALUES (?,?,?,?,?)",
                    (
                        soa_id,
                        data_origin_type_uid,
                        data_origin_type_codelist_table,
                        "C188727",
                        new_data_origin_type,
                    ),
                )
                cur.execute(
                    "UPDATE arm SET data_origin_type=? WHERE id=? AND soa_id=?",
                    (data_origin_type_uid, arm_id, soa_id),
                )
        conn.commit()

    cur.execute(
        """
        SELECT id,name,label,description,type,data_origin_type,order_index,arm_uid FROM arm WHERE id=? AND soa_id=?
        """,
        (arm_id, soa_id),
    )
    r = cur.fetchone()
    conn.close()
    after = {
        "id": r[0],
        "name": r[1],
        "label": r[2],
        "description": r[3],
        "type": r[4],
        "data_origin_type": r[5],
        "order_index": r[6],
        "arm_uid": r[7],
    }
    mutable = {
        "name",
        "label",
        "description",
        "type",
        "dataOriginType",
    }
    updated_fields = [
        f for f in mutable if (before.get(f) or None) != (after.get(f) or None)
    ]
    _record_arm_audit(
        soa_id,
        "update",
        arm_id,
        before=before,
        after={**after, "updated_fields": updated_fields},
    )
    return {**after, "updated_fields": updated_fields}


# UI code for updating Arm
@router.post("/ui/soa/{soa_id}/arms/{arm_id}/update")
def ui_update_arm(
    request: Request,
    soa_id: int,
    arm_id: int,
    name: Optional[str] = Form(None),
    label: Optional[str] = Form(None),
    description: Optional[str] = Form(None),
    type: Optional[str] = Form(None),
    data_origin_type: Optional[str] = Form(None),
):
    if not soa_exists(soa_id):
        raise HTTPException(404, "SOA not found")

    payload = ArmUpdate(
        name=name,
        label=label,
        description=description,
        type=type,
        data_origin_type=data_origin_type,
    )
    update_arm(soa_id, arm_id, payload)
    return RedirectResponse(url=f"/ui/soa/{int(soa_id)}/arms", status_code=303)


# API endpoint for deleting an Arm
@router.delete(
    "/soa/{soa_id}/arms/{arm_id}", response_class=JSONResponse, response_model=None
)
def delete_arm(soa_id: int, arm_id: int):
    if not soa_exists(soa_id):
        raise HTTPException(404, "SOA not found")

    conn = _connect()
    cur = conn.cursor()
    cur.execute(
        "SELECT id,arm_uid,name,label FROM arm WHERE id=? AND soa_id=?",
        (arm_id, soa_id),
    )
    row = cur.fetchone()
    if not row:
        conn.close()
        raise HTTPException(404, "Arm not found")

    before = {
        "id": row[0],
        "arm_uid": row[1],
        "name": row[2],
        "label": row[3],
    }
    cur.execute("DELETE FROM arm WHERE id=?", (arm_id,))
    conn.commit()
    conn.close()
    _record_arm_audit(soa_id, "delete", arm_id, before=before, after=None)
    return {"deleted": True, "id": arm_id}


# UI code for deleting Arm
@router.post("/ui/soa/{soa_id}/arms/{arm_id}/delete")
def ui_delete_arm(request: Request, soa_id: int, arm_id: int):
    delete_arm(soa_id, arm_id)
    return RedirectResponse(url=f"/ui/soa/{int(soa_id)}/arms", status_code=303)


# API endpoint for reordering Arms  <- Deprecated (no longer needed)
@router.post("/arms/reorder", response_class=JSONResponse)
def reorder_arms_api(soa_id: int, order: List[int]):
    if not soa_exists(soa_id):
        raise HTTPException(404, "SOA not found")
    if not order:
        raise HTTPException(400, "Order list required")
    conn = _connect()
    cur = conn.cursor()
    cur.execute("SELECT id FROM arm WHERE soa_id=? ORDER BY order_index", (soa_id,))
    old_order = [r[0] for r in cur.fetchall()]
    cur.execute("SELECT id FROM arm WHERE soa_id=?", (soa_id,))
    existing = {r[0] for r in cur.fetchall()}
    if set(order) - existing:
        conn.close()
        raise HTTPException(400, "Order contains invalid arm id")
    for idx, aid in enumerate(order, start=1):
        cur.execute("UPDATE arm SET order_index=? WHERE id=?", (idx, aid))
    conn.commit()
    conn.close()
    _record_reorder_audit(soa_id, "arm", old_order, order)
    _record_arm_audit(
        soa_id,
        "reorder",
        arm_id=None,
        before={"old_order": old_order},
        after={"new_order": order},
    )
    return {"ok": True, "old_order": old_order, "new_order": order}
