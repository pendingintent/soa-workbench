import json
import logging
import os
from datetime import datetime, timezone
from typing import List, Optional

from fastapi import APIRouter, HTTPException, Request, Form, Body
from fastapi.responses import JSONResponse, HTMLResponse, RedirectResponse
from fastapi.templating import Jinja2Templates

from ..schemas import EpochCreate, EpochUpdate
from ..utils import (
    load_epoch_type_map,
    get_next_code_uid as _get_next_code_uid,
    soa_exists,
    get_latest_sdtm_ct_href,
    table_has_columns as _table_has_columns,
)
from ..db import _connect as _connect

DB_PATH = os.environ.get("SOA_BUILDER_DB", "soa_builder_web.db")

router = APIRouter()
logger = logging.getLogger("soa_builder.web.routers.epochs")
templates = Jinja2Templates(
    directory=os.path.join(os.path.dirname(__file__), "..", "templates")
)


def _nz(s: Optional[str]) -> Optional[str]:
    s = (s or "").strip()
    return s or None


def _record_epoch_audit(
    soa_id: int,
    action: str,
    epoch_id: Optional[int],
    before: Optional[dict] = None,
    after: Optional[dict] = None,
):
    try:
        conn = _connect()
        cur = conn.cursor()
        cur.execute(
            "INSERT INTO epoch_audit (soa_id, epoch_id, action, before_json, after_json, performed_at) VALUES (?,?,?,?,?,?)",
            (
                soa_id,
                epoch_id,
                action,
                json.dumps(before) if before else None,
                json.dumps(after) if after else None,
                datetime.now(timezone.utc).isoformat(),
            ),
        )
        conn.commit()
        conn.close()
    except Exception as e:
        logger.exception(
            "_record_epoch_audit failed soa_id=%s epoch_id=%s action=%s: %s",
            soa_id,
            epoch_id,
            action,
            e,
        )


# API endpoint for listing epochs
@router.get("/soa/{soa_id}/epochs", response_class=JSONResponse, response_model=None)
def list_epochs(soa_id: int):
    if not soa_exists(soa_id):
        raise HTTPException(404, "SOA not found")

    conn = _connect()
    cur = conn.cursor()
    cur.execute(
        "SELECT id,name,order_index,epoch_seq,epoch_label,epoch_description,epoch_uid,type FROM epoch WHERE soa_id=? ORDER BY order_index",
        (soa_id,),
    )
    rows = [
        {
            "id": r[0],
            "name": r[1],
            "order_index": r[2],
            "epoch_seq": r[3],
            "epoch_label": r[4],
            "epoch_description": r[5],
            "epoch_uid": r[6],
            "type": r[7],
        }
        for r in cur.fetchall()
    ]
    conn.close()
    return rows


# UI code for listing epochs
@router.get("/ui/soa/{soa_id}/epochs", response_class=HTMLResponse)
def ui_list_epochs(request: Request, soa_id: int):
    if not soa_exists(soa_id):
        raise HTTPException(404, "SOA not found")

    epochs = list_epochs(soa_id)

    # resolve epoch.type (code_uid) -> conceptId from code table
    conn = _connect()
    cur = conn.cursor()
    cur.execute(
        "SELECT code_uid, code FROM code WHERE soa_id=? AND codelist_code='C99079'",
        (soa_id,),
    )
    type_rows = cur.fetchall()
    conn.close()
    type_code_map = {row[0]: row[1] for row in type_rows if row[0]}
    for e in epochs:
        code_uid = e.get("type")
        concept_id = type_code_map.get(code_uid, "")
        if not concept_id and code_uid:
            concept_id = code_uid
        e["type_concept_id"] = type_code_map.get(code_uid, "")

    # Epoch Type options (C99079) must come from CDISC API only
    epoch_type_options = load_epoch_type_map()

    return templates.TemplateResponse(
        request,
        "epochs.html",
        {
            "request": request,
            "soa_id": soa_id,
            "epochs": epochs,
            "epoch_type_options": epoch_type_options,
        },
    )


# API endpoint for creating epoch
@router.post(
    "/soa/{soa_id}/epochs",
    response_class=JSONResponse,
    status_code=200,
    response_model=None,
)
def add_epoch(soa_id: int, payload: EpochCreate):
    if not soa_exists(soa_id):
        raise HTTPException(404, "SOA not found")

    name = (payload.name or "").strip()
    if not name:
        raise HTTPException(400, "Epoch name required")

    conn = _connect()
    cur = conn.cursor()

    # epoch_seq
    cur.execute("SELECT MAX(epoch_seq) FROM epoch WHERE soa_id=?", (soa_id,))
    row = cur.fetchone()
    next_seq = (row[0] or 0) + 1

    # order index
    cur.execute(
        "SELECT COALESCE(MAX(order_index),0) FROM epoch WHERE soa_id=?",
        (soa_id,),
    )
    next_ord = (cur.fetchone() or [0])[0] + 1

    # Code to create epoch_uid and increment order_index
    cur.execute(
        "SELECT epoch_uid FROM epoch WHERE soa_id=? AND epoch_uid LIKE 'StudyEpoch_%'",
        (soa_id,),
    )
    existing_uids = [r[0] for r in cur.fetchall() if r[0]]
    used_nums = set()
    for uid in existing_uids:
        if uid.startswith("StudyEpoch_"):
            tail = uid[len("StudyEpoch_") :]
            if tail.isdigit():
                used_nums.add(int(tail))
            else:
                logger.warning(
                    "Invalid epoch_uid format encountered (ignored): %s", uid
                )
    # Always pick max(existing) + 1, do not fill gaps
    next_n = (max(used_nums) if used_nums else 0) + 1
    new_uid = f"StudyEpoch_{next_n}"
    # Generate Code_{N} got type **only if value selected
    epoch_type_value = (payload.type or "").strip()
    epoch_type = None
    if epoch_type_value:
        epoch_type = _get_next_code_uid(cur, soa_id)
        logger.info("epoch type'%s", epoch_type)
        epoch_type_slug = get_latest_sdtm_ct_href() or ""
        epoch_type_codelist_table = (
            f"/mdr/ct/packages/{epoch_type_slug}"
            if epoch_type_slug
            else "/mdr/ct/packages"
        )
        cur.execute(
            "INSERT INTO code (soa_id, code_uid, codelist_table, codelist_code, code) VALUES (?,?,?,?,?)",
            (
                soa_id,
                epoch_type,
                epoch_type_codelist_table,
                "C99079",
                epoch_type_value,
            ),
        )

    cur.execute(
        """
        INSERT INTO epoch (soa_id,name,epoch_label,epoch_description,order_index,epoch_seq,epoch_uid,type) VALUES (?,?,?,?,?,?,?,?)
        """,
        (
            soa_id,
            name,
            _nz(payload.epoch_label),
            _nz(payload.epoch_description),
            next_ord,
            next_seq,
            new_uid,
            epoch_type,
        ),
    )
    epoch_id = cur.lastrowid
    conn.commit()
    conn.close()
    after = {
        "id": epoch_id,
        "name": name,
        "epoch_uid": new_uid,
        "label": (payload.epoch_label or "").strip() or None,
        "description": (payload.epoch_description or "").strip() or None,
        "type": (epoch_type or "").strip() or None,
        "order_index": next_ord,
        "epoch_seq": next_seq,
    }
    _record_epoch_audit(soa_id, "create", epoch_id, before=None, after=after)
    return after


# UI code for creating epoch
@router.post("/ui/soa/{soa_id}/epochs/create")
def ui_create_epoch(
    request: Request,
    soa_id: int,
    name: str = Form(...),
    label: Optional[str] = Form(None),
    description: Optional[str] = Form(None),
    type: Optional[str] = Form(None),
):
    if not soa_exists(soa_id):
        raise HTTPException(404, "SOA not found")

    payload = EpochCreate(
        name=name,
        epoch_label=label,
        epoch_description=description,
        type=type,
    )
    add_epoch(soa_id, payload)
    return RedirectResponse(url=f"/ui/soa/{int(soa_id)}/epochs", status_code=303)


# API endpoint for updating epoch
@router.patch("/soa/{soa_id}/epochs/{epoch_id}", response_class=JSONResponse)
def update_epoch(soa_id: int, epoch_id: int, payload: EpochUpdate):
    if not soa_exists(soa_id):
        raise HTTPException(404, "SOA not found")

    conn = _connect()
    cur = conn.cursor()
    cur.execute(
        """
        SELECT id,epoch_uid,name,order_index,epoch_seq,epoch_label,epoch_description,type FROM epoch WHERE id=? AND soa_id=?
        """,
        (epoch_id, soa_id),
    )
    row = cur.fetchone()
    if not row:
        raise HTTPException(404, f"Epoch id={int(epoch_id)} not found")

    before = {
        "id": row[0],
        "epoch_uid": row[1],
        "name": row[2],
        "order_index": row[3],
        "epoch_seq": row[4],
        "label": row[5],
        "description": row[6],
        "type": row[7],
    }
    new_name = (payload.name if payload.name is not None else before["name"]) or ""
    new_label = (
        payload.epoch_label if payload.epoch_label is not None else before["label"]
    )
    new_description = (
        payload.epoch_description
        if payload.epoch_description is not None
        else before["description"]
    )

    cur.execute(
        """
        UPDATE epoch SET name=?, epoch_label=?, epoch_description=? WHERE id=? AND soa_id=?
        """,
        (
            _nz(new_name),
            _nz(new_label),
            _nz(new_description),
            epoch_id,
            soa_id,
        ),
    )
    conn.commit()

    new_type = payload.type if payload.type is not None else None
    type_uid = before["type"]
    type_package_slug = get_latest_sdtm_ct_href() or ""
    type_codelist_table = (
        f"/mdr/ct/packages/{type_package_slug}"
        if type_package_slug
        else "/mdr/ct/packages"
    )

    if new_type is not None:
        if not type_uid:
            type_uid = _get_next_code_uid(cur, soa_id)
            cur.execute(
                "INSERT INTO code (soa_id, code_uid, codelist_table, codelist_code, code) VALUES (?,?,?,?,?)",
                (
                    soa_id,
                    type_uid,
                    type_codelist_table,
                    "C99079",
                    new_type,
                ),
            )
            cur.execute(
                "UPDATE epoch SET type=? WHERE id=? AND soa_id=?",
                (type_uid, epoch_id, soa_id),
            )
        else:
            cur.execute(
                "UPDATE code SET code=? WHERE soa_id=? AND code_uid=?",
                (new_type, soa_id, type_uid),
            )
            if cur.rowcount == 0:
                type_uid = _get_next_code_uid(cur, soa_id)
                cur.execute(
                    "INSERT INTO code (soa_id, code_uid, codelist_table, codelist_code, code) VALUES (?,?,?,?,?)",
                    (
                        soa_id,
                        type_uid,
                        type_codelist_table,
                        "C99079",
                        new_type,
                    ),
                )
                cur.execute(
                    "UPDATE epoch SET type=? WHERE id=? AND soa_id=?",
                    (type_uid, epoch_id, soa_id),
                )
        conn.commit()

    cur.execute(
        """
        SELECT id,epoch_uid,name,order_index,epoch_seq,epoch_label,epoch_description,type FROM epoch WHERE id=? AND soa_id=?
        """,
        (epoch_id, soa_id),
    )
    r = cur.fetchone()
    conn.close()
    after = {
        "id": r[0],
        "epoch_uid": r[1],
        "name": r[2],
        "order_index": r[3],
        "epoch_seq": r[4],
        "label": r[5],
        "description": r[6],
        "type": r[7],
    }
    mutable = {
        "name",
        "label",
        "description",
        "type",
    }
    updated_fields = [
        f for f in mutable if (before.get(f) or None) != (after.get(f) or None)
    ]
    _record_epoch_audit(
        soa_id,
        "udpate",
        epoch_id,
        before=before,
        after={**after, "updated_fields": updated_fields},
    )
    return {**after, "updated_fields": updated_fields}


# UI code to update epoch
@router.post("/ui/soa/{soa_id}/epochs/{epoch_id}/update")
def ui_update_epoch(
    request: Request,
    soa_id: int,
    epoch_id: int,
    name: Optional[str] = Form(None),
    label: Optional[str] = Form(None),
    description: Optional[str] = Form(None),
    type: Optional[str] = Form(None),
):
    if not soa_exists(soa_id):
        raise HTTPException(404, "SOA not found")

    payload = EpochUpdate(
        name=name,
        epoch_label=label,
        epoch_description=description,
        type=type,
    )
    update_epoch(soa_id, epoch_id, payload)
    return RedirectResponse(url=f"/ui/soa/{int(soa_id)}/epochs", status_code=303)


# API endpoint for deleting epoch
@router.delete(
    "/soa/{soa_id}/epochs/{epoch_id}", response_class=JSONResponse, response_model=None
)
def delete_epoch(soa_id: int, epoch_id: int):
    if not soa_exists(soa_id):
        raise HTTPException(404, "SOA not found")

    conn = _connect()
    cur = conn.cursor()
    cur.execute(
        "SELECT id,name,epoch_label,type FROM epoch WHERE soa_id=? AND id=?",
        (soa_id, epoch_id),
    )
    row = cur.fetchone()

    before = {
        "id": row[0],
        "name": row[1],
        "label": row[2],
        "type": row[3],
    }
    cur.execute(
        "DELETE FROM epoch WHERE soa_id=? AND id=?",
        (soa_id, epoch_id),
    )
    conn.commit()
    # reindex remaining epochs' epoch_seq sequentially
    cur.execute(
        "SELECT id FROM epoch WHERE soa_id=? ORDER BY epoch_seq",
        (soa_id,),
    )
    remaining = [r[0] for r in cur.fetchall()]
    for idx, eid in enumerate(remaining, start=1):
        cur.execute("UPDATE epoch SET epoch_seq=? WHERE id=?", (idx, eid))
    conn.commit()
    conn.close()
    _record_epoch_audit(soa_id, "delete", epoch_id, before=before, after=None)
    return {"deleted": True, "id": epoch_id}


# UI code to delete epoch
@router.post("/ui/soa/{soa_id}/epochs/{epoch_id}/delete")
def ui_delete_epoch(request: Request, soa_id: int, epoch_id: int):
    delete_epoch(soa_id, epoch_id)
    return RedirectResponse(url=f"/ui/soa/{int(soa_id)}/epochs", status_code=303)


# Deprecated
@router.get("/soa/{soa_id}/epochs/{epoch_id}")
def get_epoch(soa_id: int, epoch_id: int):
    if not soa_exists(soa_id):
        raise HTTPException(404, "SOA not found")
    conn = _connect()
    cur = conn.cursor()
    has_uid = _table_has_columns(cur, "epoch", ("epoch_uid",))
    if has_uid:
        cur.execute(
            "SELECT id,name,order_index,epoch_seq,epoch_label,epoch_description,epoch_uid FROM epoch WHERE id=? AND soa_id=?",
            (epoch_id, soa_id),
        )
        row = cur.fetchone()
    else:
        cur.execute(
            "SELECT id,name,order_index,epoch_seq,epoch_label,epoch_description FROM epoch WHERE id=? AND soa_id=?",
            (epoch_id, soa_id),
        )
        row = cur.fetchone()
    conn.close()
    if not row:
        raise HTTPException(404, "Epoch not found")
    if has_uid:
        eid, name, order_index, epoch_seq, epoch_label, epoch_description, epoch_uid = (
            row
        )
        return {
            "id": eid,
            "soa_id": soa_id,
            "name": name,
            "order_index": order_index,
            "epoch_seq": epoch_seq,
            "epoch_label": epoch_label,
            "epoch_description": epoch_description,
            "epoch_uid": epoch_uid,
        }
    else:
        eid, name, order_index, epoch_seq, epoch_label, epoch_description = row
        return {
            "id": eid,
            "soa_id": soa_id,
            "name": name,
            "order_index": order_index,
            "epoch_seq": epoch_seq,
            "epoch_label": epoch_label,
            "epoch_description": epoch_description,
            "epoch_uid": f"StudyEpoch_{epoch_seq or eid}",
        }


# Deprecated
@router.post("/soa/{soa_id}/epochs/{epoch_id}/metadata")
def update_epoch_metadata(soa_id: int, epoch_id: int, payload: EpochUpdate):
    if not soa_exists(soa_id):
        raise HTTPException(404, "SOA not found")
    conn = _connect()
    cur = conn.cursor()
    cur.execute("SELECT 1 FROM epoch WHERE id=? AND soa_id=?", (epoch_id, soa_id))
    if not cur.fetchone():
        conn.close()
        raise HTTPException(404, "Epoch not found")
    cur.execute(
        "SELECT id,name,order_index,epoch_seq,epoch_label,epoch_description FROM epoch WHERE id=?",
        (epoch_id,),
    )
    b = cur.fetchone()
    before = None
    if b:
        before = {
            "id": b[0],
            "name": b[1],
            "order_index": b[2],
            "epoch_seq": b[3],
            "epoch_label": b[4],
            "epoch_description": b[5],
        }
    # Include current type in before snapshot
    try:
        cur.execute("SELECT type FROM epoch WHERE id=?", (epoch_id,))
        tr = cur.fetchone()
        if before is not None:
            before["type"] = tr[0] if tr else None
    except Exception as e:
        logger.debug(
            "update_epoch_metadata type fetch failed soa_id=%s epoch_id=%s: %s",
            soa_id,
            epoch_id,
            e,
        )
    sets = []
    vals = []
    if payload.name is not None:
        sets.append("name=?")
        vals.append((payload.name or "").strip() or None)
    if payload.epoch_label is not None:
        sets.append("epoch_label=?")
        vals.append((payload.epoch_label or "").strip() or None)
    if payload.epoch_description is not None:
        sets.append("epoch_description=?")
        vals.append((payload.epoch_description or "").strip() or None)
    if sets:
        vals.append(epoch_id)
        cur.execute(f"UPDATE epoch SET {', '.join(sets)} WHERE id=?", vals)
        conn.commit()
    cur.execute(
        "SELECT id,name,order_index,epoch_seq,epoch_label,epoch_description FROM epoch WHERE id=?",
        (epoch_id,),
    )
    row = cur.fetchone()
    conn.close()
    after = {
        "id": row[0],
        "name": row[1],
        "order_index": row[2],
        "epoch_seq": row[3],
        "epoch_label": row[4],
        "epoch_description": row[5],
    }
    mutable = ["name", "epoch_label", "epoch_description"]
    updated_fields = [f for f in mutable if before and before.get(f) != after.get(f)]
    _record_epoch_audit(
        soa_id,
        "update",
        epoch_id,
        before=before,
        after={**after, "updated_fields": updated_fields},
    )
    return {**after, "updated_fields": updated_fields}


@router.post("/soa/{soa_id}/epochs/reorder", response_class=JSONResponse)
def reorder_epochs_api(
    soa_id: int,
    order: List[int] = Body(..., embed=True),  # <‑- read JSON body: {"order":[...]}
):
    if not soa_exists(soa_id):
        raise HTTPException(404, "SOA not found")
    if not order:
        raise HTTPException(400, "Order list required")

    conn = _connect()
    cur = conn.cursor()
    cur.execute(
        "SELECT id,name FROM epoch WHERE soa_id=? ORDER BY order_index", (soa_id,)
    )
    rows = cur.fetchall()
    old_order = [r[0] for r in rows]  # IDs for API response
    id_to_name = {r[0]: r[1] for r in rows}
    old_order_names = [r[1] for r in rows]  # Names for audit

    cur.execute("SELECT id,name FROM epoch WHERE soa_id=?", (soa_id,))
    existing = {r[0] for r in cur.fetchall()}
    if set(order) - existing:
        conn.close()
        raise HTTPException(400, "Order contains invalid epoch id")

    for idx, eid in enumerate(order, start=1):
        cur.execute("UPDATE epoch SET order_index=? WHERE id=?", (idx, eid))
    conn.commit()
    conn.close()

    def _epoch_types_snapshot_router(soa_id_int: int) -> List[dict]:
        conn_s = _connect()
        cur_s = conn_s.cursor()
        cur_s.execute(
            "SELECT id,type FROM epoch WHERE soa_id=? ORDER BY order_index",
            (soa_id_int,),
        )
        rows_s = cur_s.fetchall()
        conn_s.close()
        return [{"id": rid, "type": rtype} for rid, rtype in rows_s]

    new_order_names = [id_to_name.get(eid, str(eid)) for eid in order]

    _record_epoch_audit(
        soa_id,
        "reorder",
        epoch_id=None,
        before={
            "old_order": old_order_names,
            "types": _epoch_types_snapshot_router(soa_id),
        },
        after={"new_order": new_order_names},
    )
    return JSONResponse({"ok": True, "old_order": old_order, "new_order": order})
