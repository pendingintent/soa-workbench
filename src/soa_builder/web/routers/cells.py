import json
import logging
import os
from typing import List, Optional

from fastapi import APIRouter, HTTPException, Request, Form, Body
from fastapi.responses import JSONResponse, HTMLResponse, RedirectResponse
from fastapi.templating import Jinja2Templates

from ..audit import _record_study_cell_audit
from ..db import _connect
from ..schemas import StudyCellCreate, StudyCellUpdate
from ..utils import soa_exists

router = APIRouter()
logger = logging.getLogger("soa_builder.web.routers.cells")
templates = Jinja2Templates(
    directory=os.path.join(os.path.dirname(__file__), "..", "templates")
)


# Helper: Normalization
def _nz(s: Optional[str]) -> Optional[str]:
    s = (s or "").strip()
    return s or None


# Helper: calculate UID
def _next_study_cell_uid(cur, soa_id: int) -> str:
    """Compute next StudyCell_N unique within an SoA.

    Checks both the live table and the audit trail so that UIDs from
    deleted study cells are never reused.
    """
    max_n = 0

    # Current rows
    cur.execute("SELECT study_cell_uid FROM study_cell WHERE soa_id=?", (soa_id,))
    for (uid,) in cur.fetchall():
        if isinstance(uid, str) and uid.startswith("StudyCell_"):
            try:
                n = int(uid.split("_")[-1])
                if n > max_n:
                    max_n = n
            except Exception:
                pass

    # Historically used UIDs from audit trail
    cur.execute(
        "SELECT before_json, after_json FROM study_cell_audit WHERE soa_id=?",
        (soa_id,),
    )
    for before_raw, after_raw in cur.fetchall():
        for raw in (before_raw, after_raw):
            if not raw:
                continue
            try:
                uid = json.loads(raw).get("study_cell_uid", "")
                if isinstance(uid, str) and uid.startswith("StudyCell_"):
                    n = int(uid.split("_")[-1])
                    if n > max_n:
                        max_n = n
            except Exception:
                pass

    return f"StudyCell_{max_n + 1}"


# API endpoint for listing study cells
@router.get(
    "/soa/{soa_id}/study_cells", response_class=JSONResponse, response_model=None
)
def list_study_cells(soa_id: int):
    if not soa_exists(soa_id):
        raise HTTPException(404, "SOA not found")

    conn = _connect()
    cur = conn.cursor()
    cur.execute(
        """
        SELECT sc.id,sc.study_cell_uid,sc.order_index,a.name,a.label,e.name,e.epoch_label,el.name,el.label
        FROM study_cell sc
        INNER JOIN arm a ON sc.soa_id=a.soa_id AND sc.arm_uid=a.arm_uid
        INNER JOIN epoch e ON sc.soa_id=e.soa_id AND sc.epoch_uid=e.epoch_uid
        INNER JOIN element el ON sc.soa_id=el.soa_id AND sc.element_uid=el.element_id
        WHERE sc.soa_id=? ORDER BY sc.order_index, sc.study_cell_uid
        """,
        (soa_id,),
    )
    rows = [
        {
            "study_cell_id": r[0],
            "study_cell_uid": r[1],
            "order_index": r[2],
            "arm_name": r[3],
            "arm_label": r[4],
            "epoch_name": r[5],
            "epoch_label": r[6],
            "element_name": r[7],
            "element_label": r[8],
        }
        for r in cur.fetchall()
    ]
    conn.close()
    return rows


# API endpoint for creating study_cell
@router.post(
    "/soa/{soa_id}/study_cells",
    response_class=JSONResponse,
    status_code=201,
    response_model=None,
)
def add_study_cell(soa_id: int, payload: StudyCellCreate):
    if not soa_exists(soa_id):
        raise HTTPException(404, "SOA not found")

    arm_uid = (payload.arm_uid or "").strip()
    epoch_uid = (payload.epoch_uid or "").strip()
    element_uid = (payload.element_uid or "").strip()
    if not arm_uid or not epoch_uid or not element_uid:
        raise HTTPException(400, "arm_uid, epoch_uid, and element_uid are required")

    conn = _connect()
    cur = conn.cursor()

    # Validate arm exists
    cur.execute("SELECT 1 FROM arm WHERE soa_id=? AND arm_uid=?", (soa_id, arm_uid))
    if not cur.fetchone():
        conn.close()
        raise HTTPException(404, "Arm not found")

    # Validate epoch exists
    cur.execute(
        "SELECT 1 FROM epoch WHERE soa_id=? AND epoch_uid=?", (soa_id, epoch_uid)
    )
    if not cur.fetchone():
        conn.close()
        raise HTTPException(404, "Epoch not found")

    # Validate element exists
    cur.execute("PRAGMA table_info(element)")
    cols = {r[1] for r in cur.fetchall()}
    if "element_id" in cols:
        cur.execute(
            "SELECT 1 FROM element WHERE soa_id=? AND element_id=?",
            (soa_id, element_uid),
        )
        if not cur.fetchone():
            conn.close()
            raise HTTPException(404, "Element not found")

    # order_index
    cur.execute(
        "SELECT COALESCE(MAX(order_index), 0) FROM study_cell WHERE soa_id=?",
        (soa_id,),
    )
    next_ord = (cur.fetchone() or [0])[0] + 1

    # Duplicate prevention
    cur.execute(
        "SELECT id FROM study_cell WHERE soa_id=? AND arm_uid=? AND epoch_uid=? AND element_uid=?",
        (soa_id, arm_uid, epoch_uid, element_uid),
    )
    if cur.fetchone():
        conn.close()
        raise HTTPException(
            409, "Study cell already exists for this arm/epoch/element combination"
        )

    sc_uid = _next_study_cell_uid(cur, soa_id)
    cur.execute(
        "INSERT INTO study_cell (soa_id, study_cell_uid, order_index, arm_uid, epoch_uid, element_uid) VALUES (?,?,?,?,?,?)",
        (soa_id, sc_uid, next_ord, arm_uid, epoch_uid, element_uid),
    )
    sc_id = cur.lastrowid
    conn.commit()
    conn.close()

    after = {
        "study_cell_id": sc_id,
        "study_cell_uid": sc_uid,
        "order_index": next_ord,
        "arm_uid": arm_uid,
        "epoch_uid": epoch_uid,
        "element_uid": element_uid,
    }
    _record_study_cell_audit(soa_id, "create", sc_id, before=None, after=after)
    return after


# API endpoint for updating study_cell
@router.patch(
    "/soa/{soa_id}/study_cells/{study_cell_id}",
    response_class=JSONResponse,
    response_model=None,
)
def update_study_cell(soa_id: int, study_cell_id: int, payload: StudyCellUpdate):
    if not soa_exists(soa_id):
        raise HTTPException(404, "SOA not found")

    conn = _connect()
    cur = conn.cursor()
    cur.execute(
        "SELECT id, arm_uid, epoch_uid, element_uid FROM study_cell WHERE id=? AND soa_id=?",
        (study_cell_id, soa_id),
    )
    row = cur.fetchone()
    if not row:
        conn.close()
        raise HTTPException(404, "Study Cell not found")

    _, curr_arm, curr_epoch, curr_el = row
    new_arm = (
        (payload.arm_uid if payload.arm_uid is not None else curr_arm) or ""
    ).strip() or curr_arm
    new_epoch = (
        (payload.epoch_uid if payload.epoch_uid is not None else curr_epoch) or ""
    ).strip() or curr_epoch
    new_el = (
        (payload.element_uid if payload.element_uid is not None else curr_el) or ""
    ).strip() or curr_el

    # Duplicate check
    cur.execute(
        "SELECT id FROM study_cell WHERE soa_id=? AND arm_uid=? AND epoch_uid=? AND element_uid=? AND id<>?",
        (soa_id, new_arm, new_epoch, new_el, study_cell_id),
    )
    if cur.fetchone():
        conn.close()
        raise HTTPException(409, "Duplicate Study Cell exists")

    before = {
        "arm_uid": curr_arm,
        "epoch_uid": curr_epoch,
        "element_uid": curr_el,
    }
    cur.execute(
        "UPDATE study_cell SET arm_uid=?, epoch_uid=?, element_uid=? WHERE id=? AND soa_id=?",
        (new_arm, new_epoch, new_el, study_cell_id, soa_id),
    )
    conn.commit()
    conn.close()

    after = {
        "arm_uid": new_arm,
        "epoch_uid": new_epoch,
        "element_uid": new_el,
    }
    _record_study_cell_audit(
        soa_id, "update", study_cell_id, before=before, after=after
    )
    return {**after, "study_cell_id": study_cell_id}


# API endpoint for deleting study_cell
@router.delete(
    "/soa/{soa_id}/study_cells/{study_cell_id}",
    response_class=JSONResponse,
    response_model=None,
)
def delete_study_cell(soa_id: int, study_cell_id: int):
    if not soa_exists(soa_id):
        raise HTTPException(404, "SOA not found")

    conn = _connect()
    cur = conn.cursor()
    cur.execute(
        "SELECT study_cell_uid, arm_uid, epoch_uid, element_uid FROM study_cell WHERE id=? AND soa_id=?",
        (study_cell_id, soa_id),
    )
    row = cur.fetchone()
    if not row:
        conn.close()
        raise HTTPException(404, "Study Cell not found")

    before = {
        "study_cell_uid": row[0],
        "arm_uid": row[1],
        "epoch_uid": row[2],
        "element_uid": row[3],
    }
    cur.execute(
        "DELETE FROM study_cell WHERE id=? AND soa_id=?", (study_cell_id, soa_id)
    )
    conn.commit()
    conn.close()

    _record_study_cell_audit(soa_id, "delete", study_cell_id, before=before, after=None)
    return {"deleted": True, "id": study_cell_id}


# UI code for listing study cells
@router.get("/ui/soa/{soa_id}/study_cells", response_class=HTMLResponse)
def ui_list_study_cells(request: Request, soa_id: int):
    if not soa_exists(soa_id):
        raise HTTPException(404, "SOA not found")

    conn = _connect()
    cur = conn.cursor()

    # Study cells with resolved names (LEFT JOIN to handle missing references)
    cur.execute(
        "SELECT sc.id, sc.study_cell_uid, sc.arm_uid, sc.epoch_uid, sc.element_uid, "
        "       e.name AS element_name, a.name AS arm_name, ep.name AS epoch_name "
        "FROM study_cell sc "
        "LEFT JOIN element e ON e.element_id = sc.element_uid AND e.soa_id = sc.soa_id "
        "LEFT JOIN arm a ON a.arm_uid = sc.arm_uid AND a.soa_id = sc.soa_id "
        "LEFT JOIN epoch ep ON ep.epoch_uid = sc.epoch_uid AND ep.soa_id = sc.soa_id "
        "WHERE sc.soa_id=? ORDER BY sc.order_index, sc.id",
        (soa_id,),
    )
    study_cells = [
        {
            "id": r[0],
            "study_cell_uid": r[1],
            "arm_uid": r[2],
            "epoch_uid": r[3],
            "element_uid": r[4],
            "element_name": r[5],
            "arm_name": r[6],
            "epoch_name": r[7],
        }
        for r in cur.fetchall()
    ]

    # Arms for dropdown
    cur.execute(
        "SELECT id, name, arm_uid FROM arm WHERE soa_id=? ORDER BY order_index",
        (soa_id,),
    )
    arms = [{"id": r[0], "name": r[1], "arm_uid": r[2]} for r in cur.fetchall()]

    # Epochs for dropdown
    cur.execute(
        "SELECT id, name, epoch_uid, epoch_seq FROM epoch WHERE soa_id=? ORDER BY order_index",
        (soa_id,),
    )
    epochs = [
        {"id": r[0], "name": r[1], "epoch_uid": r[2], "epoch_seq": r[3]}
        for r in cur.fetchall()
    ]

    # Elements for dropdown
    cur.execute(
        "SELECT id, name, element_id FROM element WHERE soa_id=? ORDER BY order_index",
        (soa_id,),
    )
    elements = [{"id": r[0], "name": r[1], "element_id": r[2]} for r in cur.fetchall()]

    # Study metadata
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
        "study_cells.html",
        {
            "request": request,
            "soa_id": soa_id,
            "study_cells": study_cells,
            "arms": arms,
            "epochs": epochs,
            "elements": elements,
            **study_meta,
        },
    )


# UI code for creating study cell(s)
@router.post("/ui/soa/{soa_id}/study_cells/create")
def ui_create_study_cell(
    request: Request,
    soa_id: int,
    arm_uid: str = Form(...),
    epoch_uid: str = Form(...),
    element_uids: List[str] = Form(...),
):
    if not soa_exists(soa_id):
        raise HTTPException(404, "SOA not found")

    for el_uid in element_uids:
        el_uid = str(el_uid).strip()
        if not el_uid:
            continue
        payload = StudyCellCreate(
            arm_uid=arm_uid, epoch_uid=epoch_uid, element_uid=el_uid
        )
        try:
            add_study_cell(soa_id, payload)
        except HTTPException as e:
            if e.status_code == 409:  # duplicate, skip
                continue
            raise
    return RedirectResponse(url=f"/ui/soa/{int(soa_id)}/study_cells", status_code=303)


# UI code to update study cell
@router.post("/ui/soa/{soa_id}/study_cells/{study_cell_id}/update")
def ui_update_study_cell(
    request: Request,
    soa_id: int,
    study_cell_id: int,
    arm_uid: Optional[str] = Form(None),
    epoch_uid: Optional[str] = Form(None),
    element_uid: Optional[str] = Form(None),
):
    if not soa_exists(soa_id):
        raise HTTPException(404, "SOA not found")

    payload = StudyCellUpdate(
        arm_uid=arm_uid, epoch_uid=epoch_uid, element_uid=element_uid
    )
    update_study_cell(soa_id, study_cell_id, payload)
    return RedirectResponse(url=f"/ui/soa/{int(soa_id)}/study_cells", status_code=303)


# UI code to delete study cell
@router.post("/ui/soa/{soa_id}/study_cells/{study_cell_id}/delete")
def ui_delete_study_cell(request: Request, soa_id: int, study_cell_id: int):
    delete_study_cell(soa_id, study_cell_id)
    return RedirectResponse(url=f"/ui/soa/{int(soa_id)}/study_cells", status_code=303)


# API endpoint for reorder
@router.post("/soa/{soa_id}/study_cells/reorder", response_class=JSONResponse)
def reorder_study_cells_api(
    soa_id: int,
    order: List[int] = Body(..., embed=True),
):
    if not soa_exists(soa_id):
        raise HTTPException(404, "SOA not found")

    if not order:
        raise HTTPException(400, "Order list required")

    conn = _connect()
    cur = conn.cursor()
    cur.execute(
        "SELECT id,study_cell_uid FROM study_cell WHERE soa_id=? ORDER BY order_index",
        (soa_id,),
    )
    rows = cur.fetchall()
    old_order = [r[0] for r in rows]  # for API response
    id_to_uid = {r[0]: r[1] for r in rows}
    old_order_uids = [r[1] for r in rows]  # for audit

    cur.execute(
        "SELECT id,study_cell_uid FROM study_cell WHERE soa_id=?",
        (soa_id,),
    )
    existing = {r[0] for r in rows}
    if set(order) - existing:
        conn.close()
        raise HTTPException(400, "order contains invalid study_cell id")

    for idx, scid in enumerate(order, start=1):
        cur.execute("UPDATE study_cell SET order_index=? WHERE id=?", (idx, scid))
    conn.commit()
    conn.close()

    new_order_uids = [id_to_uid.get(scid, str(scid)) for scid in order]

    _record_study_cell_audit(
        soa_id,
        "reorder",
        study_cell_id=None,
        before={
            "old_order": old_order_uids,
        },
        after={"new_order": new_order_uids},
    )
    return JSONResponse({"ok": True, "old_order": old_order, "new_order": order})
