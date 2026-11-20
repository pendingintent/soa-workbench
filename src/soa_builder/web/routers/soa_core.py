"""Core SoA JSON API: create/read/update SoA, matrix cells, import."""

import logging
from fastapi import APIRouter
from ..initialize_database import _connect
from ..routers import activities as activities_router
from ..schemas import CellCreate
from ..schemas import MatrixImport
from ..schemas import SOACreate
from ..schemas import SOAMetadataUpdate
from ..utils import find_visit_cell
from ..utils import get_next_instance_uid
from ..utils import soa_exists
from fastapi import HTTPException
from typing import Any
from typing import List
from ..audit import _record_activity_audit
from ..services.matrix import _fetch_matrix
from ..services.matrix import _reindex
from ..services.studies import DuplicateStudyIdError
from ..services.studies import create_soa_record

logger = logging.getLogger("soa_builder.concepts")
router = APIRouter()


# API endpoint for creating new Study/SOA
@router.post("/soa")
def create_soa(payload: SOACreate):
    """Create new Schedule of Activities"""
    try:
        soa_id = create_soa_record(
            payload.name,
            payload.study_id,
            payload.study_label,
            payload.study_description,
        )
    except DuplicateStudyIdError as exc:
        raise HTTPException(400, str(exc)) from exc
    return {
        "id": soa_id,
        "name": payload.name,
        "study_id": payload.study_id,
        "study_label": payload.study_label,
        "study_description": payload.study_description,
    }


# API endpoint for returning Study/SOA metadata
@router.get("/soa/{soa_id}")
def get_soa(soa_id: int):
    """Return SoA by ID"""
    if not soa_exists(soa_id):
        raise HTTPException(404, "SOA not found")
    visits, activities, cells = _fetch_matrix(soa_id)
    # Fetch epochs
    conn_ep = _connect()
    cur_ep = conn_ep.cursor()
    cur_ep.execute(
        "SELECT id,name,order_index,epoch_seq,epoch_label,epoch_description FROM epoch WHERE soa_id=? ORDER BY order_index",
        (soa_id,),
    )
    epochs = [
        dict(
            id=r[0],
            name=r[1],
            order_index=r[2],
            epoch_seq=r[3],
            epoch_label=r[4],
            epoch_description=r[5],
        )
        for r in cur_ep.fetchall()
    ]
    conn_ep.close()
    # Also include study metadata if present
    conn = _connect()
    cur = conn.cursor()
    cur.execute(
        "SELECT study_id, study_label, study_description FROM soa WHERE id=?", (soa_id,)
    )
    meta_row = cur.fetchone()
    conn.close()
    study_meta = (
        {
            "study_id": meta_row[0],
            "study_label": meta_row[1],
            "study_description": meta_row[2],
        }
        if meta_row
        else {}
    )
    return {
        "id": soa_id,
        **study_meta,
        "epochs": epochs,
        "visits": visits,
        "activities": activities,
        "cells": cells,
    }


# API endpoint for updating Study/SOA metadata
@router.post("/soa/{soa_id}/metadata")
def update_soa_metadata(soa_id: int, payload: SOAMetadataUpdate):
    """Update metadata for SoA/Study."""
    if not soa_exists(soa_id):
        raise HTTPException(404, "SOA not found")
    conn = _connect()
    cur = conn.cursor()
    # Fetch current study_id to enforce non-blank persistence
    cur.execute("SELECT study_id FROM soa WHERE id=?", (soa_id,))
    row = cur.fetchone()
    current_study_id = row[0] if row else None
    proposed = (payload.study_id or "").strip()
    if proposed == "" and current_study_id:
        # Ignore clearing attempt – keep existing value
        new_study_id = current_study_id
    else:
        new_study_id = proposed or None
    if new_study_id:
        cur.execute(
            "SELECT id FROM soa WHERE study_id=? AND id<>?", (new_study_id, soa_id)
        )
        if cur.fetchone():
            conn.close()
            raise HTTPException(400, "study_id already exists")
    # If there was no previous study_id and none provided now, reject
    if not current_study_id and not new_study_id:
        conn.close()
        raise HTTPException(400, "study_id is required and cannot be blank")
    cur.execute(
        "UPDATE soa SET study_id=?, study_label=?, study_description=? WHERE id=?",
        (
            new_study_id,
            (payload.study_label or "").strip() or None,
            (payload.study_description or "").strip() or None,
            soa_id,
        ),
    )
    conn.commit()
    conn.close()
    return {"id": soa_id, "updated": True}


# API endpoint for marking a matrix cell association between an Activity and Encounter/Visit
@router.post("/soa/{soa_id}/cells")
def set_cell(soa_id: int, payload: CellCreate):
    """Set 'X' in SoA Matrix cell."""
    if not soa_exists(soa_id):
        raise HTTPException(404, "SOA not found")
    conn = _connect()
    cur = conn.cursor()
    # Upsert semantics: find existing (visit- or instance-keyed)
    row, inst_id = find_visit_cell(cur, soa_id, payload.visit_id, payload.activity_id)
    # If blank status => delete existing cell (clear) and do not create new row
    if payload.status.strip() == "":
        if row:
            cur.execute("DELETE FROM matrix_cells WHERE id=?", (row[0],))
            cid = row[0]
            conn.commit()
            conn.close()
            return {"cell_id": cid, "status": "", "deleted": True}
        conn.close()
        return {"cell_id": None, "status": "", "deleted": False}
    if row:
        # Update existing matrix cell status
        cur.execute(
            "UPDATE matrix_cells SET status=? WHERE id=?",
            (payload.status, row[0]),
        )
        cid = row[0]
    else:
        if inst_id is None:
            logger.warning(
                "visit-only matrix cell written (soa=%s visit=%s): visit "
                "does not map to exactly one instance; use the "
                "instance-based endpoints",
                soa_id,
                payload.visit_id,
            )
        cur.execute(
            "INSERT INTO matrix_cells (soa_id, visit_id, instance_id, "
            "activity_id, status) VALUES (?,?,?,?,?)",
            (
                soa_id,
                payload.visit_id,
                inst_id,
                payload.activity_id,
                payload.status,
            ),
        )
        cid = cur.lastrowid
    conn.commit()
    conn.close()
    # Cells without an instance never render in the matrix or export
    return {
        "cell_id": cid,
        "status": payload.status,
        "instance_resolved": inst_id is not None,
    }


# API endpoint fr returning a matrix for a Study/SOA
@router.get("/soa/{soa_id}/matrix")
def get_matrix(soa_id: int):
    """Return SoA Matrix for Schedule Activity Instances, Activities and assigned Matrix Cells."""
    if not soa_exists(soa_id):
        raise HTTPException(404, "SOA not found")

    instances, activities, cells = _fetch_matrix(soa_id)
    return {"instances": instances, "activities": activities, "cells": cells}


@router.post("/soa/{soa_id}/cells_instance")
def set_cell_instance(soa_id: int, payload: dict):
    """Set matrix cell by instance_id instead of visit_id. Body: {instance_id, activity_id, status}"""
    if not soa_exists(soa_id):
        raise HTTPException(404, "SOA not found")
    instance_id = int(payload.get("instance_id") or 0)
    activity_id = int(payload.get("activity_id") or 0)
    status = str(payload.get("status") or "").strip()
    if not instance_id or not activity_id:
        raise HTTPException(400, "instance_id and activity_id required")
    conn = _connect()
    cur = conn.cursor()
    cur.execute(
        "SELECT 1 FROM instances WHERE id=? AND soa_id=?", (instance_id, soa_id)
    )
    if cur.fetchone() is None:
        conn.close()
        raise HTTPException(404, f"Instance {instance_id} not found in SOA")
    cur.execute("SELECT 1 FROM activity WHERE id=? AND soa_id=?", (activity_id, soa_id))
    if cur.fetchone() is None:
        conn.close()
        raise HTTPException(404, f"Activity {activity_id} not found in SOA")
    cur.execute(
        "SELECT id FROM matrix_cells WHERE soa_id=? AND instance_id=? AND activity_id=?",
        (soa_id, instance_id, activity_id),
    )
    row = cur.fetchone()
    if status == "":
        if row:
            cur.execute("DELETE FROM matrix_cells WHERE id=?", (row[0],))
            cid = row[0]
            conn.commit()
            conn.close()
            return {"cell_id": cid, "status": "", "deleted": True}
        conn.close()
        return {"cell_id": None, "status": "", "deleted": False}
    if row:
        cur.execute("UPDATE matrix_cells SET status=? WHERE id=?", (status, row[0]))
        cid = row[0]
    else:
        cur.execute(
            "INSERT INTO matrix_cells (soa_id, instance_id, activity_id, status) VALUES (?,?,?,?)",
            (soa_id, instance_id, activity_id, status),
        )
        cid = cur.lastrowid
    conn.commit()
    conn.close()
    return {"cell_id": cid, "status": status}


# API endpoint for importing a Matrix (Not Used)
@router.post("/soa/{soa_id}/matrix/import")
def import_matrix(soa_id: int, payload: MatrixImport):
    """Import SoA Matrix."""
    if not soa_exists(soa_id):
        raise HTTPException(404, "SOA not found")
    if not payload.instances:
        raise HTTPException(400, "instances list empty")
    if not payload.activities:
        raise HTTPException(400, "activities list empty")
    instance_count = len(payload.instances)
    for act in payload.activities:
        if len(act.statuses) != instance_count:
            raise HTTPException(
                400,
                f"Activity '{act.name}' statuses length {len(act.statuses)} != instances length {instance_count}",
            )
    conn = _connect()
    cur = conn.cursor()
    if payload.reset:
        cur.execute("DELETE FROM matrix_cells WHERE soa_id=?", (soa_id,))
        cur.execute("DELETE FROM instances WHERE soa_id=?", (soa_id,))
        cur.execute("DELETE FROM activity WHERE soa_id=?", (soa_id,))

    # Insert instances
    cur.execute("PRAGMA table_info(instances)")
    inst_cols = {row[1] for row in cur.fetchall()}
    has_label = "label" in inst_cols
    has_instance_uid = "instance_uid" in inst_cols
    ordered_instance_ids: List[int] = []
    for inst in payload.instances:
        cols = ["soa_id", "name"]
        vals: List[Any] = [soa_id, inst.name.strip()]
        if has_label:
            cols.append("label")
            vals.append((inst.label or inst.name).strip())
        if has_instance_uid:
            cols.append("instance_uid")
            vals.append(get_next_instance_uid(cur, soa_id))
        cur.execute(
            f"INSERT INTO instances ({','.join(cols)}) VALUES ({','.join(['?'] * len(vals))})",
            vals,
        )
        ordered_instance_ids.append(cur.lastrowid)

    # Insert activities
    cur.execute("PRAGMA table_info(activity)")
    act_cols = {row[1] for row in cur.fetchall()}
    has_order_index = "order_index" in act_cols
    has_activity_uid = "activity_uid" in act_cols
    cur.execute(
        "SELECT COALESCE(MAX(order_index), 0) FROM activity WHERE soa_id=?", (soa_id,)
    )
    next_order = (cur.fetchone() or [0])[0] + 1
    activity_id_map: List[int] = []

    for a in payload.activities:
        cols = ["soa_id", "name"]
        vals = [soa_id, a.name.strip()]
        if has_order_index:
            cols.append("order_index")
            vals.append(next_order)
            next_order += 1
        if has_activity_uid:
            cols.append("activity_uid")
            vals.append(activities_router._next_activity_uid(cur, soa_id))
        cur.execute(
            f"INSERT INTO activity ({','.join(cols)}) VALUES ({','.join(['?'] * len(vals))})",
            vals,
        )
        activity_id_map.append(cur.lastrowid)

    # Insert cells
    cells_inserted = 0
    for a_idx, a in enumerate(payload.activities):
        aid = activity_id_map[a_idx]
        for inst_idx, status in enumerate(a.statuses):
            status_str = (status or "").strip()
            if not status_str:
                continue
            cur.execute(
                "INSERT INTO matrix_cells (soa_id, instance_id, activity_id, status) VALUES (?,?,?,?)",
                (soa_id, ordered_instance_ids[inst_idx], aid, status_str),
            )
            cells_inserted += 1

    conn.commit()
    conn.close()
    return {
        "instances_added": len(ordered_instance_ids),
        "activities_added": len(payload.activities),
        "cells_inserted": cells_inserted,
    }


# API endpoint for deleting an Activity
@router.delete("/soa/{soa_id}/activities/{activity_id}")
def delete_activity(soa_id: int, activity_id: int):
    """Delete Activity from an SoA."""
    if not soa_exists(soa_id):
        raise HTTPException(404, "SOA not found")
    conn = _connect()
    cur = conn.cursor()
    cur.execute("SELECT 1 FROM activity WHERE id=? AND soa_id=?", (activity_id, soa_id))
    if not cur.fetchone():
        conn.close()
        raise HTTPException(404, "Activity not found")
    cur.execute(
        "SELECT id,name,order_index FROM activity WHERE id=?",
        (activity_id,),
    )
    b = cur.fetchone()
    before = None
    if b:
        before = {"id": b[0], "name": b[1], "order_index": b[2]}
    cur.execute(
        "DELETE FROM matrix_cells WHERE soa_id=? AND activity_id=?",
        (soa_id, activity_id),
    )
    cur.execute("DELETE FROM activity WHERE id=?", (activity_id,))
    conn.commit()
    conn.close()
    _reindex("activity", soa_id)
    _record_activity_audit(soa_id, "delete", activity_id, before=before, after=None)
    return {"deleted_activity_id": activity_id}
