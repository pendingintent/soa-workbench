from typing import List, Optional

from fastapi import APIRouter, HTTPException, Request, Form
import os
import logging
from fastapi.responses import JSONResponse, HTMLResponse, RedirectResponse

from ..audit import _record_reorder_audit, _record_visit_audit
from ..db import _connect
from ..utils import (
    soa_exists,
    get_next_code_uid as _get_next_code_uid,
    get_study_transition_rules,
    get_epoch_id,
    get_timing_id,
    get_encounter_type_sv,
    load_environmental_setting_options,
    get_latest_sdtm_ct_href,
)
from ..schemas import VisitCreate, VisitUpdate
from fastapi.templating import Jinja2Templates

router = APIRouter()
logger = logging.getLogger("soa_builder.web.routers.encounters")
templates = Jinja2Templates(
    directory=os.path.join(os.path.dirname(__file__), "..", "templates")
)


def _nz(s: Optional[str]) -> Optional[str]:
    s = (s or "").strip()
    return s or None


# API endpoint to list encounters for an SOA
@router.get("/soa/{soa_id}/visits", response_class=JSONResponse, response_model=None)
def list_visits(soa_id: int):
    if not soa_exists(soa_id):
        raise HTTPException(404, "SOA not found")

    conn = _connect()
    cur = conn.cursor()
    cur.execute(
        """
        SELECT id,encounter_uid,name,label,description,type,environmentalSettings,transitionStartRule,
        transitionEndRule,epoch_id,scheduledAtId,order_index FROM visit WHERE soa_id=? ORDER BY order_index, id
        """,
        (soa_id,),
    )
    rows = [
        {
            "id": r[0],
            "encounter_uid": r[1],
            "name": r[2],
            "label": r[3],
            "description": r[4],
            "type": r[5],
            "environmentalSettings": r[6],
            "transitionStartRule": r[7],
            "transitionEndRule": r[8],
            "epoch_id": r[9],
            "scheduledAtId": r[10],
            "order_index": r[11],
        }
        for r in cur.fetchall()
    ]
    conn.close()
    return rows


def _load_code_value_map(soa_id: int) -> dict[str, str]:
    conn = _connect()
    cur = conn.cursor()
    cur.execute(
        "SELECT code_uid, code FROM code WHERE soa_id=?",
        (soa_id,),
    )
    rows = cur.fetchall()
    conn.close()
    return {row[0]: row[1] for row in rows if row[0]}


# UI code to list encounters in an SOA
@router.get("/ui/soa/{soa_id}/visits", response_class=HTMLResponse)
def ui_list_visits(request: Request, soa_id: int):
    if not soa_exists(soa_id):
        raise HTTPException(404, "SOA not found")

    code_map = _load_code_value_map(soa_id)
    environmental_setting_options = load_environmental_setting_options()
    env_option_lookup = {
        str(opt["conceptId"]).strip(): str(opt["submissionValue"]).strip()
        for opt in environmental_setting_options
    }

    encounters = list_visits(soa_id)
    for e in encounters:
        tsv = get_encounter_type_sv(soa_id, e.get("type") or "")

        e["type_submission_value"] = tsv[0] if tsv else None

        code_uid = e.get("environmentalSettings") or ""
        concept_id = code_map.get(code_uid, "") if code_uid else ""
        e["environmental_concept_id"] = concept_id
        e["environmental_submission_value"] = env_option_lookup.get(concept_id)

    transition_rule_options = get_study_transition_rules(soa_id)
    epoch_options = get_epoch_id(soa_id)
    timing_options = get_timing_id(soa_id)

    logger.info(environmental_setting_options)

    return templates.TemplateResponse(
        request,
        "encounters.html",
        {
            "request": request,
            "soa_id": soa_id,
            "encounters": encounters,
            "transition_rule_options": transition_rule_options,
            "epoch_options": epoch_options,
            "timing_options": timing_options,
            "environmental_setting_options": environmental_setting_options,
        },
    )


# API endpoint to return a visit <- Deprecated
@router.get("/soa/visits/{visit_id}", response_class=JSONResponse)
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


# API endpoint to create an encounter
@router.post(
    "/soa/{soa_id}/visits",
    response_class=JSONResponse,
    status_code=201,
    response_model=None,
)
def add_visit(soa_id: int, payload: VisitCreate):
    if not soa_exists(soa_id):
        raise HTTPException(404, "SOA not found")

    name = (payload.name or "").strip()
    if not name:
        raise HTTPException(400, "Encounter name required")

    conn = _connect()
    cur = conn.cursor()

    # order_index
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

    # Generate Code_{N} for encounter.type
    type = _get_next_code_uid(cur, soa_id)
    logger.info("type=%s", type)

    if type:
        cur.execute(
            "INSERT INTO code (soa_id, code_uid, codelist_table, codelist_code, code) VALUES (?,?,?,?,?)",
            (
                soa_id,
                type,
                "ddf_terminology",
                "C188728",
                "C25716",
            ),
        )

    # Generate Code_{N} for environmentalSettings.type
    environmentalSettings = _get_next_code_uid(cur, soa_id)
    logger.info("environmentalSettings=%s", environmentalSettings)
    env_code_value = (payload.environmentalSettings or "").strip() or None
    env_package_slug = get_latest_sdtm_ct_href() or ""
    env_codelist_table = (
        f"/mdr/ct/packages/{env_package_slug}"
        if env_package_slug
        else "/mdr/ct/packages"
    )

    if environmentalSettings:
        cur.execute(
            "INSERT INTO code (soa_id, code_uid, codelist_table, codelist_code, code) VALUES (?,?,?,?,?)",
            (
                soa_id,
                environmentalSettings,
                env_codelist_table,
                "C127262",
                env_code_value,
            ),
        )

    cur.execute(
        """
        INSERT INTO visit (soa_id,name,label,order_index,epoch_id,encounter_uid,
        description,type,environmentalSettings,transitionStartRule,transitionEndRule,scheduledAtId)
        VALUES (?,?,?,?,?,?,?,?,?,?,?,?)
        """,
        (
            soa_id,
            name,
            _nz(payload.label),
            next_ord,
            payload.epoch_id,
            new_uid,
            _nz(payload.description),
            type,
            environmentalSettings,
            _nz(payload.transitionStartRule),
            _nz(payload.transitionEndRule),
            _nz(payload.scheduledAtId),
        ),
    )
    encounter_id = cur.lastrowid
    conn.commit()
    conn.close()
    after = {
        "id": encounter_id,
        "name": payload.name,
        "label": (payload.label or "").strip() or None,
        "description": (payload.description or "").strip() or None,
        "type": (payload.type or "").strip() or None,
        "environmental_settings": (payload.environmentalSettings or "").strip() or None,
        "transitionStartRule": (payload.transitionStartRule or "").strip() or None,
        "transitionEndRule": (payload.transitionEndRule or "").strip() or None,
        "scheduledAtId": (payload.scheduledAtId or "").strip() or None,
    }
    _record_visit_audit(soa_id, "create", encounter_id, before=None, after=after)
    # Backwards-compatible field expected in tests
    return after


# UI code to create an encounter for an SOA
@router.post("/ui/soa/{soa_id}/visits/create")
def ui_create_visit(
    request: Request,
    soa_id: int,
    name: str = Form(...),
    label: Optional[str] = Form(None),
    description: Optional[str] = Form(None),
    epoch_id: Optional[str] = Form(None),
    transitionStartRule: Optional[str] = Form(None),
    transitionEndRule: Optional[str] = Form(None),
    scheduledAtId: Optional[str] = Form(None),
    environmentalSettings: Optional[str] = Form(None),
):
    if not soa_exists(soa_id):
        raise HTTPException(404, "SOA not found")

    # Coerce empty epoch_id from form to None, otherwise to int
    parsed_epoch_id: Optional[int] = None
    if epoch_id is not None:
        eid = str(epoch_id).strip()
        if eid:
            try:
                parsed_epoch_id = int(eid)
            except ValueError:
                parsed_epoch_id = None

    payload = VisitCreate(
        name=name,
        label=label,
        description=description,
        epoch_id=parsed_epoch_id,
        transitionStartRule=transitionStartRule,
        transitionEndRule=transitionEndRule,
        scheduledAtId=scheduledAtId,
        environmentalSettings=environmentalSettings,
    )

    add_visit(soa_id, payload)
    return RedirectResponse(url=f"/ui/soa/{int(soa_id)}/visits", status_code=303)


# API endpoint to update an encounter
@router.patch("/soa/{soa_id}/visits/{visit_id}", response_class=JSONResponse)
def update_visit(soa_id: int, visit_id: int, payload: VisitUpdate):
    if not soa_exists(soa_id):
        raise HTTPException(404, "SOA not found")

    conn = _connect()
    cur = conn.cursor()
    cur.execute(
        """
        SELECT id,encounter_uid,name,label,description,type,environmentalSettings,transitionStartRule,
        transitionEndRule,epoch_id,scheduledAtId,order_index FROM visit WHERE id=? AND soa_id=? ORDER BY order_index, id
        """,
        (visit_id, soa_id),
    )
    row = cur.fetchone()
    if not row:
        conn.close()
        raise HTTPException(404, f"Encounter id={int(visit_id)} not found")

    before = {
        "id": row[0],
        "encounter_uid": row[1],
        "name": row[2],
        "label": row[3],
        "description": row[4],
        "type": row[5],
        "environmentalSettings": row[6],
        "transitionStartRule": row[7],
        "transitionEndRule": row[8],
        "epoch_id": row[9],
        "scheduledAtId": row[10],
        "order_index": row[11],
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
    new_description = (
        payload.description
        if payload.description is not None
        else before["description"]
    )
    new_epoch_id = (
        payload.epoch_id if payload.epoch_id is not None else before["epoch_id"]
    )
    new_transition_start_rule = (
        payload.transitionStartRule
        if payload.transitionStartRule is not None
        else before["transitionStartRule"]
    )
    new_transition_end_rule = (
        payload.transitionEndRule
        if payload.transitionEndRule is not None
        else before["transitionEndRule"]
    )
    new_scheduled_at_id = (
        payload.scheduledAtId
        if payload.scheduledAtId is not None
        else before["scheduledAtId"]
    )
    new_environmental_value = (
        (payload.environmentalSettings or "").strip()
        if payload.environmentalSettings is not None
        else None
    )
    env_code_uid = before["environmentalSettings"]
    env_package_slug = get_latest_sdtm_ct_href() or ""
    env_codelist_table = (
        f"/mdr/ct/packages/{env_package_slug}"
        if env_package_slug
        else "/mdr/ct/packages"
    )

    cur.execute(
        "UPDATE visit SET name=?, label=?, epoch_id=?, description=?,transitionStartRule=?,transitionEndRule=?,scheduledAtId=? WHERE id=? AND soa_id=?",
        (
            _nz(new_name),
            _nz(new_label),
            new_epoch_id,
            _nz(new_description),
            _nz(new_transition_start_rule),
            _nz(new_transition_end_rule),
            _nz(new_scheduled_at_id),
            visit_id,
            soa_id,
        ),
    )
    conn.commit()

    if new_environmental_value is not None:
        if not env_code_uid:
            env_code_uid = _get_next_code_uid(cur, soa_id)
            cur.execute(
                "INSERT INTO code (soa_id, code_uid, codelist_table, codelist_code, code) VALUES (?,?,?,?,?)",
                (
                    soa_id,
                    env_code_uid,
                    env_codelist_table,
                    "C127262",
                    new_environmental_value,
                ),
            )
            cur.execute(
                "UPDATE visit SET environmentalSettings=? WHERE id=? AND soa_id=?",
                (env_code_uid, visit_id, soa_id),
            )
        else:
            cur.execute(
                "UPDATE code SET code=? WHERE soa_id=? AND code_uid=?",
                (new_environmental_value, soa_id, env_code_uid),
            )
            if cur.rowcount == 0:
                env_code_uid = _get_next_code_uid(cur, soa_id)
                cur.execute(
                    "INSERT INTO code (soa_id, code_uid, codelist_table, codelist_code, code) VALUES (?,?,?,?,?)",
                    (
                        soa_id,
                        env_code_uid,
                        env_codelist_table,
                        "C127262",
                        new_environmental_value,
                    ),
                )
                cur.execute(
                    "UPDATE visit SET environmentalSettings=? WHERE id=? AND soa_id=?",
                    (env_code_uid, visit_id, soa_id),
                )

        conn.commit()

    cur.execute(
        """
        SELECT id,encounter_uid,name,label,description,type,environmentalSettings,transitionStartRule,
        transitionEndRule,epoch_id,scheduledAtId,order_index FROM visit WHERE id=? AND soa_id=? ORDER BY order_index, id
        """,
        (
            visit_id,
            soa_id,
        ),
    )
    r = cur.fetchone()
    conn.close()
    after = {
        "id": r[0],
        "encounter_uid": r[1],
        "name": r[2],
        "label": r[3],
        "description": r[4],
        "type": r[5],
        "environmentalSettings": r[6],
        "transitionStartRule": r[7],
        "transitionEndRule": r[8],
        "epoch_id": r[9],
        "scheduledAtId": r[10],
        "order_index": r[11],
    }

    mutable = [
        "name",
        "label",
        "epoch_id",
        "description",
        "transitionStartRule",
        "transitionEndRule",
        "scheduledAtId",
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
    return {**after, "updated_fields": updated_fields}


# UI code to update an encounter for an SOA
@router.post("/ui/soa/{soa_id}/visits/{visit_id}/update")
def ui_update_visit(
    request: Request,
    soa_id: int,
    visit_id: int,
    name: Optional[str] = Form(None),
    label: Optional[str] = Form(None),
    description: Optional[str] = Form(None),
    epoch_id: Optional[int] = Form(None),
    transitionStartRule: Optional[str] = Form(None),
    transitionEndRule: Optional[str] = Form(None),
    scheduledAtId: Optional[str] = Form(None),
    environmentalSettings: Optional[str] = Form(None),
):
    payload = VisitUpdate(
        name=name,
        label=label,
        description=description,
        epoch_id=epoch_id,
        transitionStartRule=transitionStartRule,
        transitionEndRule=transitionEndRule,
        scheduledAtId=scheduledAtId,
        environmentalSettings=environmentalSettings,
    )
    update_visit(soa_id, visit_id, payload)
    return RedirectResponse(url=f"/ui/soa/{int(soa_id)}/visits", status_code=303)


# API endpoint to delete a visit from an SOA
@router.delete(
    "/soa/{soa_id}/visits/{visit_id}",
    response_class=JSONResponse,
    response_model=None,
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


# UI code to delete an encounter from an SOA
@router.post("/ui/soa/{soa_id}/visits/{visit_id}/delete")
def ui_delete_visit(request: Request, soa_id: int, visit_id: int):
    delete_visit(soa_id, visit_id)
    return RedirectResponse(url=f"/ui/soa/{int(soa_id)}/visits", status_code=303)


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
