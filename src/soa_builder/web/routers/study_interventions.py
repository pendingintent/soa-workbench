import json
import logging
import os
from typing import Any, Dict, List, Optional

from fastapi import APIRouter, Form, HTTPException, Request
from fastapi.responses import HTMLResponse, JSONResponse
from fastapi.templating import Jinja2Templates

from ..audit import _record_study_intervention_audit
from ..codelist_config import (
    INTERVENTION_ROLE_CODELIST,
    INTERVENTION_TYPE_CODELIST,
    INTERVENTION_UNIT_CODELIST,
)
from ..db import _connect
from ..utils import (
    get_ddf_ct_rows,
    get_latest_ddf_ct_href,
    get_latest_protocol_ct_href,
    get_latest_sdtm_ct_href,
    get_next_alias_code_uid,
    get_next_code_uid,
    get_next_quantity_uid,
    get_protocol_ct_rows,
    get_sdtm_ct_rows,
    soa_exists,
)

router = APIRouter(prefix="/soa/{soa_id}")
ui_router = APIRouter()
logger = logging.getLogger("soa_builder.web.routers.study_interventions")
templates = Jinja2Templates(
    directory=os.path.join(os.path.dirname(__file__), "..", "templates")
)


# ---------------------------------------------------------------------------
# UID helpers
# ---------------------------------------------------------------------------


def _next_intervention_uid(cur, soa_id: int) -> str:
    """Return next StudyIntervention_N UID, never reusing deleted UIDs."""
    max_n = 0
    cur.execute(
        "SELECT intervention_uid FROM study_intervention"
        " WHERE soa_id=? AND intervention_uid LIKE 'StudyIntervention_%'",
        (soa_id,),
    )
    for (uid,) in cur.fetchall():
        if isinstance(uid, str) and uid.startswith("StudyIntervention_"):
            try:
                n = int(uid.split("_")[-1])
                if n > max_n:
                    max_n = n
            except (ValueError, IndexError):
                pass
    cur.execute(
        "SELECT before_json, after_json FROM study_intervention_audit WHERE soa_id=?",
        (soa_id,),
    )
    for before_raw, after_raw in cur.fetchall():
        for raw in (before_raw, after_raw):
            if not raw:
                continue
            try:
                uid = json.loads(raw).get("intervention_uid", "")
                if isinstance(uid, str) and uid.startswith("StudyIntervention_"):
                    n = int(uid.split("_")[-1])
                    if n > max_n:
                        max_n = n
            except Exception:
                pass
    return f"StudyIntervention_{max_n + 1}"


# ---------------------------------------------------------------------------
# CT option helpers
# ---------------------------------------------------------------------------


def _ct_version_from_href(href: Optional[str]) -> str:
    if not href:
        return ""
    parts = href.split("-")
    return "-".join(parts[-3:]) if len(parts) >= 3 else ""


def _ddf_ct_version() -> str:
    return _ct_version_from_href(get_latest_ddf_ct_href())


def _protocol_ct_version() -> str:
    return _ct_version_from_href(get_latest_protocol_ct_href())


def _sdtm_ct_version() -> str:
    return _ct_version_from_href(get_latest_sdtm_ct_href())


def _get_role_options() -> List[Dict[str, str]]:
    payload = get_ddf_ct_rows()
    rows = payload.get("rows") or []
    options = [
        {
            "code": r["code"],
            "label": (
                r.get("submission_value") or r.get("preferred_term") or r["code"]
            ),
        }
        for r in rows
        if r.get("codelist_code") == INTERVENTION_ROLE_CODELIST and r.get("code")
    ]
    return sorted(options, key=lambda o: o["label"].lower())


def _get_type_options() -> List[Dict[str, str]]:
    payload = get_protocol_ct_rows()
    rows = payload.get("rows") or []
    options = [
        {
            "code": r["code"],
            "label": (
                r.get("preferred_term") or r.get("submission_value") or r["code"]
            ),
        }
        for r in rows
        if r.get("codelist_code") == INTERVENTION_TYPE_CODELIST and r.get("code")
    ]
    return sorted(options, key=lambda o: o["label"].lower())


def _get_unit_options() -> List[Dict[str, str]]:
    payload = get_sdtm_ct_rows()
    rows = payload.get("rows") or []
    options = [
        {
            "code": r["code"],
            "label": (
                r.get("preferred_term") or r.get("submission_value") or r["code"]
            ),
        }
        for r in rows
        if r.get("codelist_code") == INTERVENTION_UNIT_CODELIST and r.get("code")
    ]
    return sorted(options, key=lambda o: o["label"].lower())


# ---------------------------------------------------------------------------
# Code / AliasCode helpers
# ---------------------------------------------------------------------------


def _insert_code(
    cur,
    soa_id: int,
    concept_id: str,
    decode: str,
    version: str,
) -> str:
    """Insert a code row and return Code_N UID."""
    code_uid = get_next_code_uid(cur, soa_id)
    cur.execute(
        "INSERT INTO code"
        " (soa_id, code_uid, code, decode, code_system, code_system_version)"
        " VALUES (?,?,?,?,?,?)",
        (
            soa_id,
            code_uid,
            concept_id,
            decode,
            "http://www.cdisc.org",
            version,
        ),
    )
    return code_uid


def _insert_alias_for_unit(cur, soa_id: int, code_uid: str) -> str:
    """Insert an alias_code row wrapping code_uid and return AliasCode_N UID."""
    alias_uid = get_next_alias_code_uid(cur, soa_id)
    cur.execute(
        "INSERT INTO alias_code (soa_id, alias_code_uid, standard_code) VALUES (?,?,?)",
        (soa_id, alias_uid, code_uid),
    )
    return alias_uid


def _delete_code(cur, soa_id: int, code_uid: Optional[str]) -> None:
    if not code_uid:
        return
    cur.execute(
        "DELETE FROM code WHERE soa_id=? AND code_uid=?",
        (soa_id, code_uid),
    )


def _delete_alias(cur, soa_id: int, alias_uid: Optional[str]) -> None:
    """Delete alias_code row and its linked code row."""
    if not alias_uid:
        return
    cur.execute(
        "SELECT standard_code FROM alias_code WHERE soa_id=? AND alias_code_uid=?",
        (soa_id, alias_uid),
    )
    row = cur.fetchone()
    if row:
        _delete_code(cur, soa_id, row[0])
    cur.execute(
        "DELETE FROM alias_code WHERE soa_id=? AND alias_code_uid=?",
        (soa_id, alias_uid),
    )


# ---------------------------------------------------------------------------
# List helper
# ---------------------------------------------------------------------------


def _list_interventions(soa_id: int) -> List[Dict[str, Any]]:
    conn = _connect()
    cur = conn.cursor()
    cur.execute(
        "SELECT si.id, si.intervention_uid, si.name, si.label, si.description,"
        " si.role_code_uid, rc.decode AS role_decode,"
        " si.type_code_uid, tc.decode AS type_decode,"
        " si.mrd_quantity_uid, si.mrd_value, si.mrd_unit_alias_uid"
        " FROM study_intervention si"
        " LEFT JOIN code rc ON rc.code_uid=si.role_code_uid AND rc.soa_id=si.soa_id"
        " LEFT JOIN code tc ON tc.code_uid=si.type_code_uid AND tc.soa_id=si.soa_id"
        " WHERE si.soa_id=? ORDER BY si.order_index, si.id",
        (soa_id,),
    )
    rows = cur.fetchall()

    result = []
    for r in rows:
        (
            iid,
            intervention_uid,
            name,
            label,
            description,
            role_code_uid,
            role_decode,
            type_code_uid,
            type_decode,
            mrd_quantity_uid,
            mrd_value,
            mrd_unit_alias_uid,
        ) = r

        # Fetch unit decode via alias → code chain
        unit_decode = ""
        if mrd_unit_alias_uid:
            cur.execute(
                "SELECT c.decode FROM alias_code a"
                " JOIN code c ON c.code_uid=a.standard_code AND c.soa_id=a.soa_id"
                " WHERE a.soa_id=? AND a.alias_code_uid=?",
                (soa_id, mrd_unit_alias_uid),
            )
            unit_row = cur.fetchone()
            if unit_row:
                unit_decode = unit_row[0] or ""

        # Fetch codes[] entries
        cur.execute(
            "SELECT sic.id, sic.code_uid,"
            " c.code, c.code_system, c.code_system_version, c.decode"
            " FROM study_intervention_code sic"
            " JOIN code c ON c.code_uid=sic.code_uid AND c.soa_id=sic.soa_id"
            " WHERE sic.soa_id=? AND sic.intervention_id=?"
            " ORDER BY sic.order_index, sic.id",
            (soa_id, iid),
        )
        codes = [
            {
                "id": cr[0],
                "code_uid": cr[1],
                "code": cr[2] or "",
                "code_system": cr[3] or "",
                "code_system_version": cr[4] or "",
                "decode": cr[5] or "",
            }
            for cr in cur.fetchall()
        ]

        result.append(
            {
                "id": iid,
                "intervention_uid": intervention_uid,
                "name": name,
                "label": label or "",
                "description": description or "",
                "role_code_uid": role_code_uid,
                "role_decode": role_decode or "",
                "type_code_uid": type_code_uid,
                "type_decode": type_decode or "",
                "mrd_quantity_uid": mrd_quantity_uid,
                "mrd_value": mrd_value,
                "mrd_unit_alias_uid": mrd_unit_alias_uid,
                "unit_decode": unit_decode,
                "codes": codes,
            }
        )
    conn.close()
    return result


# ---------------------------------------------------------------------------
# JSON API endpoints
# ---------------------------------------------------------------------------


@router.get("/study-interventions", response_class=JSONResponse)
def list_study_interventions(soa_id: int):
    if not soa_exists(soa_id):
        raise HTTPException(404, "SOA not found")
    return _list_interventions(soa_id)


@router.post("/study-interventions", status_code=201, response_class=JSONResponse)
def create_study_intervention(soa_id: int, body: dict):
    if not soa_exists(soa_id):
        raise HTTPException(404, "SOA not found")
    name = (body.get("name") or "").strip()
    if not name:
        raise HTTPException(400, "name is required")

    role_concept_id = (body.get("role_concept_id") or "").strip()
    role_decode = (body.get("role_decode") or "").strip()
    role_version = (body.get("role_version") or _ddf_ct_version()).strip()

    type_concept_id = (body.get("type_concept_id") or "").strip()
    type_decode = (body.get("type_decode") or "").strip()
    type_version = (body.get("type_version") or _protocol_ct_version()).strip()

    mrd_value_raw = body.get("mrd_value")
    mrd_unit_concept_id = (body.get("mrd_unit_concept_id") or "").strip()
    mrd_unit_decode = (body.get("mrd_unit_decode") or "").strip()
    mrd_unit_version = (body.get("mrd_unit_version") or _sdtm_ct_version()).strip()

    conn = _connect()
    cur = conn.cursor()
    try:
        role_code_uid = None
        if role_concept_id:
            role_code_uid = _insert_code(
                cur, soa_id, role_concept_id, role_decode, role_version
            )

        type_code_uid = None
        if type_concept_id:
            type_code_uid = _insert_code(
                cur, soa_id, type_concept_id, type_decode, type_version
            )

        mrd_quantity_uid = None
        mrd_unit_alias_uid = None
        mrd_value = None
        if mrd_value_raw is not None and mrd_unit_concept_id:
            try:
                mrd_value = float(mrd_value_raw)
            except (TypeError, ValueError):
                mrd_value = None
            if mrd_value is not None:
                unit_code_uid = _insert_code(
                    cur,
                    soa_id,
                    mrd_unit_concept_id,
                    mrd_unit_decode,
                    mrd_unit_version,
                )
                mrd_unit_alias_uid = _insert_alias_for_unit(cur, soa_id, unit_code_uid)
                mrd_quantity_uid = get_next_quantity_uid(cur, soa_id)

        cur.execute(
            "SELECT COALESCE(MAX(order_index), 0)"
            " FROM study_intervention WHERE soa_id=?",
            (soa_id,),
        )
        order_index = (cur.fetchone()[0] or 0) + 1

        intervention_uid = _next_intervention_uid(cur, soa_id)
        cur.execute(
            "INSERT INTO study_intervention"
            " (soa_id, intervention_uid, name, label, description,"
            " role_code_uid, type_code_uid,"
            " mrd_quantity_uid, mrd_value, mrd_unit_alias_uid,"
            " order_index)"
            " VALUES (?,?,?,?,?,?,?,?,?,?,?)",
            (
                soa_id,
                intervention_uid,
                name,
                body.get("label") or None,
                body.get("description") or None,
                role_code_uid,
                type_code_uid,
                mrd_quantity_uid,
                mrd_value,
                mrd_unit_alias_uid,
                order_index,
            ),
        )
        intervention_id = cur.lastrowid
        conn.commit()
    except Exception:
        conn.rollback()
        conn.close()
        raise
    conn.close()

    _record_study_intervention_audit(
        soa_id,
        "create",
        intervention_id,
        after={"intervention_uid": intervention_uid, "name": name},
    )
    return {
        "id": intervention_id,
        "intervention_uid": intervention_uid,
        "name": name,
    }


@router.delete(
    "/study-interventions/{intervention_id}",
    status_code=200,
    response_class=JSONResponse,
)
def delete_study_intervention(soa_id: int, intervention_id: int):
    if not soa_exists(soa_id):
        raise HTTPException(404, "SOA not found")
    conn = _connect()
    cur = conn.cursor()
    cur.execute(
        "SELECT id, intervention_uid, name, role_code_uid, type_code_uid,"
        " mrd_unit_alias_uid"
        " FROM study_intervention WHERE id=? AND soa_id=?",
        (intervention_id, soa_id),
    )
    row = cur.fetchone()
    if not row:
        conn.close()
        raise HTTPException(404, "Study intervention not found")
    (iid, intervention_uid, name, role_uid, type_uid, alias_uid) = row
    before = {"intervention_uid": intervention_uid, "name": name}

    # Cascade-delete codes[] entries and their code rows
    cur.execute(
        "SELECT code_uid FROM study_intervention_code"
        " WHERE soa_id=? AND intervention_id=?",
        (soa_id, iid),
    )
    for (cu,) in cur.fetchall():
        _delete_code(cur, soa_id, cu)
    cur.execute(
        "DELETE FROM study_intervention_code WHERE soa_id=? AND intervention_id=?",
        (soa_id, iid),
    )

    _delete_code(cur, soa_id, role_uid)
    _delete_code(cur, soa_id, type_uid)
    _delete_alias(cur, soa_id, alias_uid)

    cur.execute(
        "DELETE FROM study_intervention WHERE id=? AND soa_id=?",
        (iid, soa_id),
    )
    # Reorder
    cur.execute(
        "SELECT id FROM study_intervention WHERE soa_id=? ORDER BY order_index, id",
        (soa_id,),
    )
    for idx, (r,) in enumerate(cur.fetchall(), start=1):
        cur.execute("UPDATE study_intervention SET order_index=? WHERE id=?", (idx, r))
    conn.commit()
    conn.close()
    _record_study_intervention_audit(soa_id, "delete", iid, before=before)
    return {"deleted": intervention_uid}


# ---------------------------------------------------------------------------
# JSON API: codes[] sub-resource
# ---------------------------------------------------------------------------


@router.post(
    "/study-interventions/{intervention_id}/codes",
    status_code=201,
    response_class=JSONResponse,
)
def add_intervention_code(soa_id: int, intervention_id: int, body: dict):
    if not soa_exists(soa_id):
        raise HTTPException(404, "SOA not found")
    code_val = (body.get("code") or "").strip()
    code_system = (body.get("code_system") or "").strip()
    code_system_version = (body.get("code_system_version") or "").strip()
    decode = (body.get("decode") or "").strip()

    conn = _connect()
    cur = conn.cursor()
    cur.execute(
        "SELECT id FROM study_intervention WHERE id=? AND soa_id=?",
        (intervention_id, soa_id),
    )
    if not cur.fetchone():
        conn.close()
        raise HTTPException(404, "Study intervention not found")

    try:
        code_uid = get_next_code_uid(cur, soa_id)
        cur.execute(
            "INSERT INTO code"
            " (soa_id, code_uid, code, decode, code_system, code_system_version)"
            " VALUES (?,?,?,?,?,?)",
            (soa_id, code_uid, code_val, decode, code_system, code_system_version),
        )
        cur.execute(
            "SELECT COALESCE(MAX(order_index), 0)"
            " FROM study_intervention_code"
            " WHERE soa_id=? AND intervention_id=?",
            (soa_id, intervention_id),
        )
        order_index = (cur.fetchone()[0] or 0) + 1
        cur.execute(
            "INSERT INTO study_intervention_code"
            " (soa_id, intervention_id, code_uid, order_index)"
            " VALUES (?,?,?,?)",
            (soa_id, intervention_id, code_uid, order_index),
        )
        entry_id = cur.lastrowid
        conn.commit()
    except Exception:
        conn.rollback()
        conn.close()
        raise
    conn.close()
    return {"id": entry_id, "code_uid": code_uid}


@router.delete(
    "/study-interventions/{intervention_id}/codes/{code_entry_id}",
    status_code=200,
    response_class=JSONResponse,
)
def delete_intervention_code(soa_id: int, intervention_id: int, code_entry_id: int):
    if not soa_exists(soa_id):
        raise HTTPException(404, "SOA not found")
    conn = _connect()
    cur = conn.cursor()
    cur.execute(
        "SELECT sic.id, sic.code_uid"
        " FROM study_intervention_code sic"
        " WHERE sic.id=? AND sic.soa_id=? AND sic.intervention_id=?",
        (code_entry_id, soa_id, intervention_id),
    )
    row = cur.fetchone()
    if not row:
        conn.close()
        raise HTTPException(404, "Code entry not found")
    (entry_id, code_uid) = row
    _delete_code(cur, soa_id, code_uid)
    cur.execute("DELETE FROM study_intervention_code WHERE id=?", (entry_id,))
    conn.commit()
    conn.close()
    return {"deleted": entry_id}


# ---------------------------------------------------------------------------
# HTMX UI endpoints
# ---------------------------------------------------------------------------


def _partial_response(request: Request, soa_id: int) -> HTMLResponse:
    interventions = _list_interventions(soa_id)
    return templates.TemplateResponse(
        request,
        "study_interventions_partial.html",
        {
            "soa_id": soa_id,
            "interventions": interventions,
        },
    )


@ui_router.get(
    "/ui/soa/{soa_id}/study-interventions",
    response_class=HTMLResponse,
    name="ui_list_study_interventions",
)
def ui_list_study_interventions(request: Request, soa_id: int):
    if not soa_exists(soa_id):
        raise HTTPException(404, "SOA not found")
    conn = _connect()
    cur = conn.cursor()
    cur.execute("SELECT name, study_label FROM soa WHERE id=?", (soa_id,))
    row = cur.fetchone()
    conn.close()
    soa_name = row[0] if row else ""
    study_label = row[1] if row else None
    interventions = _list_interventions(soa_id)
    role_options = _get_role_options()
    type_options = _get_type_options()
    unit_options = _get_unit_options()
    return templates.TemplateResponse(
        request,
        "study_interventions.html",
        {
            "soa_id": soa_id,
            "soa_name": soa_name,
            "study_label": study_label,
            "interventions": interventions,
            "role_options": role_options,
            "type_options": type_options,
            "unit_options": unit_options,
        },
    )


@ui_router.post(
    "/ui/soa/{soa_id}/study-interventions-add",
    response_class=HTMLResponse,
)
def ui_study_interventions_add(
    request: Request,
    soa_id: int,
    name: str = Form(""),
    label: str = Form(""),
    description: str = Form(""),
    role_code: str = Form(""),
    role_decode: str = Form(""),
    type_code: str = Form(""),
    type_decode: str = Form(""),
    mrd_value: str = Form(""),
    mrd_unit_code: str = Form(""),
    mrd_unit_decode: str = Form(""),
):
    if not soa_exists(soa_id):
        raise HTTPException(404, "SOA not found")
    name = name.strip()
    if not name:
        raise HTTPException(400, "name is required")

    ddf_ver = _ddf_ct_version()
    proto_ver = _protocol_ct_version()
    sdtm_ver = _sdtm_ct_version()

    conn = _connect()
    cur = conn.cursor()
    try:
        role_code_uid = None
        if role_code.strip():
            role_code_uid = _insert_code(
                cur, soa_id, role_code.strip(), role_decode.strip(), ddf_ver
            )

        type_code_uid = None
        if type_code.strip():
            type_code_uid = _insert_code(
                cur, soa_id, type_code.strip(), type_decode.strip(), proto_ver
            )

        mrd_quantity_uid = None
        mrd_unit_alias_uid = None
        mrd_float = None
        if mrd_value.strip() and mrd_unit_code.strip():
            try:
                mrd_float = float(mrd_value.strip())
            except ValueError:
                mrd_float = None
            if mrd_float is not None:
                unit_code_uid = _insert_code(
                    cur,
                    soa_id,
                    mrd_unit_code.strip(),
                    mrd_unit_decode.strip(),
                    sdtm_ver,
                )
                mrd_unit_alias_uid = _insert_alias_for_unit(cur, soa_id, unit_code_uid)
                mrd_quantity_uid = get_next_quantity_uid(cur, soa_id)

        cur.execute(
            "SELECT COALESCE(MAX(order_index), 0)"
            " FROM study_intervention WHERE soa_id=?",
            (soa_id,),
        )
        order_index = (cur.fetchone()[0] or 0) + 1
        intervention_uid = _next_intervention_uid(cur, soa_id)
        cur.execute(
            "INSERT INTO study_intervention"
            " (soa_id, intervention_uid, name, label, description,"
            " role_code_uid, type_code_uid,"
            " mrd_quantity_uid, mrd_value, mrd_unit_alias_uid,"
            " order_index)"
            " VALUES (?,?,?,?,?,?,?,?,?,?,?)",
            (
                soa_id,
                intervention_uid,
                name,
                label.strip() or None,
                description.strip() or None,
                role_code_uid,
                type_code_uid,
                mrd_quantity_uid,
                mrd_float,
                mrd_unit_alias_uid,
                order_index,
            ),
        )
        intervention_id = cur.lastrowid
        conn.commit()
    except Exception:
        conn.rollback()
        conn.close()
        raise
    conn.close()

    _record_study_intervention_audit(
        soa_id,
        "create",
        intervention_id,
        after={
            "intervention_uid": intervention_uid,
            "name": name,
            "mrd_quantity_uid": mrd_quantity_uid,
        },
    )
    return _partial_response(request, soa_id)


@ui_router.post(
    "/ui/soa/{soa_id}/study-interventions/{intervention_id}/delete",
    response_class=HTMLResponse,
)
def ui_study_interventions_delete(request: Request, soa_id: int, intervention_id: int):
    if not soa_exists(soa_id):
        raise HTTPException(404, "SOA not found")
    conn = _connect()
    cur = conn.cursor()
    cur.execute(
        "SELECT id, intervention_uid, name, role_code_uid, type_code_uid,"
        " mrd_unit_alias_uid"
        " FROM study_intervention WHERE id=? AND soa_id=?",
        (intervention_id, soa_id),
    )
    row = cur.fetchone()
    if not row:
        conn.close()
        raise HTTPException(404, "Study intervention not found")
    (iid, intervention_uid, name, role_uid, type_uid, alias_uid) = row
    before = {"intervention_uid": intervention_uid, "name": name}

    cur.execute(
        "SELECT code_uid FROM study_intervention_code"
        " WHERE soa_id=? AND intervention_id=?",
        (soa_id, iid),
    )
    for (cu,) in cur.fetchall():
        _delete_code(cur, soa_id, cu)
    cur.execute(
        "DELETE FROM study_intervention_code WHERE soa_id=? AND intervention_id=?",
        (soa_id, iid),
    )

    _delete_code(cur, soa_id, role_uid)
    _delete_code(cur, soa_id, type_uid)
    _delete_alias(cur, soa_id, alias_uid)

    cur.execute(
        "DELETE FROM study_intervention WHERE id=? AND soa_id=?",
        (iid, soa_id),
    )
    cur.execute(
        "SELECT id FROM study_intervention WHERE soa_id=? ORDER BY order_index, id",
        (soa_id,),
    )
    for idx, (r,) in enumerate(cur.fetchall(), start=1):
        cur.execute("UPDATE study_intervention SET order_index=? WHERE id=?", (idx, r))
    conn.commit()
    conn.close()
    _record_study_intervention_audit(soa_id, "delete", iid, before=before)
    return _partial_response(request, soa_id)


@ui_router.post(
    "/ui/soa/{soa_id}/study-interventions/{intervention_id}/codes-add",
    response_class=HTMLResponse,
)
def ui_intervention_codes_add(
    request: Request,
    soa_id: int,
    intervention_id: int,
    code: str = Form(""),
    code_system: str = Form(""),
    code_system_version: str = Form(""),
    decode: str = Form(""),
):
    if not soa_exists(soa_id):
        raise HTTPException(404, "SOA not found")
    conn = _connect()
    cur = conn.cursor()
    cur.execute(
        "SELECT id FROM study_intervention WHERE id=? AND soa_id=?",
        (intervention_id, soa_id),
    )
    if not cur.fetchone():
        conn.close()
        raise HTTPException(404, "Study intervention not found")
    try:
        code_uid = get_next_code_uid(cur, soa_id)
        cur.execute(
            "INSERT INTO code"
            " (soa_id, code_uid, code, decode, code_system, code_system_version)"
            " VALUES (?,?,?,?,?,?)",
            (
                soa_id,
                code_uid,
                code.strip(),
                decode.strip(),
                code_system.strip(),
                code_system_version.strip(),
            ),
        )
        cur.execute(
            "SELECT COALESCE(MAX(order_index), 0)"
            " FROM study_intervention_code"
            " WHERE soa_id=? AND intervention_id=?",
            (soa_id, intervention_id),
        )
        order_index = (cur.fetchone()[0] or 0) + 1
        cur.execute(
            "INSERT INTO study_intervention_code"
            " (soa_id, intervention_id, code_uid, order_index)"
            " VALUES (?,?,?,?)",
            (soa_id, intervention_id, code_uid, order_index),
        )
        conn.commit()
    except Exception:
        conn.rollback()
        conn.close()
        raise
    conn.close()
    return _partial_response(request, soa_id)


@ui_router.post(
    "/ui/soa/{soa_id}/study-interventions/{intervention_id}"
    "/codes/{code_entry_id}/delete",
    response_class=HTMLResponse,
)
def ui_intervention_codes_delete(
    request: Request,
    soa_id: int,
    intervention_id: int,
    code_entry_id: int,
):
    if not soa_exists(soa_id):
        raise HTTPException(404, "SOA not found")
    conn = _connect()
    cur = conn.cursor()
    cur.execute(
        "SELECT sic.id, sic.code_uid"
        " FROM study_intervention_code sic"
        " WHERE sic.id=? AND sic.soa_id=? AND sic.intervention_id=?",
        (code_entry_id, soa_id, intervention_id),
    )
    row = cur.fetchone()
    if not row:
        conn.close()
        raise HTTPException(404, "Code entry not found")
    (entry_id, code_uid) = row
    _delete_code(cur, soa_id, code_uid)
    cur.execute("DELETE FROM study_intervention_code WHERE id=?", (entry_id,))
    conn.commit()
    conn.close()
    return _partial_response(request, soa_id)
