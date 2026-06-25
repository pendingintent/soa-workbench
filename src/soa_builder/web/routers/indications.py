import json
import logging
import os
from typing import Any, Dict, List

from fastapi import APIRouter, Form, HTTPException, Request
from fastapi.responses import HTMLResponse, JSONResponse
from fastapi.templating import Jinja2Templates

from ..audit import _record_indication_audit
from ..db import _connect
from ..utils import get_next_code_uid, soa_exists

router = APIRouter(prefix="/soa/{soa_id}")
ui_router = APIRouter()
logger = logging.getLogger("soa_builder.web.routers.indications")
templates = Jinja2Templates(
    directory=os.path.join(os.path.dirname(__file__), "..", "templates")
)


# ---------------------------------------------------------------------------
# UID helpers
# ---------------------------------------------------------------------------


def _next_indication_uid(cur, soa_id: int) -> str:
    """Return next Indication_N UID, never reusing deleted UIDs."""
    max_n = 0
    cur.execute(
        "SELECT indication_uid FROM indication"
        " WHERE soa_id=? AND indication_uid LIKE 'Indication_%'",
        (soa_id,),
    )
    for (uid,) in cur.fetchall():
        if isinstance(uid, str) and uid.startswith("Indication_"):
            try:
                n = int(uid.split("_")[-1])
                if n > max_n:
                    max_n = n
            except (ValueError, IndexError):
                pass
    cur.execute(
        "SELECT before_json, after_json FROM indication_audit WHERE soa_id=?",
        (soa_id,),
    )
    for before_raw, after_raw in cur.fetchall():
        for raw in (before_raw, after_raw):
            if not raw:
                continue
            try:
                uid = json.loads(raw).get("indication_uid", "")
                if isinstance(uid, str) and uid.startswith("Indication_"):
                    n = int(uid.split("_")[-1])
                    if n > max_n:
                        max_n = n
            except Exception:
                pass
    return f"Indication_{max_n + 1}"


# ---------------------------------------------------------------------------
# List helpers
# ---------------------------------------------------------------------------


def _list_indications(soa_id: int) -> List[Dict[str, Any]]:
    conn = _connect()
    cur = conn.cursor()
    cur.execute(
        "SELECT id, indication_uid, name, label, description, is_rare_disease"
        " FROM indication WHERE soa_id=? ORDER BY order_index, id",
        (soa_id,),
    )
    rows = cur.fetchall()

    result = []
    for r in rows:
        iid, indication_uid, name, label, description, is_rare_disease = r

        cur.execute(
            "SELECT ic.id, ic.code_uid, c.code, c.code_system,"
            " c.code_system_version, c.decode"
            " FROM indication_code ic"
            " JOIN code c ON c.code_uid = ic.code_uid AND c.soa_id = ic.soa_id"
            " WHERE ic.soa_id=? AND ic.indication_id=?"
            " ORDER BY ic.order_index, ic.id",
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
                "indication_uid": indication_uid,
                "name": name,
                "label": label or "",
                "description": description or "",
                "is_rare_disease": bool(is_rare_disease),
                "codes": codes,
            }
        )
    conn.close()
    return result


# ---------------------------------------------------------------------------
# JSON API endpoints
# ---------------------------------------------------------------------------


@router.get("/indications", response_class=JSONResponse)
def list_indications(soa_id: int):
    if not soa_exists(soa_id):
        raise HTTPException(404, "SOA not found")
    return _list_indications(soa_id)


@router.post("/indications", status_code=201, response_class=JSONResponse)
def create_indication(soa_id: int, body: dict):
    if not soa_exists(soa_id):
        raise HTTPException(404, "SOA not found")
    name = (body.get("name") or "").strip()
    if not name:
        raise HTTPException(400, "name is required")

    is_rare_disease = 1 if body.get("is_rare_disease") else 0

    conn = _connect()
    cur = conn.cursor()
    try:
        cur.execute(
            "SELECT COALESCE(MAX(order_index), 0) FROM indication WHERE soa_id=?",
            (soa_id,),
        )
        order_index = (cur.fetchone()[0] or 0) + 1
        indication_uid = _next_indication_uid(cur, soa_id)
        cur.execute(
            "INSERT INTO indication"
            " (soa_id, indication_uid, name, label, description,"
            " is_rare_disease, order_index)"
            " VALUES (?,?,?,?,?,?,?)",
            (
                soa_id,
                indication_uid,
                name,
                body.get("label") or None,
                body.get("description") or None,
                is_rare_disease,
                order_index,
            ),
        )
        indication_id = cur.lastrowid
        conn.commit()
    except Exception:
        conn.rollback()
        conn.close()
        raise
    conn.close()
    _record_indication_audit(
        soa_id,
        "create",
        indication_id,
        after={"indication_uid": indication_uid, "name": name},
    )
    return {
        "id": indication_id,
        "indication_uid": indication_uid,
        "name": name,
    }


@router.delete(
    "/indications/{indication_id}",
    status_code=200,
    response_class=JSONResponse,
)
def delete_indication(soa_id: int, indication_id: int):
    if not soa_exists(soa_id):
        raise HTTPException(404, "SOA not found")
    conn = _connect()
    cur = conn.cursor()
    cur.execute(
        "SELECT id, indication_uid, name FROM indication WHERE id=? AND soa_id=?",
        (indication_id, soa_id),
    )
    row = cur.fetchone()
    if not row:
        conn.close()
        raise HTTPException(404, "Indication not found")
    (iid, indication_uid, name) = row
    before = {"indication_uid": indication_uid, "name": name}

    # Cascade: delete code rows from shared code table then junction
    cur.execute(
        "SELECT code_uid FROM indication_code WHERE soa_id=? AND indication_id=?",
        (soa_id, iid),
    )
    code_uids = [r[0] for r in cur.fetchall()]
    for code_uid in code_uids:
        cur.execute(
            "DELETE FROM code WHERE soa_id=? AND code_uid=?",
            (soa_id, code_uid),
        )
    cur.execute(
        "DELETE FROM indication_code WHERE soa_id=? AND indication_id=?",
        (soa_id, iid),
    )
    cur.execute(
        "DELETE FROM indication WHERE id=? AND soa_id=?",
        (iid, soa_id),
    )
    cur.execute(
        "SELECT id FROM indication WHERE soa_id=? ORDER BY order_index, id",
        (soa_id,),
    )
    for idx, (r,) in enumerate(cur.fetchall(), start=1):
        cur.execute("UPDATE indication SET order_index=? WHERE id=?", (idx, r))
    conn.commit()
    conn.close()
    _record_indication_audit(soa_id, "delete", iid, before=before)
    return {"deleted": indication_uid}


# ---------------------------------------------------------------------------
# JSON API: codes sub-resource
# ---------------------------------------------------------------------------


@router.post(
    "/indications/{indication_id}/codes",
    status_code=201,
    response_class=JSONResponse,
)
def add_indication_code(soa_id: int, indication_id: int, body: dict):
    if not soa_exists(soa_id):
        raise HTTPException(404, "SOA not found")

    conn = _connect()
    cur = conn.cursor()
    cur.execute(
        "SELECT id FROM indication WHERE id=? AND soa_id=?",
        (indication_id, soa_id),
    )
    if not cur.fetchone():
        conn.close()
        raise HTTPException(404, "Indication not found")

    try:
        cur.execute(
            "SELECT COALESCE(MAX(order_index), 0)"
            " FROM indication_code WHERE soa_id=? AND indication_id=?",
            (soa_id, indication_id),
        )
        order_index = (cur.fetchone()[0] or 0) + 1
        code_uid = get_next_code_uid(cur, soa_id)
        cur.execute(
            "INSERT INTO code"
            " (soa_id, code_uid, code, decode, code_system, code_system_version)"
            " VALUES (?,?,?,?,?,?)",
            (
                soa_id,
                code_uid,
                body.get("code") or "",
                body.get("decode") or "",
                body.get("code_system") or "",
                body.get("code_system_version") or "",
            ),
        )
        cur.execute(
            "INSERT INTO indication_code"
            " (soa_id, indication_id, code_uid, order_index)"
            " VALUES (?,?,?,?)",
            (soa_id, indication_id, code_uid, order_index),
        )
        link_id = cur.lastrowid
        conn.commit()
    except Exception:
        conn.rollback()
        conn.close()
        raise
    conn.close()
    return {"id": link_id, "code_uid": code_uid}


@router.delete(
    "/indications/{indication_id}/codes/{code_id}",
    status_code=200,
    response_class=JSONResponse,
)
def delete_indication_code(soa_id: int, indication_id: int, code_id: int):
    if not soa_exists(soa_id):
        raise HTTPException(404, "SOA not found")
    conn = _connect()
    cur = conn.cursor()
    cur.execute(
        "SELECT ic.id, ic.code_uid FROM indication_code ic"
        " WHERE ic.id=? AND ic.soa_id=? AND ic.indication_id=?",
        (code_id, soa_id, indication_id),
    )
    row = cur.fetchone()
    if not row:
        conn.close()
        raise HTTPException(404, "Code not found")
    (_, code_uid) = row
    cur.execute(
        "DELETE FROM indication_code WHERE id=?",
        (code_id,),
    )
    cur.execute(
        "DELETE FROM code WHERE soa_id=? AND code_uid=?",
        (soa_id, code_uid),
    )
    conn.commit()
    conn.close()
    return {"deleted": code_uid}


# ---------------------------------------------------------------------------
# HTMX UI endpoints
# ---------------------------------------------------------------------------


def _partial_response(request: Request, soa_id: int) -> HTMLResponse:
    indications = _list_indications(soa_id)
    return templates.TemplateResponse(
        request,
        "indications_partial.html",
        {
            "soa_id": soa_id,
            "indications": indications,
        },
    )


@ui_router.post(
    "/ui/soa/{soa_id}/indications-add",
    response_class=HTMLResponse,
)
def ui_indications_add(
    request: Request,
    soa_id: int,
    name: str = Form(""),
    label: str = Form(""),
    description: str = Form(""),
    is_rare_disease: str = Form(""),
):
    if not soa_exists(soa_id):
        raise HTTPException(404, "SOA not found")
    name = name.strip()
    if not name:
        raise HTTPException(400, "name is required")

    is_rare = 1 if is_rare_disease else 0

    conn = _connect()
    cur = conn.cursor()
    try:
        cur.execute(
            "SELECT COALESCE(MAX(order_index), 0) FROM indication WHERE soa_id=?",
            (soa_id,),
        )
        order_index = (cur.fetchone()[0] or 0) + 1
        indication_uid = _next_indication_uid(cur, soa_id)
        cur.execute(
            "INSERT INTO indication"
            " (soa_id, indication_uid, name, label, description,"
            " is_rare_disease, order_index)"
            " VALUES (?,?,?,?,?,?,?)",
            (
                soa_id,
                indication_uid,
                name,
                label.strip() or None,
                description.strip() or None,
                is_rare,
                order_index,
            ),
        )
        indication_id = cur.lastrowid
        conn.commit()
    except Exception:
        conn.rollback()
        conn.close()
        raise
    conn.close()
    _record_indication_audit(
        soa_id,
        "create",
        indication_id,
        after={"indication_uid": indication_uid, "name": name},
    )
    return _partial_response(request, soa_id)


@ui_router.post(
    "/ui/soa/{soa_id}/indications/{indication_id}/delete",
    response_class=HTMLResponse,
)
def ui_indications_delete(request: Request, soa_id: int, indication_id: int):
    if not soa_exists(soa_id):
        raise HTTPException(404, "SOA not found")
    conn = _connect()
    cur = conn.cursor()
    cur.execute(
        "SELECT id, indication_uid, name FROM indication WHERE id=? AND soa_id=?",
        (indication_id, soa_id),
    )
    row = cur.fetchone()
    if not row:
        conn.close()
        raise HTTPException(404, "Indication not found")
    (iid, indication_uid, name) = row
    before = {"indication_uid": indication_uid, "name": name}

    cur.execute(
        "SELECT code_uid FROM indication_code WHERE soa_id=? AND indication_id=?",
        (soa_id, iid),
    )
    code_uids = [r[0] for r in cur.fetchall()]
    for code_uid in code_uids:
        cur.execute(
            "DELETE FROM code WHERE soa_id=? AND code_uid=?",
            (soa_id, code_uid),
        )
    cur.execute(
        "DELETE FROM indication_code WHERE soa_id=? AND indication_id=?",
        (soa_id, iid),
    )
    cur.execute(
        "DELETE FROM indication WHERE id=? AND soa_id=?",
        (iid, soa_id),
    )
    cur.execute(
        "SELECT id FROM indication WHERE soa_id=? ORDER BY order_index, id",
        (soa_id,),
    )
    for idx, (r,) in enumerate(cur.fetchall(), start=1):
        cur.execute("UPDATE indication SET order_index=? WHERE id=?", (idx, r))
    conn.commit()
    conn.close()
    _record_indication_audit(soa_id, "delete", iid, before=before)
    return _partial_response(request, soa_id)


@ui_router.post(
    "/ui/soa/{soa_id}/indications/{indication_id}/codes-add",
    response_class=HTMLResponse,
)
def ui_indication_codes_add(
    request: Request,
    soa_id: int,
    indication_id: int,
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
        "SELECT id FROM indication WHERE id=? AND soa_id=?",
        (indication_id, soa_id),
    )
    if not cur.fetchone():
        conn.close()
        raise HTTPException(404, "Indication not found")

    try:
        cur.execute(
            "SELECT COALESCE(MAX(order_index), 0)"
            " FROM indication_code WHERE soa_id=? AND indication_id=?",
            (soa_id, indication_id),
        )
        order_index = (cur.fetchone()[0] or 0) + 1
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
            "INSERT INTO indication_code"
            " (soa_id, indication_id, code_uid, order_index)"
            " VALUES (?,?,?,?)",
            (soa_id, indication_id, code_uid, order_index),
        )
        conn.commit()
    except Exception:
        conn.rollback()
        conn.close()
        raise
    conn.close()
    return _partial_response(request, soa_id)


@ui_router.post(
    "/ui/soa/{soa_id}/indications/{indication_id}/codes/{code_id}/delete",
    response_class=HTMLResponse,
)
def ui_indication_codes_delete(
    request: Request,
    soa_id: int,
    indication_id: int,
    code_id: int,
):
    if not soa_exists(soa_id):
        raise HTTPException(404, "SOA not found")
    conn = _connect()
    cur = conn.cursor()
    cur.execute(
        "SELECT ic.id, ic.code_uid FROM indication_code ic"
        " WHERE ic.id=? AND ic.soa_id=? AND ic.indication_id=?",
        (code_id, soa_id, indication_id),
    )
    row = cur.fetchone()
    if not row:
        conn.close()
        raise HTTPException(404, "Code not found")
    (_, code_uid) = row
    cur.execute("DELETE FROM indication_code WHERE id=?", (code_id,))
    cur.execute(
        "DELETE FROM code WHERE soa_id=? AND code_uid=?",
        (soa_id, code_uid),
    )
    conn.commit()
    conn.close()
    return _partial_response(request, soa_id)
