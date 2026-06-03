import json
import logging
import os
from typing import Any, Dict, List

from fastapi import APIRouter, Form, HTTPException, Request
from fastapi.responses import HTMLResponse, JSONResponse
from fastapi.templating import Jinja2Templates

from ..audit import _record_estimand_audit
from ..db import _connect
from ..utils import get_next_intercurrent_event_uid, soa_exists
from .indications import _list_indications

router = APIRouter(prefix="/soa/{soa_id}")
ui_router = APIRouter()
logger = logging.getLogger("soa_builder.web.routers.estimands")
templates = Jinja2Templates(
    directory=os.path.join(os.path.dirname(__file__), "..", "templates")
)


# ---------------------------------------------------------------------------
# UID helpers
# ---------------------------------------------------------------------------


def _next_estimand_uid(cur, soa_id: int) -> str:
    """Return next Estimand_N UID, never reusing deleted UIDs."""
    max_n = 0
    cur.execute(
        "SELECT estimand_uid FROM estimand"
        " WHERE soa_id=? AND estimand_uid LIKE 'Estimand_%'",
        (soa_id,),
    )
    for (uid,) in cur.fetchall():
        if isinstance(uid, str) and uid.startswith("Estimand_"):
            try:
                n = int(uid.split("_")[-1])
                if n > max_n:
                    max_n = n
            except (ValueError, IndexError):
                pass
    cur.execute(
        "SELECT before_json, after_json FROM estimand_audit WHERE soa_id=?",
        (soa_id,),
    )
    for before_raw, after_raw in cur.fetchall():
        for raw in (before_raw, after_raw):
            if not raw:
                continue
            try:
                uid = json.loads(raw).get("estimand_uid", "")
                if isinstance(uid, str) and uid.startswith("Estimand_"):
                    n = int(uid.split("_")[-1])
                    if n > max_n:
                        max_n = n
            except Exception:
                pass
    return f"Estimand_{max_n + 1}"


# ---------------------------------------------------------------------------
# List helpers
# ---------------------------------------------------------------------------


def _list_available_interventions(soa_id: int) -> List[Dict[str, str]]:
    """Return [{intervention_uid, name}] for the soa, alpha sorted by name."""
    conn = _connect()
    cur = conn.cursor()
    cur.execute(
        "SELECT intervention_uid, name FROM study_intervention"
        " WHERE soa_id=? ORDER BY name COLLATE NOCASE",
        (soa_id,),
    )
    rows = cur.fetchall()
    conn.close()
    return [{"intervention_uid": r[0], "name": r[1]} for r in rows]


def _list_available_endpoints(soa_id: int) -> List[Dict[str, str]]:
    """Return [{endpoint_uid, name}] for the soa, alpha sorted by name."""
    conn = _connect()
    cur = conn.cursor()
    cur.execute(
        "SELECT endpoint_uid, name FROM endpoint"
        " WHERE soa_id=? ORDER BY name COLLATE NOCASE",
        (soa_id,),
    )
    rows = cur.fetchall()
    conn.close()
    return [{"endpoint_uid": r[0], "name": r[1]} for r in rows]


def _list_estimands(soa_id: int) -> List[Dict[str, Any]]:
    conn = _connect()
    cur = conn.cursor()
    cur.execute(
        "SELECT id, estimand_uid, name, label, description, population_summary"
        " FROM estimand WHERE soa_id=? ORDER BY order_index, id",
        (soa_id,),
    )
    rows = cur.fetchall()

    # Build endpoint name lookup
    cur.execute(
        "SELECT endpoint_uid, name FROM endpoint WHERE soa_id=?",
        (soa_id,),
    )
    endpoint_name_map = {r[0]: r[1] for r in cur.fetchall()}

    # Build intervention name lookup
    cur.execute(
        "SELECT intervention_uid, name FROM study_intervention WHERE soa_id=?",
        (soa_id,),
    )
    intervention_name_map = {r[0]: r[1] for r in cur.fetchall()}

    result = []
    for r in rows:
        eid, estimand_uid, name, label, description, population_summary = r

        # Fetch variables of interest (endpoint UIDs)
        cur.execute(
            "SELECT endpoint_uid FROM estimand_variable"
            " WHERE soa_id=? AND estimand_id=? ORDER BY order_index, id",
            (soa_id, eid),
        )
        variable_uids = [row[0] for row in cur.fetchall()]

        # Fetch linked intervention UIDs
        cur.execute(
            "SELECT intervention_uid FROM estimand_intervention"
            " WHERE soa_id=? AND estimand_id=? ORDER BY order_index, id",
            (soa_id, eid),
        )
        intervention_uids = [row[0] for row in cur.fetchall()]

        # Fetch intercurrent events
        cur.execute(
            "SELECT id, event_uid, name, label, description, text, strategy"
            " FROM intercurrent_event"
            " WHERE soa_id=? AND estimand_id=? ORDER BY order_index, id",
            (soa_id, eid),
        )
        intercurrent_events = [
            {
                "id": ir[0],
                "event_uid": ir[1],
                "name": ir[2],
                "label": ir[3] or "",
                "description": ir[4] or "",
                "text": ir[5] or "",
                "strategy": ir[6] or "",
            }
            for ir in cur.fetchall()
        ]

        result.append(
            {
                "id": eid,
                "estimand_uid": estimand_uid,
                "name": name,
                "label": label or "",
                "description": description or "",
                "population_summary": population_summary or "",
                "variable_uids": variable_uids,
                "variable_names": [
                    endpoint_name_map.get(uid, uid) for uid in variable_uids
                ],
                "intervention_uids": intervention_uids,
                "intervention_names": [
                    intervention_name_map.get(uid, uid) for uid in intervention_uids
                ],
                "intercurrent_events": intercurrent_events,
            }
        )
    conn.close()
    return result


# ---------------------------------------------------------------------------
# JSON API endpoints
# ---------------------------------------------------------------------------


@router.get("/estimands", response_class=JSONResponse)
def list_estimands(soa_id: int):
    if not soa_exists(soa_id):
        raise HTTPException(404, "SOA not found")
    return _list_estimands(soa_id)


@router.post("/estimands", status_code=201, response_class=JSONResponse)
def create_estimand(soa_id: int, body: dict):
    if not soa_exists(soa_id):
        raise HTTPException(404, "SOA not found")
    name = (body.get("name") or "").strip()
    if not name:
        raise HTTPException(400, "name is required")

    intervention_uids = [u for u in (body.get("intervention_uids") or []) if u]
    variable_uids = [u for u in (body.get("variable_uids") or []) if u]

    conn = _connect()
    cur = conn.cursor()
    try:
        cur.execute(
            "SELECT COALESCE(MAX(order_index), 0) FROM estimand WHERE soa_id=?",
            (soa_id,),
        )
        order_index = (cur.fetchone()[0] or 0) + 1
        estimand_uid = _next_estimand_uid(cur, soa_id)
        cur.execute(
            "INSERT INTO estimand"
            " (soa_id, estimand_uid, name, label, description,"
            " population_summary, order_index)"
            " VALUES (?,?,?,?,?,?,?)",
            (
                soa_id,
                estimand_uid,
                name,
                body.get("label") or None,
                body.get("description") or None,
                body.get("population_summary") or None,
                order_index,
            ),
        )
        estimand_id = cur.lastrowid
        for idx, uid in enumerate(variable_uids, start=1):
            cur.execute(
                "INSERT INTO estimand_variable"
                " (soa_id, estimand_id, endpoint_uid, order_index)"
                " VALUES (?,?,?,?)",
                (soa_id, estimand_id, uid, idx),
            )
        for idx, uid in enumerate(intervention_uids, start=1):
            cur.execute(
                "INSERT INTO estimand_intervention"
                " (soa_id, estimand_id, intervention_uid, order_index)"
                " VALUES (?,?,?,?)",
                (soa_id, estimand_id, uid, idx),
            )
        conn.commit()
    except Exception:
        conn.rollback()
        conn.close()
        raise
    conn.close()
    _record_estimand_audit(
        soa_id,
        "create",
        estimand_id,
        after={"estimand_uid": estimand_uid, "name": name},
    )
    return {"id": estimand_id, "estimand_uid": estimand_uid, "name": name}


@router.delete(
    "/estimands/{estimand_id}",
    status_code=200,
    response_class=JSONResponse,
)
def delete_estimand(soa_id: int, estimand_id: int):
    if not soa_exists(soa_id):
        raise HTTPException(404, "SOA not found")
    conn = _connect()
    cur = conn.cursor()
    cur.execute(
        "SELECT id, estimand_uid, name FROM estimand WHERE id=? AND soa_id=?",
        (estimand_id, soa_id),
    )
    row = cur.fetchone()
    if not row:
        conn.close()
        raise HTTPException(404, "Estimand not found")
    (eid, estimand_uid, name) = row
    before = {"estimand_uid": estimand_uid, "name": name}

    cur.execute(
        "DELETE FROM intercurrent_event WHERE soa_id=? AND estimand_id=?",
        (soa_id, eid),
    )
    cur.execute(
        "DELETE FROM estimand_intervention WHERE soa_id=? AND estimand_id=?",
        (soa_id, eid),
    )
    cur.execute(
        "DELETE FROM estimand_variable WHERE soa_id=? AND estimand_id=?",
        (soa_id, eid),
    )
    cur.execute(
        "DELETE FROM estimand WHERE id=? AND soa_id=?",
        (eid, soa_id),
    )
    cur.execute(
        "SELECT id FROM estimand WHERE soa_id=? ORDER BY order_index, id",
        (soa_id,),
    )
    for idx, (r,) in enumerate(cur.fetchall(), start=1):
        cur.execute("UPDATE estimand SET order_index=? WHERE id=?", (idx, r))
    conn.commit()
    conn.close()
    _record_estimand_audit(soa_id, "delete", eid, before=before)
    return {"deleted": estimand_uid}


# ---------------------------------------------------------------------------
# JSON API: variable-of-interest (endpoint) sub-resource
# ---------------------------------------------------------------------------


@router.post(
    "/estimands/{estimand_id}/variables",
    status_code=201,
    response_class=JSONResponse,
)
def add_estimand_variable(soa_id: int, estimand_id: int, body: dict):
    if not soa_exists(soa_id):
        raise HTTPException(404, "SOA not found")
    endpoint_uid = (body.get("endpoint_uid") or "").strip()
    if not endpoint_uid:
        raise HTTPException(400, "endpoint_uid is required")

    conn = _connect()
    cur = conn.cursor()
    cur.execute(
        "SELECT id FROM estimand WHERE id=? AND soa_id=?",
        (estimand_id, soa_id),
    )
    if not cur.fetchone():
        conn.close()
        raise HTTPException(404, "Estimand not found")

    cur.execute(
        "SELECT id FROM estimand_variable"
        " WHERE soa_id=? AND estimand_id=? AND endpoint_uid=?",
        (soa_id, estimand_id, endpoint_uid),
    )
    if cur.fetchone():
        conn.close()
        return {"id": None, "endpoint_uid": endpoint_uid}

    try:
        cur.execute(
            "SELECT COALESCE(MAX(order_index), 0)"
            " FROM estimand_variable WHERE soa_id=? AND estimand_id=?",
            (soa_id, estimand_id),
        )
        order_index = (cur.fetchone()[0] or 0) + 1
        cur.execute(
            "INSERT INTO estimand_variable"
            " (soa_id, estimand_id, endpoint_uid, order_index)"
            " VALUES (?,?,?,?)",
            (soa_id, estimand_id, endpoint_uid, order_index),
        )
        link_id = cur.lastrowid
        conn.commit()
    except Exception:
        conn.rollback()
        conn.close()
        raise
    conn.close()
    return {"id": link_id, "endpoint_uid": endpoint_uid}


@router.delete(
    "/estimands/{estimand_id}/variables/{endpoint_uid}",
    status_code=200,
    response_class=JSONResponse,
)
def remove_estimand_variable(soa_id: int, estimand_id: int, endpoint_uid: str):
    if not soa_exists(soa_id):
        raise HTTPException(404, "SOA not found")
    conn = _connect()
    cur = conn.cursor()
    cur.execute(
        "DELETE FROM estimand_variable"
        " WHERE soa_id=? AND estimand_id=? AND endpoint_uid=?",
        (soa_id, estimand_id, endpoint_uid),
    )
    conn.commit()
    conn.close()
    return {"deleted": endpoint_uid}


# ---------------------------------------------------------------------------
# JSON API: intercurrent-events sub-resource
# ---------------------------------------------------------------------------


@router.post(
    "/estimands/{estimand_id}/intercurrent-events",
    status_code=201,
    response_class=JSONResponse,
)
def add_intercurrent_event(soa_id: int, estimand_id: int, body: dict):
    if not soa_exists(soa_id):
        raise HTTPException(404, "SOA not found")
    name = (body.get("name") or "").strip()
    if not name:
        raise HTTPException(400, "name is required")

    conn = _connect()
    cur = conn.cursor()
    cur.execute(
        "SELECT id FROM estimand WHERE id=? AND soa_id=?",
        (estimand_id, soa_id),
    )
    if not cur.fetchone():
        conn.close()
        raise HTTPException(404, "Estimand not found")

    try:
        cur.execute(
            "SELECT COALESCE(MAX(order_index), 0)"
            " FROM intercurrent_event WHERE soa_id=? AND estimand_id=?",
            (soa_id, estimand_id),
        )
        order_index = (cur.fetchone()[0] or 0) + 1
        event_uid = get_next_intercurrent_event_uid(cur, soa_id)
        cur.execute(
            "INSERT INTO intercurrent_event"
            " (soa_id, estimand_id, event_uid, name, label, description,"
            " text, strategy, order_index)"
            " VALUES (?,?,?,?,?,?,?,?,?)",
            (
                soa_id,
                estimand_id,
                event_uid,
                name,
                body.get("label") or None,
                body.get("description") or None,
                body.get("text") or None,
                body.get("strategy") or None,
                order_index,
            ),
        )
        ice_id = cur.lastrowid
        conn.commit()
    except Exception:
        conn.rollback()
        conn.close()
        raise
    conn.close()
    return {"id": ice_id, "event_uid": event_uid}


@router.delete(
    "/estimands/{estimand_id}/intercurrent-events/{ice_id}",
    status_code=200,
    response_class=JSONResponse,
)
def delete_intercurrent_event(soa_id: int, estimand_id: int, ice_id: int):
    if not soa_exists(soa_id):
        raise HTTPException(404, "SOA not found")
    conn = _connect()
    cur = conn.cursor()
    cur.execute(
        "SELECT id, event_uid FROM intercurrent_event"
        " WHERE id=? AND soa_id=? AND estimand_id=?",
        (ice_id, soa_id, estimand_id),
    )
    row = cur.fetchone()
    if not row:
        conn.close()
        raise HTTPException(404, "Intercurrent event not found")
    (rid, event_uid) = row
    cur.execute("DELETE FROM intercurrent_event WHERE id=?", (rid,))
    conn.commit()
    conn.close()
    return {"deleted": event_uid}


# ---------------------------------------------------------------------------
# JSON API: intervention link sub-resource
# ---------------------------------------------------------------------------


@router.post(
    "/estimands/{estimand_id}/interventions",
    status_code=201,
    response_class=JSONResponse,
)
def add_estimand_intervention(soa_id: int, estimand_id: int, body: dict):
    if not soa_exists(soa_id):
        raise HTTPException(404, "SOA not found")
    intervention_uid = (body.get("intervention_uid") or "").strip()
    if not intervention_uid:
        raise HTTPException(400, "intervention_uid is required")

    conn = _connect()
    cur = conn.cursor()
    cur.execute(
        "SELECT id FROM estimand WHERE id=? AND soa_id=?",
        (estimand_id, soa_id),
    )
    if not cur.fetchone():
        conn.close()
        raise HTTPException(404, "Estimand not found")

    # Check not already linked
    cur.execute(
        "SELECT id FROM estimand_intervention"
        " WHERE soa_id=? AND estimand_id=? AND intervention_uid=?",
        (soa_id, estimand_id, intervention_uid),
    )
    if cur.fetchone():
        conn.close()
        return {"id": None, "intervention_uid": intervention_uid}

    try:
        cur.execute(
            "SELECT COALESCE(MAX(order_index), 0)"
            " FROM estimand_intervention WHERE soa_id=? AND estimand_id=?",
            (soa_id, estimand_id),
        )
        order_index = (cur.fetchone()[0] or 0) + 1
        cur.execute(
            "INSERT INTO estimand_intervention"
            " (soa_id, estimand_id, intervention_uid, order_index)"
            " VALUES (?,?,?,?)",
            (soa_id, estimand_id, intervention_uid, order_index),
        )
        link_id = cur.lastrowid
        conn.commit()
    except Exception:
        conn.rollback()
        conn.close()
        raise
    conn.close()
    return {"id": link_id, "intervention_uid": intervention_uid}


@router.delete(
    "/estimands/{estimand_id}/interventions/{intervention_uid}",
    status_code=200,
    response_class=JSONResponse,
)
def remove_estimand_intervention(soa_id: int, estimand_id: int, intervention_uid: str):
    if not soa_exists(soa_id):
        raise HTTPException(404, "SOA not found")
    conn = _connect()
    cur = conn.cursor()
    cur.execute(
        "DELETE FROM estimand_intervention"
        " WHERE soa_id=? AND estimand_id=? AND intervention_uid=?",
        (soa_id, estimand_id, intervention_uid),
    )
    conn.commit()
    conn.close()
    return {"deleted": intervention_uid}


# ---------------------------------------------------------------------------
# HTMX UI endpoints
# ---------------------------------------------------------------------------


def _partial_response(request: Request, soa_id: int) -> HTMLResponse:
    estimands = _list_estimands(soa_id)
    available_interventions = _list_available_interventions(soa_id)
    available_endpoints = _list_available_endpoints(soa_id)
    return templates.TemplateResponse(
        request,
        "estimands_partial.html",
        {
            "soa_id": soa_id,
            "estimands": estimands,
            "available_interventions": available_interventions,
            "available_endpoints": available_endpoints,
        },
    )


@ui_router.get(
    "/ui/soa/{soa_id}/estimands",
    response_class=HTMLResponse,
    name="ui_list_estimands",
)
def ui_list_estimands(request: Request, soa_id: int):
    if not soa_exists(soa_id):
        raise HTTPException(404, "SOA not found")
    conn = _connect()
    cur = conn.cursor()
    cur.execute("SELECT name, study_label FROM soa WHERE id=?", (soa_id,))
    row = cur.fetchone()
    conn.close()
    soa_name = row[0] if row else ""
    study_label = row[1] if row else None
    estimands = _list_estimands(soa_id)
    available_interventions = _list_available_interventions(soa_id)
    available_endpoints = _list_available_endpoints(soa_id)
    indications = _list_indications(soa_id)
    return templates.TemplateResponse(
        request,
        "estimands.html",
        {
            "soa_id": soa_id,
            "soa_name": soa_name,
            "study_label": study_label,
            "estimands": estimands,
            "available_interventions": available_interventions,
            "available_endpoints": available_endpoints,
            "indications": indications,
        },
    )


@ui_router.post(
    "/ui/soa/{soa_id}/estimands-add",
    response_class=HTMLResponse,
)
def ui_estimands_add(
    request: Request,
    soa_id: int,
    name: str = Form(""),
    label: str = Form(""),
    description: str = Form(""),
    population_summary: str = Form(""),
    variable_uids: List[str] = Form(default=[]),
    intervention_uids: List[str] = Form(default=[]),
):
    if not soa_exists(soa_id):
        raise HTTPException(404, "SOA not found")
    name = name.strip()
    if not name:
        raise HTTPException(400, "name is required")

    linked_vars = [u for u in variable_uids if u.strip()]
    linked_uids = [u for u in intervention_uids if u.strip()]

    conn = _connect()
    cur = conn.cursor()
    try:
        cur.execute(
            "SELECT COALESCE(MAX(order_index), 0) FROM estimand WHERE soa_id=?",
            (soa_id,),
        )
        order_index = (cur.fetchone()[0] or 0) + 1
        estimand_uid = _next_estimand_uid(cur, soa_id)
        cur.execute(
            "INSERT INTO estimand"
            " (soa_id, estimand_uid, name, label, description,"
            " population_summary, order_index)"
            " VALUES (?,?,?,?,?,?,?)",
            (
                soa_id,
                estimand_uid,
                name,
                label.strip() or None,
                description.strip() or None,
                population_summary.strip() or None,
                order_index,
            ),
        )
        estimand_id = cur.lastrowid
        for idx, uid in enumerate(linked_vars, start=1):
            cur.execute(
                "INSERT INTO estimand_variable"
                " (soa_id, estimand_id, endpoint_uid, order_index)"
                " VALUES (?,?,?,?)",
                (soa_id, estimand_id, uid, idx),
            )
        for idx, uid in enumerate(linked_uids, start=1):
            cur.execute(
                "INSERT INTO estimand_intervention"
                " (soa_id, estimand_id, intervention_uid, order_index)"
                " VALUES (?,?,?,?)",
                (soa_id, estimand_id, uid, idx),
            )
        conn.commit()
    except Exception:
        conn.rollback()
        conn.close()
        raise
    conn.close()
    _record_estimand_audit(
        soa_id,
        "create",
        estimand_id,
        after={"estimand_uid": estimand_uid, "name": name},
    )
    return _partial_response(request, soa_id)


@ui_router.post(
    "/ui/soa/{soa_id}/estimands/{estimand_id}/delete",
    response_class=HTMLResponse,
)
def ui_estimands_delete(request: Request, soa_id: int, estimand_id: int):
    if not soa_exists(soa_id):
        raise HTTPException(404, "SOA not found")
    conn = _connect()
    cur = conn.cursor()
    cur.execute(
        "SELECT id, estimand_uid, name FROM estimand WHERE id=? AND soa_id=?",
        (estimand_id, soa_id),
    )
    row = cur.fetchone()
    if not row:
        conn.close()
        raise HTTPException(404, "Estimand not found")
    (eid, estimand_uid, name) = row
    before = {"estimand_uid": estimand_uid, "name": name}

    cur.execute(
        "DELETE FROM intercurrent_event WHERE soa_id=? AND estimand_id=?",
        (soa_id, eid),
    )
    cur.execute(
        "DELETE FROM estimand_intervention WHERE soa_id=? AND estimand_id=?",
        (soa_id, eid),
    )
    cur.execute(
        "DELETE FROM estimand_variable WHERE soa_id=? AND estimand_id=?",
        (soa_id, eid),
    )
    cur.execute(
        "DELETE FROM estimand WHERE id=? AND soa_id=?",
        (eid, soa_id),
    )
    cur.execute(
        "SELECT id FROM estimand WHERE soa_id=? ORDER BY order_index, id",
        (soa_id,),
    )
    for idx, (r,) in enumerate(cur.fetchall(), start=1):
        cur.execute("UPDATE estimand SET order_index=? WHERE id=?", (idx, r))
    conn.commit()
    conn.close()
    _record_estimand_audit(soa_id, "delete", eid, before=before)
    return _partial_response(request, soa_id)


@ui_router.post(
    "/ui/soa/{soa_id}/estimands/{estimand_id}/variables-add",
    response_class=HTMLResponse,
)
def ui_estimand_variables_add(
    request: Request,
    soa_id: int,
    estimand_id: int,
    endpoint_uid: str = Form(""),
):
    if not soa_exists(soa_id):
        raise HTTPException(404, "SOA not found")
    endpoint_uid = endpoint_uid.strip()
    if not endpoint_uid:
        return _partial_response(request, soa_id)

    conn = _connect()
    cur = conn.cursor()
    cur.execute(
        "SELECT id FROM estimand WHERE id=? AND soa_id=?",
        (estimand_id, soa_id),
    )
    if not cur.fetchone():
        conn.close()
        raise HTTPException(404, "Estimand not found")

    cur.execute(
        "SELECT id FROM estimand_variable"
        " WHERE soa_id=? AND estimand_id=? AND endpoint_uid=?",
        (soa_id, estimand_id, endpoint_uid),
    )
    if not cur.fetchone():
        try:
            cur.execute(
                "SELECT COALESCE(MAX(order_index), 0)"
                " FROM estimand_variable WHERE soa_id=? AND estimand_id=?",
                (soa_id, estimand_id),
            )
            order_index = (cur.fetchone()[0] or 0) + 1
            cur.execute(
                "INSERT INTO estimand_variable"
                " (soa_id, estimand_id, endpoint_uid, order_index)"
                " VALUES (?,?,?,?)",
                (soa_id, estimand_id, endpoint_uid, order_index),
            )
            conn.commit()
        except Exception:
            conn.rollback()
            conn.close()
            raise
    conn.close()
    return _partial_response(request, soa_id)


@ui_router.post(
    "/ui/soa/{soa_id}/estimands/{estimand_id}/variables/{endpoint_uid}/delete",
    response_class=HTMLResponse,
)
def ui_estimand_variables_delete(
    request: Request,
    soa_id: int,
    estimand_id: int,
    endpoint_uid: str,
):
    if not soa_exists(soa_id):
        raise HTTPException(404, "SOA not found")
    conn = _connect()
    cur = conn.cursor()
    cur.execute(
        "DELETE FROM estimand_variable"
        " WHERE soa_id=? AND estimand_id=? AND endpoint_uid=?",
        (soa_id, estimand_id, endpoint_uid),
    )
    conn.commit()
    conn.close()
    return _partial_response(request, soa_id)


@ui_router.post(
    "/ui/soa/{soa_id}/estimands/{estimand_id}/intercurrent-events-add",
    response_class=HTMLResponse,
)
def ui_intercurrent_events_add(
    request: Request,
    soa_id: int,
    estimand_id: int,
    name: str = Form(""),
    label: str = Form(""),
    description: str = Form(""),
    text: str = Form(""),
    strategy: str = Form(""),
):
    if not soa_exists(soa_id):
        raise HTTPException(404, "SOA not found")
    name = name.strip()
    if not name:
        raise HTTPException(400, "name is required")

    conn = _connect()
    cur = conn.cursor()
    cur.execute(
        "SELECT id FROM estimand WHERE id=? AND soa_id=?",
        (estimand_id, soa_id),
    )
    if not cur.fetchone():
        conn.close()
        raise HTTPException(404, "Estimand not found")
    try:
        cur.execute(
            "SELECT COALESCE(MAX(order_index), 0)"
            " FROM intercurrent_event WHERE soa_id=? AND estimand_id=?",
            (soa_id, estimand_id),
        )
        order_index = (cur.fetchone()[0] or 0) + 1
        event_uid = get_next_intercurrent_event_uid(cur, soa_id)
        cur.execute(
            "INSERT INTO intercurrent_event"
            " (soa_id, estimand_id, event_uid, name, label, description,"
            " text, strategy, order_index)"
            " VALUES (?,?,?,?,?,?,?,?,?)",
            (
                soa_id,
                estimand_id,
                event_uid,
                name,
                label.strip() or None,
                description.strip() or None,
                text.strip() or None,
                strategy.strip() or None,
                order_index,
            ),
        )
        conn.commit()
    except Exception:
        conn.rollback()
        conn.close()
        raise
    conn.close()
    return _partial_response(request, soa_id)


@ui_router.post(
    "/ui/soa/{soa_id}/estimands/{estimand_id}/intercurrent-events/{ice_id}/delete",
    response_class=HTMLResponse,
)
def ui_intercurrent_events_delete(
    request: Request,
    soa_id: int,
    estimand_id: int,
    ice_id: int,
):
    if not soa_exists(soa_id):
        raise HTTPException(404, "SOA not found")
    conn = _connect()
    cur = conn.cursor()
    cur.execute(
        "SELECT id FROM intercurrent_event WHERE id=? AND soa_id=? AND estimand_id=?",
        (ice_id, soa_id, estimand_id),
    )
    if not cur.fetchone():
        conn.close()
        raise HTTPException(404, "Intercurrent event not found")
    cur.execute("DELETE FROM intercurrent_event WHERE id=?", (ice_id,))
    conn.commit()
    conn.close()
    return _partial_response(request, soa_id)


@ui_router.post(
    "/ui/soa/{soa_id}/estimands/{estimand_id}/interventions-add",
    response_class=HTMLResponse,
)
def ui_estimand_interventions_add(
    request: Request,
    soa_id: int,
    estimand_id: int,
    intervention_uid: str = Form(""),
):
    if not soa_exists(soa_id):
        raise HTTPException(404, "SOA not found")
    intervention_uid = intervention_uid.strip()
    if not intervention_uid:
        return _partial_response(request, soa_id)

    conn = _connect()
    cur = conn.cursor()
    cur.execute(
        "SELECT id FROM estimand WHERE id=? AND soa_id=?",
        (estimand_id, soa_id),
    )
    if not cur.fetchone():
        conn.close()
        raise HTTPException(404, "Estimand not found")

    # Skip if already linked
    cur.execute(
        "SELECT id FROM estimand_intervention"
        " WHERE soa_id=? AND estimand_id=? AND intervention_uid=?",
        (soa_id, estimand_id, intervention_uid),
    )
    if not cur.fetchone():
        try:
            cur.execute(
                "SELECT COALESCE(MAX(order_index), 0)"
                " FROM estimand_intervention WHERE soa_id=? AND estimand_id=?",
                (soa_id, estimand_id),
            )
            order_index = (cur.fetchone()[0] or 0) + 1
            cur.execute(
                "INSERT INTO estimand_intervention"
                " (soa_id, estimand_id, intervention_uid, order_index)"
                " VALUES (?,?,?,?)",
                (soa_id, estimand_id, intervention_uid, order_index),
            )
            conn.commit()
        except Exception:
            conn.rollback()
            conn.close()
            raise
    conn.close()
    return _partial_response(request, soa_id)


@ui_router.post(
    "/ui/soa/{soa_id}/estimands/{estimand_id}/interventions/{intervention_uid}/delete",
    response_class=HTMLResponse,
)
def ui_estimand_interventions_delete(
    request: Request,
    soa_id: int,
    estimand_id: int,
    intervention_uid: str,
):
    if not soa_exists(soa_id):
        raise HTTPException(404, "SOA not found")
    conn = _connect()
    cur = conn.cursor()
    cur.execute(
        "DELETE FROM estimand_intervention"
        " WHERE soa_id=? AND estimand_id=? AND intervention_uid=?",
        (soa_id, estimand_id, intervention_uid),
    )
    conn.commit()
    conn.close()
    return _partial_response(request, soa_id)
