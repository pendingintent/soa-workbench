import json
import logging

from fastapi import APIRouter, Form, HTTPException, Request
from fastapi.responses import HTMLResponse, JSONResponse, RedirectResponse

from ..audit import _record_endpoint_audit
from ..db import _connect
from ..schemas import EndpointCreate, EndpointUpdate
from ..utils import (
    get_latest_ddf_ct_href,
    get_next_code_uid,
    soa_exists,
)

router = APIRouter(prefix="/soa/{soa_id}")
ui_router = APIRouter()
logger = logging.getLogger("soa_builder.web.routers.endpoints")

_ENDPOINT_LEVEL_CODELIST = "C188726"


def _next_endpoint_uid(cur, soa_id: int) -> str:
    """Return next Endpoint_N UID, never reusing deleted UIDs."""
    max_n = 0
    cur.execute(
        "SELECT endpoint_uid FROM endpoint WHERE soa_id=? "
        "AND endpoint_uid LIKE 'Endpoint_%'",
        (soa_id,),
    )
    for (uid,) in cur.fetchall():
        if isinstance(uid, str) and uid.startswith("Endpoint_"):
            try:
                n = int(uid.split("_")[-1])
                if n > max_n:
                    max_n = n
            except (ValueError, IndexError):
                pass
    cur.execute(
        "SELECT before_json, after_json FROM endpoint_audit WHERE soa_id=?",
        (soa_id,),
    )
    for before_raw, after_raw in cur.fetchall():
        for raw in (before_raw, after_raw):
            if not raw:
                continue
            try:
                uid = json.loads(raw).get("endpoint_uid", "")
                if isinstance(uid, str) and uid.startswith("Endpoint_"):
                    n = int(uid.split("_")[-1])
                    if n > max_n:
                        max_n = n
            except Exception:
                pass
    return f"Endpoint_{max_n + 1}"


def _row_to_dict(row) -> dict:
    keys = [
        "id",
        "soa_id",
        "endpoint_uid",
        "objective_uid",
        "name",
        "label",
        "description",
        "text",
        "purpose",
        "level_code_uid",
        "order_index",
    ]
    return dict(zip(keys, row))


def _objective_exists(cur, soa_id: int, objective_uid: str) -> bool:
    cur.execute(
        "SELECT 1 FROM objective WHERE soa_id=? AND objective_uid=?",
        (soa_id, objective_uid),
    )
    return cur.fetchone() is not None


def _insert_level_code(cur, soa_id: int, submission_value: str) -> str:
    code_uid = get_next_code_uid(cur, soa_id)
    slug = get_latest_ddf_ct_href() or ""
    codelist_table = f"/mdr/ct/packages/{slug}" if slug else "/mdr/ct/packages"
    cur.execute(
        "INSERT INTO code_association "
        "(soa_id, code_uid, codelist_table, codelist_code, code) "
        "VALUES (?,?,?,?,?)",
        (
            soa_id,
            code_uid,
            codelist_table,
            _ENDPOINT_LEVEL_CODELIST,
            submission_value,
        ),
    )
    return code_uid


def _delete_level_code(cur, soa_id: int, code_uid: str | None) -> None:
    if not code_uid:
        return
    cur.execute(
        "DELETE FROM code_association WHERE soa_id=? AND code_uid=?",
        (soa_id, code_uid),
    )


# ---------------------------------------------------------------------------
# JSON API endpoints
# ---------------------------------------------------------------------------


@router.get("/endpoints", response_class=JSONResponse)
def list_endpoints(soa_id: int):
    if not soa_exists(soa_id):
        raise HTTPException(404, "SOA not found")
    conn = _connect()
    cur = conn.cursor()
    cur.execute(
        "SELECT id,soa_id,endpoint_uid,objective_uid,name,label,"
        "description,text,purpose,level_code_uid,order_index "
        "FROM endpoint WHERE soa_id=? ORDER BY order_index, id",
        (soa_id,),
    )
    rows = [_row_to_dict(r) for r in cur.fetchall()]
    conn.close()
    return JSONResponse(rows)


@router.post("/endpoints", response_class=JSONResponse)
def create_endpoint(soa_id: int, body: EndpointCreate):
    if not soa_exists(soa_id):
        raise HTTPException(404, "SOA not found")
    name = (body.name or "").strip()
    level = (body.level or "").strip()
    objective_uid = (body.objective_uid or "").strip()
    if not name:
        raise HTTPException(400, "Endpoint name required")
    if not level:
        raise HTTPException(400, "Endpoint level required")
    if not objective_uid:
        raise HTTPException(400, "Parent objective_uid required")

    conn = _connect()
    cur = conn.cursor()
    if not _objective_exists(cur, soa_id, objective_uid):
        conn.close()
        raise HTTPException(400, f"Objective {objective_uid!r} not found for this SOA")

    cur.execute(
        "SELECT COALESCE(MAX(order_index),0) FROM endpoint WHERE soa_id=?",
        (soa_id,),
    )
    next_ord = (cur.fetchone() or [0])[0] + 1
    endpoint_uid = _next_endpoint_uid(cur, soa_id)
    level_code_uid = _insert_level_code(cur, soa_id, level)

    label = (body.label or "").strip() or None
    description = (body.description or "").strip() or None
    text = (body.text or "").strip() or None
    purpose = (body.purpose or "").strip() or None

    cur.execute(
        "INSERT INTO endpoint "
        "(soa_id,endpoint_uid,objective_uid,name,label,description,"
        "text,purpose,level_code_uid,order_index) "
        "VALUES (?,?,?,?,?,?,?,?,?,?)",
        (
            soa_id,
            endpoint_uid,
            objective_uid,
            name,
            label,
            description,
            text,
            purpose,
            level_code_uid,
            next_ord,
        ),
    )
    endpoint_id = cur.lastrowid
    conn.commit()
    conn.close()

    after = {
        "id": endpoint_id,
        "endpoint_uid": endpoint_uid,
        "objective_uid": objective_uid,
        "name": name,
        "label": label,
        "description": description,
        "text": text,
        "purpose": purpose,
        "level_code_uid": level_code_uid,
        "level": level,
        "order_index": next_ord,
    }
    _record_endpoint_audit(soa_id, "create", endpoint_id, before=None, after=after)
    return JSONResponse(after, status_code=201)


@router.patch("/endpoints/{endpoint_id}", response_class=JSONResponse)
def update_endpoint(soa_id: int, endpoint_id: int, body: EndpointUpdate):
    if not soa_exists(soa_id):
        raise HTTPException(404, "SOA not found")
    conn = _connect()
    cur = conn.cursor()
    cur.execute(
        "SELECT id,soa_id,endpoint_uid,objective_uid,name,label,"
        "description,text,purpose,level_code_uid,order_index "
        "FROM endpoint WHERE id=? AND soa_id=?",
        (endpoint_id, soa_id),
    )
    row = cur.fetchone()
    if not row:
        conn.close()
        raise HTTPException(404, "Endpoint not found")
    before = _row_to_dict(row)

    new_objective_uid = before["objective_uid"]
    if body.objective_uid is not None:
        candidate = body.objective_uid.strip() or None
        if candidate is not None and not _objective_exists(cur, soa_id, candidate):
            conn.close()
            raise HTTPException(400, f"Objective {candidate!r} not found for this SOA")
        new_objective_uid = candidate

    if body.name is not None:
        new_name = body.name.strip()
        if not new_name:
            conn.close()
            raise HTTPException(400, "Endpoint name cannot be empty")
    else:
        new_name = before["name"]
    new_label = (
        (body.label.strip() or None) if body.label is not None else before["label"]
    )
    new_desc = (
        (body.description.strip() or None)
        if body.description is not None
        else before["description"]
    )
    new_text = (body.text.strip() or None) if body.text is not None else before["text"]
    new_purpose = (
        (body.purpose.strip() or None)
        if body.purpose is not None
        else before["purpose"]
    )

    new_level_code_uid = before["level_code_uid"]
    if body.level is not None:
        new_level = body.level.strip()
        if not new_level:
            conn.close()
            raise HTTPException(400, "Endpoint level cannot be empty")
        if before["level_code_uid"]:
            cur.execute(
                "UPDATE code_association SET code=? WHERE soa_id=? AND code_uid=?",
                (new_level, soa_id, before["level_code_uid"]),
            )
        else:
            new_level_code_uid = _insert_level_code(cur, soa_id, new_level)
    else:
        cur.execute(
            "SELECT code FROM code_association WHERE soa_id=? AND code_uid=?",
            (soa_id, before["level_code_uid"]),
        )
        lvl_row = cur.fetchone()
        new_level = lvl_row[0] if lvl_row else None

    cur.execute(
        "UPDATE endpoint SET objective_uid=?, name=?, label=?, "
        "description=?, text=?, purpose=?, level_code_uid=? "
        "WHERE id=? AND soa_id=?",
        (
            new_objective_uid,
            new_name,
            new_label,
            new_desc,
            new_text,
            new_purpose,
            new_level_code_uid,
            endpoint_id,
            soa_id,
        ),
    )
    conn.commit()
    conn.close()

    after = {
        **before,
        "objective_uid": new_objective_uid,
        "name": new_name,
        "label": new_label,
        "description": new_desc,
        "text": new_text,
        "purpose": new_purpose,
        "level_code_uid": new_level_code_uid,
        "level": new_level,
    }
    _record_endpoint_audit(soa_id, "update", endpoint_id, before=before, after=after)
    return JSONResponse(after)


@router.delete("/endpoints/{endpoint_id}", response_class=JSONResponse)
def delete_endpoint(soa_id: int, endpoint_id: int):
    if not soa_exists(soa_id):
        raise HTTPException(404, "SOA not found")
    conn = _connect()
    cur = conn.cursor()
    cur.execute(
        "SELECT id,soa_id,endpoint_uid,objective_uid,name,label,"
        "description,text,purpose,level_code_uid,order_index "
        "FROM endpoint WHERE id=? AND soa_id=?",
        (endpoint_id, soa_id),
    )
    row = cur.fetchone()
    if not row:
        conn.close()
        raise HTTPException(404, "Endpoint not found")
    before = _row_to_dict(row)

    _delete_level_code(cur, soa_id, before["level_code_uid"])
    cur.execute(
        "DELETE FROM endpoint WHERE id=? AND soa_id=?",
        (endpoint_id, soa_id),
    )

    # Reindex remaining endpoints
    cur.execute(
        "SELECT id FROM endpoint WHERE soa_id=? ORDER BY order_index, id",
        (soa_id,),
    )
    remaining = [r[0] for r in cur.fetchall()]
    for idx, eid in enumerate(remaining, start=1):
        cur.execute("UPDATE endpoint SET order_index=? WHERE id=?", (idx, eid))
    conn.commit()
    conn.close()

    _record_endpoint_audit(soa_id, "delete", endpoint_id, before=before, after=None)
    return JSONResponse({"deleted": endpoint_id})


# ---------------------------------------------------------------------------
# UI form endpoints
# ---------------------------------------------------------------------------


@ui_router.post("/ui/soa/{soa_id}/endpoints/create", response_class=HTMLResponse)
def ui_create_endpoint(
    request: Request,
    soa_id: int,
    name: str = Form(...),
    level: str = Form(...),
    objective_uid: str = Form(...),
    label: str | None = Form(None),
    description: str | None = Form(None),
    text: str | None = Form(None),
    purpose: str | None = Form(None),
):
    if not soa_exists(soa_id):
        raise HTTPException(404, "SOA not found")
    create_endpoint(
        soa_id,
        EndpointCreate(
            name=name,
            level=level,
            objective_uid=objective_uid,
            label=label,
            description=description,
            text=text,
            purpose=purpose,
        ),
    )
    safe_soa_id = int(soa_id)
    redirect_url = f"/ui/soa/{safe_soa_id}/objectives"
    if request.headers.get("HX-Request") == "true":
        return HTMLResponse("", headers={"HX-Redirect": redirect_url})
    return RedirectResponse(redirect_url, status_code=303)


@ui_router.post(
    "/ui/soa/{soa_id}/endpoints/{endpoint_id}/update",
    response_class=HTMLResponse,
)
def ui_update_endpoint(
    request: Request,
    soa_id: int,
    endpoint_id: int,
    name: str | None = Form(None),
    level: str | None = Form(None),
    objective_uid: str | None = Form(None),
    label: str | None = Form(None),
    description: str | None = Form(None),
    text: str | None = Form(None),
    purpose: str | None = Form(None),
):
    if not soa_exists(soa_id):
        raise HTTPException(404, "SOA not found")
    update_endpoint(
        soa_id,
        endpoint_id,
        EndpointUpdate(
            name=name,
            level=level,
            objective_uid=objective_uid,
            label=label,
            description=description,
            text=text,
            purpose=purpose,
        ),
    )
    safe_soa_id = int(soa_id)
    redirect_url = f"/ui/soa/{safe_soa_id}/objectives"
    if request.headers.get("HX-Request") == "true":
        return HTMLResponse("", headers={"HX-Redirect": redirect_url})
    return RedirectResponse(redirect_url, status_code=303)


@ui_router.post(
    "/ui/soa/{soa_id}/endpoints/{endpoint_id}/delete",
    response_class=HTMLResponse,
)
def ui_delete_endpoint(
    request: Request,
    soa_id: int,
    endpoint_id: int,
):
    if not soa_exists(soa_id):
        raise HTTPException(404, "SOA not found")
    delete_endpoint(soa_id, endpoint_id)
    safe_soa_id = int(soa_id)
    redirect_url = f"/ui/soa/{safe_soa_id}/objectives"
    if request.headers.get("HX-Request") == "true":
        return HTMLResponse("", headers={"HX-Redirect": redirect_url})
    return RedirectResponse(redirect_url, status_code=303)
