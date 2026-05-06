import html
import json
import logging
import os

from fastapi import APIRouter, Form, HTTPException, Request
from fastapi.responses import HTMLResponse, JSONResponse
from fastapi.templating import Jinja2Templates

from ..audit import _record_endpoint_audit, _record_objective_audit
from ..db import _connect
from ..schemas import ObjectiveCreate, ObjectiveUpdate
from ..utils import (
    get_ddf_ct_codelist_map,
    get_latest_ddf_ct_href,
    get_next_code_uid,
    soa_exists,
)

router = APIRouter(prefix="/soa/{soa_id}")
ui_router = APIRouter()
logger = logging.getLogger("soa_builder.web.routers.objectives")
templates = Jinja2Templates(
    directory=os.path.join(os.path.dirname(__file__), "..", "templates")
)


def _load_objectives_context(soa_id: int) -> dict:
    """Build template context for objectives + endpoints UI page."""
    c188725_map = get_ddf_ct_codelist_map("C188725")
    c188726_map = get_ddf_ct_codelist_map("C188726")
    objective_level_options = sorted({v for v in c188725_map.values() if v})
    endpoint_level_options = sorted({v for v in c188726_map.values() if v})

    conn = _connect()
    cur = conn.cursor()
    cur.execute(
        "SELECT code_uid, code FROM code_association "
        "WHERE soa_id=? AND codelist_code IN ('C188725','C188726')",
        (soa_id,),
    )
    level_code_to_sv = {
        code_uid: (code_val or "") for code_uid, code_val in cur.fetchall()
    }
    cur.execute(
        "SELECT id,objective_uid,name,label,description,text,"
        "level_code_uid,order_index "
        "FROM objective WHERE soa_id=? ORDER BY order_index, id",
        (soa_id,),
    )
    objectives = [
        {
            "id": r[0],
            "objective_uid": r[1],
            "name": r[2],
            "label": r[3],
            "description": r[4],
            "text": r[5],
            "level_code_uid": r[6],
            "level": level_code_to_sv.get(r[6], ""),
            "order_index": r[7],
        }
        for r in cur.fetchall()
    ]
    cur.execute(
        "SELECT id,endpoint_uid,objective_uid,name,label,description,"
        "text,purpose,level_code_uid,order_index "
        "FROM endpoint WHERE soa_id=? ORDER BY order_index, id",
        (soa_id,),
    )
    endpoints_by_objective: dict = {}
    orphan_endpoints: list = []
    for r in cur.fetchall():
        ep = {
            "id": r[0],
            "endpoint_uid": r[1],
            "objective_uid": r[2],
            "name": r[3],
            "label": r[4],
            "description": r[5],
            "text": r[6],
            "purpose": r[7],
            "level_code_uid": r[8],
            "level": level_code_to_sv.get(r[8], ""),
            "order_index": r[9],
        }
        if ep["objective_uid"]:
            endpoints_by_objective.setdefault(ep["objective_uid"], []).append(ep)
        else:
            orphan_endpoints.append(ep)
    conn.close()

    return {
        "objectives": objectives,
        "endpoints_by_objective": endpoints_by_objective,
        "orphan_endpoints": orphan_endpoints,
        "objective_level_options": objective_level_options,
        "endpoint_level_options": endpoint_level_options,
    }


_OBJECTIVE_LEVEL_CODELIST = "C188725"


def _next_objective_uid(cur, soa_id: int) -> str:
    """Return next Objective_N UID, never reusing deleted UIDs."""
    max_n = 0
    cur.execute(
        "SELECT objective_uid FROM objective WHERE soa_id=? "
        "AND objective_uid LIKE 'Objective_%'",
        (soa_id,),
    )
    for (uid,) in cur.fetchall():
        if isinstance(uid, str) and uid.startswith("Objective_"):
            try:
                n = int(uid.split("_")[-1])
                if n > max_n:
                    max_n = n
            except (ValueError, IndexError):
                pass
    cur.execute(
        "SELECT before_json, after_json FROM objective_audit WHERE soa_id=?",
        (soa_id,),
    )
    for before_raw, after_raw in cur.fetchall():
        for raw in (before_raw, after_raw):
            if not raw:
                continue
            try:
                uid = json.loads(raw).get("objective_uid", "")
                if isinstance(uid, str) and uid.startswith("Objective_"):
                    n = int(uid.split("_")[-1])
                    if n > max_n:
                        max_n = n
            except Exception:
                pass
    return f"Objective_{max_n + 1}"


def _row_to_dict(row) -> dict:
    keys = [
        "id",
        "soa_id",
        "objective_uid",
        "name",
        "label",
        "description",
        "text",
        "level_code_uid",
        "order_index",
    ]
    return dict(zip(keys, row))


def _insert_level_code(cur, soa_id: int, submission_value: str) -> str:
    """Insert a code_association row for the objective level and return
    the generated Code_N UID."""
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
            _OBJECTIVE_LEVEL_CODELIST,
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


@router.get("/objectives", response_class=JSONResponse)
def list_objectives(soa_id: int):
    if not soa_exists(soa_id):
        raise HTTPException(404, "SOA not found")
    conn = _connect()
    cur = conn.cursor()
    cur.execute(
        "SELECT id,soa_id,objective_uid,name,label,description,text,"
        "level_code_uid,order_index "
        "FROM objective WHERE soa_id=? ORDER BY order_index, id",
        (soa_id,),
    )
    rows = [_row_to_dict(r) for r in cur.fetchall()]
    conn.close()
    return JSONResponse(rows)


@router.post("/objectives", response_class=JSONResponse)
def create_objective(soa_id: int, body: ObjectiveCreate):
    if not soa_exists(soa_id):
        raise HTTPException(404, "SOA not found")
    name = (body.name or "").strip()
    level = (body.level or "").strip()
    if not name:
        raise HTTPException(400, "Objective name required")
    if not level:
        raise HTTPException(400, "Objective level required")

    conn = _connect()
    cur = conn.cursor()
    cur.execute(
        "SELECT COALESCE(MAX(order_index),0) FROM objective WHERE soa_id=?",
        (soa_id,),
    )
    next_ord = (cur.fetchone() or [0])[0] + 1
    objective_uid = _next_objective_uid(cur, soa_id)
    level_code_uid = _insert_level_code(cur, soa_id, level)

    label = (body.label or "").strip() or None
    description = (body.description or "").strip() or None
    text = (body.text or "").strip() or None

    cur.execute(
        "INSERT INTO objective "
        "(soa_id,objective_uid,name,label,description,text,"
        "level_code_uid,order_index) VALUES (?,?,?,?,?,?,?,?)",
        (
            soa_id,
            objective_uid,
            name,
            label,
            description,
            text,
            level_code_uid,
            next_ord,
        ),
    )
    objective_id = cur.lastrowid
    conn.commit()
    conn.close()

    after = {
        "id": objective_id,
        "objective_uid": objective_uid,
        "name": name,
        "label": label,
        "description": description,
        "text": text,
        "level_code_uid": level_code_uid,
        "level": level,
        "order_index": next_ord,
    }
    _record_objective_audit(soa_id, "create", objective_id, before=None, after=after)
    return JSONResponse(after, status_code=201)


@router.patch("/objectives/{objective_id}", response_class=JSONResponse)
def update_objective(soa_id: int, objective_id: int, body: ObjectiveUpdate):
    if not soa_exists(soa_id):
        raise HTTPException(404, "SOA not found")
    conn = _connect()
    cur = conn.cursor()
    cur.execute(
        "SELECT id,soa_id,objective_uid,name,label,description,text,"
        "level_code_uid,order_index "
        "FROM objective WHERE id=? AND soa_id=?",
        (objective_id, soa_id),
    )
    row = cur.fetchone()
    if not row:
        conn.close()
        raise HTTPException(404, "Objective not found")
    before = _row_to_dict(row)

    if body.name is not None:
        new_name = body.name.strip()
        if not new_name:
            conn.close()
            raise HTTPException(400, "Objective name cannot be empty")
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

    new_level_code_uid = before["level_code_uid"]
    if body.level is not None:
        new_level = body.level.strip()
        if not new_level:
            conn.close()
            raise HTTPException(400, "Objective level cannot be empty")
        if before["level_code_uid"]:
            # Update the submission value in the existing Code_N row.
            cur.execute(
                "UPDATE code_association SET code=? WHERE soa_id=? AND code_uid=?",
                (new_level, soa_id, before["level_code_uid"]),
            )
        else:
            new_level_code_uid = _insert_level_code(cur, soa_id, new_level)

    cur.execute(
        "UPDATE objective SET name=?, label=?, description=?, text=?, "
        "level_code_uid=? WHERE id=? AND soa_id=?",
        (
            new_name,
            new_label,
            new_desc,
            new_text,
            new_level_code_uid,
            objective_id,
            soa_id,
        ),
    )
    conn.commit()
    conn.close()

    after = {
        **before,
        "name": new_name,
        "label": new_label,
        "description": new_desc,
        "text": new_text,
        "level_code_uid": new_level_code_uid,
    }
    _record_objective_audit(soa_id, "update", objective_id, before=before, after=after)
    return JSONResponse(after)


@router.delete("/objectives/{objective_id}", response_class=JSONResponse)
def delete_objective(soa_id: int, objective_id: int):
    if not soa_exists(soa_id):
        raise HTTPException(404, "SOA not found")
    conn = _connect()
    cur = conn.cursor()
    cur.execute(
        "SELECT id,soa_id,objective_uid,name,label,description,text,"
        "level_code_uid,order_index "
        "FROM objective WHERE id=? AND soa_id=?",
        (objective_id, soa_id),
    )
    row = cur.fetchone()
    if not row:
        conn.close()
        raise HTTPException(404, "Objective not found")
    before = _row_to_dict(row)

    # Orphan child endpoints: set objective_uid to NULL
    orphaned: list[dict] = []
    cur.execute(
        "SELECT id,endpoint_uid,objective_uid FROM endpoint "
        "WHERE soa_id=? AND objective_uid=?",
        (soa_id, before["objective_uid"]),
    )
    for ep_id, ep_uid, ep_parent in cur.fetchall():
        orphaned.append(
            {
                "id": ep_id,
                "endpoint_uid": ep_uid,
                "objective_uid_before": ep_parent,
            }
        )
    if orphaned:
        cur.execute(
            "UPDATE endpoint SET objective_uid=NULL WHERE soa_id=? AND objective_uid=?",
            (soa_id, before["objective_uid"]),
        )

    _delete_level_code(cur, soa_id, before["level_code_uid"])
    cur.execute(
        "DELETE FROM objective WHERE id=? AND soa_id=?",
        (objective_id, soa_id),
    )

    # Reindex remaining objectives
    cur.execute(
        "SELECT id FROM objective WHERE soa_id=? ORDER BY order_index, id",
        (soa_id,),
    )
    remaining = [r[0] for r in cur.fetchall()]
    for idx, oid in enumerate(remaining, start=1):
        cur.execute("UPDATE objective SET order_index=? WHERE id=?", (idx, oid))
    conn.commit()
    conn.close()

    _record_objective_audit(soa_id, "delete", objective_id, before=before, after=None)
    for entry in orphaned:
        _record_endpoint_audit(
            soa_id,
            "update",
            entry["id"],
            before={
                "endpoint_uid": entry["endpoint_uid"],
                "objective_uid": entry["objective_uid_before"],
            },
            after={
                "endpoint_uid": entry["endpoint_uid"],
                "objective_uid": None,
                "orphaned_by_objective_delete": before["objective_uid"],
            },
        )
    return JSONResponse({"deleted": objective_id, "orphaned_endpoints": len(orphaned)})


# ---------------------------------------------------------------------------
# UI form endpoints
# ---------------------------------------------------------------------------


@ui_router.get(
    "/ui/soa/{soa_id}/objectives",
    response_class=HTMLResponse,
    name="ui_list_objectives",
)
def ui_list_objectives(request: Request, soa_id: int):
    if not soa_exists(soa_id):
        raise HTTPException(404, "SOA not found")
    conn = _connect()
    cur = conn.cursor()
    cur.execute(
        "SELECT name, study_id, study_label FROM soa WHERE id=?",
        (soa_id,),
    )
    row = cur.fetchone()
    conn.close()
    context = {
        "soa_id": soa_id,
        "study_name": row[0] if row else "",
        "study_id_value": row[1] if row else "",
        "study_label": row[2] if row else "",
        **_load_objectives_context(soa_id),
    }
    return templates.TemplateResponse(request, "objectives.html", context)


@ui_router.post("/ui/soa/{soa_id}/objectives/create", response_class=HTMLResponse)
def ui_create_objective(
    request: Request,
    soa_id: int,
    name: str = Form(...),
    level: str = Form(...),
    label: str | None = Form(None),
    description: str | None = Form(None),
    text: str | None = Form(None),
):
    if not soa_exists(soa_id):
        raise HTTPException(404, "SOA not found")
    create_objective(
        soa_id,
        ObjectiveCreate(
            name=name,
            level=level,
            label=label,
            description=description,
            text=text,
        ),
    )
    redirect_url = f"/ui/soa/{soa_id}/objectives"
    if request.headers.get("HX-Request") == "true":
        return HTMLResponse("", headers={"HX-Redirect": redirect_url})
    safe_soa_id = html.escape(str(soa_id))
    return HTMLResponse(
        f"<script>window.location='/ui/soa/{safe_soa_id}/objectives';</script>"
    )


@ui_router.post(
    "/ui/soa/{soa_id}/objectives/{objective_id}/update",
    response_class=HTMLResponse,
)
def ui_update_objective(
    request: Request,
    soa_id: int,
    objective_id: int,
    name: str | None = Form(None),
    level: str | None = Form(None),
    label: str | None = Form(None),
    description: str | None = Form(None),
    text: str | None = Form(None),
):
    if not soa_exists(soa_id):
        raise HTTPException(404, "SOA not found")
    update_objective(
        soa_id,
        objective_id,
        ObjectiveUpdate(
            name=name,
            level=level,
            label=label,
            description=description,
            text=text,
        ),
    )
    redirect_url = f"/ui/soa/{soa_id}/objectives"
    if request.headers.get("HX-Request") == "true":
        return HTMLResponse("", headers={"HX-Redirect": redirect_url})
    safe_soa_id = html.escape(str(soa_id))
    return HTMLResponse(
        f"<script>window.location='/ui/soa/{safe_soa_id}/objectives';</script>"
    )


@ui_router.post(
    "/ui/soa/{soa_id}/objectives/{objective_id}/delete",
    response_class=HTMLResponse,
)
def ui_delete_objective(
    request: Request,
    soa_id: int,
    objective_id: int,
):
    if not soa_exists(soa_id):
        raise HTTPException(404, "SOA not found")
    delete_objective(soa_id, objective_id)
    redirect_url = f"/ui/soa/{soa_id}/objectives"
    if request.headers.get("HX-Request") == "true":
        return HTMLResponse("", headers={"HX-Redirect": redirect_url})
    safe_soa_id = html.escape(str(soa_id))
    return HTMLResponse(
        f"<script>window.location='/ui/soa/{safe_soa_id}/objectives';</script>"
    )
