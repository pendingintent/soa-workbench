import json
import logging
import os
from typing import Optional

from fastapi import APIRouter, Form, HTTPException, Request
from fastapi.responses import HTMLResponse, JSONResponse, RedirectResponse
from fastapi.templating import Jinja2Templates

from ..audit import _record_bc_surrogate_audit
from ..db import _connect
from ..schemas import BCSurrogateCreate, BCSurrogateUpdate
from ..utils import soa_exists

router = APIRouter()
ui_router = APIRouter()
logger = logging.getLogger("soa_builder.web.routers.bc_surrogates")
templates = Jinja2Templates(
    directory=os.path.join(os.path.dirname(__file__), "..", "templates")
)


def _nz(v: Optional[str]) -> Optional[str]:
    """Return None for blank strings, otherwise the stripped value."""
    if v is None:
        return None
    v = v.strip()
    return v if v else None


def _next_surrogate_uid(cur, soa_id: int) -> str:
    """Generate next BiomedicalConceptSurrogate_N UID (monotonic, never reuses).

    Scans both the live table and the audit trail so deleted UIDs are never recycled.
    """
    prefix = "BiomedicalConceptSurrogate_"
    max_n = 0

    cur.execute(
        "SELECT surrogate_uid FROM biomedical_concept_surrogate WHERE soa_id=?",
        (soa_id,),
    )
    for (uid,) in cur.fetchall():
        if isinstance(uid, str) and uid.startswith(prefix):
            try:
                n = int(uid[len(prefix) :])
                if n > max_n:
                    max_n = n
            except (ValueError, IndexError):
                pass

    cur.execute(
        "SELECT before_json, after_json FROM biomedical_concept_surrogate_audit WHERE soa_id=?",
        (soa_id,),
    )
    for before_raw, after_raw in cur.fetchall():
        for raw in (before_raw, after_raw):
            if not raw:
                continue
            try:
                uid = json.loads(raw).get("surrogate_uid", "")
                if isinstance(uid, str) and uid.startswith(prefix):
                    n = int(uid[len(prefix) :])
                    if n > max_n:
                        max_n = n
            except Exception:
                pass

    return f"{prefix}{max_n + 1}"


# ---------------------------------------------------------------------------
# API — list
# ---------------------------------------------------------------------------


@router.get(
    "/soa/{soa_id}/bc-surrogates", response_class=JSONResponse, response_model=None
)
def list_bc_surrogates(soa_id: int):
    if not soa_exists(soa_id):
        raise HTTPException(404, "SOA not found")
    conn = _connect()
    cur = conn.cursor()
    cur.execute(
        "SELECT id, surrogate_uid, name, label, description, reference "
        "FROM biomedical_concept_surrogate WHERE soa_id=? ORDER BY id",
        (soa_id,),
    )
    rows = [
        {
            "id": r[0],
            "surrogate_uid": r[1],
            "name": r[2],
            "label": r[3],
            "description": r[4],
            "reference": r[5],
        }
        for r in cur.fetchall()
    ]
    conn.close()
    return rows


# ---------------------------------------------------------------------------
# API — create
# ---------------------------------------------------------------------------


@router.post(
    "/soa/{soa_id}/bc-surrogates", response_class=JSONResponse, response_model=None
)
def create_bc_surrogate(soa_id: int, payload: BCSurrogateCreate):
    if not soa_exists(soa_id):
        raise HTTPException(404, "SOA not found")
    name = (payload.name or "").strip()
    if not name:
        raise HTTPException(400, "Surrogate name required")

    conn = _connect()
    cur = conn.cursor()
    uid = _next_surrogate_uid(cur, soa_id)
    cur.execute(
        "INSERT INTO biomedical_concept_surrogate (soa_id, surrogate_uid, name, label, description, reference) VALUES (?,?,?,?,?,?)",
        (
            soa_id,
            uid,
            name,
            _nz(payload.label),
            _nz(payload.description),
            _nz(payload.reference),
        ),
    )
    surrogate_id = cur.lastrowid
    conn.commit()
    conn.close()
    after = {
        "id": surrogate_id,
        "surrogate_uid": uid,
        "name": name,
        "label": _nz(payload.label),
        "description": _nz(payload.description),
        "reference": _nz(payload.reference),
    }
    _record_bc_surrogate_audit(soa_id, "create", surrogate_id, before=None, after=after)
    return after


# ---------------------------------------------------------------------------
# API — update
# ---------------------------------------------------------------------------


@router.patch(
    "/soa/{soa_id}/bc-surrogates/{surrogate_id}",
    response_class=JSONResponse,
    response_model=None,
)
def update_bc_surrogate(soa_id: int, surrogate_id: int, payload: BCSurrogateUpdate):
    if not soa_exists(soa_id):
        raise HTTPException(404, "SOA not found")
    conn = _connect()
    cur = conn.cursor()
    cur.execute(
        "SELECT id, surrogate_uid, name, label, description, reference "
        "FROM biomedical_concept_surrogate WHERE id=? AND soa_id=?",
        (surrogate_id, soa_id),
    )
    row = cur.fetchone()
    if not row:
        conn.close()
        raise HTTPException(404, "Surrogate not found")
    before = {
        "id": row[0],
        "surrogate_uid": row[1],
        "name": row[2],
        "label": row[3],
        "description": row[4],
        "reference": row[5],
    }

    new_name = _nz(payload.name) if payload.name is not None else before["name"]
    new_label = _nz(payload.label) if payload.label is not None else before["label"]
    new_desc = (
        _nz(payload.description)
        if payload.description is not None
        else before["description"]
    )
    new_ref = (
        _nz(payload.reference) if payload.reference is not None else before["reference"]
    )

    cur.execute(
        "UPDATE biomedical_concept_surrogate SET name=?, label=?, description=?, reference=? WHERE id=? AND soa_id=?",
        (new_name, new_label, new_desc, new_ref, surrogate_id, soa_id),
    )
    conn.commit()
    conn.close()
    after = {
        **before,
        "name": new_name,
        "label": new_label,
        "description": new_desc,
        "reference": new_ref,
    }
    _record_bc_surrogate_audit(
        soa_id, "update", surrogate_id, before=before, after=after
    )
    return after


# ---------------------------------------------------------------------------
# API — delete
# ---------------------------------------------------------------------------


@router.delete(
    "/soa/{soa_id}/bc-surrogates/{surrogate_id}",
    response_class=JSONResponse,
    response_model=None,
)
def delete_bc_surrogate(soa_id: int, surrogate_id: int):
    if not soa_exists(soa_id):
        raise HTTPException(404, "SOA not found")
    conn = _connect()
    cur = conn.cursor()
    cur.execute(
        "SELECT id, surrogate_uid, name FROM biomedical_concept_surrogate WHERE id=? AND soa_id=?",
        (surrogate_id, soa_id),
    )
    row = cur.fetchone()
    if not row:
        conn.close()
        raise HTTPException(404, "Surrogate not found")
    before = {"id": row[0], "surrogate_uid": row[1], "name": row[2]}
    # Remove activity links first
    cur.execute(
        "DELETE FROM activity_surrogate WHERE soa_id=? AND surrogate_uid=?",
        (soa_id, row[1]),
    )
    cur.execute(
        "DELETE FROM biomedical_concept_surrogate WHERE id=? AND soa_id=?",
        (surrogate_id, soa_id),
    )
    conn.commit()
    conn.close()
    _record_bc_surrogate_audit(
        soa_id, "delete", surrogate_id, before=before, after=None
    )
    return {"deleted": True, "id": surrogate_id}


# ---------------------------------------------------------------------------
# API — link / unlink surrogate ↔ activity
# ---------------------------------------------------------------------------


@router.post(
    "/soa/{soa_id}/activities/{activity_id}/bc-surrogates/{surrogate_id}",
    response_class=JSONResponse,
    response_model=None,
)
def link_surrogate_to_activity(soa_id: int, activity_id: int, surrogate_id: int):
    if not soa_exists(soa_id):
        raise HTTPException(404, "SOA not found")
    conn = _connect()
    cur = conn.cursor()
    cur.execute(
        "SELECT activity_uid FROM activity WHERE id=? AND soa_id=?",
        (activity_id, soa_id),
    )
    act_row = cur.fetchone()
    if not act_row:
        conn.close()
        raise HTTPException(404, "Activity not found")
    activity_uid = act_row[0]

    cur.execute(
        "SELECT surrogate_uid FROM biomedical_concept_surrogate WHERE id=? AND soa_id=?",
        (surrogate_id, soa_id),
    )
    sur_row = cur.fetchone()
    if not sur_row:
        conn.close()
        raise HTTPException(404, "Surrogate not found")
    surrogate_uid = sur_row[0]

    cur.execute(
        "INSERT OR IGNORE INTO activity_surrogate (soa_id, activity_uid, surrogate_uid) VALUES (?,?,?)",
        (soa_id, activity_uid, surrogate_uid),
    )
    conn.commit()
    conn.close()
    return {
        "linked": True,
        "activity_uid": activity_uid,
        "surrogate_uid": surrogate_uid,
    }


@router.delete(
    "/soa/{soa_id}/activities/{activity_id}/bc-surrogates/{surrogate_id}",
    response_class=JSONResponse,
    response_model=None,
)
def unlink_surrogate_from_activity(soa_id: int, activity_id: int, surrogate_id: int):
    if not soa_exists(soa_id):
        raise HTTPException(404, "SOA not found")
    conn = _connect()
    cur = conn.cursor()
    cur.execute(
        "SELECT activity_uid FROM activity WHERE id=? AND soa_id=?",
        (activity_id, soa_id),
    )
    act_row = cur.fetchone()
    if not act_row:
        conn.close()
        raise HTTPException(404, "Activity not found")
    activity_uid = act_row[0]

    cur.execute(
        "SELECT surrogate_uid FROM biomedical_concept_surrogate WHERE id=? AND soa_id=?",
        (surrogate_id, soa_id),
    )
    sur_row = cur.fetchone()
    if not sur_row:
        conn.close()
        raise HTTPException(404, "Surrogate not found")
    surrogate_uid = sur_row[0]

    cur.execute(
        "DELETE FROM activity_surrogate WHERE soa_id=? AND activity_uid=? AND surrogate_uid=?",
        (soa_id, activity_uid, surrogate_uid),
    )
    conn.commit()
    conn.close()
    return {
        "unlinked": True,
        "activity_uid": activity_uid,
        "surrogate_uid": surrogate_uid,
    }


# ---------------------------------------------------------------------------
# UI — create / update / delete (form POST → redirect)
# ---------------------------------------------------------------------------


@ui_router.post("/ui/soa/{soa_id}/bc-surrogates/create", response_class=HTMLResponse)
def ui_create_bc_surrogate(
    request: Request,
    soa_id: int,
    name: str = Form(...),
    label: str = Form(""),
    description: str = Form(""),
    reference: str = Form(""),
):
    if not soa_exists(soa_id):
        raise HTTPException(404, "SOA not found")
    name = name.strip()
    if not name:
        raise HTTPException(400, "Surrogate name required")

    conn = _connect()
    cur = conn.cursor()
    uid = _next_surrogate_uid(cur, soa_id)
    cur.execute(
        "INSERT INTO biomedical_concept_surrogate (soa_id, surrogate_uid, name, label, description, reference) VALUES (?,?,?,?,?,?)",
        (soa_id, uid, name, _nz(label), _nz(description), _nz(reference)),
    )
    surrogate_id = cur.lastrowid
    conn.commit()
    conn.close()
    _record_bc_surrogate_audit(
        soa_id,
        "create",
        surrogate_id,
        before=None,
        after={
            "surrogate_uid": uid,
            "name": name,
            "label": _nz(label),
            "description": _nz(description),
            "reference": _nz(reference),
        },
    )
    return RedirectResponse(f"/ui/soa/{soa_id}/activities", status_code=303)


@ui_router.post(
    "/ui/soa/{soa_id}/bc-surrogates/{surrogate_id}/update", response_class=HTMLResponse
)
def ui_update_bc_surrogate(
    request: Request,
    soa_id: int,
    surrogate_id: int,
    name: str = Form(...),
    label: str = Form(""),
    description: str = Form(""),
    reference: str = Form(""),
):
    if not soa_exists(soa_id):
        raise HTTPException(404, "SOA not found")
    conn = _connect()
    cur = conn.cursor()
    cur.execute(
        "SELECT id, surrogate_uid, name, label, description, reference "
        "FROM biomedical_concept_surrogate WHERE id=? AND soa_id=?",
        (surrogate_id, soa_id),
    )
    row = cur.fetchone()
    if not row:
        conn.close()
        raise HTTPException(404, "Surrogate not found")
    before = {
        "id": row[0],
        "surrogate_uid": row[1],
        "name": row[2],
        "label": row[3],
        "description": row[4],
        "reference": row[5],
    }

    new_name = name.strip() or before["name"]
    new_label = _nz(label)
    new_desc = _nz(description)
    new_ref = _nz(reference)
    cur.execute(
        "UPDATE biomedical_concept_surrogate SET name=?, label=?, description=?, reference=? WHERE id=? AND soa_id=?",
        (new_name, new_label, new_desc, new_ref, surrogate_id, soa_id),
    )
    conn.commit()
    conn.close()
    after = {
        **before,
        "name": new_name,
        "label": new_label,
        "description": new_desc,
        "reference": new_ref,
    }
    _record_bc_surrogate_audit(
        soa_id, "update", surrogate_id, before=before, after=after
    )
    return RedirectResponse(f"/ui/soa/{soa_id}/activities", status_code=303)


@ui_router.post(
    "/ui/soa/{soa_id}/bc-surrogates/{surrogate_id}/delete", response_class=HTMLResponse
)
def ui_delete_bc_surrogate(request: Request, soa_id: int, surrogate_id: int):
    if not soa_exists(soa_id):
        raise HTTPException(404, "SOA not found")
    conn = _connect()
    cur = conn.cursor()
    cur.execute(
        "SELECT id, surrogate_uid, name FROM biomedical_concept_surrogate WHERE id=? AND soa_id=?",
        (surrogate_id, soa_id),
    )
    row = cur.fetchone()
    if not row:
        conn.close()
        raise HTTPException(404, "Surrogate not found")
    before = {"id": row[0], "surrogate_uid": row[1], "name": row[2]}
    cur.execute(
        "DELETE FROM activity_surrogate WHERE soa_id=? AND surrogate_uid=?",
        (soa_id, row[1]),
    )
    cur.execute(
        "DELETE FROM biomedical_concept_surrogate WHERE id=? AND soa_id=?",
        (surrogate_id, soa_id),
    )
    conn.commit()
    conn.close()
    _record_bc_surrogate_audit(
        soa_id, "delete", surrogate_id, before=before, after=None
    )
    return RedirectResponse(f"/ui/soa/{soa_id}/activities", status_code=303)


# ---------------------------------------------------------------------------
# UI — HTMX add/remove surrogate link on activity (returns concepts_cell partial)
# ---------------------------------------------------------------------------


def _render_concepts_cell(request: Request, soa_id: int, activity_id: int):
    """Re-render the concepts_cell partial after a surrogate/group link/unlink."""
    conn = _connect()
    cur = conn.cursor()

    # Fetch activity uid
    cur.execute(
        "SELECT activity_uid FROM activity WHERE id=? AND soa_id=?",
        (activity_id, soa_id),
    )
    act_row = cur.fetchone()
    if not act_row:
        conn.close()
        raise HTTPException(404, "Activity not found")
    activity_uid = act_row[0]

    # Fetch linked BC concepts (include concept_group_uid and group name)
    cur.execute("PRAGMA table_info(activity_concept)")
    ac_cols = {r[1] for r in cur.fetchall()}
    has_group_uid = "concept_group_uid" in ac_cols
    if has_group_uid:
        cur.execute(
            "SELECT ac.concept_code, ac.concept_title, ac.concept_group_uid, "
            "cg.name AS group_name "
            "FROM activity_concept ac "
            "LEFT JOIN concept_group cg ON cg.concept_group_uid=ac.concept_group_uid "
            "WHERE ac.activity_id=? AND ac.soa_id=? "
            "ORDER BY ac.concept_group_uid NULLS LAST, ac.id",
            (activity_id, soa_id),
        )
        selected_list = [
            {
                "code": r[0],
                "title": r[1],
                "dss_title": "",
                "dss_href": "",
                "concept_group_uid": r[2],
                "group_name": r[3],
            }
            for r in cur.fetchall()
        ]
    else:
        cur.execute(
            "SELECT concept_code, concept_title "
            "FROM activity_concept WHERE activity_id=? AND soa_id=?",
            (activity_id, soa_id),
        )
        selected_list = [
            {
                "code": r[0],
                "title": r[1],
                "dss_title": "",
                "dss_href": "",
                "concept_group_uid": None,
                "group_name": None,
            }
            for r in cur.fetchall()
        ]
    selected_codes = [c["code"] for c in selected_list]

    # Fetch linked surrogates
    cur.execute(
        "SELECT bcs.id, bcs.surrogate_uid, bcs.name, bcs.label "
        "FROM activity_surrogate asr "
        "JOIN biomedical_concept_surrogate bcs "
        "ON bcs.surrogate_uid=asr.surrogate_uid AND bcs.soa_id=asr.soa_id "
        "WHERE asr.activity_uid=? AND asr.soa_id=?",
        (activity_uid, soa_id),
    )
    selected_surrogate_list = [
        {"id": r[0], "surrogate_uid": r[1], "name": r[2], "label": r[3]}
        for r in cur.fetchall()
    ]
    selected_surrogate_uids = [s["surrogate_uid"] for s in selected_surrogate_list]

    # Fetch all surrogates for this SOA (for the dropdown)
    cur.execute(
        "SELECT id, surrogate_uid, name, label "
        "FROM biomedical_concept_surrogate WHERE soa_id=? ORDER BY id",
        (soa_id,),
    )
    surrogates = [
        {"id": r[0], "surrogate_uid": r[1], "name": r[2], "label": r[3]}
        for r in cur.fetchall()
    ]

    # Fetch all concept groups (for the dropdown)
    cur.execute(
        "SELECT id, concept_group_uid, name, label FROM concept_group ORDER BY id"
    )
    concept_groups = [
        {"id": r[0], "concept_group_uid": r[1], "name": r[2], "label": r[3]}
        for r in cur.fetchall()
    ]
    activity_group_uids = list(
        {c["concept_group_uid"] for c in selected_list if c["concept_group_uid"]}
    )
    conn.close()

    # Fetch BC concepts list (for the dropdown)
    from ..app import fetch_biomedical_concepts as _app_fetch_concepts

    concepts = _app_fetch_concepts()

    return templates.TemplateResponse(
        request,
        "concepts_cell.html",
        {
            "request": request,
            "soa_id": soa_id,
            "activity_id": activity_id,
            "selected_list": selected_list,
            "selected_codes": selected_codes,
            "selected_surrogate_list": selected_surrogate_list,
            "selected_surrogate_uids": selected_surrogate_uids,
            "concepts": concepts,
            "surrogates": surrogates,
            "concept_groups": concept_groups,
            "activity_group_uids": activity_group_uids,
            "edit": False,
        },
    )


@ui_router.post(
    "/ui/soa/{soa_id}/activity/{activity_id}/bc-surrogates/add",
    response_class=HTMLResponse,
)
def ui_add_surrogate_to_activity(
    request: Request,
    soa_id: int,
    activity_id: int,
    surrogate_uid: str = Form(...),
):
    if not soa_exists(soa_id):
        raise HTTPException(404, "SOA not found")
    conn = _connect()
    cur = conn.cursor()
    cur.execute(
        "SELECT activity_uid FROM activity WHERE id=? AND soa_id=?",
        (activity_id, soa_id),
    )
    act_row = cur.fetchone()
    if not act_row:
        conn.close()
        raise HTTPException(404, "Activity not found")
    activity_uid = act_row[0]

    cur.execute(
        "SELECT id FROM biomedical_concept_surrogate WHERE surrogate_uid=? AND soa_id=?",
        (surrogate_uid, soa_id),
    )
    if not cur.fetchone():
        conn.close()
        raise HTTPException(404, "Surrogate not found")

    cur.execute(
        "INSERT OR IGNORE INTO activity_surrogate (soa_id, activity_uid, surrogate_uid) VALUES (?,?,?)",
        (soa_id, activity_uid, surrogate_uid),
    )
    conn.commit()
    conn.close()
    return _render_concepts_cell(request, soa_id, activity_id)


@ui_router.post(
    "/ui/soa/{soa_id}/activity/{activity_id}/bc-surrogates/remove",
    response_class=HTMLResponse,
)
def ui_remove_surrogate_from_activity(
    request: Request,
    soa_id: int,
    activity_id: int,
    surrogate_uid: str = Form(...),
):
    if not soa_exists(soa_id):
        raise HTTPException(404, "SOA not found")
    conn = _connect()
    cur = conn.cursor()
    cur.execute(
        "SELECT activity_uid FROM activity WHERE id=? AND soa_id=?",
        (activity_id, soa_id),
    )
    act_row = cur.fetchone()
    if not act_row:
        conn.close()
        raise HTTPException(404, "Activity not found")
    activity_uid = act_row[0]

    cur.execute(
        "DELETE FROM activity_surrogate WHERE soa_id=? AND activity_uid=? AND surrogate_uid=?",
        (soa_id, activity_uid, surrogate_uid),
    )
    conn.commit()
    conn.close()
    return _render_concepts_cell(request, soa_id, activity_id)
