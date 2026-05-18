import logging
import os
from typing import List, Optional

from fastapi import APIRouter, BackgroundTasks, Form, HTTPException, Request
from fastapi.responses import HTMLResponse, JSONResponse, RedirectResponse
from fastapi.templating import Jinja2Templates
from pydantic import BaseModel

from ..db import _connect
from ..utils import (
    get_next_concept_uid as _get_next_concept_uid,
    soa_exists,
)

router = APIRouter()
ui_router = APIRouter()
logger = logging.getLogger("soa_builder.web.routers.concept_groups")
templates = Jinja2Templates(
    directory=os.path.join(os.path.dirname(__file__), "..", "templates")
)


def _nz(v: Optional[str]) -> Optional[str]:
    """Return None for blank strings, otherwise the stripped value."""
    if v is None:
        return None
    v = v.strip()
    return v if v else None


def _next_group_uid(cur) -> str:
    """Generate next ConceptGroup_N UID (globally monotonic, never reuses)."""
    prefix = "ConceptGroup_"
    max_n = 0
    cur.execute("SELECT concept_group_uid FROM concept_group")
    for (uid,) in cur.fetchall():
        if isinstance(uid, str) and uid.startswith(prefix):
            try:
                n = int(uid[len(prefix) :])
                if n > max_n:
                    max_n = n
            except (ValueError, IndexError):
                pass
    return f"{prefix}{max_n + 1}"


def _fetch_group_row(cur, group_id: int):
    """Return (id, concept_group_uid, name) or raise 404."""
    cur.execute(
        "SELECT id, concept_group_uid, name FROM concept_group WHERE id=?",
        (group_id,),
    )
    row = cur.fetchone()
    if not row:
        raise HTTPException(404, "Group not found")
    return row


def _expand_group_to_activity(
    cur, soa_id: int, activity_id: int, activity_uid: str, group_uid: str
) -> int:
    """Insert all group concepts into activity_concept for the given activity.

    Skips concepts already present (any source). Returns count added.
    """
    from ..app import _upsert_biomedical_concept

    cur.execute(
        "SELECT concept_code, concept_title "
        "FROM concept_group_concept WHERE concept_group_uid=?",
        (group_uid,),
    )
    concepts = cur.fetchall()
    added = 0
    for code, title in concepts:
        cur.execute(
            "SELECT 1 FROM activity_concept "
            "WHERE activity_id=? AND soa_id=? AND concept_code=?",
            (activity_id, soa_id, code),
        )
        if cur.fetchone():
            continue
        concept_uid = _get_next_concept_uid(cur, soa_id)
        cur.execute(
            "INSERT INTO activity_concept "
            "(soa_id, activity_id, activity_uid, concept_uid, "
            "concept_code, concept_title, concept_group_uid) "
            "VALUES (?,?,?,?,?,?,?)",
            (
                soa_id,
                activity_id,
                activity_uid,
                concept_uid,
                code,
                title,
                group_uid,
            ),
        )
        _upsert_biomedical_concept(cur, soa_id, concept_uid, title, code)
        added += 1

    # Also insert surrogates that belong to this group
    cur.execute(
        "SELECT surrogate_uid FROM biomedical_concept_surrogate "
        "WHERE concept_group_uid=? AND soa_id=?",
        (group_uid, soa_id),
    )
    surrogate_rows = cur.fetchall()
    for (surrogate_uid,) in surrogate_rows:
        cur.execute(
            "SELECT 1 FROM activity_surrogate "
            "WHERE soa_id=? AND activity_uid=? AND surrogate_uid=?",
            (soa_id, activity_uid, surrogate_uid),
        )
        if cur.fetchone():
            continue
        cur.execute(
            "INSERT INTO activity_surrogate "
            "(soa_id, activity_uid, surrogate_uid, concept_group_uid) "
            "VALUES (?,?,?,?)",
            (soa_id, activity_uid, surrogate_uid, group_uid),
        )
        added += 1

    return added


# ---------------------------------------------------------------------------
# Pydantic schemas (local to this router)
# ---------------------------------------------------------------------------


class ConceptGroupCreate(BaseModel):
    name: str
    label: Optional[str] = None
    description: Optional[str] = None


class ConceptGroupUpdate(BaseModel):
    name: Optional[str] = None
    label: Optional[str] = None
    description: Optional[str] = None


class ConceptsAdd(BaseModel):
    concept_codes: List[str]


class CategoryAdd(BaseModel):
    category: str


# ---------------------------------------------------------------------------
# API — list
# ---------------------------------------------------------------------------


@router.get("/concept-groups", response_class=JSONResponse, response_model=None)
def list_concept_groups():
    conn = _connect()
    cur = conn.cursor()
    cur.execute(
        "SELECT id, concept_group_uid, name, label, description "
        "FROM concept_group ORDER BY id"
    )
    groups = [
        {
            "id": r[0],
            "concept_group_uid": r[1],
            "name": r[2],
            "label": r[3],
            "description": r[4],
        }
        for r in cur.fetchall()
    ]
    for g in groups:
        cur.execute(
            "SELECT concept_code, concept_title "
            "FROM concept_group_concept WHERE concept_group_uid=? ORDER BY id",
            (g["concept_group_uid"],),
        )
        g["concepts"] = [{"code": r[0], "title": r[1]} for r in cur.fetchall()]
    conn.close()
    return groups


# ---------------------------------------------------------------------------
# API — create
# ---------------------------------------------------------------------------


@router.post("/concept-groups", response_class=JSONResponse, response_model=None)
def create_concept_group(payload: ConceptGroupCreate):
    name = (payload.name or "").strip()
    if not name:
        raise HTTPException(400, "Group name required")
    conn = _connect()
    cur = conn.cursor()
    uid = _next_group_uid(cur)
    cur.execute(
        "INSERT INTO concept_group "
        "(concept_group_uid, name, label, description) VALUES (?,?,?,?)",
        (uid, name, _nz(payload.label), _nz(payload.description)),
    )
    group_id = cur.lastrowid
    conn.commit()
    conn.close()
    return {
        "id": group_id,
        "concept_group_uid": uid,
        "name": name,
        "label": _nz(payload.label),
        "description": _nz(payload.description),
    }


# ---------------------------------------------------------------------------
# API — update
# ---------------------------------------------------------------------------


@router.patch(
    "/concept-groups/{group_id}",
    response_class=JSONResponse,
    response_model=None,
)
def update_concept_group(group_id: int, payload: ConceptGroupUpdate):
    conn = _connect()
    cur = conn.cursor()
    cur.execute(
        "SELECT id, concept_group_uid, name, label, description "
        "FROM concept_group WHERE id=?",
        (group_id,),
    )
    row = cur.fetchone()
    if not row:
        conn.close()
        raise HTTPException(404, "Group not found")
    new_name = _nz(payload.name) if payload.name is not None else row[2]
    new_label = _nz(payload.label) if payload.label is not None else row[3]
    new_desc = _nz(payload.description) if payload.description is not None else row[4]
    cur.execute(
        "UPDATE concept_group SET name=?, label=?, description=? WHERE id=?",
        (new_name, new_label, new_desc, group_id),
    )
    conn.commit()
    conn.close()
    return {
        "id": row[0],
        "concept_group_uid": row[1],
        "name": new_name,
        "label": new_label,
        "description": new_desc,
    }


# ---------------------------------------------------------------------------
# API — delete
# ---------------------------------------------------------------------------


@router.delete(
    "/concept-groups/{group_id}",
    response_class=JSONResponse,
    response_model=None,
)
def delete_concept_group(group_id: int):
    conn = _connect()
    cur = conn.cursor()
    row = _fetch_group_row(cur, group_id)
    uid = row[1]
    cur.execute("DELETE FROM concept_group_concept WHERE concept_group_uid=?", (uid,))
    cur.execute("PRAGMA table_info(activity_concept)")
    if "concept_group_uid" in {r[1] for r in cur.fetchall()}:
        cur.execute("DELETE FROM activity_concept WHERE concept_group_uid=?", (uid,))
    cur.execute("DELETE FROM concept_group WHERE id=?", (group_id,))
    conn.commit()
    conn.close()
    return {"deleted": True, "id": group_id}


# ---------------------------------------------------------------------------
# API — add/remove individual concepts from group definition
# ---------------------------------------------------------------------------


@router.post(
    "/concept-groups/{group_id}/concepts",
    response_class=JSONResponse,
    response_model=None,
)
def add_concepts_to_group(group_id: int, payload: ConceptsAdd):
    conn = _connect()
    cur = conn.cursor()
    row = _fetch_group_row(cur, group_id)
    uid = row[1]
    from ..app import fetch_biomedical_concepts as _fetch_concepts

    lookup = {c["code"]: c["title"] for c in _fetch_concepts()}
    added = 0
    for code in payload.concept_codes:
        code = code.strip()
        if not code:
            continue
        title = lookup.get(code, code)
        cur.execute(
            "INSERT OR IGNORE INTO concept_group_concept "
            "(concept_group_uid, concept_code, concept_title) VALUES (?,?,?)",
            (uid, code, title),
        )
        if cur.rowcount:
            added += 1
    conn.commit()
    conn.close()
    return {"added": added}


@router.delete(
    "/concept-groups/{group_id}/concepts/{concept_code:path}",
    response_class=JSONResponse,
    response_model=None,
)
def remove_concept_from_group(group_id: int, concept_code: str):
    conn = _connect()
    cur = conn.cursor()
    row = _fetch_group_row(cur, group_id)
    uid = row[1]
    cur.execute(
        "DELETE FROM concept_group_concept "
        "WHERE concept_group_uid=? AND concept_code=?",
        (uid, concept_code),
    )
    conn.commit()
    conn.close()
    return {"removed": True, "concept_code": concept_code}


# ---------------------------------------------------------------------------
# API — bulk add from CDISC category
# ---------------------------------------------------------------------------


@router.post(
    "/concept-groups/{group_id}/concepts/from-category",
    response_class=JSONResponse,
    response_model=None,
)
def add_category_to_group(group_id: int, payload: CategoryAdd):
    conn = _connect()
    cur = conn.cursor()
    row = _fetch_group_row(cur, group_id)
    uid = row[1]
    from ..app import (
        fetch_biomedical_concepts_by_category as _fetch_by_cat,
    )

    concepts = _fetch_by_cat(payload.category)
    added = 0
    for c in concepts:
        cur.execute(
            "INSERT OR IGNORE INTO concept_group_concept "
            "(concept_group_uid, concept_code, concept_title) VALUES (?,?,?)",
            (uid, c["code"], c.get("title", c["code"])),
        )
        if cur.rowcount:
            added += 1
    conn.commit()
    conn.close()
    return {"added": added, "category": payload.category}


# ---------------------------------------------------------------------------
# API — assign / unassign group to/from activity
# ---------------------------------------------------------------------------


@router.post(
    "/soa/{soa_id}/activities/{activity_id}/concept-groups/{group_id}",
    response_class=JSONResponse,
    response_model=None,
)
def assign_group_to_activity(soa_id: int, activity_id: int, group_id: int):
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

    row = _fetch_group_row(cur, group_id)
    group_uid = row[1]

    added = _expand_group_to_activity(cur, soa_id, activity_id, activity_uid, group_uid)
    conn.commit()
    conn.close()
    return {"assigned": True, "added": added, "group_uid": group_uid}


@router.delete(
    "/soa/{soa_id}/activities/{activity_id}/concept-groups/{group_id}",
    response_class=JSONResponse,
    response_model=None,
)
def unassign_group_from_activity(soa_id: int, activity_id: int, group_id: int):
    if not soa_exists(soa_id):
        raise HTTPException(404, "SOA not found")
    conn = _connect()
    cur = conn.cursor()
    row = _fetch_group_row(cur, group_id)
    group_uid = row[1]
    cur.execute(
        "DELETE FROM activity_concept "
        "WHERE activity_id=? AND soa_id=? AND concept_group_uid=?",
        (activity_id, soa_id, group_uid),
    )
    conn.commit()
    conn.close()
    return {"unassigned": True, "group_uid": group_uid}


# ---------------------------------------------------------------------------
# UI — concept group management page
# ---------------------------------------------------------------------------


@ui_router.get("/ui/concept-groups", response_class=HTMLResponse)
def ui_list_concept_groups(request: Request):
    conn = _connect()
    cur = conn.cursor()
    cur.execute(
        "SELECT id, concept_group_uid, name, label, description "
        "FROM concept_group ORDER BY id"
    )
    groups = [
        {
            "id": r[0],
            "concept_group_uid": r[1],
            "name": r[2],
            "label": r[3],
            "description": r[4],
        }
        for r in cur.fetchall()
    ]
    for g in groups:
        # Concepts sorted alphabetically by title
        cur.execute(
            "SELECT concept_code, concept_title "
            "FROM concept_group_concept WHERE concept_group_uid=? "
            "ORDER BY concept_title COLLATE NOCASE ASC",
            (g["concept_group_uid"],),
        )
        g["concepts"] = [{"code": r[0], "title": r[1]} for r in cur.fetchall()]
        # Surrogates assigned to this group (from all SOAs)
        cur.execute(
            "SELECT bcs.id, bcs.soa_id, bcs.surrogate_uid, bcs.name, bcs.label, "
            "s.name AS soa_name "
            "FROM biomedical_concept_surrogate bcs "
            "JOIN soa s ON s.id=bcs.soa_id "
            "WHERE bcs.concept_group_uid=? "
            "ORDER BY bcs.soa_id, bcs.name COLLATE NOCASE",
            (g["concept_group_uid"],),
        )
        g["surrogates"] = [
            {
                "id": r[0],
                "soa_id": r[1],
                "surrogate_uid": r[2],
                "name": r[3],
                "label": r[4],
                "soa_name": r[5],
            }
            for r in cur.fetchall()
        ]

    # All surrogates (all SOAs) not yet assigned to any group, for add dropdown
    cur.execute(
        "SELECT bcs.id, bcs.soa_id, bcs.surrogate_uid, bcs.name, bcs.label, "
        "s.name AS soa_name "
        "FROM biomedical_concept_surrogate bcs "
        "JOIN soa s ON s.id=bcs.soa_id "
        "WHERE bcs.concept_group_uid IS NULL "
        "ORDER BY s.name COLLATE NOCASE, bcs.name COLLATE NOCASE"
    )
    all_unassigned_surrogates = [
        {
            "id": r[0],
            "soa_id": r[1],
            "surrogate_uid": r[2],
            "name": r[3],
            "label": r[4],
            "soa_name": r[5],
        }
        for r in cur.fetchall()
    ]
    conn.close()
    from ..app import (
        fetch_biomedical_concept_categories as _fetch_cats,
        fetch_biomedical_concepts as _fetch_concepts,
    )

    return templates.TemplateResponse(
        request,
        "concept_groups.html",
        {
            "request": request,
            "groups": groups,
            "concepts": _fetch_concepts(),
            "categories": _fetch_cats(),
            "all_unassigned_surrogates": all_unassigned_surrogates,
        },
    )


# ---------------------------------------------------------------------------
# UI — create / update / delete (form POST → redirect to list)
# ---------------------------------------------------------------------------


@ui_router.post("/ui/concept-groups/create", response_class=HTMLResponse)
def ui_create_concept_group(
    request: Request,
    name: str = Form(...),
    label: str = Form(""),
    description: str = Form(""),
    copy_from_group_id: str = Form(""),
):
    name = name.strip()
    if not name:
        raise HTTPException(400, "Group name required")
    conn = _connect()
    cur = conn.cursor()
    uid = _next_group_uid(cur)
    cur.execute(
        "INSERT INTO concept_group "
        "(concept_group_uid, name, label, description) VALUES (?,?,?,?)",
        (uid, name, _nz(label), _nz(description)),
    )
    # Copy concepts from an existing group if requested
    copy_id = int(copy_from_group_id) if copy_from_group_id.strip() else None
    if copy_id:
        cur.execute(
            "SELECT concept_group_uid FROM concept_group WHERE id=?", (copy_id,)
        )
        src_row = cur.fetchone()
        if src_row:
            cur.execute(
                "INSERT OR IGNORE INTO concept_group_concept "
                "(concept_group_uid, concept_code, concept_title) "
                "SELECT ?, concept_code, concept_title "
                "FROM concept_group_concept WHERE concept_group_uid=?",
                (uid, src_row[0]),
            )
    conn.commit()
    conn.close()
    return RedirectResponse("/ui/concept-groups", status_code=303)


@ui_router.post("/ui/concept-groups/{group_id}/update", response_class=HTMLResponse)
def ui_update_concept_group(
    request: Request,
    group_id: int,
    name: str = Form(...),
    label: str = Form(""),
    description: str = Form(""),
):
    conn = _connect()
    cur = conn.cursor()
    cur.execute("SELECT id, name FROM concept_group WHERE id=?", (group_id,))
    row = cur.fetchone()
    if not row:
        conn.close()
        raise HTTPException(404, "Group not found")
    new_name = name.strip() or row[1]
    cur.execute(
        "UPDATE concept_group SET name=?, label=?, description=? WHERE id=?",
        (new_name, _nz(label), _nz(description), group_id),
    )
    conn.commit()
    conn.close()
    return RedirectResponse("/ui/concept-groups", status_code=303)


@ui_router.post("/ui/concept-groups/{group_id}/delete", response_class=HTMLResponse)
def ui_delete_concept_group(request: Request, group_id: int):
    conn = _connect()
    cur = conn.cursor()
    cur.execute(
        "SELECT id, concept_group_uid FROM concept_group WHERE id=?",
        (group_id,),
    )
    row = cur.fetchone()
    if not row:
        conn.close()
        raise HTTPException(404, "Group not found")
    uid = row[1]
    cur.execute("DELETE FROM concept_group_concept WHERE concept_group_uid=?", (uid,))
    cur.execute("PRAGMA table_info(activity_concept)")
    if "concept_group_uid" in {r[1] for r in cur.fetchall()}:
        cur.execute("DELETE FROM activity_concept WHERE concept_group_uid=?", (uid,))
    cur.execute("DELETE FROM concept_group WHERE id=?", (group_id,))
    conn.commit()
    conn.close()
    return RedirectResponse("/ui/concept-groups", status_code=303)


# ---------------------------------------------------------------------------
# UI — add/remove concept from group definition
# ---------------------------------------------------------------------------


@ui_router.post(
    "/ui/concept-groups/{group_id}/concepts/add", response_class=HTMLResponse
)
def ui_add_concept_to_group(
    request: Request,
    group_id: int,
    concept_code: str = Form(...),
):
    code = concept_code.strip()
    if not code:
        raise HTTPException(400, "concept_code required")
    conn = _connect()
    cur = conn.cursor()
    cur.execute("SELECT concept_group_uid FROM concept_group WHERE id=?", (group_id,))
    row = cur.fetchone()
    if not row:
        conn.close()
        raise HTTPException(404, "Group not found")
    uid = row[0]
    from ..app import fetch_biomedical_concepts as _fetch_concepts

    lookup = {c["code"]: c["title"] for c in _fetch_concepts()}
    title = lookup.get(code, code)
    cur.execute(
        "INSERT OR IGNORE INTO concept_group_concept "
        "(concept_group_uid, concept_code, concept_title) VALUES (?,?,?)",
        (uid, code, title),
    )
    conn.commit()
    conn.close()
    return RedirectResponse("/ui/concept-groups", status_code=303)


@ui_router.post(
    "/ui/concept-groups/{group_id}/concepts/remove",
    response_class=HTMLResponse,
)
def ui_remove_concept_from_group(
    request: Request,
    group_id: int,
    concept_code: str = Form(...),
):
    conn = _connect()
    cur = conn.cursor()
    cur.execute("SELECT concept_group_uid FROM concept_group WHERE id=?", (group_id,))
    row = cur.fetchone()
    if not row:
        conn.close()
        raise HTTPException(404, "Group not found")
    uid = row[0]
    cur.execute(
        "DELETE FROM concept_group_concept "
        "WHERE concept_group_uid=? AND concept_code=?",
        (uid, concept_code.strip()),
    )
    conn.commit()
    conn.close()
    return RedirectResponse("/ui/concept-groups", status_code=303)


@ui_router.post(
    "/ui/concept-groups/{group_id}/concepts/add-category",
    response_class=HTMLResponse,
)
def ui_add_category_to_group(
    request: Request,
    group_id: int,
    category: str = Form(...),
):
    conn = _connect()
    cur = conn.cursor()
    cur.execute("SELECT concept_group_uid FROM concept_group WHERE id=?", (group_id,))
    row = cur.fetchone()
    if not row:
        conn.close()
        raise HTTPException(404, "Group not found")
    uid = row[0]
    from ..app import (
        fetch_biomedical_concepts_by_category as _fetch_by_cat,
    )

    concepts = _fetch_by_cat(category.strip())
    for c in concepts:
        cur.execute(
            "INSERT OR IGNORE INTO concept_group_concept "
            "(concept_group_uid, concept_code, concept_title) VALUES (?,?,?)",
            (uid, c["code"], c.get("title", c["code"])),
        )
    conn.commit()
    conn.close()
    return RedirectResponse("/ui/concept-groups", status_code=303)


# ---------------------------------------------------------------------------
# UI — add/remove surrogate from group definition
# ---------------------------------------------------------------------------


@ui_router.post(
    "/ui/concept-groups/{group_id}/surrogates/add", response_class=HTMLResponse
)
def ui_add_surrogate_to_group(
    request: Request,
    group_id: int,
    surrogate_id: int = Form(...),
):
    conn = _connect()
    cur = conn.cursor()
    cur.execute("SELECT concept_group_uid FROM concept_group WHERE id=?", (group_id,))
    row = cur.fetchone()
    if not row:
        conn.close()
        raise HTTPException(404, "Group not found")
    group_uid = row[0]
    cur.execute(
        "UPDATE biomedical_concept_surrogate SET concept_group_uid=? WHERE id=?",
        (group_uid, surrogate_id),
    )
    conn.commit()
    conn.close()
    return RedirectResponse("/ui/concept-groups", status_code=303)


@ui_router.post(
    "/ui/concept-groups/{group_id}/surrogates/remove",
    response_class=HTMLResponse,
)
def ui_remove_surrogate_from_group(
    request: Request,
    group_id: int,
    surrogate_id: int = Form(...),
):
    conn = _connect()
    cur = conn.cursor()
    cur.execute(
        "UPDATE biomedical_concept_surrogate SET concept_group_uid=NULL WHERE id=?",
        (surrogate_id,),
    )
    conn.commit()
    conn.close()
    return RedirectResponse("/ui/concept-groups", status_code=303)


# ---------------------------------------------------------------------------
# UI — HTMX add/remove group on activity (returns concepts_cell partial)
# ---------------------------------------------------------------------------


@ui_router.post(
    "/ui/soa/{soa_id}/activity/{activity_id}/concept-groups/add",
    response_class=HTMLResponse,
)
def ui_add_group_to_activity(
    request: Request,
    background_tasks: BackgroundTasks,
    soa_id: int,
    activity_id: int,
    concept_group_uid: str = Form(...),
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
        "SELECT 1 FROM concept_group WHERE concept_group_uid=?",
        (concept_group_uid,),
    )
    if not cur.fetchone():
        conn.close()
        raise HTTPException(404, "Group not found")

    # Fetch group concepts before expanding (for background enrichment)
    cur.execute(
        "SELECT concept_code FROM concept_group_concept WHERE concept_group_uid=?",
        (concept_group_uid,),
    )
    codes = [r[0] for r in cur.fetchall()]

    _expand_group_to_activity(cur, soa_id, activity_id, activity_uid, concept_group_uid)
    conn.commit()
    conn.close()

    from ..app import (
        _enrich_biomedical_concept_bg,
        _enrich_code_bg,
        _populate_biomedical_concept_properties_bg,
    )

    for code in codes:
        background_tasks.add_task(_enrich_biomedical_concept_bg, code, soa_id)
        background_tasks.add_task(_enrich_code_bg, code, soa_id)
        background_tasks.add_task(
            _populate_biomedical_concept_properties_bg,
            code,
            None,
            soa_id,
        )

    from .bc_surrogates import _render_concepts_cell

    return _render_concepts_cell(request, soa_id, activity_id)


@ui_router.post(
    "/ui/soa/{soa_id}/activity/{activity_id}/concept-groups/remove",
    response_class=HTMLResponse,
)
def ui_remove_group_from_activity(
    request: Request,
    soa_id: int,
    activity_id: int,
    concept_group_uid: str = Form(...),
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
        "DELETE FROM activity_concept "
        "WHERE activity_id=? AND soa_id=? AND concept_group_uid=?",
        (activity_id, soa_id, concept_group_uid),
    )
    cur.execute(
        "DELETE FROM activity_surrogate "
        "WHERE activity_uid=? AND soa_id=? AND concept_group_uid=?",
        (activity_uid, soa_id, concept_group_uid),
    )
    conn.commit()
    conn.close()

    from .bc_surrogates import _render_concepts_cell

    return _render_concepts_cell(request, soa_id, activity_id)
