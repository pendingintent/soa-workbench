import html
import json
import logging

import os
import time
from typing import List

from fastapi import APIRouter, BackgroundTasks, Body, HTTPException, Request, Form
from fastapi.responses import JSONResponse, HTMLResponse, RedirectResponse
from fastapi.templating import Jinja2Templates

from ..audit import _record_activity_audit, _record_reorder_audit
from ..db import _connect
from ..schemas import ActivityCreate, ActivityUpdate, BulkActivities
from ..utils import (
    soa_exists,
    table_has_columns as _table_has_columns,
    get_next_concept_uid as _get_next_concept_uid,
    get_cdisc_api_key as _get_cdisc_api_key,
    get_cdisc_crf_api_key as _get_cdisc_crf_api_key,
    get_cdisc_crf_subscription_key as _get_cdisc_crf_subscription_key,
)
import html as _html

_ACT_CONCEPT_CACHE = {"data": None, "fetched_at": 0}
_ACT_CONCEPT_TTL = int(os.environ.get("SOA_BUILDER_CACHE_TTL", "3600"))

router = APIRouter(prefix="/soa/{soa_id}")
ui_router = APIRouter()
logger = logging.getLogger("soa_builder.web.routers.activities")
templates = Jinja2Templates(
    directory=os.path.join(os.path.dirname(__file__), "..", "templates")
)


def fetch_biomedical_concepts(force: bool = False):
    override_json = os.environ.get("CDISC_CONCEPTS_JSON")
    if override_json:
        try:
            data = json.loads(override_json)
            # Normalize data into an iterable list of dicts
            if isinstance(data, dict):
                # Common patterns: {'items': [...]}, {'concepts': [...]}, or direct properties
                candidate_lists = []
                for key in ["items", "concepts", "data"]:
                    val = data.get(key)
                    if isinstance(val, list):
                        candidate_lists.append(val)
                if candidate_lists:
                    iterable = candidate_lists[0]
                else:
                    # Fallback: treat dict values; filter only list of dicts or single dicts
                    vals = []
                    for v in data.values():
                        if isinstance(v, list):
                            vals.extend([x for x in v if isinstance(x, dict)])
                        elif isinstance(v, dict):
                            vals.append(v)
                    iterable = vals
            elif isinstance(data, list):
                iterable = data
            else:
                iterable = []
            concepts = []
            for c in iterable:
                if not isinstance(c, dict):
                    continue
                code = c.get("code") or c.get("concept_code")
                title = c.get("title") or c.get("concept_title") or code
                if code:
                    concepts.append({"code": code, "title": title})
            return concepts
        except Exception as e:
            logger.debug("fetch_biomedical_concepts override JSON parse failed: %s", e)
            return []
    now = time.time()
    if (
        not force
        and _ACT_CONCEPT_CACHE["data"]
        and now - _ACT_CONCEPT_CACHE["fetched_at"] < _ACT_CONCEPT_TTL
    ):
        return _ACT_CONCEPT_CACHE["data"]
    # Remote fetch intentionally omitted here to prevent dependency & circular import; return empty list (titles fallback to codes)
    _ACT_CONCEPT_CACHE["data"] = []
    _ACT_CONCEPT_CACHE["fetched_at"] = now
    return []


# Removed local _soa_exists; using shared utils.soa_exists


@router.get("/activities", response_class=JSONResponse)
def list_activities(soa_id: int):
    if not soa_exists(soa_id):
        raise HTTPException(404, "SOA not found")
    conn = _connect()
    cur = conn.cursor()
    cur.execute(
        "SELECT id,name,order_index,activity_uid,label,description FROM activity WHERE soa_id=? ORDER BY order_index",
        (soa_id,),
    )
    rows = [
        {
            "id": r[0],
            "name": r[1],
            "order_index": r[2],
            "activity_uid": r[3],
            "label": r[4],
            "description": r[5],
        }
        for r in cur.fetchall()
    ]
    conn.close()
    return JSONResponse(rows)


@router.get("/activities/{activity_id}", response_class=JSONResponse)
def get_activity(soa_id: int, activity_id: int):
    if not soa_exists(soa_id):
        raise HTTPException(404, "SOA not found")
    conn = _connect()
    cur = conn.cursor()
    cur.execute(
        "SELECT id,name,order_index,activity_uid,label,description FROM activity WHERE id=? AND soa_id=?",
        (activity_id, soa_id),
    )
    row = cur.fetchone()
    conn.close()
    if not row:
        raise HTTPException(404, "Activity not found")
    return {
        "id": row[0],
        "soa_id": soa_id,
        "name": row[1],
        "order_index": row[2],
        "activity_uid": row[3],
        "label": row[4],
        "description": row[5],
    }


def _next_activity_uid(cur, soa_id: int) -> str:
    """Return the next Activity_N UID, never reusing a deleted one.

    Scans both the live table and the audit trail so deleted UIDs are
    never recycled — matching the pattern used by _next_study_cell_uid.
    """
    max_n = 0
    cur.execute("SELECT activity_uid FROM activity WHERE soa_id=?", (soa_id,))
    for (uid,) in cur.fetchall():
        if isinstance(uid, str) and uid.startswith("Activity_"):
            try:
                n = int(uid.split("_")[-1])
                if n > max_n:
                    max_n = n
            except (ValueError, IndexError):
                pass
    cur.execute(
        "SELECT before_json, after_json FROM activity_audit WHERE soa_id=?",
        (soa_id,),
    )
    for before_raw, after_raw in cur.fetchall():
        for raw in (before_raw, after_raw):
            if not raw:
                continue
            try:
                uid = json.loads(raw).get("activity_uid", "")
                if isinstance(uid, str) and uid.startswith("Activity_"):
                    n = int(uid.split("_")[-1])
                    if n > max_n:
                        max_n = n
            except Exception:
                pass
    return f"Activity_{max_n + 1}"


@router.post("/activities", response_class=JSONResponse)
def add_activity(soa_id: int, payload: ActivityCreate):
    if not soa_exists(soa_id):
        raise HTTPException(404, "SOA not found")
    conn = _connect()
    cur = conn.cursor()
    # Determine next order_index
    cur.execute(
        "SELECT COALESCE(MAX(order_index),0) FROM activity WHERE soa_id=?", (soa_id,)
    )
    order_index = (cur.fetchone() or [0])[0] + 1
    activity_uid = _next_activity_uid(cur, soa_id)

    name = (payload.name or "").strip()
    label = (payload.label or "").strip() or None
    description = (payload.description or "").strip() or None
    if not name:
        conn.close()
        raise HTTPException(400, "Name required")

    # Insert guarding for legacy schemas that may not have label/description
    cur.execute("PRAGMA table_info(activity)")
    cols = {r[1] for r in cur.fetchall()}
    if "label" in cols and "description" in cols:
        cur.execute(
            "INSERT INTO activity (soa_id,name,order_index,activity_uid,label,description) VALUES (?,?,?,?,?,?)",
            (soa_id, name, order_index, activity_uid, label, description),
        )
    else:
        cur.execute(
            "INSERT INTO activity (soa_id,name,order_index,activity_uid) VALUES (?,?,?,?)",
            (soa_id, name, order_index, activity_uid),
        )
    aid = cur.lastrowid
    conn.commit()
    conn.close()

    after = {
        "id": aid,
        "name": name,
        "order_index": order_index,
        "activity_uid": activity_uid,
        "label": label,
        "description": description,
    }
    _record_activity_audit(soa_id, "create", aid, before=None, after=after)
    return {
        "activity_id": aid,
        "order_index": order_index,
        "activity_uid": activity_uid,
    }


@router.post("/activities/add", response_class=HTMLResponse)
def ui_add_activity(
    request: Request,
    soa_id: int,
    name: str | None = Form(None),
    label: str | None = Form(None),
    description: str | None = Form(None),
):
    """UI form handler to add an Activity, then redirect to the edit page.

    Accepts standard form fields and reuses the JSON create logic.
    """
    if not soa_exists(soa_id):
        raise HTTPException(404, "SOA not found")
    payload = ActivityCreate(name=name or "", label=label, description=description)
    add_activity(soa_id, payload)
    redirect_url = f"/ui/soa/{int(soa_id)}/activities"
    if request.headers.get("HX-Request") == "true":
        return HTMLResponse("", headers={"HX-Redirect": redirect_url})
    safe_redirect_url = html.escape(redirect_url)
    return HTMLResponse(f"<script>window.location='{safe_redirect_url}';</script>")


@router.patch("/activities/{activity_id}", response_class=JSONResponse)
def update_activity(soa_id: int, activity_id: int, payload: ActivityUpdate):
    if not soa_exists(soa_id):
        raise HTTPException(404, "SOA not found")
    conn = _connect()
    cur = conn.cursor()
    cur.execute(
        "SELECT id,name,order_index,activity_uid,label,description FROM activity WHERE id=? AND soa_id=?",
        (activity_id, soa_id),
    )
    row = cur.fetchone()
    if not row:
        conn.close()
        raise HTTPException(404, "Activity not found")

    before = {
        "id": row[0],
        "name": row[1],
        "order_index": row[2],
        "activity_uid": row[3],
        "label": row[4],
        "description": row[5],
    }

    # Apply payload with trimming; None means "unchanged"
    new_name = (payload.name if payload.name is not None else before["name"]) or ""
    new_label = payload.label if payload.label is not None else before["label"]
    new_description = (
        payload.description
        if payload.description is not None
        else before["description"]
    )

    new_name = new_name.strip()
    new_label = (new_label or "").strip() or None
    new_description = (new_description or "").strip() or None

    cur.execute(
        "UPDATE activity SET name=?, label=?, description=? WHERE id=? AND soa_id=?",
        (new_name or None, new_label, new_description, activity_id, soa_id),
    )
    conn.commit()
    cur.execute(
        "SELECT id,name,order_index,activity_uid,label,description FROM activity WHERE id=? AND soa_id=?",
        (activity_id, soa_id),
    )
    r = cur.fetchone()
    conn.close()

    after = {
        "id": r[0],
        "name": r[1],
        "order_index": r[2],
        "activity_uid": r[3],
        "label": r[4],
        "description": r[5],
    }

    # Correct updated_fields calculation
    updated_fields = []
    for fld in ("name", "label", "description"):
        if (before.get(fld) or None) != (after.get(fld) or None):
            updated_fields.append(fld)

    _record_activity_audit(
        soa_id,
        "update",
        activity_id,
        before=before,
        after={**after, "updated_fields": updated_fields},
    )
    return JSONResponse({**after, "updated_fields": updated_fields})


@router.post("/activities/{activity_id}/update", response_class=HTMLResponse)
def ui_update_activity(
    request: Request,
    soa_id: int,
    activity_id: int,
    name: str | None = Form(None),
    label: str | None = Form(None),
    description: str | None = Form(None),
):
    """UI form handler to update an Activity and redirect back to edit page.

    This wraps the JSON update endpoint and returns an HTML redirect suitable
    for both full page and HTMX requests.
    """
    if not soa_exists(soa_id):
        raise HTTPException(404, "SOA not found")
    payload = ActivityUpdate(name=name, label=label, description=description)
    # Reuse the JSON handler for business logic/audit
    update_activity(soa_id, activity_id, payload)
    redirect_url = f"/ui/soa/{int(soa_id)}/activities"
    if request.headers.get("HX-Request") == "true":
        return HTMLResponse("", headers={"HX-Redirect": redirect_url})
    safe_redirect_url = html.escape(redirect_url)
    return HTMLResponse(f"<script>window.location='{safe_redirect_url}';</script>")


@router.post("/activities/reorder", response_class=JSONResponse)
def reorder_activities_api(soa_id: int, order: List[int] = Body(..., embed=True)):
    if not soa_exists(soa_id):
        raise HTTPException(404, "SOA not found")
    if not order:
        raise HTTPException(400, "Order list required")

    conn = _connect()
    cur = conn.cursor()

    # Validate IDs exist in this SOA
    cur.execute("SELECT id FROM activity WHERE soa_id=?", (soa_id,))
    existing = {r[0] for r in cur.fetchall()}
    if set(order) - existing:
        conn.close()
        raise HTTPException(400, "Order contains invalid activity id")

    cur.execute(
        "SELECT id FROM activity WHERE soa_id=? ORDER BY order_index", (soa_id,)
    )
    old_order = [r[0] for r in cur.fetchall()]

    before_rows = {
        r[0]: r[1]
        for r in cur.execute(
            "SELECT id,order_index FROM activity WHERE soa_id=?", (soa_id,)
        ).fetchall()
    }

    # Apply new order_index
    for idx, aid in enumerate(order, start=1):
        cur.execute(
            "UPDATE activity SET order_index=? WHERE id=? AND soa_id=?",
            (idx, aid, soa_id),
        )

    after_rows = {
        r[0]: r[1]
        for r in cur.execute(
            "SELECT id,order_index FROM activity WHERE soa_id=?", (soa_id,)
        ).fetchall()
    }

    conn.commit()
    conn.close()

    _record_reorder_audit(soa_id, "activity", old_order, order)

    reorder_details = [
        {
            "id": aid,
            "before_order_index": before_rows.get(aid),
            "after_order_index": after_rows.get(aid),
        }
        for aid in order
    ]

    _record_activity_audit(
        soa_id,
        "reorder",
        activity_id=None,
        before={"old_order": old_order},
        after={"new_order": order, "details": reorder_details},
    )
    return JSONResponse({"ok": True, "old_order": old_order, "new_order": order})


@router.post("/activities/bulk", response_class=JSONResponse)
def add_activities_bulk(soa_id: int, payload: BulkActivities):
    if not soa_exists(soa_id):
        raise HTTPException(404, "SOA not found")
    names = [n.strip() for n in payload.names if n and n.strip()]
    if not names:
        return {"added": 0, "skipped": 0, "details": []}
    conn = _connect()
    cur = conn.cursor()
    cur.execute("SELECT name FROM activity WHERE soa_id=?", (soa_id,))
    existing = set(r[0].lower() for r in cur.fetchall())
    cur.execute("SELECT COUNT(*) FROM activity WHERE soa_id=?", (soa_id,))
    count = cur.fetchone()[0]
    order_index = count
    added = []
    skipped = []
    for name in names:
        lname = name.lower()
        if lname in existing:
            skipped.append(name)
            continue
        order_index += 1
        cur.execute(
            "INSERT INTO activity (soa_id,name,order_index,activity_uid) VALUES (?,?,?,?)",
            (soa_id, name, order_index, _next_activity_uid(cur, soa_id)),
        )
        added.append(name)
        existing.add(lname)
    conn.commit()
    conn.close()
    return {
        "added": len(added),
        "skipped": len(skipped),
        "details": {"added": added, "skipped": skipped},
    }


@router.post("/activities/{activity_id}/concepts", response_class=JSONResponse)
def set_activity_concepts(
    soa_id: int,
    activity_id: int,
    concept_codes: List[str],
    background_tasks: BackgroundTasks,
):
    if not soa_exists(soa_id):
        raise HTTPException(404, "SOA not found")
    conn = _connect()
    cur = conn.cursor()
    cur.execute("SELECT 1 FROM activity WHERE id=? AND soa_id=?", (activity_id, soa_id))
    if not cur.fetchone():
        conn.close()
        raise HTTPException(404, "Activity not found")
    # Clear existing mappings; include soa_id if column exists
    ac_has_soa = _table_has_columns(cur, "activity_concept", ("soa_id",))
    ac_has_actuid = _table_has_columns(cur, "activity_concept", ("activity_uid",))
    ac_has_conceptuid = _table_has_columns(cur, "activity_concept", ("concept_uid",))
    # Capture existing pairs before delete for cascade cleanup
    if ac_has_soa:
        if ac_has_conceptuid:
            cur.execute(
                "SELECT concept_code, concept_uid FROM activity_concept"
                " WHERE activity_id=? AND soa_id=?",
                (activity_id, soa_id),
            )
        else:
            cur.execute(
                "SELECT concept_code, NULL FROM activity_concept"
                " WHERE activity_id=? AND soa_id=?",
                (activity_id, soa_id),
            )
    else:
        cur.execute(
            "SELECT concept_code, NULL FROM activity_concept WHERE activity_id=?",
            (activity_id,),
        )
    old_pairs = cur.fetchall()
    if ac_has_soa:
        cur.execute(
            "DELETE FROM activity_concept WHERE activity_id=? AND soa_id=?",
            (activity_id, soa_id),
        )
    else:
        cur.execute("DELETE FROM activity_concept WHERE activity_id=?", (activity_id,))
    concepts = fetch_biomedical_concepts()
    lookup = {c["code"]: c["title"] for c in concepts}
    # Fetch activity_uid once for inserts
    cur.execute("SELECT activity_uid FROM activity WHERE id=?", (activity_id,))
    row = cur.fetchone()
    activity_uid = row[0] if row else None
    from ..app import (
        _upsert_biomedical_concept,
        _enrich_biomedical_concept_bg,
        _enrich_code_bg,
        _cleanup_orphaned_concept_rows,
        _populate_biomedical_concept_properties_bg,
    )

    inserted = 0
    for code in concept_codes:
        ccode = code.strip()
        if not ccode:
            continue
        title = lookup.get(ccode, ccode)
        concept_uid = _get_next_concept_uid(cur, soa_id) if ac_has_conceptuid else None
        if ac_has_soa and ac_has_actuid:
            if ac_has_conceptuid:
                cur.execute(
                    "INSERT INTO activity_concept (soa_id, activity_id, activity_uid, concept_uid, concept_code, concept_title) VALUES (?,?,?,?,?,?)",
                    (soa_id, activity_id, activity_uid, concept_uid, ccode, title),
                )
            else:
                cur.execute(
                    "INSERT INTO activity_concept (soa_id, activity_id, activity_uid, concept_code, concept_title) VALUES (?,?,?,?,?)",
                    (soa_id, activity_id, activity_uid, ccode, title),
                )
        elif ac_has_actuid:
            if ac_has_conceptuid:
                cur.execute(
                    "INSERT INTO activity_concept (activity_id, activity_uid, concept_uid, concept_code, concept_title) VALUES (?,?,?,?,?)",
                    (activity_id, activity_uid, concept_uid, ccode, title),
                )
            else:
                cur.execute(
                    "INSERT INTO activity_concept (activity_id, activity_uid, concept_code, concept_title) VALUES (?,?,?,?)",
                    (activity_id, activity_uid, ccode, title),
                )
        elif ac_has_soa:
            if ac_has_conceptuid:
                cur.execute(
                    "INSERT INTO activity_concept (soa_id, activity_id, concept_uid, concept_code, concept_title) VALUES (?,?,?,?,?)",
                    (soa_id, activity_id, concept_uid, ccode, title),
                )
            else:
                cur.execute(
                    "INSERT INTO activity_concept (soa_id, activity_id, concept_code, concept_title) VALUES (?,?,?,?)",
                    (soa_id, activity_id, ccode, title),
                )
        else:
            if ac_has_conceptuid:
                cur.execute(
                    "INSERT INTO activity_concept (activity_id, concept_uid, concept_code, concept_title) VALUES (?,?,?,?)",
                    (activity_id, concept_uid, ccode, title),
                )
            else:
                cur.execute(
                    "INSERT INTO activity_concept (activity_id, concept_code, concept_title) VALUES (?,?,?)",
                    (activity_id, ccode, title),
                )
        _upsert_biomedical_concept(cur, soa_id, concept_uid, title, ccode)
        background_tasks.add_task(_enrich_biomedical_concept_bg, ccode, soa_id)
        background_tasks.add_task(_enrich_code_bg, ccode, soa_id)
        background_tasks.add_task(
            _populate_biomedical_concept_properties_bg,
            ccode,
            concept_uid,
            soa_id,
        )
        inserted += 1
    _cleanup_orphaned_concept_rows(cur, soa_id, old_pairs)
    conn.commit()
    conn.close()
    return {"activity_id": activity_id, "concepts_set": inserted}


# ---------------------------------------------------------------------------
# UI routes (served via ui_router, no prefix)
# ---------------------------------------------------------------------------


def _reindex_activities(soa_id: int):
    """Re-number order_index after a delete. activity_uid is immutable and never changed."""
    conn = _connect()
    cur = conn.cursor()
    cur.execute(
        "SELECT id FROM activity WHERE soa_id=? ORDER BY order_index", (soa_id,)
    )
    ids = [r[0] for r in cur.fetchall()]
    for idx, _id in enumerate(ids, start=1):
        cur.execute("UPDATE activity SET order_index=? WHERE id=?", (idx, _id))
    conn.commit()
    conn.close()


@ui_router.get("/ui/soa/{soa_id}/activities", response_class=HTMLResponse)
def ui_list_activities(request: Request, soa_id: int):
    if not soa_exists(soa_id):
        raise HTTPException(404, "SOA not found")

    conn = _connect()
    cur = conn.cursor()
    cur.execute(
        "SELECT id,name,order_index,activity_uid,label,description FROM activity WHERE soa_id=? ORDER BY order_index",
        (soa_id,),
    )
    activities = [
        {
            "id": r[0],
            "name": r[1],
            "order_index": r[2],
            "activity_uid": r[3],
            "label": r[4],
            "description": r[5],
        }
        for r in cur.fetchall()
    ]

    # Fetch activity concepts for all activities in this SOA
    activity_concepts: dict = {}
    has_group = _table_has_columns(cur, "activity_concept", ("concept_group_uid",))
    has_category = _table_has_columns(cur, "activity_concept", ("bc_category_name",))
    if _table_has_columns(cur, "activity_concept", ("soa_id",)):
        if has_group and has_category:
            cur.execute(
                "SELECT ac.activity_id, ac.concept_code, ac.concept_title, "
                "ac.concept_group_uid, cg.name, ac.bc_category_name "
                "FROM activity_concept ac "
                "LEFT JOIN concept_group cg "
                "ON cg.concept_group_uid=ac.concept_group_uid "
                "WHERE ac.soa_id=? "
                "ORDER BY ac.bc_category_name NULLS LAST, "
                "ac.concept_group_uid NULLS LAST, ac.id",
                (soa_id,),
            )
        elif has_group:
            cur.execute(
                "SELECT ac.activity_id, ac.concept_code, ac.concept_title, "
                "ac.concept_group_uid, cg.name, NULL "
                "FROM activity_concept ac "
                "LEFT JOIN concept_group cg "
                "ON cg.concept_group_uid=ac.concept_group_uid "
                "WHERE ac.soa_id=? "
                "ORDER BY ac.concept_group_uid NULLS LAST, ac.id",
                (soa_id,),
            )
        else:
            cur.execute(
                "SELECT activity_id, concept_code, concept_title, NULL, NULL, NULL "
                "FROM activity_concept WHERE soa_id=?",
                (soa_id,),
            )
    else:
        activity_ids = [a["id"] for a in activities]
        if activity_ids:
            placeholders = ",".join("?" * len(activity_ids))
            cur.execute(
                f"SELECT activity_id, concept_code, concept_title, NULL, NULL, NULL "
                f"FROM activity_concept WHERE activity_id IN ({placeholders})",
                activity_ids,
            )
        else:
            cur.execute("SELECT 1 WHERE 0")  # no-op
    for row in cur.fetchall():
        aid, code, title = row[0], row[1], row[2]
        concept_group_uid = row[3] if has_group else None
        group_name = row[4] if has_group else None
        bc_category_name = row[5] if has_category else None
        activity_concepts.setdefault(aid, []).append(
            {
                "code": code,
                "title": title,
                "concept_group_uid": concept_group_uid,
                "group_name": group_name,
                "bc_category_name": bc_category_name,
                "assigned_dss": [],
                "assigned_crf": [],
            }
        )

    # Fetch DSS assignments from the new many-to-many table
    cur.execute(
        "SELECT activity_id, concept_code, id, dss_title, dss_href, dss_display"
        " FROM activity_concept_dss WHERE soa_id=?",
        (soa_id,),
    )
    for aid, code, row_id, dss_title, dss_href, dss_display in cur.fetchall():
        for ac in activity_concepts.get(aid, []):
            if ac["code"] == code:
                ac["assigned_dss"].append(
                    {
                        "id": row_id,
                        "dss_title": dss_title,
                        "dss_href": dss_href,
                        "dss_display": dss_display or dss_title,
                    }
                )
                break

    # Fetch CRF assignments
    cur.execute(
        "SELECT activity_id, concept_code, id, crf_title, crf_href"
        " FROM activity_concept_crf WHERE soa_id=?",
        (soa_id,),
    )
    for aid, code, row_id, crf_title, crf_href in cur.fetchall():
        for ac in activity_concepts.get(aid, []):
            if ac["code"] == code:
                ac["assigned_crf"].append(
                    {
                        "id": row_id,
                        "crf_title": crf_title,
                        "crf_href": crf_href,
                    }
                )
                break

    # Fetch concept groups globally (for the dropdown in concepts_cell)
    cur.execute(
        "SELECT id, concept_group_uid, name, label FROM concept_group ORDER BY id"
    )
    concept_groups = [
        {"id": r[0], "concept_group_uid": r[1], "name": r[2], "label": r[3]}
        for r in cur.fetchall()
    ]
    conn.close()

    # Fetch CDISC BC categories list (for the category dropdown in concepts_cell)
    from ..app import fetch_biomedical_concept_categories as _fetch_cats

    bc_categories_list = _fetch_cats()

    # Fetch biomedical concepts list (lazy import to avoid circular dependency)
    from ..app import fetch_biomedical_concepts as _app_fetch_concepts

    concepts = _app_fetch_concepts()

    # Fetch surrogates for this SOA
    conn = _connect()
    cur = conn.cursor()
    cur.execute(
        "SELECT id, surrogate_uid, name, label, description, reference FROM biomedical_concept_surrogate WHERE soa_id=? ORDER BY id",
        (soa_id,),
    )
    surrogates = [
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

    # Per-activity surrogate mappings: activity_id -> [surrogate dicts]
    cur.execute(
        "SELECT a.id, bcs.id, bcs.surrogate_uid, bcs.name, bcs.label "
        "FROM activity_surrogate asr "
        "JOIN activity a ON a.activity_uid=asr.activity_uid AND a.soa_id=asr.soa_id "
        "JOIN biomedical_concept_surrogate bcs ON bcs.surrogate_uid=asr.surrogate_uid AND bcs.soa_id=asr.soa_id "
        "WHERE asr.soa_id=?",
        (soa_id,),
    )
    activity_surrogates: dict = {}
    for row in cur.fetchall():
        activity_id, sur_id, sur_uid, sur_name, sur_label = row
        activity_surrogates.setdefault(activity_id, []).append(
            {
                "id": sur_id,
                "surrogate_uid": sur_uid,
                "name": sur_name,
                "label": sur_label,
            }
        )

    cur.execute(
        "SELECT study_id, study_label, study_description, name, created_at FROM soa WHERE id=?",
        (soa_id,),
    )
    meta_row = cur.fetchone()
    conn.close()
    study_id, study_label, study_description, study_name, study_created_at = meta_row

    return templates.TemplateResponse(
        request,
        "activities.html",
        {
            "request": request,
            "soa_id": soa_id,
            "activities": activities,
            "activity_concepts": activity_concepts,
            "concepts": concepts,
            "surrogates": surrogates,
            "activity_surrogates": activity_surrogates,
            "concept_groups": concept_groups,
            "bc_categories_list": bc_categories_list,
            "study_id": study_id,
            "study_label": study_label,
            "study_description": study_description,
            "study_name": study_name,
        },
    )


@ui_router.post("/ui/soa/{soa_id}/activities/concepts_refresh")
def ui_refresh_concepts_activities(request: Request, soa_id: int):
    """Refresh biomedical concepts cache, then redirect back to activities page."""
    if not soa_exists(soa_id):
        raise HTTPException(404, "SOA not found")
    from ..app import fetch_biomedical_concepts as _app_fetch_concepts

    _app_fetch_concepts(force=True)
    redirect_url = f"/ui/soa/{int(soa_id)}/activities"
    if request.headers.get("HX-Request") == "true":
        return HTMLResponse("", headers={"HX-Redirect": redirect_url})
    return RedirectResponse(url=redirect_url, status_code=303)


@ui_router.post("/ui/soa/{soa_id}/activities/create")
def ui_create_activity(
    request: Request,
    soa_id: int,
    name: str | None = Form(None),
    label: str | None = Form(None),
    description: str | None = Form(None),
):
    if not soa_exists(soa_id):
        raise HTTPException(404, "SOA not found")
    payload = ActivityCreate(name=name or "", label=label, description=description)
    add_activity(soa_id, payload)
    return RedirectResponse(url=f"/ui/soa/{int(soa_id)}/activities", status_code=303)


@ui_router.post("/ui/soa/{soa_id}/activities/{activity_id}/delete")
def ui_delete_activity_page(request: Request, soa_id: int, activity_id: int):
    if not soa_exists(soa_id):
        raise HTTPException(404, "SOA not found")
    conn = _connect()
    cur = conn.cursor()
    cur.execute(
        "SELECT id,name,order_index FROM activity WHERE id=? AND soa_id=?",
        (activity_id, soa_id),
    )
    row = cur.fetchone()
    if not row:
        conn.close()
        raise HTTPException(404, "Activity not found")
    before = {"id": row[0], "name": row[1], "order_index": row[2]}
    # Capture concept pairs before deleting for cascade cleanup
    ac_has_conceptuid = _table_has_columns(cur, "activity_concept", ("concept_uid",))
    if ac_has_conceptuid:
        cur.execute(
            "SELECT concept_code, concept_uid FROM activity_concept"
            " WHERE activity_id=? AND soa_id=?",
            (activity_id, soa_id),
        )
    else:
        cur.execute(
            "SELECT concept_code, NULL FROM activity_concept"
            " WHERE activity_id=? AND soa_id=?",
            (activity_id, soa_id),
        )
    old_pairs = cur.fetchall()
    cur.execute(
        "DELETE FROM matrix_cells WHERE soa_id=? AND activity_id=?",
        (soa_id, activity_id),
    )
    cur.execute(
        "DELETE FROM activity_concept WHERE activity_id=? AND soa_id=?",
        (activity_id, soa_id),
    )
    from ..app import _cleanup_orphaned_concept_rows

    _cleanup_orphaned_concept_rows(cur, soa_id, old_pairs)
    cur.execute("DELETE FROM activity WHERE id=?", (activity_id,))
    conn.commit()
    conn.close()
    _reindex_activities(soa_id)
    _record_activity_audit(soa_id, "delete", activity_id, before=before, after=None)
    return RedirectResponse(url=f"/ui/soa/{int(soa_id)}/activities", status_code=303)


# ---------------------------------------------------------------------------
# DSS assignment endpoints
# ---------------------------------------------------------------------------


def _render_dss_cell(request, soa_id, activity_id):
    """Helper: render the dss_cell.html partial for a single activity."""
    conn = _connect()
    cur = conn.cursor()

    # Fetch concept rows for this activity
    if _table_has_columns(cur, "activity_concept", ("soa_id",)):
        cur.execute(
            "SELECT concept_code, concept_title"
            " FROM activity_concept WHERE activity_id=? AND soa_id=?",
            (activity_id, soa_id),
        )
    else:
        cur.execute(
            "SELECT concept_code, concept_title"
            " FROM activity_concept WHERE activity_id=?",
            (activity_id,),
        )
    concepts_list = [
        {"code": r[0], "title": r[1], "assigned_dss": [], "assigned_crf": []}
        for r in cur.fetchall()
    ]

    # Fetch DSS assignments from the new table
    concept_index = {c["code"]: c for c in concepts_list}
    cur.execute(
        "SELECT concept_code, id, dss_title, dss_href"
        " FROM activity_concept_dss"
        " WHERE soa_id=? AND activity_id=?",
        (soa_id, activity_id),
    )
    cur.execute(
        "SELECT concept_code, id, dss_title, dss_href, dss_display"
        " FROM activity_concept_dss"
        " WHERE soa_id=? AND activity_id=?",
        (soa_id, activity_id),
    )
    for code, row_id, dss_title, dss_href, dss_display in cur.fetchall():
        if code in concept_index:
            concept_index[code]["assigned_dss"].append(
                {
                    "id": row_id,
                    "dss_title": dss_title,
                    "dss_href": dss_href,
                    "dss_display": dss_display or dss_title,
                }
            )
    conn.close()

    activity_concepts = {activity_id: concepts_list}
    html = templates.get_template("dss_cell.html").render(
        request=request,
        soa_id=soa_id,
        activity_id=activity_id,
        activity_concepts=activity_concepts,
    )
    return HTMLResponse(html)


@ui_router.get(
    "/ui/soa/{soa_id}/activity/{activity_id}/concept/{concept_code}/dss/options",
    response_class=HTMLResponse,
)
def ui_dss_options(
    soa_id: int,
    activity_id: int,
    concept_code: str,
):
    """Return <option> elements for unassigned DSS for a concept (lazy load)."""
    from ..app import fetch_sdtm_specializations as _app_fetch_dss

    available = _app_fetch_dss(code=concept_code)

    conn = _connect()
    cur = conn.cursor()
    cur.execute(
        "SELECT dss_title FROM activity_concept_dss"
        " WHERE soa_id=? AND activity_id=? AND concept_code=?",
        (soa_id, activity_id, concept_code),
    )
    assigned_ids = {r[0] for r in cur.fetchall()}
    conn.close()

    options = ['<option value="" disabled selected>Select DSS...</option>']
    for d in available:
        dss_id = d["href"].rstrip("/").split("/")[-1]
        if dss_id not in assigned_ids:
            # value = dss_id||href||display_title
            value = f"{dss_id}||{d['href']}||{d['title']}"
            title_escaped = _html.escape(str(d["title"]), quote=True)
            value_escaped = _html.escape(value, quote=True)
            options.append(f'<option value="{value_escaped}">{title_escaped}</option>')
    return HTMLResponse("\n".join(options))


@ui_router.post(
    "/ui/soa/{soa_id}/activity/{activity_id}/concept/{concept_code}/dss",
    response_class=HTMLResponse,
)
def ui_save_dss_assignment(
    request: Request,
    soa_id: int,
    activity_id: int,
    concept_code: str,
    dss_selection: str = Form(""),
):
    """Add a DSS assignment for a specific concept on an activity."""
    if not soa_exists(soa_id):
        raise HTTPException(404, "SOA not found")

    conn = _connect()
    cur = conn.cursor()
    cur.execute(
        "SELECT 1 FROM activity WHERE id=? AND soa_id=?",
        (activity_id, soa_id),
    )
    if not cur.fetchone():
        conn.close()
        raise HTTPException(404, "Activity not found")

    # Parse selection value (datasetSpecializationId||href||display_title)
    selection = dss_selection.strip()
    if not selection or "||" not in selection:
        conn.close()
        return _render_dss_cell(request, soa_id, activity_id)
    parts = selection.split("||", 2)
    new_title = parts[0]
    new_href = parts[1] if len(parts) > 1 else ""
    new_display = parts[2] if len(parts) > 2 else new_title
    if not new_title or not new_href:
        conn.close()
        return _render_dss_cell(request, soa_id, activity_id)

    # Prevent duplicate assignments
    cur.execute(
        "SELECT id FROM activity_concept_dss"
        " WHERE soa_id=? AND activity_id=? AND concept_code=? AND dss_title=?",
        (soa_id, activity_id, concept_code, new_title),
    )
    if cur.fetchone():
        conn.close()
        return _render_dss_cell(request, soa_id, activity_id)

    cur.execute(
        "INSERT INTO activity_concept_dss"
        " (soa_id, activity_id, concept_code, dss_title, dss_href, dss_display)"
        " VALUES (?, ?, ?, ?, ?, ?)",
        (soa_id, activity_id, concept_code, new_title, new_href, new_display),
    )
    conn.commit()
    conn.close()

    _record_activity_audit(
        soa_id,
        "add_dss",
        activity_id,
        before=None,
        after={
            "concept_code": concept_code,
            "dss_title": new_title,
            "dss_href": new_href,
        },
    )

    return _render_dss_cell(request, soa_id, activity_id)


@ui_router.post(
    "/ui/soa/{soa_id}/activity/{activity_id}"
    "/concept/{concept_code}/dss/{dss_row_id}/delete",
    response_class=HTMLResponse,
)
def ui_delete_dss_assignment(
    request: Request,
    soa_id: int,
    activity_id: int,
    concept_code: str,
    dss_row_id: int,
):
    """Remove a single DSS assignment from activity_concept_dss."""
    if not soa_exists(soa_id):
        raise HTTPException(404, "SOA not found")

    conn = _connect()
    cur = conn.cursor()

    # Capture the row before deletion for audit
    cur.execute(
        "SELECT dss_title, dss_href FROM activity_concept_dss WHERE id=? AND soa_id=?",
        (dss_row_id, soa_id),
    )
    row = cur.fetchone()
    if not row:
        conn.close()
        return _render_dss_cell(request, soa_id, activity_id)
    old_title, old_href = row

    cur.execute(
        "DELETE FROM activity_concept_dss WHERE id=? AND soa_id=?",
        (dss_row_id, soa_id),
    )

    conn.commit()
    conn.close()

    _record_activity_audit(
        soa_id,
        "remove_dss",
        activity_id,
        before={
            "concept_code": concept_code,
            "dss_title": old_title,
            "dss_href": old_href,
        },
        after=None,
    )

    return _render_dss_cell(request, soa_id, activity_id)


@ui_router.get(
    "/ui/soa/{soa_id}/activity/{activity_id}/dss_cell",
    response_class=HTMLResponse,
)
def ui_get_dss_cell(request: Request, soa_id: int, activity_id: int):
    """Return the DSS cell partial for an activity."""
    if not soa_exists(soa_id):
        raise HTTPException(404, "SOA not found")
    return _render_dss_cell(request, soa_id, activity_id)


@ui_router.get(
    "/ui/soa/{soa_id}/dss/detail",
    response_class=HTMLResponse,
)
def ui_dss_detail(request: Request, soa_id: int, href: str = "", title: str = ""):
    """Detail page for a single DSS, fetched by href."""
    if not soa_exists(soa_id):
        raise HTTPException(404, "SOA not found")

    import requests as _requests

    api_key = _get_cdisc_api_key()
    subscription_key = os.environ.get("CDISC_SUBSCRIPTION_KEY")
    unified_key = subscription_key or api_key
    headers: dict = {}
    if unified_key:
        headers["Ocp-Apim-Subscription-Key"] = unified_key
    if api_key:
        headers["Authorization"] = f"Bearer {api_key}"
        headers["api-key"] = api_key

    from urllib.parse import urlparse as _urlparse, urlunparse as _urlunparse

    _bc_base = os.environ.get(
        "CDISC_BC_API_BASE_URL",
        "https://api.library.cdisc.org/api/cosmos/v2",
    )
    _p = _urlparse(_bc_base)

    status = None
    error = None
    pretty_json = None
    raw_text_snippet = None
    data = None
    if href:
        _parsed_href = _urlparse(href)
        if _parsed_href.netloc != _p.netloc or _parsed_href.scheme not in (
            "http",
            "https",
        ):
            error = "Invalid href: only CDISC Library API URLs are permitted."
            return templates.TemplateResponse(
                "dss_detail.html",
                {
                    "request": request,
                    "soa_id": soa_id,
                    "href": href,
                    "title": title,
                    "status": status,
                    "error": error,
                    "pretty_json": pretty_json,
                    "variables": [],
                    "summary": {},
                },
            )
        _safe_url = _urlunparse(
            (
                _p.scheme,
                _p.netloc,
                _parsed_href.path,
                _parsed_href.params,
                _parsed_href.query,
                "",
            )
        )
        try:
            resp = _requests.get(
                _safe_url,
                headers=headers,
                timeout=int(os.environ.get("CDISC_REQUEST_TIMEOUT", "15")),
                allow_redirects=False,
            )
            status = resp.status_code
            raw_text_snippet = resp.text[:500]
            if resp.status_code == 200:
                try:
                    data = resp.json()
                except ValueError:
                    error = "200 OK but response was not valid JSON"
                    data = None
                if data is not None:
                    try:
                        pretty_json = json.dumps(data, indent=2, sort_keys=True)
                    except Exception:
                        pretty_json = json.dumps(data, indent=2)
            else:
                error = f"HTTP {resp.status_code} retrieving specialization"
        except Exception as e:
            error = f"Fetch error: {e}"[:300]
    else:
        error = "No href provided."

    # Extract variables list and summary fields for structured display
    variables = []
    summary = {}
    if data and isinstance(data, dict):
        variables = data.get("variables", [])
        for key in (
            "datasetSpecializationId",
            "domain",
            "shortName",
            "source",
            "sdtmigStartVersion",
            "sdtmigEndVersion",
        ):
            val = data.get(key)
            if val is not None and val != "":
                summary[key] = val
        # Nested objects: check top-level and _links
        links = data.get("_links", {})
        pbc = data.get("parentBiomedicalConcept") or links.get(
            "parentBiomedicalConcept"
        )
        if isinstance(pbc, dict):
            parts = []
            if pbc.get("shortName"):
                parts.append(pbc["shortName"])
            elif pbc.get("title"):
                parts.append(pbc["title"])
            if pbc.get("conceptId"):
                parts.append(f"({pbc['conceptId']})")
            if parts:
                summary["parentBiomedicalConcept"] = " ".join(parts)
        ppkg = data.get("parentPackage") or links.get("parentPackage")
        if isinstance(ppkg, dict):
            parts = []
            if ppkg.get("name"):
                parts.append(ppkg["name"])
            elif ppkg.get("title"):
                parts.append(ppkg["title"])
            if ppkg.get("type"):
                parts.append(f"[{ppkg['type']}]")
            if parts:
                summary["parentPackage"] = " ".join(parts)
            ppkg_href = ppkg.get("href", "")
            if ppkg_href and not ppkg_href.startswith("http"):
                ppkg_href = f"https://api.library.cdisc.org{ppkg_href}"
            if ppkg_href:
                summary["parentPackageHref"] = ppkg_href

    return templates.TemplateResponse(
        request,
        "sdtm_specialization_detail.html",
        {
            "index": 0,
            "title": title or "(untitled)",
            "href": href,
            "status": status,
            "error": error,
            "pretty_json": pretty_json,
            "raw_text_snippet": raw_text_snippet,
            "missing_key": unified_key is None,
            "total": 1,
            "back_url": f"/ui/soa/{soa_id}/activities",
            "summary": summary,
            "variables": variables,
        },
    )


# ---------------------------------------------------------------------------
# CRF Specializations — cell helper + CRUD endpoints
# ---------------------------------------------------------------------------


def _render_crf_cell(request, soa_id, activity_id):
    """Helper: render the crf_cell.html partial for a single activity."""
    conn = _connect()
    cur = conn.cursor()

    if _table_has_columns(cur, "activity_concept", ("soa_id",)):
        cur.execute(
            "SELECT concept_code, concept_title"
            " FROM activity_concept WHERE activity_id=? AND soa_id=?",
            (activity_id, soa_id),
        )
    else:
        cur.execute(
            "SELECT concept_code, concept_title"
            " FROM activity_concept WHERE activity_id=?",
            (activity_id,),
        )
    concepts_list = [
        {"code": r[0], "title": r[1], "assigned_crf": []} for r in cur.fetchall()
    ]

    concept_index = {c["code"]: c for c in concepts_list}
    cur.execute(
        "SELECT concept_code, id, crf_title, crf_href"
        " FROM activity_concept_crf"
        " WHERE soa_id=? AND activity_id=?",
        (soa_id, activity_id),
    )
    for code, row_id, crf_title, crf_href in cur.fetchall():
        if code in concept_index:
            concept_index[code]["assigned_crf"].append(
                {
                    "id": row_id,
                    "crf_title": crf_title,
                    "crf_href": crf_href,
                }
            )
    conn.close()

    activity_concepts = {activity_id: concepts_list}
    html = templates.get_template("crf_cell.html").render(
        request=request,
        soa_id=soa_id,
        activity_id=activity_id,
        activity_concepts=activity_concepts,
    )
    return HTMLResponse(html)


@ui_router.get(
    "/ui/soa/{soa_id}/activity/{activity_id}/concept/{concept_code}/crf/options",
    response_class=HTMLResponse,
)
def ui_crf_options(
    soa_id: int,
    activity_id: int,
    concept_code: str,
):
    """Return <option> elements for unassigned CRF specializations (lazy load)."""
    from ..app import fetch_crf_specializations as _app_fetch_crf

    available = _app_fetch_crf(code=concept_code)

    conn = _connect()
    cur = conn.cursor()
    cur.execute(
        "SELECT crf_href FROM activity_concept_crf"
        " WHERE soa_id=? AND activity_id=? AND concept_code=?",
        (soa_id, activity_id, concept_code),
    )
    assigned_hrefs = {r[0] for r in cur.fetchall()}
    conn.close()

    options = ['<option value="" disabled selected>Select CRF...</option>']
    for d in available:
        href = d.get("href") or ""
        if href not in assigned_hrefs:
            title = d.get("title") or href
            value = f"{title}||{href}"
            title_escaped = _html.escape(str(title), quote=True)
            value_escaped = _html.escape(value, quote=True)
            options.append(f'<option value="{value_escaped}">{title_escaped}</option>')
    return HTMLResponse("\n".join(options))


@ui_router.post(
    "/ui/soa/{soa_id}/activity/{activity_id}/concept/{concept_code}/crf",
    response_class=HTMLResponse,
)
def ui_save_crf_assignment(
    request: Request,
    soa_id: int,
    activity_id: int,
    concept_code: str,
    crf_selection: str = Form(""),
):
    """Add a CRF specialization assignment for a concept on an activity."""
    if not soa_exists(soa_id):
        raise HTTPException(404, "SOA not found")

    conn = _connect()
    cur = conn.cursor()
    cur.execute(
        "SELECT 1 FROM activity WHERE id=? AND soa_id=?",
        (activity_id, soa_id),
    )
    if not cur.fetchone():
        conn.close()
        raise HTTPException(404, "Activity not found")

    selection = crf_selection.strip()
    if not selection or "||" not in selection:
        conn.close()
        return _render_crf_cell(request, soa_id, activity_id)
    parts = selection.split("||", 1)
    new_title = parts[0]
    new_href = parts[1] if len(parts) > 1 else ""
    if not new_title or not new_href:
        conn.close()
        return _render_crf_cell(request, soa_id, activity_id)

    # Prevent duplicate assignments (guard on href)
    cur.execute(
        "SELECT id FROM activity_concept_crf"
        " WHERE soa_id=? AND activity_id=? AND concept_code=? AND crf_href=?",
        (soa_id, activity_id, concept_code, new_href),
    )
    if cur.fetchone():
        conn.close()
        return _render_crf_cell(request, soa_id, activity_id)

    cur.execute(
        "INSERT INTO activity_concept_crf"
        " (soa_id, activity_id, concept_code, crf_title, crf_href)"
        " VALUES (?, ?, ?, ?, ?)",
        (soa_id, activity_id, concept_code, new_title, new_href),
    )
    conn.commit()
    conn.close()

    _record_activity_audit(
        soa_id,
        "add_crf",
        activity_id,
        before=None,
        after={
            "concept_code": concept_code,
            "crf_title": new_title,
            "crf_href": new_href,
        },
    )

    return _render_crf_cell(request, soa_id, activity_id)


@ui_router.post(
    "/ui/soa/{soa_id}/activity/{activity_id}"
    "/concept/{concept_code}/crf/{crf_row_id}/delete",
    response_class=HTMLResponse,
)
def ui_delete_crf_assignment(
    request: Request,
    soa_id: int,
    activity_id: int,
    concept_code: str,
    crf_row_id: int,
):
    """Remove a single CRF specialization assignment."""
    if not soa_exists(soa_id):
        raise HTTPException(404, "SOA not found")

    conn = _connect()
    cur = conn.cursor()

    cur.execute(
        "SELECT crf_title, crf_href FROM activity_concept_crf WHERE id=? AND soa_id=?",
        (crf_row_id, soa_id),
    )
    row = cur.fetchone()
    if not row:
        conn.close()
        return _render_crf_cell(request, soa_id, activity_id)
    old_title, old_href = row

    cur.execute(
        "DELETE FROM activity_concept_crf WHERE id=? AND soa_id=?",
        (crf_row_id, soa_id),
    )
    conn.commit()
    conn.close()

    _record_activity_audit(
        soa_id,
        "remove_crf",
        activity_id,
        before={
            "concept_code": concept_code,
            "crf_title": old_title,
            "crf_href": old_href,
        },
        after=None,
    )

    return _render_crf_cell(request, soa_id, activity_id)


@ui_router.get(
    "/ui/soa/{soa_id}/activity/{activity_id}/crf_cell",
    response_class=HTMLResponse,
)
def ui_get_crf_cell(request: Request, soa_id: int, activity_id: int):
    """Return the CRF cell partial for an activity."""
    if not soa_exists(soa_id):
        raise HTTPException(404, "SOA not found")
    return _render_crf_cell(request, soa_id, activity_id)


@ui_router.get(
    "/ui/soa/{soa_id}/activity/{activity_id}/crf/detail",
    response_class=HTMLResponse,
)
def ui_crf_detail_from_activity(
    request: Request,
    soa_id: int,
    activity_id: int,
    href: str = "",
    title: str = "",
):
    """Detail page for a single CRF specialization, linked from the activities page."""
    if not soa_exists(soa_id):
        raise HTTPException(404, "SOA not found")

    import requests as _requests
    import json as _json

    api_key = _get_cdisc_crf_api_key()
    subscription_key = _get_cdisc_crf_subscription_key()
    unified_key = subscription_key or api_key
    headers: dict = {}
    if unified_key:
        headers["Ocp-Apim-Subscription-Key"] = unified_key
    if api_key:
        headers["Authorization"] = f"Bearer {api_key}"
        headers["api-key"] = api_key

    from urllib.parse import urlparse as _urlparse, urlunparse as _urlunparse

    _crf_base = os.environ.get(
        "CDISC_CRF_API_BASE_URL",
    ) or os.environ.get(
        "CDISC_BC_API_BASE_URL",
        "https://api.library.cdisc.org/api/cosmos/v2",
    )
    _p = _urlparse(_crf_base)

    spec_data = None
    spec_json = None
    error = None
    status = None
    if href:
        _parsed_href = _urlparse(href)
        if _parsed_href.netloc != _p.netloc or _parsed_href.scheme not in (
            "http",
            "https",
        ):
            error = "Invalid href: only CDISC Library API URLs are permitted."
        else:
            _safe_url = _urlunparse(
                (
                    _p.scheme,
                    _p.netloc,
                    _parsed_href.path,
                    _parsed_href.params,
                    _parsed_href.query,
                    "",
                )
            )
            try:
                resp = _requests.get(
                    _safe_url,
                    headers=headers,
                    timeout=int(os.environ.get("CDISC_REQUEST_TIMEOUT", "15")),
                    allow_redirects=False,
                )
                status = resp.status_code
                if resp.status_code == 200:
                    try:
                        spec_data = resp.json()
                        spec_json = _json.dumps(spec_data, indent=2, sort_keys=True)
                    except ValueError:
                        error = "200 OK but response was not valid JSON"
                else:
                    error = f"HTTP {resp.status_code} retrieving CRF specialization"
            except Exception as e:
                error = f"Fetch error: {e}"[:300]
    else:
        error = "No href provided."

    return templates.TemplateResponse(
        request,
        "crf_specialization_detail.html",
        {
            "title": title or "(untitled)",
            "href": href,
            "status": status,
            "error": error,
            "spec_data": spec_data,
            "spec_json": spec_json,
            "missing_key": unified_key is None,
            "back_url": f"/ui/soa/{soa_id}/activities",
        },
    )
