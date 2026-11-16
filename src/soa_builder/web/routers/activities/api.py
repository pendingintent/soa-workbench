"""activities: api (split from activities.py)."""

from ._shared import (
    ActivityCreate,
    ActivityUpdate,
    BackgroundTasks,
    Body,
    BulkActivities,
    Form,
    HTMLResponse,
    HTTPException,
    JSONResponse,
    List,
    Request,
    _connect,
    _get_next_concept_uid,
    _record_activity_audit,
    _record_reorder_audit,
    _table_has_columns,
    html,
    router,
    soa_exists,
)
from .helpers import _next_activity_uid, fetch_biomedical_concepts


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
    """Reconcile assigned concept codes; unchanged codes keep their UID untouched."""
    if not soa_exists(soa_id):
        raise HTTPException(404, "SOA not found")
    conn = _connect()
    cur = conn.cursor()
    cur.execute("SELECT 1 FROM activity WHERE id=? AND soa_id=?", (activity_id, soa_id))
    if not cur.fetchone():
        conn.close()
        raise HTTPException(404, "Activity not found")
    ac_has_soa = _table_has_columns(cur, "activity_concept", ("soa_id",))
    ac_has_actuid = _table_has_columns(cur, "activity_concept", ("activity_uid",))
    ac_has_conceptuid = _table_has_columns(cur, "activity_concept", ("concept_uid",))
    # Capture existing (code, uid) pairs to diff against the request.
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
    old_codes = {code for code, _uid in old_pairs}
    new_codes = list(dict.fromkeys(c.strip() for c in concept_codes if c.strip()))
    new_codes_set = set(new_codes)
    to_remove_pairs = [
        (code, uid) for code, uid in old_pairs if code not in new_codes_set
    ]
    to_add = [code for code in new_codes if code not in old_codes]

    if to_remove_pairs:
        remove_codes = [code for code, _uid in to_remove_pairs]
        placeholders = ",".join("?" for _ in remove_codes)
        if ac_has_soa:
            cur.execute(
                "DELETE FROM activity_concept WHERE activity_id=? AND soa_id=?"
                f" AND concept_code IN ({placeholders})",
                (activity_id, soa_id, *remove_codes),
            )
        else:
            cur.execute(
                "DELETE FROM activity_concept WHERE activity_id=?"
                f" AND concept_code IN ({placeholders})",
                (activity_id, *remove_codes),
            )
    concepts = fetch_biomedical_concepts()
    lookup = {c["code"]: c["title"] for c in concepts}
    # Fetch activity_uid once for inserts
    cur.execute("SELECT activity_uid FROM activity WHERE id=?", (activity_id,))
    row = cur.fetchone()
    activity_uid = row[0] if row else None
    from ...services.concepts import (
        _upsert_biomedical_concept,
        _enrich_biomedical_concept_bg,
        _enrich_code_bg,
        _cleanup_orphaned_concept_rows,
        _populate_biomedical_concept_properties_bg,
    )

    inserted = 0
    for ccode in to_add:
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
    _cleanup_orphaned_concept_rows(cur, soa_id, to_remove_pairs)
    conn.commit()
    conn.close()
    return {"activity_id": activity_id, "concepts_set": len(new_codes)}
