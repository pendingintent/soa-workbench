"""soa_ui: activities (split from soa_ui.py)."""

from ._shared import (
    BackgroundTasks,
    Form,
    HTMLResponse,
    HTTPException,
    List,
    Request,
    _cleanup_orphaned_concept_rows,
    _connect,
    _enrich_biomedical_concept_bg,
    _enrich_code_bg,
    _get_activity_concepts,
    _get_activity_surrogates,
    _get_concept_groups_for_cell,
    _get_next_concept_uid,
    _record_activity_audit,
    _record_reorder_audit,
    _table_has_columns,
    _upsert_biomedical_concept,
    activities_router,
    concepts_service,
    delete_activity,
    router,
    soa_exists,
    templates,
)


# API endpoint for adding a BC to an activity
@router.post(
    "/ui/soa/{soa_id}/activity/{activity_id}/concepts/add", response_class=HTMLResponse
)
def ui_add_activity_concept(
    request: Request,
    soa_id: int,
    activity_id: int,
    background_tasks: BackgroundTasks,
    concept_code: str = Form(...),
):
    """Add Biomedical Concept to an Activity."""
    if not activity_id:
        raise HTTPException(400, "Missing activity_id")
    if not soa_exists(soa_id):
        raise HTTPException(404, "SOA not found")
    code = concept_code.strip()
    if not code:
        raise HTTPException(400, "Empty concept_code")
    concepts = concepts_service.fetch_biomedical_concepts()
    lookup = {c["code"]: c["title"] for c in concepts}
    title = lookup.get(code, code)
    conn = _connect()
    cur = conn.cursor()
    cur.execute("SELECT 1 FROM activity WHERE id=? AND soa_id=?", (activity_id, soa_id))
    if not cur.fetchone():
        conn.close()
        raise HTTPException(404, "Activity not found")
    # Check existence; include soa_id if column exists
    ac_has_soa = _table_has_columns(cur, "activity_concept", ("soa_id",))
    ac_has_actuid = _table_has_columns(cur, "activity_concept", ("activity_uid",))
    ac_has_conceptuid = _table_has_columns(cur, "activity_concept", ("concept_uid",))
    if ac_has_soa:
        cur.execute(
            "SELECT 1 FROM activity_concept WHERE activity_id=? AND concept_code=? AND soa_id=?",
            (activity_id, code, soa_id),
        )
    else:
        cur.execute(
            "SELECT 1 FROM activity_concept WHERE activity_id=? AND concept_code=?",
            (activity_id, code),
        )
    if not cur.fetchone():
        # Fetch activity_uid once
        cur.execute("SELECT activity_uid FROM activity WHERE id=?", (activity_id,))
        rr = cur.fetchone()
        activity_uid = rr[0] if rr else None
        concept_uid = _get_next_concept_uid(cur, soa_id) if ac_has_conceptuid else None
        if ac_has_soa and ac_has_actuid:
            if ac_has_conceptuid:
                cur.execute(
                    "INSERT INTO activity_concept (soa_id, activity_id, activity_uid, concept_uid, concept_code, concept_title) VALUES (?,?,?,?,?,?)",
                    (soa_id, activity_id, activity_uid, concept_uid, code, title),
                )
            else:
                cur.execute(
                    "INSERT INTO activity_concept (soa_id, activity_id, activity_uid, concept_code, concept_title) VALUES (?,?,?,?,?)",
                    (soa_id, activity_id, activity_uid, code, title),
                )
        elif ac_has_actuid:
            if ac_has_conceptuid:
                cur.execute(
                    "INSERT INTO activity_concept (activity_id, activity_uid, concept_uid, concept_code, concept_title) VALUES (?,?,?,?,?)",
                    (activity_id, activity_uid, concept_uid, code, title),
                )
            else:
                cur.execute(
                    "INSERT INTO activity_concept (activity_id, activity_uid, concept_code, concept_title) VALUES (?,?,?,?)",
                    (activity_id, activity_uid, code, title),
                )
        elif ac_has_soa:
            if ac_has_conceptuid:
                cur.execute(
                    "INSERT INTO activity_concept (soa_id, activity_id, concept_uid, concept_code, concept_title) VALUES (?,?,?,?,?)",
                    (soa_id, activity_id, concept_uid, code, title),
                )
            else:
                cur.execute(
                    "INSERT INTO activity_concept (soa_id, activity_id, concept_code, concept_title) VALUES (?,?,?,?)",
                    (soa_id, activity_id, code, title),
                )
        else:
            if ac_has_conceptuid:
                cur.execute(
                    "INSERT INTO activity_concept (activity_id, concept_uid, concept_code, concept_title) VALUES (?,?,?,?)",
                    (activity_id, concept_uid, code, title),
                )
            else:
                cur.execute(
                    "INSERT INTO activity_concept (activity_id, concept_code, concept_title) VALUES (?,?,?)",
                    (activity_id, code, title),
                )
        _upsert_biomedical_concept(cur, soa_id, concept_uid, title, code)
        conn.commit()
        background_tasks.add_task(_enrich_biomedical_concept_bg, code, soa_id)
        background_tasks.add_task(_enrich_code_bg, code, soa_id)
    conn.close()
    selected = _get_activity_concepts(activity_id)
    surrogates, selected_surrogate_list, selected_surrogate_uids = (
        _get_activity_surrogates(soa_id, activity_id)
    )
    concept_groups, activity_group_uids = _get_concept_groups_for_cell(
        soa_id, activity_id
    )
    concepts_html = templates.get_template("concepts_cell.html").render(
        request=request,
        soa_id=soa_id,
        activity_id=activity_id,
        concepts=concepts,
        selected_codes=[s["code"] for s in selected],
        selected_list=selected,
        surrogates=surrogates,
        selected_surrogate_list=selected_surrogate_list,
        selected_surrogate_uids=selected_surrogate_uids,
        concept_groups=concept_groups,
        activity_group_uids=activity_group_uids,
        edit=False,
    )
    dss_html = activities_router._render_dss_cell(
        request, soa_id, activity_id
    ).body.decode()
    # Wrap DSS cell in OOB swap so HTMX updates it alongside the concepts cell
    safe_activity_id = int(activity_id)
    dss_oob = dss_html.replace(
        f'id="dss-cell-{safe_activity_id}"',
        f'id="dss-cell-{safe_activity_id}" hx-swap-oob="outerHTML:#dss-cell-{safe_activity_id}"',
        1,
    )
    return HTMLResponse(concepts_html + dss_oob)


# UI endpoint for removing a BC from an Activity
@router.post(
    "/ui/soa/{soa_id}/activity/{activity_id}/concepts/remove",
    response_class=HTMLResponse,
)
def ui_remove_activity_concept(
    request: Request, soa_id: int, activity_id: int, concept_code: str = Form(...)
):
    """Remove Biomedical Concept from Activity."""
    if not activity_id:
        raise HTTPException(400, "Missing activity_id")
    if not soa_exists(soa_id):
        raise HTTPException(404, "SOA not found")
    code = concept_code.strip()
    if not code:
        raise HTTPException(400, "Empty concept_code")
    conn = _connect()
    cur = conn.cursor()
    # Delete mapping; include soa_id if column exists
    cur.execute("PRAGMA table_info(activity_concept)")
    ac_cols = {r[1] for r in cur.fetchall()}
    # Capture the concept_uid (if column exists) before deleting
    if "concept_uid" in ac_cols and "soa_id" in ac_cols:
        cur.execute(
            "SELECT concept_code, concept_uid FROM activity_concept"
            " WHERE activity_id=? AND concept_code=? AND soa_id=?",
            (activity_id, code, soa_id),
        )
    elif "soa_id" in ac_cols:
        cur.execute(
            "SELECT concept_code, NULL FROM activity_concept"
            " WHERE activity_id=? AND concept_code=? AND soa_id=?",
            (activity_id, code, soa_id),
        )
    else:
        cur.execute(
            "SELECT concept_code, NULL FROM activity_concept"
            " WHERE activity_id=? AND concept_code=?",
            (activity_id, code),
        )
    old_pairs = cur.fetchall()
    if "soa_id" in ac_cols:
        cur.execute(
            "DELETE FROM activity_concept WHERE activity_id=? AND concept_code=? AND soa_id=?",
            (activity_id, code, soa_id),
        )
    else:
        cur.execute(
            "DELETE FROM activity_concept WHERE activity_id=? AND concept_code=?",
            (activity_id, code),
        )
    if "soa_id" in ac_cols:
        _cleanup_orphaned_concept_rows(cur, soa_id, old_pairs)
    conn.commit()
    conn.close()
    concepts = concepts_service.fetch_biomedical_concepts()
    selected = _get_activity_concepts(activity_id)
    surrogates, selected_surrogate_list, selected_surrogate_uids = (
        _get_activity_surrogates(soa_id, activity_id)
    )
    concept_groups, activity_group_uids = _get_concept_groups_for_cell(
        soa_id, activity_id
    )
    concepts_html = templates.get_template("concepts_cell.html").render(
        request=request,
        soa_id=soa_id,
        activity_id=activity_id,
        concepts=concepts,
        selected_codes=[s["code"] for s in selected],
        selected_list=selected,
        surrogates=surrogates,
        selected_surrogate_list=selected_surrogate_list,
        selected_surrogate_uids=selected_surrogate_uids,
        concept_groups=concept_groups,
        activity_group_uids=activity_group_uids,
        edit=False,
    )
    dss_html = activities_router._render_dss_cell(
        request, soa_id, activity_id
    ).body.decode()
    safe_activity_id = int(activity_id)
    dss_oob = dss_html.replace(
        f'id="dss-cell-{safe_activity_id}"',
        f'id="dss-cell-{safe_activity_id}" hx-swap-oob="outerHTML:#dss-cell-{safe_activity_id}"',
        1,
    )
    return HTMLResponse(concepts_html + dss_oob)


# UI endpoint for adding an Activity
@router.post("/ui/soa/{soa_id}/add_activity", response_class=HTMLResponse)
def ui_add_activity(request: Request, soa_id: int, name: str = Form(...)):
    """Add an Activity to an SoA."""
    if not soa_exists(soa_id):
        raise HTTPException(404, "SOA not found")
    nm = (name or "").strip()
    if not nm:
        raise HTTPException(400, "Name required")
    conn = _connect()
    cur = conn.cursor()
    cur.execute("SELECT COUNT(*) FROM activity WHERE soa_id=?", (soa_id,))
    order_index = cur.fetchone()[0] + 1
    cur.execute(
        "INSERT INTO activity (soa_id,name,order_index,activity_uid) VALUES (?,?,?,?)",
        (soa_id, nm, order_index, activities_router._next_activity_uid(cur, soa_id)),
    )
    aid = cur.lastrowid
    conn.commit()
    conn.close()
    _record_activity_audit(
        soa_id,
        "create",
        aid,
        before=None,
        after={
            "id": aid,
            "name": nm,
            "order_index": order_index,
            "activity_uid": f"Activity_{order_index}",
        },
    )
    # If HTMX, redirect back to edit page; otherwise script redirect
    if request.headers.get("HX-Request") == "true":
        return HTMLResponse("", headers={"HX-Redirect": f"/ui/soa/{soa_id}/edit"})
    return HTMLResponse(
        f"<script>window.location='/ui/soa/{int(soa_id)}/edit';</script>"
    )


# UI endpoint for setting a BC to an Activity
@router.post(
    "/ui/soa/{soa_id}/activity/{activity_id}/concepts", response_class=HTMLResponse
)
def ui_set_activity_concepts(
    request: Request,
    soa_id: int,
    activity_id: int,
    background_tasks: BackgroundTasks,
    concept_codes: List[str] = Form([]),
):
    """Form handler to set Biomedical Concepts related to an Activity."""
    activities_router.set_activity_concepts(
        soa_id, activity_id, concept_codes, background_tasks
    )
    # Queue background DSS lookup for any concept without a DSS assigned
    conn = _connect()
    cur = conn.cursor()
    if _table_has_columns(cur, "activity_concept", ("soa_id",)):
        cur.execute(
            "SELECT concept_code FROM activity_concept"
            " WHERE activity_id=? AND soa_id=? AND (dss_title IS NULL OR dss_title='')",
            (activity_id, soa_id),
        )
    else:
        cur.execute(
            "SELECT concept_code FROM activity_concept"
            " WHERE activity_id=? AND (dss_title IS NULL OR dss_title='')",
            (activity_id,),
        )
    conn.close()
    # HTMX inline update support
    if request.headers.get("HX-Request") == "true":
        concepts = concepts_service.fetch_biomedical_concepts()
        conn = _connect()
        cur = conn.cursor()
        if _table_has_columns(cur, "activity_concept", ("soa_id",)):
            cur.execute(
                "SELECT concept_code, concept_title FROM activity_concept WHERE activity_id=? AND soa_id=?",
                (activity_id, soa_id),
            )
        else:
            cur.execute(
                "SELECT concept_code, concept_title FROM activity_concept WHERE activity_id=?",
                (activity_id,),
            )
        selected = [{"code": c, "title": t} for c, t in cur.fetchall()]
        conn.close()
        surrogates, selected_surrogate_list, selected_surrogate_uids = (
            _get_activity_surrogates(soa_id, activity_id)
        )
        concept_groups, activity_group_uids = _get_concept_groups_for_cell(
            soa_id, activity_id
        )
        html = templates.get_template("concepts_cell.html").render(
            request=request,
            soa_id=soa_id,
            activity_id=activity_id,
            concepts=concepts,
            selected_codes=[s["code"] for s in selected],
            selected_list=selected,
            surrogates=surrogates,
            selected_surrogate_list=selected_surrogate_list,
            selected_surrogate_uids=selected_surrogate_uids,
            concept_groups=concept_groups,
            activity_group_uids=activity_group_uids,
            edit=False,
        )
        return HTMLResponse(html)
    return HTMLResponse(
        f"<script>window.location='/ui/soa/{int(soa_id)}/edit';</script>"
    )


# UI endpoint for deleting an Activity
@router.post("/ui/soa/{soa_id}/delete_activity", response_class=HTMLResponse)
def ui_delete_activity(request: Request, soa_id: int, activity_id: int = Form(...)):
    """Form handler to delete an Activity"""
    delete_activity(soa_id, activity_id)
    return HTMLResponse(
        f"<script>window.location='/ui/soa/{int(soa_id)}/edit';</script>"
    )


# UI endpoint for reordering Activities
@router.post("/ui/soa/{soa_id}/reorder_activities", response_class=HTMLResponse)
def ui_reorder_activities(request: Request, soa_id: int, order: str = Form("")):
    """Persist new activity ordering. 'order' is a comma-separated list of activity IDs in desired order."""
    if not soa_exists(soa_id):
        raise HTTPException(404, "SOA not found")
    ids = [int(x) for x in order.split(",") if x.strip().isdigit()]
    if not ids:
        return HTMLResponse("Invalid order", status_code=400)
    conn = _connect()
    cur = conn.cursor()
    # Capture previous order
    cur.execute(
        "SELECT id FROM activity WHERE soa_id=? ORDER BY order_index", (soa_id,)
    )
    old_order = [r[0] for r in cur.fetchall()]
    cur.execute("SELECT id FROM activity WHERE soa_id=?", (soa_id,))
    existing = {r[0] for r in cur.fetchall()}
    if set(ids) - existing:
        conn.close()
        return HTMLResponse("Order contains invalid activity id", status_code=400)
    for idx, aid in enumerate(ids, start=1):
        cur.execute("UPDATE activity SET order_index=? WHERE id=?", (idx, aid))
    conn.commit()
    conn.close()
    if old_order != ids:  # app.py's private recorder skipped no-op reorders
        _record_reorder_audit(soa_id, "activity", old_order, ids)
    return HTMLResponse("OK")
