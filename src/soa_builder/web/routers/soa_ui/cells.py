"""soa_ui: cells (split from soa_ui.py)."""

from ...services.matrix import instance_cell_exists
from ...services.matrix import set_instance_cell
from ._shared import (
    CellCreate,
    Form,
    HTMLResponse,
    HTTPException,
    Optional,
    Request,
    _connect,
    _get_activity_surrogates,
    _get_bc_categories_for_cell,
    _get_concept_groups_for_cell,
    _render_activity_td,
    _render_cell_td,
    _render_encounter_th,
    _render_epoch_th,
    _table_has_columns,
    concepts_service,
    find_visit_cell,
    router,
    set_cell,
    soa_exists,
    templates,
)


@router.post("/ui/soa/{soa_id}/toggle_cell_instance", response_class=HTMLResponse)
def ui_toggle_cell_instance(
    request: Request,
    soa_id: int,
    instance_id: int = Form(...),
    activity_id: int = Form(...),
):
    """Toggle assignment by instance_id."""
    if not soa_exists(soa_id):
        raise HTTPException(404, "SOA not found")
    on = not instance_cell_exists(soa_id, instance_id, activity_id)
    set_instance_cell(soa_id, instance_id, activity_id, on)
    return HTMLResponse(
        _render_cell_td(soa_id, instance_id, activity_id, "X" if on else "", None)
    )


# UI endpoint for displaying BC(s) assigned to an Activity
@router.get(
    "/ui/soa/{soa_id}/activity/{activity_id}/concepts_cell", response_class=HTMLResponse
)
def ui_activity_concepts_cell(
    request: Request, soa_id: int, activity_id: int, edit: int = 0
):
    """Form handler to return Biomedical Concepts related to an Activity"""
    # Defensive guard: if activity_id is somehow falsy (should not happen for valid int path param)
    # surface a clear 400 error rather than proceeding and causing confusing downstream behavior.
    if not activity_id:
        raise HTTPException(status_code=400, detail="Missing activity_id")
    if not soa_exists(soa_id):
        raise HTTPException(404, "SOA not found")
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
    bc_categories_list, activity_category_names = _get_bc_categories_for_cell(
        soa_id, activity_id
    )
    return HTMLResponse(
        templates.get_template("concepts_cell.html").render(
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
            bc_categories_list=bc_categories_list,
            activity_category_names=activity_category_names,
            edit=bool(edit),
        )
    )


# UI endpoint for assigning an Activity to an Encounter/Visit
@router.post("/ui/soa/{soa_id}/set_cell", response_class=HTMLResponse)
def ui_set_cell(
    request: Request,
    soa_id: int,
    visit_id: int = Form(...),
    activity_id: int = Form(...),
    status: str = Form("X"),
):
    """Form handler to set 'X' in SoA Matrix Cell."""
    result = set_cell(
        soa_id, CellCreate(visit_id=visit_id, activity_id=activity_id, status=status)
    )
    return HTMLResponse(result.get("status", ""))


# UI endpoint for toggling assignment of an Activity to an Encounter/Visit
@router.post("/ui/soa/{soa_id}/toggle_cell", response_class=HTMLResponse)
def ui_toggle_cell(
    request: Request,
    soa_id: int,
    activity_id: int = Form(...),
    visit_id: Optional[int] = Form(None),
    instance_id: Optional[int] = Form(None),
):
    """Toggle"""
    if not soa_exists(soa_id):
        raise HTTPException(404, "SOA not found")
    # Determine current status
    conn = _connect()
    cur = conn.cursor()
    if instance_id:
        # Instance-based toggle
        cur.execute(
            "SELECT status,id FROM matrix_cells WHERE soa_id=? AND instance_id=? AND activity_id=?",
            (soa_id, int(instance_id), activity_id),
        )
        row = cur.fetchone()
        if row:
            cur.execute("DELETE FROM matrix_cells WHERE id=?", (row[1],))
            conn.commit()
            conn.close()
            return HTMLResponse(
                _render_cell_td(soa_id, int(instance_id), activity_id, "", None)
            )
        else:
            cur.execute(
                "INSERT INTO matrix_cells (soa_id, instance_id, activity_id, status) VALUES (?,?,?,?)",
                (soa_id, int(instance_id), activity_id, "X"),
            )
            conn.commit()
            conn.close()
            return HTMLResponse(
                _render_cell_td(soa_id, int(instance_id), activity_id, "X", None)
            )
    else:
        # Legacy visit-based toggle
        if visit_id is None:
            conn.close()
            raise HTTPException(400, "visit_id or instance_id required")
        row, inst_id = find_visit_cell(cur, soa_id, int(visit_id), activity_id)
        if row:
            cur.execute("DELETE FROM matrix_cells WHERE id=?", (row[0],))
            conn.commit()
            conn.close()
            current = ""
        else:
            if inst_id is None:
                conn.close()
                raise HTTPException(
                    409,
                    "visit does not map to exactly one instance; "
                    "toggle the cell with instance_id instead",
                )
            cur.execute(
                "INSERT INTO matrix_cells (soa_id, visit_id, instance_id, "
                "activity_id, status) VALUES (?,?,?,?,?)",
                (soa_id, int(visit_id), inst_id, activity_id, "X"),
            )
            conn.commit()
            conn.close()
            current = "X"
        # Legacy path: visit-based cells don't have superscript support
        return templates.TemplateResponse(
            request,
            "_legacy_matrix_cell.html",
            {
                "soa_id": soa_id,
                "visit_id": int(visit_id),
                "activity_id": activity_id,
                "current": current,
            },
        )


@router.get(
    "/ui/soa/{soa_id}/cell_superscript_edit/{instance_id}/{activity_id}",
    response_class=HTMLResponse,
)
def ui_cell_superscript_edit(
    request: Request,
    soa_id: int,
    instance_id: int,
    activity_id: int,
):
    """Return edit-mode <td> for superscript inline editing."""
    if not soa_exists(soa_id):
        raise HTTPException(404, "SOA not found")
    conn = _connect()
    cur = conn.cursor()
    cur.execute(
        "SELECT superscript FROM matrix_cells WHERE soa_id=? AND instance_id=? AND activity_id=?",
        (soa_id, instance_id, activity_id),
    )
    row = cur.fetchone()
    conn.close()
    if not row:
        raise HTTPException(404, "Cell not found")
    return templates.TemplateResponse(
        request,
        "_superscript_editor.html",
        {
            "tag": "td",
            "css_class": "cell cell-editing",
            "show_x": True,
            "colspan": None,
            "post_url": f"/ui/soa/{soa_id}/cell_superscript/{instance_id}/{activity_id}",
            "view_url": f"/ui/soa/{soa_id}/cell_superscript_view/{instance_id}/{activity_id}",
            "value": row[0] or "",
        },
    )


@router.post(
    "/ui/soa/{soa_id}/cell_superscript/{instance_id}/{activity_id}",
    response_class=HTMLResponse,
)
def ui_cell_superscript_save(
    request: Request,
    soa_id: int,
    instance_id: int,
    activity_id: int,
    superscript: Optional[str] = Form(None),
):
    """Save superscript value for a cell and return rendered <td>."""
    if not soa_exists(soa_id):
        raise HTTPException(404, "SOA not found")
    # Normalise empty string to NULL
    sup_val = superscript.strip() if superscript else None
    conn = _connect()
    cur = conn.cursor()
    cur.execute(
        "UPDATE matrix_cells SET superscript=? WHERE soa_id=? AND instance_id=? AND activity_id=?",
        (sup_val, soa_id, instance_id, activity_id),
    )
    # If no rows were updated, the target cell does not exist (or does not belong to this SOA)
    if cur.rowcount == 0:
        conn.close()
        raise HTTPException(404, "Matrix cell not found")
    # Read back the actual status and superscript from the database to render an accurate cell
    cur.execute(
        "SELECT status, superscript FROM matrix_cells WHERE soa_id=? AND instance_id=? AND activity_id=?",
        (soa_id, instance_id, activity_id),
    )
    row = cur.fetchone()
    conn.commit()
    conn.close()
    status = row[0] if row else ""
    sup_val_db = row[1] if row else None
    return HTMLResponse(
        _render_cell_td(soa_id, instance_id, activity_id, status or "", sup_val_db)
    )


@router.get(
    "/ui/soa/{soa_id}/cell_superscript_view/{instance_id}/{activity_id}",
    response_class=HTMLResponse,
)
def ui_cell_superscript_view(
    request: Request,
    soa_id: int,
    instance_id: int,
    activity_id: int,
):
    """Return rendered (view-mode) <td> — used for cancel."""
    if not soa_exists(soa_id):
        raise HTTPException(404, "SOA not found")
    conn = _connect()
    cur = conn.cursor()
    cur.execute(
        "SELECT status, superscript FROM matrix_cells WHERE soa_id=? AND instance_id=? AND activity_id=?",
        (soa_id, instance_id, activity_id),
    )
    row = cur.fetchone()
    conn.close()
    status = row[0] if row else ""
    sup_val = row[1] if row else None
    return HTMLResponse(
        _render_cell_td(soa_id, instance_id, activity_id, status or "", sup_val)
    )


@router.get(
    "/ui/soa/{soa_id}/activity_superscript_edit/{activity_id}",
    response_class=HTMLResponse,
)
def ui_activity_superscript_edit(
    request: Request,
    soa_id: int,
    activity_id: int,
):
    """Return edit-mode <td> for activity superscript inline editing."""
    if not soa_exists(soa_id):
        raise HTTPException(404, "SOA not found")
    conn = _connect()
    cur = conn.cursor()
    cur.execute(
        "SELECT superscript FROM activity WHERE soa_id=? AND id=?",
        (soa_id, activity_id),
    )
    row = cur.fetchone()
    conn.close()
    if not row:
        raise HTTPException(404, "Activity not found")
    return templates.TemplateResponse(
        request,
        "_superscript_editor.html",
        {
            "tag": "td",
            "css_class": "cell-editing",
            "show_x": False,
            "colspan": None,
            "post_url": f"/ui/soa/{soa_id}/activity_superscript/{activity_id}",
            "view_url": f"/ui/soa/{soa_id}/activity_superscript_view/{activity_id}",
            "value": row[0] or "",
        },
    )


@router.post(
    "/ui/soa/{soa_id}/activity_superscript/{activity_id}",
    response_class=HTMLResponse,
)
def ui_activity_superscript_save(
    request: Request,
    soa_id: int,
    activity_id: int,
    superscript: Optional[str] = Form(None),
):
    """Save superscript value for an activity and return rendered <td>."""
    if not soa_exists(soa_id):
        raise HTTPException(404, "SOA not found")
    # Normalise empty string to NULL
    sup_val = superscript.strip() if superscript else None
    conn = _connect()
    cur = conn.cursor()
    cur.execute(
        "UPDATE activity SET superscript=? WHERE soa_id=? AND id=?",
        (sup_val, soa_id, activity_id),
    )
    if cur.rowcount == 0:
        conn.close()
        raise HTTPException(404, "Activity not found")
    cur.execute(
        "SELECT name, label, superscript FROM activity WHERE soa_id=? AND id=?",
        (soa_id, activity_id),
    )
    row = cur.fetchone()
    conn.commit()
    conn.close()
    name, label, sup_val_db = row
    display_name = label or name
    return HTMLResponse(
        _render_activity_td(soa_id, activity_id, display_name, sup_val_db)
    )


@router.get(
    "/ui/soa/{soa_id}/activity_superscript_view/{activity_id}",
    response_class=HTMLResponse,
)
def ui_activity_superscript_view(
    request: Request,
    soa_id: int,
    activity_id: int,
):
    """Return rendered (view-mode) <td> — used for cancel."""
    if not soa_exists(soa_id):
        raise HTTPException(404, "SOA not found")
    conn = _connect()
    cur = conn.cursor()
    cur.execute(
        "SELECT name, label, superscript FROM activity WHERE soa_id=? AND id=?",
        (soa_id, activity_id),
    )
    row = cur.fetchone()
    conn.close()
    if not row:
        raise HTTPException(404, "Activity not found")
    name, label, sup_val = row
    display_name = label or name
    return HTMLResponse(_render_activity_td(soa_id, activity_id, display_name, sup_val))


@router.get(
    "/ui/soa/{soa_id}/encounter_superscript_edit/{encounter_id}",
    response_class=HTMLResponse,
)
def ui_encounter_superscript_edit(
    request: Request,
    soa_id: int,
    encounter_id: int,
):
    """Return edit-mode <th> for encounter superscript inline editing."""
    if not soa_exists(soa_id):
        raise HTTPException(404, "SOA not found")
    conn = _connect()
    cur = conn.cursor()
    cur.execute(
        "SELECT superscript FROM visit WHERE soa_id=? AND id=?",
        (soa_id, encounter_id),
    )
    row = cur.fetchone()
    conn.close()
    if not row:
        raise HTTPException(404, "Encounter not found")
    return templates.TemplateResponse(
        request,
        "_superscript_editor.html",
        {
            "tag": "th",
            "css_class": "cell-editing",
            "show_x": False,
            "colspan": None,
            "post_url": f"/ui/soa/{soa_id}/encounter_superscript/{encounter_id}",
            "view_url": f"/ui/soa/{soa_id}/encounter_superscript_view/{encounter_id}",
            "value": row[0] or "",
        },
    )


@router.post(
    "/ui/soa/{soa_id}/encounter_superscript/{encounter_id}",
    response_class=HTMLResponse,
)
def ui_encounter_superscript_save(
    request: Request,
    soa_id: int,
    encounter_id: int,
    superscript: Optional[str] = Form(None),
):
    """Save superscript value for an encounter (visit) and return rendered <th>."""
    if not soa_exists(soa_id):
        raise HTTPException(404, "SOA not found")
    sup_val = superscript.strip() if superscript else None
    conn = _connect()
    cur = conn.cursor()
    cur.execute(
        "UPDATE visit SET superscript=? WHERE soa_id=? AND id=?",
        (sup_val, soa_id, encounter_id),
    )
    if cur.rowcount == 0:
        conn.close()
        raise HTTPException(404, "Encounter not found")
    cur.execute(
        "SELECT name, label, superscript FROM visit WHERE soa_id=? AND id=?",
        (soa_id, encounter_id),
    )
    row = cur.fetchone()
    conn.commit()
    conn.close()
    name, label, sup_val_db = row
    display_name = label or name
    return HTMLResponse(
        _render_encounter_th(soa_id, encounter_id, display_name, sup_val_db)
    )


@router.get(
    "/ui/soa/{soa_id}/encounter_superscript_view/{encounter_id}",
    response_class=HTMLResponse,
)
def ui_encounter_superscript_view(
    request: Request,
    soa_id: int,
    encounter_id: int,
):
    """Return rendered (view-mode) <th> — used for cancel."""
    if not soa_exists(soa_id):
        raise HTTPException(404, "SOA not found")
    conn = _connect()
    cur = conn.cursor()
    cur.execute(
        "SELECT name, label, superscript FROM visit WHERE soa_id=? AND id=?",
        (soa_id, encounter_id),
    )
    row = cur.fetchone()
    conn.close()
    if not row:
        raise HTTPException(404, "Encounter not found")
    name, label, sup_val = row
    display_name = label or name
    return HTMLResponse(
        _render_encounter_th(soa_id, encounter_id, display_name, sup_val)
    )


@router.get(
    "/ui/soa/{soa_id}/epoch_superscript_edit/{epoch_id}",
    response_class=HTMLResponse,
)
def ui_epoch_superscript_edit(
    request: Request,
    soa_id: int,
    epoch_id: int,
    colspan: int = 1,
):
    """Return edit-mode <th> for epoch superscript inline editing.

    ``colspan`` is a render-time pass-through (how many matrix columns
    the epoch header currently spans) — it is not stored in the
    database and must be preserved across the edit/save/view swaps so
    the table stays aligned.
    """
    if not soa_exists(soa_id):
        raise HTTPException(404, "SOA not found")
    conn = _connect()
    cur = conn.cursor()
    cur.execute(
        "SELECT superscript FROM epoch WHERE soa_id=? AND id=?",
        (soa_id, epoch_id),
    )
    row = cur.fetchone()
    conn.close()
    if not row:
        raise HTTPException(404, "Epoch not found")
    return templates.TemplateResponse(
        request,
        "_superscript_editor.html",
        {
            "tag": "th",
            "css_class": "cell-editing",
            "show_x": False,
            "colspan": colspan,
            "post_url": f"/ui/soa/{soa_id}/epoch_superscript/{epoch_id}",
            "view_url": f"/ui/soa/{soa_id}/epoch_superscript_view/{epoch_id}?colspan={colspan}",
            "value": row[0] or "",
        },
    )


@router.post(
    "/ui/soa/{soa_id}/epoch_superscript/{epoch_id}",
    response_class=HTMLResponse,
)
def ui_epoch_superscript_save(
    request: Request,
    soa_id: int,
    epoch_id: int,
    superscript: Optional[str] = Form(None),
    colspan: int = Form(1),
):
    """Save superscript value for an epoch and return rendered <th>."""
    if not soa_exists(soa_id):
        raise HTTPException(404, "SOA not found")
    sup_val = superscript.strip() if superscript else None
    conn = _connect()
    cur = conn.cursor()
    cur.execute(
        "UPDATE epoch SET superscript=? WHERE soa_id=? AND id=?",
        (sup_val, soa_id, epoch_id),
    )
    if cur.rowcount == 0:
        conn.close()
        raise HTTPException(404, "Epoch not found")
    cur.execute(
        "SELECT epoch_label, name, superscript FROM epoch WHERE soa_id=? AND id=?",
        (soa_id, epoch_id),
    )
    row = cur.fetchone()
    conn.commit()
    conn.close()
    epoch_label, name, sup_val_db = row
    display_value = epoch_label or name
    return HTMLResponse(
        _render_epoch_th(soa_id, epoch_id, display_value, colspan, sup_val_db)
    )


@router.get(
    "/ui/soa/{soa_id}/epoch_superscript_view/{epoch_id}",
    response_class=HTMLResponse,
)
def ui_epoch_superscript_view(
    request: Request,
    soa_id: int,
    epoch_id: int,
    colspan: int = 1,
):
    """Return rendered (view-mode) <th> — used for cancel."""
    if not soa_exists(soa_id):
        raise HTTPException(404, "SOA not found")
    conn = _connect()
    cur = conn.cursor()
    cur.execute(
        "SELECT epoch_label, name, superscript FROM epoch WHERE soa_id=? AND id=?",
        (soa_id, epoch_id),
    )
    row = cur.fetchone()
    conn.close()
    if not row:
        raise HTTPException(404, "Epoch not found")
    epoch_label, name, sup_val = row
    display_value = epoch_label or name
    return HTMLResponse(
        _render_epoch_th(soa_id, epoch_id, display_value, colspan, sup_val)
    )
