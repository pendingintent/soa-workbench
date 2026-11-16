"""soa_ui: elements (split from soa_ui.py)."""

import sqlite3

from ._shared import (
    ElementCreate,
    ElementUpdate,
    Form,
    HTMLResponse,
    HTTPException,
    Optional,
    Request,
    _connect,
    _record_element_audit,
    elements_router,
    logger,
    router,
    soa_exists,
)


# UI endpoint for adding an element     <- Deprecated (movd to routers/elements.py)
@router.post("/ui/soa/{soa_id}/add_element", response_class=HTMLResponse)
def ui_add_element(
    request: Request,
    soa_id: int,
    name: str = Form(...),
    label: Optional[str] = Form(None),
    description: Optional[str] = Form(None),
    element_transition_start_rule_uid: str = Form(""),
    element_transition_end_rule_uid: str = Form(""),
):
    """Form handler to add an element."""
    if not soa_exists(soa_id):
        raise HTTPException(404, "SOA not found")

    # Coerce empty testrl from Form to None, otherwise to int

    testrl_uid: Optional[str] = (
        element_transition_start_rule_uid or ""
    ).strip() or None
    teenrl_uid: Optional[str] = (element_transition_end_rule_uid or "").strip() or None

    payload = ElementCreate(
        name=name,
        label=label,
        description=description,
        testrl=testrl_uid,
        teenrl=teenrl_uid,
    )

    # Create the element via the API helper to ensure audits and ordering
    try:
        elements_router.add_element(soa_id, payload)
    except (HTTPException, sqlite3.Error) as exc:
        logger.warning("Element save from UI failed: %s", exc)

    return HTMLResponse(
        f"<script>window.location='/ui/soa/{int(soa_id)}/edit';</script>"
    )

    '''
    name = (name or "").strip()
    if not name:
        raise HTTPException(400, "Name required")
    conn = _connect()
    cur = conn.cursor()
    # Determine next order index
    cur.execute(
        "SELECT COALESCE(MAX(order_index),0) FROM element WHERE soa_id=?", (soa_id,)
    )
    next_ord = (cur.fetchone() or [0])[0] + 1
    now = datetime.now(timezone.utc).isoformat()
    # Check if legacy/non-standard element_id column exists and populate if required
    cur.execute("PRAGMA table_info(element)")
    element_cols = {r[1] for r in cur.fetchall()}
    element_identifier: Optional[str] = None
    if "element_id" in element_cols:
        # Generate StudyElement_<n> monotonically increasing for this SOA
        element_identifier = _next_element_identifier(soa_id)
        cur.execute(
            """INSERT INTO element (soa_id,name,label,description,testrl,teenrl,order_index,created_at,element_id)
            VALUES (?,?,?,?,?,?,?,?,?)""",
            (
                soa_id,
                name,
                (label or "").strip() or None,
                (description or "").strip() or None,
                (testrl or "").strip() or None,
                (teenrl or "").strip() or None,
                next_ord,
                now,
                element_identifier,
            ),
        )
    else:
        cur.execute(
            """INSERT INTO element (soa_id,name,label,description,testrl,teenrl,order_index,created_at)
            VALUES (?,?,?,?,?,?,?,?)""",
            (
                soa_id,
                name,
                (label or "").strip() or None,
                (description or "").strip() or None,
                (testrl or "").strip() or None,
                (teenrl or "").strip() or None,
                next_ord,
                now,
            ),
        )
    eid = cur.lastrowid
    conn.commit()
    conn.close()
    # Audit should store the logical StudyElement_N in element_audit.element_id, not the row id
    _record_element_audit(
        soa_id,
        "create",
        element_identifier,
        before=None,
        after={
            "id": eid,
            "name": name,
            "label": (label or "").strip() or None,
            "description": (description or "").strip() or None,
            "testrl": (testrl or "").strip() or None,
            "teenrl": (teenrl or "").strip() or None,
            "order_index": next_ord,
            "element_id": element_identifier,
        },
    )
    return HTMLResponse(
        f"<script>window.location='/ui/soa/{int(soa_id)}/edit';</script>"
    )
    '''


# UI endpoint for associating a Transition Start Rule with an Element (element.testrl)  <- Deprecated (movd to routers/elements.py)
@router.post(
    "/ui/soa/{soa_id}/set_element_transition_start_rule", response_class=HTMLResponse
)
def ui_set_element_transition_start_rule(
    request: Request,
    soa_id: int,
    element_id: int = Form(...),
    element_transition_start_rule_uid: str = Form(...),
):
    """Form handler for associating a Transition Start Rule with an Element"""
    if not soa_exists(soa_id):
        raise HTTPException(404, "SOA not found")

    new_uid = (element_transition_start_rule_uid or "").strip() or None
    conn = _connect()
    cur = conn.cursor()
    cur.execute(
        "SELECT id,name,label,description,element_id,testrl FROM element WHERE id=? AND soa_id=?",
        (element_id, soa_id),
    )
    row = cur.fetchone()
    if not row:
        conn.close()
        raise HTTPException(404, "Element not found")

    before = {
        "id": row[0],
        "name": row[1],
        "label": row[2],
        "description": row[3],
        "element_id": row[4],
        "testrl": row[5],
    }
    if new_uid is not None:
        cur.execute(
            "SELECT 1 FROM transition_rule WHERE transition_rule_uid=? AND soa_id=?",
            (
                new_uid,
                soa_id,
            ),
        )
        if not cur.fetchone():
            conn.close()
            raise HTTPException("Invalid transition rule for this SOA")

    cur.execute(
        "UPDATE element SET testrl=? WHERE id=? AND soa_id=?",
        (new_uid, element_id, soa_id),
    )
    conn.commit()

    cur.execute(
        "SELECT id,name,label,description,element_id,testrl FROM element WHERE id=? AND soa_id=?",
        (element_id, soa_id),
    )
    r = cur.fetchone()
    after = {
        "id": r[0],
        "name": r[1],
        "label": r[2],
        "description": r[3],
        "element_id": r[4],
        "testrl": r[5],
    }
    updated_fields = [
        f for f in ["testrl"] if (before.get(f) or None) != (after.get(f) or None)
    ]
    _record_element_audit(
        soa_id,
        "update",
        element_id,
        before=before,
        after={**after, "updated_fields": updated_fields},
    )
    conn.close()
    return HTMLResponse(
        f"<script>window.location='/ui/soa/{int(soa_id)}/edit';</script>"
    )


# UI endpoint for associating a Transition Start Rule with an Element (element.teenrl)  <- Deprecated (movd to routers/elements.py)
@router.post(
    "/ui/soa/{soa_id}/set_element_transition_end_rule", response_class=HTMLResponse
)
def ui_set_element_transition_end_rule(
    request: Request,
    soa_id: int,
    element_id: int = Form(...),
    element_transition_end_rule_uid: str = Form(...),
):
    """Form handler for associating a Transition End Rule with an Element"""
    if not soa_exists(soa_id):
        raise HTTPException(404, "SOA not found")

    new_uid = (element_transition_end_rule_uid or "").strip() or None

    conn = _connect()
    cur = conn.cursor()
    cur.execute(
        "SELECT id,name,label,description,element_id,teenrl FROM element WHERE id=? AND soa_id=?",
        (element_id, soa_id),
    )
    row = cur.fetchone()
    if not row:
        conn.close()
        raise HTTPException(404, "Element not found")

    before = {
        "id": row[0],
        "name": row[1],
        "label": row[2],
        "description": row[3],
        "element_id": row[4],
        "teenrl": row[5],
    }

    if new_uid is not None:
        cur.execute(
            "SELECT 1 FROM transition_rule WHERE transition_rule_uid=? AND soa_id=?",
            (new_uid, soa_id),
        )
        if not cur.fetchone():
            conn.close()
            raise HTTPException(400, "Invalid Transition Rule for this SOA")

    cur.execute(
        "UPDATE element SET teenrl=? WHERE id=? AND soa_id=?",
        (new_uid, element_id, soa_id),
    )
    conn.commit()

    cur.execute(
        "SELECT id,name,label,description,element_id,teenrl FROM element WHERE id=? AND soa_id=?",
        (element_id, soa_id),
    )
    r = cur.fetchone()
    after = {
        "id": r[0],
        "name": r[1],
        "label": r[2],
        "description": r[3],
        "element_id": r[4],
        "teenrl": r[5],
    }
    updated_fields = [
        f for f in ["teenrl"] if (before.get(f) or None) != (after.get(f) or None)
    ]
    _record_element_audit(
        soa_id,
        "update",
        element_id,
        before=before,
        after={**after, "updated_fields": updated_fields},
    )
    conn.close()
    return HTMLResponse(
        f"<script>window.location='/ui/soa/{int(soa_id)}/edit';</script>"
    )


# UI endpoint for updating an element       <- Deprecated (movd to routers/elements.py)
@router.post("/ui/soa/{soa_id}/update_element", response_class=HTMLResponse)
def ui_update_element(
    request: Request,
    soa_id: int,
    element_id: int = Form(...),
    name: Optional[str] = Form(None),
    label: Optional[str] = Form(None),
    description: Optional[str] = Form(None),
    # testrl: Optional[str] = Form(None),
    # teenrl: Optional[str] = Form(None),
):
    """Form handler to update an existing Element."""
    # Build payload with provided fields; blanks should clear values
    payload = ElementUpdate(
        name=name,
        label=label,
        description=description,
    )
    try:
        elements_router.update_element(soa_id, element_id, payload)
    except (HTTPException, sqlite3.Error) as exc:
        # Let redirect proceed; detailed errors will appear in API logs
        logger.warning("Element save from UI failed: %s", exc)

    return HTMLResponse(
        f"<script>window.location='/ui/soa/{int(soa_id)}/edit';</script>"
    )

    """
    conn = _connect()
    cur = conn.cursor()
    cur.execute("SELECT id FROM element WHERE id=? AND soa_id=?", (element_id, soa_id))
    if not cur.fetchone():
        conn.close()
        raise HTTPException(404, "Element not found")
    # Capture before
    cur.execute(
        "SELECT id,name,label,description,testrl,teenrl,order_index,created_at FROM element WHERE id=?",
        (element_id,),
    )
    b = cur.fetchone()
    before = None
    if b:
        before = {
            "id": b[0],
            "name": b[1],
            "label": b[2],
            "description": b[3],
            "testrl": b[4],
            "teenrl": b[5],
            "order_index": b[6],
            "created_at": b[7],
        }
    cur.execute(
        "UPDATE element SET name=?, label=?, description=?, testrl=?, teenrl=? WHERE id=?",
        (
            (name or "").strip() or None,
            (label or "").strip() or None,
            (description or "").strip() or None,
            (testrl or "").strip() or None,
            (teenrl or "").strip() or None,
            element_id,
        ),
    )
    conn.commit()
    # Fetch after
    cur.execute(
        "SELECT id,name,label,description,testrl,teenrl,order_index,created_at FROM element WHERE id=?",
        (element_id,),
    )
    a = cur.fetchone()
    conn.close()
    after = {
        "id": a[0],
        "name": a[1],
        "label": a[2],
        "description": a[3],
        "testrl": a[4],
        "teenrl": a[5],
        "order_index": a[6],
        "created_at": a[7],
    }
    mutable_fields = ["name", "label", "description", "testrl", "teenrl"]
    updated_fields = [
        f for f in mutable_fields if before and before.get(f) != after.get(f)
    ]
    # Fetch element.element_id for audit key
    try:
        conn_k = _connect()
        cur_k = conn_k.cursor()
        cur_k.execute("PRAGMA table_info(element)")
        cols_k = {r[1] for r in cur_k.fetchall()}
        element_uid_for_audit = None
        if "element_id" in cols_k:
            cur_k.execute(
                "SELECT element_id FROM element WHERE id=? AND soa_id=?",
                (element_id, soa_id),
            )
            row_k = cur_k.fetchone()
            element_uid_for_audit = row_k[0] if row_k else None
        conn_k.close()
    except Exception:
        element_uid_for_audit = None
    _record_element_audit(
        soa_id,
        "update",
        element_uid_for_audit,
        before=before,
        after={**after, "updated_fields": updated_fields},
    )
    return HTMLResponse(
        f"<script>window.location='/ui/soa/{int(soa_id)}/edit';</script>"
    )
    """


# UI endpoint for deleting an element       <- Deprecated (movd to routers/elements.py)
@router.post("/ui/soa/{soa_id}/delete_element", response_class=HTMLResponse)
def ui_delete_element(request: Request, soa_id: int, element_id: int = Form(...)):
    """Form handler to delete an existing Element."""
    if not soa_exists(soa_id):
        raise HTTPException(404, "SOA not found")

    try:
        # Call through router to avoid stale import bindings
        elements_router.delete_element(soa_id, element_id)
    except HTTPException:
        # swallow 404
        pass
    # If HTMX, use HX-Redirect; else script redirect
    if request.headers.get("HX-Request") == "true":
        return HTMLResponse("", headers={"HX-Redirect": f"/ui/soa/{int(soa_id)}/edit"})

    return HTMLResponse(
        f"<script>window.location='/ui/soa/{int(soa_id)}/edit';</script>"
    )

    """
    conn = _connect()
    cur = conn.cursor()
    # Capture before snapshot including logical element_id (StudyElement_N) if present
    cur.execute("PRAGMA table_info(element)")
    cols = {r[1] for r in cur.fetchall()}
    has_uid = "element_id" in cols
    if has_uid:
        cur.execute(
            "SELECT id, name, label, description, testrl, teenrl, order_index, element_id FROM element WHERE id=? AND soa_id=?",
            (element_id, soa_id),
        )
    else:
        cur.execute(
            "SELECT id, name, label, description, testrl, teenrl, order_index FROM element WHERE id=? AND soa_id=?",
            (element_id, soa_id),
        )
    row_b = cur.fetchone()
    before = None
    if row_b:
        before = {
            "id": row_b[0],
            "name": row_b[1],
            "label": row_b[2],
            "description": row_b[3],
            "testrl": row_b[4],
            "teenrl": row_b[5],
            "order_index": row_b[6],
            "element_id": (row_b[7] if has_uid else None),
        }
    # Perform delete
    cur.execute("DELETE FROM element WHERE id=? AND soa_id=?", (element_id, soa_id))
    conn.commit()
    conn.close()
    # Use logical element_id (StudyElement_N) for audit key if available
    element_uid_for_audit = (
        before.get("element_id") if isinstance(before, dict) else None
    )
    _record_element_audit(
        soa_id,
        "delete",
        element_uid_for_audit,
        before=before or {"id": element_id},
        after=None,
    )
    return HTMLResponse(
        f"<script>window.location='/ui/soa/{int(soa_id)}/edit';</script>"
    )
    """
