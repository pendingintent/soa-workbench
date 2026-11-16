"""amendments: ui (split from amendments.py)."""

from pydantic import ValidationError
from ._shared import (
    AMENDMENT_IMPACT_TYPE_CODELIST,
    AMENDMENT_REASON_CODELIST,
    DocumentContentReferenceCreate,
    Form,
    GEOGRAPHIC_SCOPE_TYPE_CODELIST,
    GOVERNANCE_DATE_TYPE_CODELIST,
    GovernanceDateCreate,
    HTMLResponse,
    HTTPException,
    List,
    Request,
    StudyAmendmentCreate,
    StudyAmendmentImpactCreate,
    StudyAmendmentReasonCreate,
    StudyChangeCreate,
    SubjectEnrollmentCreate,
    _connect,
    _record_amendment_audit,
    _record_change_audit,
    _record_enrollment_audit,
    _record_geo_scope_audit,
    _record_gov_date_audit,
    _record_impact_audit,
    _record_reason_audit,
    _record_ref_audit,
    get_ddf_ct_codelist_map,
    html,
    logger,
    soa_exists,
    templates,
    ui_router,
)
from .helpers import (
    _amendment_row,
    _geo_scope_category,
    _get_countries,
    _get_regions,
    _insert_code,
    _insert_location_code,
    _load_amendment_context,
    _next_amendment_uid,
    _next_change_uid,
    _next_enrollment_uid,
    _next_gov_date_uid,
    _next_impact_uid,
    _next_reason_uid,
    _next_ref_uid,
    _next_scope_uid,
    _ui_changes_partial,
    _ui_enrollments_partial,
    _ui_geo_scopes_and_deps,
    _ui_gov_dates_partial,
    _ui_impacts_partial,
    _ui_reasons_partial,
    _update_code_value,
)


@ui_router.get(
    "/ui/soa/{soa_id}/freeze/{freeze_id}/amendment",
    response_class=HTMLResponse,
)
def ui_amendment_edit(request: Request, soa_id: int, freeze_id: int):
    if not soa_exists(soa_id):
        raise HTTPException(404, "SOA not found")
    conn = _connect()
    cur = conn.cursor()
    cur.execute(
        "SELECT id FROM study_amendment WHERE freeze_id=? AND soa_id=?",
        (freeze_id, soa_id),
    )
    row = cur.fetchone()
    conn.close()
    if not row:
        raise HTTPException(404, "Amendment not found for this freeze")
    ctx = _load_amendment_context(soa_id, row[0])
    if not ctx:
        raise HTTPException(404, "Amendment not found")
    ctx["freeze_id"] = freeze_id
    return templates.TemplateResponse(request, "amendment_edit.html", ctx)


@ui_router.get(
    "/ui/soa/{soa_id}/amendment/form_fields",
    response_class=HTMLResponse,
)
def ui_amendment_form_fields(request: Request, soa_id: int):
    reason_terms = get_ddf_ct_codelist_map(AMENDMENT_REASON_CODELIST)
    return templates.TemplateResponse(
        request,
        "amendment_form_fields.html",
        {"reason_terms": reason_terms, "soa_id": soa_id},
    )


@ui_router.post(
    "/ui/soa/{soa_id}/freeze/{freeze_id}/amendment/create",
    response_class=HTMLResponse,
)
def ui_create_amendment(
    request: Request,
    soa_id: int,
    freeze_id: int,
    name: str = Form(...),
    number: str = Form(...),
    summary: str = Form(...),
    label: str = Form(""),
    description: str = Form(""),
    primary_reason_code: str = Form(...),
    primary_reason_other: str = Form(""),
):
    if not soa_exists(soa_id):
        raise HTTPException(404, "SOA not found")
    try:
        body = StudyAmendmentCreate(
            name=name,
            number=number,
            summary=summary,
            label=label or None,
            description=description or None,
            primary_reason_code=primary_reason_code,
            primary_reason_other=primary_reason_other or None,
        )
    except ValidationError as exc:
        logger.exception("Amendment creation validation error: %s", exc)
        if request.headers.get("HX-Request") == "true":
            return HTMLResponse(
                "<div class='error' style='color:#c62828;font-size:0.7em;'>"
                "Error: Invalid amendment data. Please check your input.</div>"
            )
        return HTMLResponse(
            "<div class='error' style='color:#c62828;'>"
            "Error: Invalid amendment data. Please check your input.</div>"
        )
    conn = _connect()
    cur = conn.cursor()
    cur.execute(
        "SELECT id FROM soa_freeze WHERE id=? AND soa_id=?",
        (freeze_id, soa_id),
    )
    if not cur.fetchone():
        conn.close()
        raise HTTPException(404, "Freeze not found")
    cur.execute(
        "SELECT id FROM study_amendment WHERE freeze_id=? AND soa_id=?",
        (freeze_id, soa_id),
    )
    if cur.fetchone():
        conn.close()
        msg = "Amendment already exists for this freeze"
        if request.headers.get("HX-Request") == "true":
            return HTMLResponse(
                f"<div class='error' style='color:#c62828;font-size:0.7em;'>"
                f"Error: {html.escape(msg)}</div>"
            )
        return HTMLResponse(
            f"<div class='error' style='color:#c62828;'>{html.escape(msg)}</div>"
        )

    amendment_uid = _next_amendment_uid(cur, soa_id)
    reason_uid = _next_reason_uid(cur, soa_id)
    code_uid = _insert_code(
        cur, soa_id, body.primary_reason_code, AMENDMENT_REASON_CODELIST
    )
    cur.execute(
        "INSERT INTO study_amendment "
        "(soa_id,freeze_id,amendment_uid,name,number,summary,label,"
        "description) VALUES (?,?,?,?,?,?,?,?)",
        (
            soa_id,
            freeze_id,
            amendment_uid,
            body.name,
            body.number,
            body.summary,
            body.label,
            body.description,
        ),
    )
    amendment_id = cur.lastrowid
    cur.execute(
        "INSERT INTO study_amendment_reason "
        "(soa_id,amendment_uid,reason_uid,role,code_uid,other_reason) "
        "VALUES (?,?,?,?,?,?)",
        (
            soa_id,
            amendment_uid,
            reason_uid,
            "primary",
            code_uid,
            body.primary_reason_other,
        ),
    )
    reason_id = cur.lastrowid
    conn.commit()
    conn.close()
    _record_amendment_audit(
        soa_id,
        "create",
        amendment_id,
        after={"amendment_uid": amendment_uid, "name": body.name},
    )
    _record_reason_audit(
        soa_id,
        "create",
        reason_id,
        after={
            "reason_uid": reason_uid,
            "role": "primary",
            "code": body.primary_reason_code,
        },
    )
    safe_soa = html.escape(str(soa_id))
    if request.headers.get("HX-Request") == "true":
        return HTMLResponse(
            "",
            headers={"HX-Redirect": f"/ui/soa/{safe_soa}/freezes"},
        )
    return HTMLResponse(
        f"<script>window.location='/ui/soa/{safe_soa}/freezes';</script>"
    )


@ui_router.post(
    "/ui/soa/{soa_id}/amendment/{amendment_id}/reasons/add",
    response_class=HTMLResponse,
)
def ui_add_secondary_reason(
    request: Request,
    soa_id: int,
    amendment_id: int,
    code: str = Form(...),
    other_reason: str = Form(""),
):
    if not soa_exists(soa_id):
        raise HTTPException(404, "SOA not found")
    try:
        body = StudyAmendmentReasonCreate(code=code, other_reason=other_reason or None)
    except ValidationError as exc:
        logger.exception("Amendment reason validation error: %s", exc)
        return HTMLResponse(
            "<div style='color:#c62828;font-size:0.8em;'>"
            "Error: Invalid reason data. Please check your input.</div>"
        )
    conn = _connect()
    cur = conn.cursor()
    am = _amendment_row(cur, soa_id, amendment_id)
    if not am:
        conn.close()
        raise HTTPException(404, "Amendment not found")
    reason_uid = _next_reason_uid(cur, soa_id)
    code_uid = _insert_code(cur, soa_id, body.code, AMENDMENT_REASON_CODELIST)
    cur.execute(
        "INSERT INTO study_amendment_reason "
        "(soa_id,amendment_uid,reason_uid,role,code_uid,other_reason) "
        "VALUES (?,?,?,?,?,?)",
        (
            soa_id,
            am["amendment_uid"],
            reason_uid,
            "secondary",
            code_uid,
            body.other_reason,
        ),
    )
    reason_id = cur.lastrowid
    conn.commit()
    conn.close()
    _record_reason_audit(
        soa_id,
        "create",
        reason_id,
        after={"reason_uid": reason_uid, "role": "secondary"},
    )
    return _ui_reasons_partial(request, soa_id, amendment_id)


@ui_router.post(
    "/ui/soa/{soa_id}/amendment/{amendment_id}/reason/{reason_id}/delete",
    response_class=HTMLResponse,
)
def ui_delete_secondary_reason(
    request: Request, soa_id: int, amendment_id: int, reason_id: int
):
    if not soa_exists(soa_id):
        raise HTTPException(404, "SOA not found")
    conn = _connect()
    cur = conn.cursor()
    cur.execute(
        "SELECT id,reason_uid,role,code_uid FROM study_amendment_reason "
        "WHERE id=? AND soa_id=?",
        (reason_id, soa_id),
    )
    row = cur.fetchone()
    if not row or row[2] == "primary":
        conn.close()
        raise HTTPException(400, "Cannot delete primary or missing reason")
    before = {"reason_uid": row[1]}
    cur.execute(
        "DELETE FROM code_association WHERE soa_id=? AND code_uid=?",
        (soa_id, row[3]),
    )
    cur.execute(
        "DELETE FROM study_amendment_reason WHERE id=? AND soa_id=?",
        (reason_id, soa_id),
    )
    conn.commit()
    conn.close()
    _record_reason_audit(soa_id, "delete", reason_id, before=before)
    return _ui_reasons_partial(request, soa_id, amendment_id)


@ui_router.post(
    "/ui/soa/{soa_id}/amendment/{amendment_id}/reason/{reason_id}/update",
    response_class=HTMLResponse,
)
def ui_update_reason(
    request: Request,
    soa_id: int,
    amendment_id: int,
    reason_id: int,
    code: str = Form(...),
    other_reason: str = Form(""),
):
    if not soa_exists(soa_id):
        raise HTTPException(404, "SOA not found")
    if code == "C17649" and not other_reason.strip():
        return HTMLResponse(
            "<div style='color:#c62828;font-size:0.8em;'>"
            "Error: Other Reason is required when code is C17649</div>"
        )
    conn = _connect()
    cur = conn.cursor()
    cur.execute(
        "SELECT id,reason_uid,code_uid FROM study_amendment_reason "
        "WHERE id=? AND soa_id=?",
        (reason_id, soa_id),
    )
    row = cur.fetchone()
    if not row:
        conn.close()
        raise HTTPException(404, "Reason not found")
    before = {"reason_uid": row[1], "code_uid": row[2]}
    _update_code_value(cur, soa_id, row[2], code, AMENDMENT_REASON_CODELIST)
    cur.execute(
        "UPDATE study_amendment_reason SET other_reason=? WHERE id=? AND soa_id=?",
        (other_reason.strip() or None, reason_id, soa_id),
    )
    conn.commit()
    conn.close()
    _record_reason_audit(
        soa_id, "update", reason_id, before=before, after={"code": code}
    )
    return _ui_reasons_partial(request, soa_id, amendment_id)


@ui_router.post(
    "/ui/soa/{soa_id}/amendment/{amendment_id}/impacts/add",
    response_class=HTMLResponse,
)
def ui_add_impact(
    request: Request,
    soa_id: int,
    amendment_id: int,
    type_code: str = Form(...),
    text: str = Form(...),
    is_substantial: str = Form(""),
):
    if not soa_exists(soa_id):
        raise HTTPException(404, "SOA not found")
    try:
        body = StudyAmendmentImpactCreate(
            type_code=type_code,
            text=text,
            is_substantial=bool(is_substantial),
        )
    except ValidationError as exc:
        logger.exception("Amendment impact validation error: %s", exc)
        return HTMLResponse(
            "<div style='color:#c62828;font-size:0.8em;'>"
            "Error: Invalid impact data. Please check your input.</div>"
        )
    conn = _connect()
    cur = conn.cursor()
    am = _amendment_row(cur, soa_id, amendment_id)
    if not am:
        conn.close()
        raise HTTPException(404, "Amendment not found")
    impact_uid = _next_impact_uid(cur, soa_id)
    type_code_uid = _insert_code(
        cur, soa_id, body.type_code, AMENDMENT_IMPACT_TYPE_CODELIST
    )
    cur.execute(
        "INSERT INTO study_amendment_impact "
        "(soa_id,amendment_uid,impact_uid,type_code_uid,text,is_substantial)"
        " VALUES (?,?,?,?,?,?)",
        (
            soa_id,
            am["amendment_uid"],
            impact_uid,
            type_code_uid,
            body.text,
            int(body.is_substantial),
        ),
    )
    impact_id = cur.lastrowid
    conn.commit()
    conn.close()
    _record_impact_audit(
        soa_id,
        "create",
        impact_id,
        after={"impact_uid": impact_uid},
    )
    return _ui_impacts_partial(request, soa_id, amendment_id)


@ui_router.post(
    "/ui/soa/{soa_id}/amendment/{amendment_id}/impact/{impact_id}/delete",
    response_class=HTMLResponse,
)
def ui_delete_impact(request: Request, soa_id: int, amendment_id: int, impact_id: int):
    if not soa_exists(soa_id):
        raise HTTPException(404, "SOA not found")
    conn = _connect()
    cur = conn.cursor()
    cur.execute(
        "SELECT id,impact_uid,type_code_uid FROM study_amendment_impact "
        "WHERE id=? AND soa_id=?",
        (impact_id, soa_id),
    )
    row = cur.fetchone()
    if not row:
        conn.close()
        raise HTTPException(404, "Impact not found")
    before = {"impact_uid": row[1]}
    cur.execute(
        "DELETE FROM code_association WHERE soa_id=? AND code_uid=?",
        (soa_id, row[2]),
    )
    cur.execute(
        "DELETE FROM study_amendment_impact WHERE id=? AND soa_id=?",
        (impact_id, soa_id),
    )
    conn.commit()
    conn.close()
    _record_impact_audit(soa_id, "delete", impact_id, before=before)
    return _ui_impacts_partial(request, soa_id, amendment_id)


@ui_router.post(
    "/ui/soa/{soa_id}/amendment/{amendment_id}/impact/{impact_id}/update",
    response_class=HTMLResponse,
)
def ui_update_impact(
    request: Request,
    soa_id: int,
    amendment_id: int,
    impact_id: int,
    type_code: str = Form(...),
    text: str = Form(...),
    is_substantial: str = Form(""),
):
    if not soa_exists(soa_id):
        raise HTTPException(404, "SOA not found")
    conn = _connect()
    cur = conn.cursor()
    cur.execute(
        "SELECT id,impact_uid,type_code_uid FROM study_amendment_impact "
        "WHERE id=? AND soa_id=?",
        (impact_id, soa_id),
    )
    row = cur.fetchone()
    if not row:
        conn.close()
        raise HTTPException(404, "Impact not found")
    before = {"impact_uid": row[1]}
    _update_code_value(cur, soa_id, row[2], type_code, AMENDMENT_IMPACT_TYPE_CODELIST)
    cur.execute(
        "UPDATE study_amendment_impact SET text=?, is_substantial=? "
        "WHERE id=? AND soa_id=?",
        (text, int(bool(is_substantial)), impact_id, soa_id),
    )
    conn.commit()
    conn.close()
    _record_impact_audit(
        soa_id,
        "update",
        impact_id,
        before=before,
        after={"type_code": type_code, "text": text},
    )
    return _ui_impacts_partial(request, soa_id, amendment_id)


@ui_router.post(
    "/ui/soa/{soa_id}/amendment/{amendment_id}/changes/add",
    response_class=HTMLResponse,
)
def ui_add_change(
    request: Request,
    soa_id: int,
    amendment_id: int,
    name: str = Form(...),
    summary: str = Form(...),
    rationale: str = Form(...),
    label: str = Form(""),
    description: str = Form(""),
):
    if not soa_exists(soa_id):
        raise HTTPException(404, "SOA not found")
    try:
        body = StudyChangeCreate(
            name=name,
            summary=summary,
            rationale=rationale,
            label=label or None,
            description=description or None,
        )
    except ValidationError as exc:
        logger.exception("Amendment change validation error: %s", exc)
        return HTMLResponse(
            "<div style='color:#c62828;font-size:0.8em;'>"
            "Error: Invalid change data. Please check your input.</div>"
        )
    conn = _connect()
    cur = conn.cursor()
    am = _amendment_row(cur, soa_id, amendment_id)
    if not am:
        conn.close()
        raise HTTPException(404, "Amendment not found")
    change_uid = _next_change_uid(cur, soa_id)
    cur.execute(
        "INSERT INTO study_change "
        "(soa_id,amendment_uid,change_uid,name,label,description,"
        "summary,rationale) VALUES (?,?,?,?,?,?,?,?)",
        (
            soa_id,
            am["amendment_uid"],
            change_uid,
            body.name,
            body.label,
            body.description,
            body.summary,
            body.rationale,
        ),
    )
    change_id = cur.lastrowid
    conn.commit()
    conn.close()
    _record_change_audit(
        soa_id,
        "create",
        change_id,
        after={"change_uid": change_uid, "name": body.name},
    )
    return _ui_changes_partial(request, soa_id, amendment_id)


@ui_router.post(
    "/ui/soa/{soa_id}/amendment/{amendment_id}/change/{change_id}/delete",
    response_class=HTMLResponse,
)
def ui_delete_change(request: Request, soa_id: int, amendment_id: int, change_id: int):
    if not soa_exists(soa_id):
        raise HTTPException(404, "SOA not found")
    conn = _connect()
    cur = conn.cursor()
    cur.execute(
        "SELECT id,change_uid FROM study_change WHERE id=? AND soa_id=?",
        (change_id, soa_id),
    )
    row = cur.fetchone()
    if not row:
        conn.close()
        raise HTTPException(404, "Change not found")
    before = {"change_uid": row[1]}
    cur.execute(
        "DELETE FROM document_content_reference WHERE soa_id=? AND change_uid=?",
        (soa_id, row[1]),
    )
    cur.execute(
        "DELETE FROM study_change WHERE id=? AND soa_id=?",
        (change_id, soa_id),
    )
    conn.commit()
    conn.close()
    _record_change_audit(soa_id, "delete", change_id, before=before)
    return _ui_changes_partial(request, soa_id, amendment_id)


@ui_router.post(
    "/ui/soa/{soa_id}/amendment/{amendment_id}/change/{change_id}/update",
    response_class=HTMLResponse,
)
def ui_update_change(
    request: Request,
    soa_id: int,
    amendment_id: int,
    change_id: int,
    name: str = Form(...),
    summary: str = Form(...),
    rationale: str = Form(...),
    label: str = Form(""),
    description: str = Form(""),
):
    if not soa_exists(soa_id):
        raise HTTPException(404, "SOA not found")
    conn = _connect()
    cur = conn.cursor()
    cur.execute(
        "SELECT id,change_uid FROM study_change WHERE id=? AND soa_id=?",
        (change_id, soa_id),
    )
    row = cur.fetchone()
    if not row:
        conn.close()
        raise HTTPException(404, "Change not found")
    before = {"change_uid": row[1]}
    cur.execute(
        "UPDATE study_change SET name=?, label=?, description=?, "
        "summary=?, rationale=? WHERE id=? AND soa_id=?",
        (
            name,
            label or None,
            description or None,
            summary,
            rationale,
            change_id,
            soa_id,
        ),
    )
    conn.commit()
    conn.close()
    _record_change_audit(
        soa_id, "update", change_id, before=before, after={"name": name}
    )
    return _ui_changes_partial(request, soa_id, amendment_id)


@ui_router.post(
    "/ui/soa/{soa_id}/change/{change_id}/sections/add",
    response_class=HTMLResponse,
)
def ui_add_section(
    request: Request,
    soa_id: int,
    change_id: int,
    section_number: str = Form(...),
    section_title: str = Form(...),
    applies_to_id: str = Form(...),
    amendment_id: int = Form(...),
):
    if not soa_exists(soa_id):
        raise HTTPException(404, "SOA not found")
    try:
        body = DocumentContentReferenceCreate(
            section_number=section_number,
            section_title=section_title,
            applies_to_id=applies_to_id,
        )
    except ValidationError as exc:
        logger.exception("Document content reference validation error: %s", exc)
        return HTMLResponse(
            "<div style='color:#c62828;font-size:0.8em;'>"
            "Error: Invalid section data. Please check your input.</div>"
        )
    conn = _connect()
    cur = conn.cursor()
    cur.execute(
        "SELECT change_uid FROM study_change WHERE id=? AND soa_id=?",
        (change_id, soa_id),
    )
    row = cur.fetchone()
    if not row:
        conn.close()
        raise HTTPException(404, "Change not found")
    ref_uid = _next_ref_uid(cur, soa_id)
    cur.execute(
        "INSERT INTO document_content_reference "
        "(soa_id,change_uid,ref_uid,section_number,section_title,"
        "applies_to_id) VALUES (?,?,?,?,?,?)",
        (
            soa_id,
            row[0],
            ref_uid,
            body.section_number,
            body.section_title,
            body.applies_to_id,
        ),
    )
    ref_id = cur.lastrowid
    conn.commit()
    conn.close()
    _record_ref_audit(
        soa_id,
        "create",
        ref_id,
        after={"ref_uid": ref_uid},
    )
    return _ui_changes_partial(request, soa_id, amendment_id)


@ui_router.post(
    "/ui/soa/{soa_id}/change/{change_id}/section/{ref_id}/delete",
    response_class=HTMLResponse,
)
def ui_delete_section(
    request: Request,
    soa_id: int,
    change_id: int,
    ref_id: int,
    amendment_id: int = Form(...),
):
    if not soa_exists(soa_id):
        raise HTTPException(404, "SOA not found")
    conn = _connect()
    cur = conn.cursor()
    cur.execute(
        "SELECT id,ref_uid FROM document_content_reference WHERE id=? AND soa_id=?",
        (ref_id, soa_id),
    )
    row = cur.fetchone()
    if not row:
        conn.close()
        raise HTTPException(404, "Section not found")
    before = {"ref_uid": row[1]}
    cur.execute(
        "DELETE FROM document_content_reference WHERE id=? AND soa_id=?",
        (ref_id, soa_id),
    )
    conn.commit()
    conn.close()
    _record_ref_audit(soa_id, "delete", ref_id, before=before)
    return _ui_changes_partial(request, soa_id, amendment_id)


@ui_router.get(
    "/ui/soa/{soa_id}/geo-scope-location-options",
    response_class=HTMLResponse,
)
def ui_geo_scope_location_options(
    request: Request,
    soa_id: int,
    type_code: str = "",
):
    conn = _connect()
    cur = conn.cursor()
    category = _geo_scope_category(type_code)
    ctx = {
        "soa_id": soa_id,
        "category": category,
        "countries": _get_countries(cur) if category == "country" else [],
        "regions": _get_regions(cur) if category == "region" else [],
    }
    conn.close()
    return templates.TemplateResponse(
        request, "amendment_geo_scope_location_options.html", ctx
    )


@ui_router.post(
    "/ui/soa/{soa_id}/amendment/{amendment_id}/geographic-scopes/add",
    response_class=HTMLResponse,
)
def ui_add_geographic_scope(
    request: Request,
    soa_id: int,
    amendment_id: int,
    type_code: str = Form(...),
    location_value: str = Form(""),
):
    if not soa_exists(soa_id):
        raise HTTPException(404, "SOA not found")
    conn = _connect()
    cur = conn.cursor()
    am = _amendment_row(cur, soa_id, amendment_id)
    if not am:
        conn.close()
        raise HTTPException(404, "Amendment not found")
    scope_uid = _next_scope_uid(cur, soa_id)
    type_code_uid = _insert_code(cur, soa_id, type_code, GEOGRAPHIC_SCOPE_TYPE_CODELIST)
    cur.execute(
        "INSERT INTO amendment_geographic_scope "
        "(soa_id,amendment_uid,scope_uid,type_code_uid) VALUES (?,?,?,?)",
        (soa_id, am["amendment_uid"], scope_uid, type_code_uid),
    )
    scope_id = cur.lastrowid
    category = _geo_scope_category(type_code)
    location_code_uid = _insert_location_code(cur, soa_id, category, location_value)
    if location_code_uid:
        cur.execute(
            "UPDATE amendment_geographic_scope SET location_code_uid=? WHERE id=?",
            (location_code_uid, scope_id),
        )
    conn.commit()
    conn.close()
    _record_geo_scope_audit(soa_id, "create", scope_id, after={"scope_uid": scope_uid})
    return _ui_geo_scopes_and_deps(request, soa_id, amendment_id)


@ui_router.post(
    "/ui/soa/{soa_id}/amendment/{amendment_id}/geographic-scope/{scope_id}/delete",
    response_class=HTMLResponse,
)
def ui_delete_geographic_scope(
    request: Request, soa_id: int, amendment_id: int, scope_id: int
):
    if not soa_exists(soa_id):
        raise HTTPException(404, "SOA not found")
    conn = _connect()
    cur = conn.cursor()
    cur.execute(
        "SELECT id,scope_uid,type_code_uid,location_code_uid "
        "FROM amendment_geographic_scope "
        "WHERE id=? AND soa_id=?",
        (scope_id, soa_id),
    )
    row = cur.fetchone()
    if not row:
        conn.close()
        raise HTTPException(404, "Geographic scope not found")
    before = {"scope_uid": row[1]}
    cur.execute(
        "DELETE FROM governance_date_geographic_scope WHERE soa_id=? AND scope_uid=?",
        (soa_id, row[1]),
    )
    cur.execute(
        "DELETE FROM code_association WHERE soa_id=? AND code_uid=?",
        (soa_id, row[2]),
    )
    if row[3]:
        cur.execute(
            "DELETE FROM code WHERE soa_id=? AND code_uid=?",
            (soa_id, row[3]),
        )
    cur.execute(
        "DELETE FROM amendment_geographic_scope WHERE id=? AND soa_id=?",
        (scope_id, soa_id),
    )
    conn.commit()
    conn.close()
    _record_geo_scope_audit(soa_id, "delete", scope_id, before=before)
    return _ui_geo_scopes_and_deps(request, soa_id, amendment_id)


@ui_router.post(
    "/ui/soa/{soa_id}/amendment/{amendment_id}/geographic-scope/{scope_id}/update",
    response_class=HTMLResponse,
)
def ui_update_geographic_scope(
    request: Request,
    soa_id: int,
    amendment_id: int,
    scope_id: int,
    type_code: str = Form(...),
    location_value: str = Form(""),
):
    if not soa_exists(soa_id):
        raise HTTPException(404, "SOA not found")
    conn = _connect()
    cur = conn.cursor()
    cur.execute(
        "SELECT id,scope_uid,type_code_uid,location_code_uid "
        "FROM amendment_geographic_scope "
        "WHERE id=? AND soa_id=?",
        (scope_id, soa_id),
    )
    row = cur.fetchone()
    if not row:
        conn.close()
        raise HTTPException(404, "Geographic scope not found")
    before = {"scope_uid": row[1]}
    _update_code_value(cur, soa_id, row[2], type_code, GEOGRAPHIC_SCOPE_TYPE_CODELIST)
    old_location_uid = row[3]
    category = _geo_scope_category(type_code)
    if old_location_uid:
        cur.execute(
            "DELETE FROM code WHERE soa_id=? AND code_uid=?",
            (soa_id, old_location_uid),
        )
    new_location_uid = _insert_location_code(cur, soa_id, category, location_value)
    cur.execute(
        "UPDATE amendment_geographic_scope SET location_code_uid=? WHERE id=?",
        (new_location_uid, scope_id),
    )
    conn.commit()
    conn.close()
    _record_geo_scope_audit(
        soa_id, "update", scope_id, before=before, after={"type_code": type_code}
    )
    return _ui_geo_scopes_and_deps(request, soa_id, amendment_id)


@ui_router.post(
    "/ui/soa/{soa_id}/amendment/{amendment_id}/enrollments/add",
    response_class=HTMLResponse,
)
def ui_add_enrollment(
    request: Request,
    soa_id: int,
    amendment_id: int,
    name: str = Form(...),
    quantity_value: float = Form(...),
    label: str = Form(""),
    description: str = Form(""),
    for_scope_uid: str = Form(""),
    for_study_cohort_id: str = Form(""),
    for_study_site_id: str = Form(""),
):
    if not soa_exists(soa_id):
        raise HTTPException(404, "SOA not found")
    try:
        body = SubjectEnrollmentCreate(
            name=name,
            quantity_value=quantity_value,
            label=label or None,
            description=description or None,
            for_scope_uid=for_scope_uid or None,
            for_study_cohort_id=for_study_cohort_id or None,
            for_study_site_id=for_study_site_id or None,
        )
    except ValidationError:
        logger.exception("Geographic scope validation error")
        return HTMLResponse(
            "<div style='color:#c62828;font-size:0.8em;'>"
            "Error: Invalid data. Please check your input.</div>"
        )
    conn = _connect()
    cur = conn.cursor()
    am = _amendment_row(cur, soa_id, amendment_id)
    if not am:
        conn.close()
        raise HTTPException(404, "Amendment not found")
    if body.for_scope_uid:
        cur.execute(
            "SELECT id FROM amendment_geographic_scope "
            "WHERE soa_id=? AND amendment_uid=? AND scope_uid=?",
            (soa_id, am["amendment_uid"], body.for_scope_uid),
        )
        if not cur.fetchone():
            conn.close()
            return HTMLResponse(
                "<div style='color:#c62828;font-size:0.8em;'>"
                "Error: scope not found on this amendment</div>"
            )
    enrollment_uid = _next_enrollment_uid(cur, soa_id)
    cur.execute(
        "INSERT INTO amendment_subject_enrollment "
        "(soa_id,amendment_uid,enrollment_uid,name,label,description,"
        "quantity_value,for_scope_uid,for_study_cohort_id,for_study_site_id)"
        " VALUES (?,?,?,?,?,?,?,?,?,?)",
        (
            soa_id,
            am["amendment_uid"],
            enrollment_uid,
            body.name,
            body.label,
            body.description,
            body.quantity_value,
            body.for_scope_uid,
            body.for_study_cohort_id,
            body.for_study_site_id,
        ),
    )
    enrollment_id = cur.lastrowid
    conn.commit()
    conn.close()
    _record_enrollment_audit(
        soa_id,
        "create",
        enrollment_id,
        after={"enrollment_uid": enrollment_uid, "name": body.name},
    )
    return _ui_enrollments_partial(request, soa_id, amendment_id)


@ui_router.post(
    "/ui/soa/{soa_id}/amendment/{amendment_id}/enrollment/{enrollment_id}/delete",
    response_class=HTMLResponse,
)
def ui_delete_enrollment(
    request: Request, soa_id: int, amendment_id: int, enrollment_id: int
):
    if not soa_exists(soa_id):
        raise HTTPException(404, "SOA not found")
    conn = _connect()
    cur = conn.cursor()
    cur.execute(
        "SELECT id,enrollment_uid FROM amendment_subject_enrollment "
        "WHERE id=? AND soa_id=?",
        (enrollment_id, soa_id),
    )
    row = cur.fetchone()
    if not row:
        conn.close()
        raise HTTPException(404, "Enrollment not found")
    before = {"enrollment_uid": row[1]}
    cur.execute(
        "DELETE FROM amendment_subject_enrollment WHERE id=? AND soa_id=?",
        (enrollment_id, soa_id),
    )
    conn.commit()
    conn.close()
    _record_enrollment_audit(soa_id, "delete", enrollment_id, before=before)
    return _ui_enrollments_partial(request, soa_id, amendment_id)


@ui_router.post(
    "/ui/soa/{soa_id}/amendment/{amendment_id}/enrollment/{enrollment_id}/update",
    response_class=HTMLResponse,
)
def ui_update_enrollment(
    request: Request,
    soa_id: int,
    amendment_id: int,
    enrollment_id: int,
    name: str = Form(...),
    quantity_value: float = Form(...),
    label: str = Form(""),
    description: str = Form(""),
    for_scope_uid: str = Form(""),
    for_study_cohort_id: str = Form(""),
    for_study_site_id: str = Form(""),
):
    if not soa_exists(soa_id):
        raise HTTPException(404, "SOA not found")
    conn = _connect()
    cur = conn.cursor()
    cur.execute(
        "SELECT id,enrollment_uid FROM amendment_subject_enrollment "
        "WHERE id=? AND soa_id=?",
        (enrollment_id, soa_id),
    )
    row = cur.fetchone()
    if not row:
        conn.close()
        raise HTTPException(404, "Enrollment not found")
    before = {"enrollment_uid": row[1]}
    cur.execute(
        "UPDATE amendment_subject_enrollment "
        "SET name=?, label=?, description=?, quantity_value=?, "
        "for_scope_uid=?, for_study_cohort_id=?, for_study_site_id=? "
        "WHERE id=? AND soa_id=?",
        (
            name,
            label or None,
            description or None,
            quantity_value,
            for_scope_uid or None,
            for_study_cohort_id or None,
            for_study_site_id or None,
            enrollment_id,
            soa_id,
        ),
    )
    conn.commit()
    conn.close()
    _record_enrollment_audit(
        soa_id,
        "update",
        enrollment_id,
        before=before,
        after={"name": name, "quantity_value": quantity_value},
    )
    return _ui_enrollments_partial(request, soa_id, amendment_id)


@ui_router.post(
    "/ui/soa/{soa_id}/amendment/{amendment_id}/governance-dates/add",
    response_class=HTMLResponse,
)
def ui_add_governance_date(
    request: Request,
    soa_id: int,
    amendment_id: int,
    name: str = Form(...),
    type_code: str = Form(...),
    date_value: str = Form(...),
    label: str = Form(""),
    description: str = Form(""),
    scope_uids: List[str] = Form(default=[]),
):
    if not soa_exists(soa_id):
        raise HTTPException(404, "SOA not found")
    try:
        body = GovernanceDateCreate(
            name=name,
            type_code=type_code,
            date_value=date_value,
            label=label or None,
            description=description or None,
            scope_uids=scope_uids,
        )
    except ValidationError:
        logger.exception("Governance date validation error")
        return HTMLResponse(
            "<div style='color:#c62828;font-size:0.8em;'>"
            "Error: Invalid data. Please check your input.</div>"
        )
    conn = _connect()
    cur = conn.cursor()
    am = _amendment_row(cur, soa_id, amendment_id)
    if not am:
        conn.close()
        raise HTTPException(404, "Amendment not found")
    date_uid = _next_gov_date_uid(cur, soa_id)
    type_code_uid = _insert_code(
        cur, soa_id, body.type_code, GOVERNANCE_DATE_TYPE_CODELIST
    )
    cur.execute(
        "INSERT INTO amendment_governance_date "
        "(soa_id,amendment_uid,date_uid,name,label,description,"
        "type_code_uid,date_value) VALUES (?,?,?,?,?,?,?,?)",
        (
            soa_id,
            am["amendment_uid"],
            date_uid,
            body.name,
            body.label,
            body.description,
            type_code_uid,
            body.date_value,
        ),
    )
    date_id = cur.lastrowid
    for s_uid in body.scope_uids:
        cur.execute(
            "INSERT INTO governance_date_geographic_scope "
            "(soa_id,date_uid,scope_uid) VALUES (?,?,?)",
            (soa_id, date_uid, s_uid),
        )
    conn.commit()
    conn.close()
    _record_gov_date_audit(
        soa_id,
        "create",
        date_id,
        after={"date_uid": date_uid, "name": body.name},
    )
    return _ui_gov_dates_partial(request, soa_id, amendment_id)


@ui_router.post(
    "/ui/soa/{soa_id}/amendment/{amendment_id}/governance-date/{date_id}/delete",
    response_class=HTMLResponse,
)
def ui_delete_governance_date(
    request: Request, soa_id: int, amendment_id: int, date_id: int
):
    if not soa_exists(soa_id):
        raise HTTPException(404, "SOA not found")
    conn = _connect()
    cur = conn.cursor()
    cur.execute(
        "SELECT id,date_uid,type_code_uid FROM amendment_governance_date "
        "WHERE id=? AND soa_id=?",
        (date_id, soa_id),
    )
    row = cur.fetchone()
    if not row:
        conn.close()
        raise HTTPException(404, "Governance date not found")
    before = {"date_uid": row[1]}
    cur.execute(
        "DELETE FROM governance_date_geographic_scope WHERE soa_id=? AND date_uid=?",
        (soa_id, row[1]),
    )
    cur.execute(
        "DELETE FROM code_association WHERE soa_id=? AND code_uid=?",
        (soa_id, row[2]),
    )
    cur.execute(
        "DELETE FROM amendment_governance_date WHERE id=? AND soa_id=?",
        (date_id, soa_id),
    )
    conn.commit()
    conn.close()
    _record_gov_date_audit(soa_id, "delete", date_id, before=before)
    return _ui_gov_dates_partial(request, soa_id, amendment_id)


@ui_router.post(
    "/ui/soa/{soa_id}/amendment/{amendment_id}/governance-date/{date_id}/update",
    response_class=HTMLResponse,
)
def ui_update_governance_date(
    request: Request,
    soa_id: int,
    amendment_id: int,
    date_id: int,
    name: str = Form(...),
    type_code: str = Form(...),
    date_value: str = Form(...),
    label: str = Form(""),
    description: str = Form(""),
):
    if not soa_exists(soa_id):
        raise HTTPException(404, "SOA not found")
    conn = _connect()
    cur = conn.cursor()
    cur.execute(
        "SELECT id,date_uid,type_code_uid FROM amendment_governance_date "
        "WHERE id=? AND soa_id=?",
        (date_id, soa_id),
    )
    row = cur.fetchone()
    if not row:
        conn.close()
        raise HTTPException(404, "Governance date not found")
    before = {"date_uid": row[1]}
    _update_code_value(cur, soa_id, row[2], type_code, GOVERNANCE_DATE_TYPE_CODELIST)
    cur.execute(
        "UPDATE amendment_governance_date "
        "SET name=?, label=?, description=?, date_value=? "
        "WHERE id=? AND soa_id=?",
        (name, label or None, description or None, date_value, date_id, soa_id),
    )
    conn.commit()
    conn.close()
    _record_gov_date_audit(
        soa_id,
        "update",
        date_id,
        before=before,
        after={"name": name, "date_value": date_value},
    )
    return _ui_gov_dates_partial(request, soa_id, amendment_id)


@ui_router.post(
    "/ui/soa/{soa_id}/amendment/{amendment_id}/governance-date/{date_id}/link-scope",
    response_class=HTMLResponse,
)
def ui_link_gov_date_scope(
    request: Request,
    soa_id: int,
    amendment_id: int,
    date_id: int,
    scope_uid: str = Form(...),
):
    if not soa_exists(soa_id):
        raise HTTPException(404, "SOA not found")
    conn = _connect()
    cur = conn.cursor()
    cur.execute(
        "SELECT date_uid FROM amendment_governance_date WHERE id=? AND soa_id=?",
        (date_id, soa_id),
    )
    row = cur.fetchone()
    if not row:
        conn.close()
        raise HTTPException(404, "Governance date not found")
    cur.execute(
        "SELECT id FROM governance_date_geographic_scope "
        "WHERE soa_id=? AND date_uid=? AND scope_uid=?",
        (soa_id, row[0], scope_uid),
    )
    if not cur.fetchone():
        cur.execute(
            "INSERT INTO governance_date_geographic_scope "
            "(soa_id,date_uid,scope_uid) VALUES (?,?,?)",
            (soa_id, row[0], scope_uid),
        )
        conn.commit()
    conn.close()
    return _ui_gov_dates_partial(request, soa_id, amendment_id)


@ui_router.post(
    "/ui/soa/{soa_id}/amendment/{amendment_id}/governance-date/{date_id}"
    "/unlink-scope/{scope_uid}",
    response_class=HTMLResponse,
)
def ui_unlink_gov_date_scope(
    request: Request,
    soa_id: int,
    amendment_id: int,
    date_id: int,
    scope_uid: str,
):
    if not soa_exists(soa_id):
        raise HTTPException(404, "SOA not found")
    conn = _connect()
    cur = conn.cursor()
    cur.execute(
        "SELECT date_uid FROM amendment_governance_date WHERE id=? AND soa_id=?",
        (date_id, soa_id),
    )
    row = cur.fetchone()
    if not row:
        conn.close()
        raise HTTPException(404, "Governance date not found")
    cur.execute(
        "DELETE FROM governance_date_geographic_scope "
        "WHERE soa_id=? AND date_uid=? AND scope_uid=?",
        (soa_id, row[0], scope_uid),
    )
    conn.commit()
    conn.close()
    return _ui_gov_dates_partial(request, soa_id, amendment_id)
