"""amendments: helpers (split from amendments.py)."""

from ...errors import MALFORMED_JSON_ERRORS
from ...utils import get_next_geographic_scope_uid
from ._shared import (
    AMENDMENT_IMPACT_TYPE_CODELIST,
    AMENDMENT_REASON_CODELIST,
    GEOGRAPHIC_SCOPE_TYPE_CODELIST,
    GOVERNANCE_DATE_TYPE_CODELIST,
    HTMLResponse,
    HTTPException,
    Optional,
    Request,
    _COUNTRY_SUBMISSION_VALUE,
    _REGION_SUBMISSION_VALUE,
    _connect,
    get_ddf_ct_codelist_map,
    get_latest_ddf_ct_href,
    get_next_code_uid,
    json,
    templates,
)


def _next_uid(cur, soa_id: int, table: str, col: str, prefix: str) -> str:
    """Return next UID for prefix, never reusing deleted UIDs."""
    max_n = 0
    cur.execute(
        f"SELECT {col} FROM {table} WHERE soa_id=? AND {col} LIKE '{prefix}%'",
        (soa_id,),
    )
    for (uid,) in cur.fetchall():
        if isinstance(uid, str) and uid.startswith(prefix):
            try:
                n = int(uid.split("_")[-1])
                if n > max_n:
                    max_n = n
            except (ValueError, IndexError):
                pass
    audit_table = f"{table}_audit"
    cur.execute(
        f"SELECT before_json, after_json FROM {audit_table} WHERE soa_id=?",
        (soa_id,),
    )
    for before_raw, after_raw in cur.fetchall():
        for raw in (before_raw, after_raw):
            if not raw:
                continue
            try:
                uid = json.loads(raw).get(col, "")
                if isinstance(uid, str) and uid.startswith(prefix):
                    n = int(uid.split("_")[-1])
                    if n > max_n:
                        max_n = n
            except MALFORMED_JSON_ERRORS:
                pass
    return f"{prefix}{max_n + 1}"


def _next_amendment_uid(cur, soa_id: int) -> str:
    return _next_uid(cur, soa_id, "study_amendment", "amendment_uid", "StudyAmendment_")


def _next_reason_uid(cur, soa_id: int) -> str:
    return _next_uid(
        cur,
        soa_id,
        "study_amendment_reason",
        "reason_uid",
        "StudyAmendmentReason_",
    )


def _next_impact_uid(cur, soa_id: int) -> str:
    return _next_uid(
        cur,
        soa_id,
        "study_amendment_impact",
        "impact_uid",
        "StudyAmendmentImpact_",
    )


def _next_change_uid(cur, soa_id: int) -> str:
    return _next_uid(cur, soa_id, "study_change", "change_uid", "StudyChange_")


def _next_ref_uid(cur, soa_id: int) -> str:
    return _next_uid(
        cur,
        soa_id,
        "document_content_reference",
        "ref_uid",
        "DocumentContentReference_",
    )


def _next_scope_uid(cur, soa_id: int) -> str:
    # Counter-backed: also skips the uids of embedded scope copies.
    return get_next_geographic_scope_uid(cur, soa_id)


def _next_enrollment_uid(cur, soa_id: int) -> str:
    return _next_uid(
        cur,
        soa_id,
        "amendment_subject_enrollment",
        "enrollment_uid",
        "SubjectEnrollment_",
    )


def _next_gov_date_uid(cur, soa_id: int) -> str:
    return _next_uid(
        cur,
        soa_id,
        "amendment_governance_date",
        "date_uid",
        "GovernanceDate_",
    )


def _insert_code(cur, soa_id: int, code_value: str, codelist_code: str) -> str:
    """Insert code_association row; decode resolved from DDF CT submission_value."""
    code_uid = get_next_code_uid(cur, soa_id)
    slug = get_latest_ddf_ct_href() or ""
    codelist_table = f"/mdr/ct/packages/{slug}" if slug else "/mdr/ct/packages"
    terms = get_ddf_ct_codelist_map(codelist_code)
    decode = terms.get(code_value, code_value)
    cur.execute(
        "INSERT INTO code_association "
        "(soa_id, code_uid, codelist_table, codelist_code, code, decode) "
        "VALUES (?,?,?,?,?,?)",
        (soa_id, code_uid, codelist_table, codelist_code, code_value, decode),
    )
    return code_uid


def _update_code_value(
    cur, soa_id: int, code_uid: str, code_value: str, codelist_code: str
) -> None:
    """Update code and decode for an existing code_association row."""
    terms = get_ddf_ct_codelist_map(codelist_code)
    decode = terms.get(code_value, code_value)
    cur.execute(
        "UPDATE code_association SET code=?, decode=? WHERE code_uid=? AND soa_id=?",
        (code_value, decode, code_uid, soa_id),
    )


def _geo_scope_category(type_code: str) -> Optional[str]:
    """Return 'country', 'region', or None for the given type C-code."""
    terms = get_ddf_ct_codelist_map(GEOGRAPHIC_SCOPE_TYPE_CODELIST)
    sv = terms.get(type_code, "")
    if sv.lower() == _COUNTRY_SUBMISSION_VALUE.lower():
        return "country"
    if sv.lower() == _REGION_SUBMISSION_VALUE.lower():
        return "region"
    return None


def _get_countries(cur) -> list:
    cur.execute(
        "SELECT country_name, country_numeric_code "
        "FROM country_codes ORDER BY country_name"
    )
    return [{"name": r[0], "code": r[1]} for r in cur.fetchall()]


def _get_regions(cur) -> list:
    cur.execute(
        "SELECT subregion, region_numeric_code "
        "FROM geographic_regions ORDER BY subregion"
    )
    return [{"name": r[0], "code": r[1]} for r in cur.fetchall()]


def _insert_location_code(
    cur, soa_id: int, category: str, location_value: str
) -> Optional[str]:
    """Insert a code row for a country or region selection; return code_uid."""
    if not location_value:
        return None
    if category == "country":
        cur.execute(
            "SELECT country_name FROM country_codes WHERE country_numeric_code=?",
            (location_value,),
        )
        row = cur.fetchone()
        if not row:
            return None
        code_uid = get_next_code_uid(cur, soa_id)
        cur.execute(
            "INSERT INTO code "
            "(soa_id,code_uid,code,decode,code_system,code_system_version)"
            " VALUES (?,?,?,?,?,?)",
            (
                soa_id,
                code_uid,
                location_value,
                row[0],
                "ISO 3166 1 Numeric Code",
                "2026",
            ),
        )
        return code_uid
    if category == "region":
        cur.execute(
            "SELECT subregion FROM geographic_regions WHERE region_numeric_code=?",
            (location_value,),
        )
        row = cur.fetchone()
        if not row:
            return None
        code_uid = get_next_code_uid(cur, soa_id)
        cur.execute(
            "INSERT INTO code "
            "(soa_id,code_uid,code,decode,code_system,code_system_version)"
            " VALUES (?,?,?,?,?,?)",
            (
                soa_id,
                code_uid,
                location_value,
                row[0],
                "UN M49",
                "2026",
            ),
        )
        return code_uid
    return None


def _amendment_row(cur, soa_id: int, amendment_id: int) -> Optional[dict]:
    cur.execute(
        "SELECT id,soa_id,freeze_id,amendment_uid,name,number,summary,"
        "label,description FROM study_amendment WHERE id=? AND soa_id=?",
        (amendment_id, soa_id),
    )
    row = cur.fetchone()
    if not row:
        return None
    return {
        "id": row[0],
        "soa_id": row[1],
        "freeze_id": row[2],
        "amendment_uid": row[3],
        "name": row[4],
        "number": row[5],
        "summary": row[6],
        "label": row[7],
        "description": row[8],
    }


def _load_amendment_data(soa_id: int, amendment_id: int) -> dict:
    """DB-only context for HTMX partial swaps — no API calls."""
    conn = _connect()
    cur = conn.cursor()

    amendment = _amendment_row(cur, soa_id, amendment_id)
    if not amendment:
        conn.close()
        return {}

    cur.execute(
        "SELECT r.id, r.reason_uid, r.role, r.other_reason, ca.code "
        "FROM study_amendment_reason r "
        "LEFT JOIN code_association ca ON ca.code_uid=r.code_uid "
        "AND ca.soa_id=r.soa_id "
        "WHERE r.soa_id=? AND r.amendment_uid=? "
        "ORDER BY r.id",
        (soa_id, amendment["amendment_uid"]),
    )
    reasons = [
        {
            "id": r[0],
            "reason_uid": r[1],
            "role": r[2],
            "other_reason": r[3],
            "code": r[4] or "",
        }
        for r in cur.fetchall()
    ]

    cur.execute(
        "SELECT i.id, i.impact_uid, i.text, i.is_substantial, ca.code "
        "FROM study_amendment_impact i "
        "LEFT JOIN code_association ca ON ca.code_uid=i.type_code_uid "
        "AND ca.soa_id=i.soa_id "
        "WHERE i.soa_id=? AND i.amendment_uid=? "
        "ORDER BY i.id",
        (soa_id, amendment["amendment_uid"]),
    )
    impacts = [
        {
            "id": r[0],
            "impact_uid": r[1],
            "text": r[2],
            "is_substantial": bool(r[3]),
            "type_code": r[4] or "",
        }
        for r in cur.fetchall()
    ]

    cur.execute(
        "SELECT id,change_uid,name,label,description,summary,rationale "
        "FROM study_change "
        "WHERE soa_id=? AND amendment_uid=? ORDER BY id",
        (soa_id, amendment["amendment_uid"]),
    )
    changes_rows = cur.fetchall()
    changes = []
    for cr in changes_rows:
        change_uid = cr[1]
        cur.execute(
            "SELECT id,ref_uid,section_number,section_title,applies_to_id "
            "FROM document_content_reference "
            "WHERE soa_id=? AND change_uid=? ORDER BY id",
            (soa_id, change_uid),
        )
        sections = [
            {
                "id": s[0],
                "ref_uid": s[1],
                "section_number": s[2],
                "section_title": s[3],
                "applies_to_id": s[4],
            }
            for s in cur.fetchall()
        ]
        changes.append(
            {
                "id": cr[0],
                "change_uid": change_uid,
                "name": cr[2],
                "label": cr[3],
                "description": cr[4],
                "summary": cr[5],
                "rationale": cr[6],
                "sections": sections,
            }
        )

    amendment_uid = amendment["amendment_uid"]

    cur.execute(
        "SELECT s.id, s.scope_uid, ca.code, s.location_code_uid, "
        "c.code, c.decode "
        "FROM amendment_geographic_scope s "
        "LEFT JOIN code_association ca "
        "ON ca.code_uid=s.type_code_uid AND ca.soa_id=s.soa_id "
        "LEFT JOIN code c "
        "ON c.code_uid=s.location_code_uid AND c.soa_id=s.soa_id "
        "WHERE s.soa_id=? AND s.amendment_uid=? ORDER BY s.id",
        (soa_id, amendment_uid),
    )
    geo_scopes = [
        {
            "id": r[0],
            "scope_uid": r[1],
            "type_code": r[2] or "",
            "location_code_uid": r[3],
            "location_code": r[4] or "",
            "location_decode": r[5] or "",
        }
        for r in cur.fetchall()
    ]

    cur.execute(
        "SELECT id, enrollment_uid, name, label, description, "
        "quantity_value, for_scope_uid, for_study_cohort_id, for_study_site_id "
        "FROM amendment_subject_enrollment "
        "WHERE soa_id=? AND amendment_uid=? ORDER BY id",
        (soa_id, amendment_uid),
    )
    enrollments = [
        {
            "id": r[0],
            "enrollment_uid": r[1],
            "name": r[2],
            "label": r[3],
            "description": r[4],
            "quantity_value": r[5],
            "for_scope_uid": r[6],
            "for_study_cohort_id": r[7],
            "for_study_site_id": r[8],
        }
        for r in cur.fetchall()
    ]

    cur.execute(
        "SELECT d.id, d.date_uid, d.name, d.label, d.description, "
        "d.date_value, ca.code "
        "FROM amendment_governance_date d "
        "LEFT JOIN code_association ca ON ca.code_uid=d.type_code_uid "
        "AND ca.soa_id=d.soa_id "
        "WHERE d.soa_id=? AND d.amendment_uid=? ORDER BY d.id",
        (soa_id, amendment_uid),
    )
    gov_dates_rows = cur.fetchall()
    gov_dates = []
    for dr in gov_dates_rows:
        cur.execute(
            "SELECT scope_uid FROM governance_date_geographic_scope "
            "WHERE soa_id=? AND date_uid=? ORDER BY id",
            (soa_id, dr[1]),
        )
        linked_scope_uids = [row[0] for row in cur.fetchall()]
        gov_dates.append(
            {
                "id": dr[0],
                "date_uid": dr[1],
                "name": dr[2],
                "label": dr[3],
                "description": dr[4],
                "date_value": dr[5],
                "type_code": dr[6] or "",
                "scope_uids": linked_scope_uids,
            }
        )

    conn.close()
    primary_list = [r for r in reasons if r["role"] == "primary"]
    return {
        "amendment": amendment,
        "primary_reason": primary_list[0] if primary_list else None,
        "secondary_reasons": [r for r in reasons if r["role"] == "secondary"],
        "impacts": impacts,
        "changes": changes,
        "geo_scopes": geo_scopes,
        "enrollments": enrollments,
        "gov_dates": gov_dates,
        "soa_id": soa_id,
    }


def _load_amendment_context(soa_id: int, amendment_id: int) -> dict:
    """Full context including codelists — used only for initial page load."""
    ctx = _load_amendment_data(soa_id, amendment_id)
    if not ctx:
        return {}
    ctx["reason_terms"] = get_ddf_ct_codelist_map(AMENDMENT_REASON_CODELIST)
    ctx["impact_terms"] = get_ddf_ct_codelist_map(AMENDMENT_IMPACT_TYPE_CODELIST)
    ctx["geo_scope_type_terms"] = get_ddf_ct_codelist_map(
        GEOGRAPHIC_SCOPE_TYPE_CODELIST
    )
    ctx["gov_date_type_terms"] = get_ddf_ct_codelist_map(GOVERNANCE_DATE_TYPE_CODELIST)
    return ctx


def _ui_reasons_partial(
    request: Request, soa_id: int, amendment_id: int
) -> HTMLResponse:
    ctx = _load_amendment_data(soa_id, amendment_id)
    if not ctx:
        raise HTTPException(404, "Amendment not found")
    ctx["reason_terms"] = get_ddf_ct_codelist_map(AMENDMENT_REASON_CODELIST)
    return templates.TemplateResponse(
        request,
        "amendment_reasons_partial.html",
        ctx,
    )


def _ui_impacts_partial(
    request: Request, soa_id: int, amendment_id: int
) -> HTMLResponse:
    ctx = _load_amendment_data(soa_id, amendment_id)
    if not ctx:
        raise HTTPException(404, "Amendment not found")
    ctx["impact_terms"] = get_ddf_ct_codelist_map(AMENDMENT_IMPACT_TYPE_CODELIST)
    return templates.TemplateResponse(
        request,
        "amendment_impacts_partial.html",
        ctx,
    )


def _ui_changes_partial(
    request: Request, soa_id: int, amendment_id: int
) -> HTMLResponse:
    ctx = _load_amendment_data(soa_id, amendment_id)
    if not ctx:
        raise HTTPException(404, "Amendment not found")
    return templates.TemplateResponse(
        request,
        "amendment_changes_partial.html",
        ctx,
    )


def _ui_geo_scopes_partial(
    request: Request, soa_id: int, amendment_id: int
) -> HTMLResponse:
    ctx = _load_amendment_data(soa_id, amendment_id)
    if not ctx:
        raise HTTPException(404, "Amendment not found")
    ctx["geo_scope_type_terms"] = get_ddf_ct_codelist_map(
        GEOGRAPHIC_SCOPE_TYPE_CODELIST
    )
    return templates.TemplateResponse(
        request,
        "amendment_geo_scopes_partial.html",
        ctx,
    )


def _ui_geo_scopes_and_deps(
    request: Request, soa_id: int, amendment_id: int
) -> HTMLResponse:
    """Geo-scopes primary + OOB refresh of enrollment and gov-dates dropdowns."""
    ctx = _load_amendment_data(soa_id, amendment_id)
    if not ctx:
        raise HTTPException(404, "Amendment not found")
    ctx["geo_scope_type_terms"] = get_ddf_ct_codelist_map(
        GEOGRAPHIC_SCOPE_TYPE_CODELIST
    )
    geo_html = templates.get_template("amendment_geo_scopes_partial.html").render(ctx)
    ctx["oob"] = True
    enroll_html = templates.get_template("amendment_enrollments_partial.html").render(
        ctx
    )
    ctx["gov_date_type_terms"] = get_ddf_ct_codelist_map(GOVERNANCE_DATE_TYPE_CODELIST)
    gov_html = templates.get_template("amendment_gov_dates_partial.html").render(ctx)
    return HTMLResponse(geo_html + enroll_html + gov_html)


def _ui_enrollments_partial(
    request: Request, soa_id: int, amendment_id: int
) -> HTMLResponse:
    ctx = _load_amendment_data(soa_id, amendment_id)
    if not ctx:
        raise HTTPException(404, "Amendment not found")
    return templates.TemplateResponse(
        request,
        "amendment_enrollments_partial.html",
        ctx,
    )


def _ui_gov_dates_partial(
    request: Request, soa_id: int, amendment_id: int
) -> HTMLResponse:
    ctx = _load_amendment_data(soa_id, amendment_id)
    if not ctx:
        raise HTTPException(404, "Amendment not found")
    ctx["gov_date_type_terms"] = get_ddf_ct_codelist_map(GOVERNANCE_DATE_TYPE_CODELIST)
    return templates.TemplateResponse(
        request,
        "amendment_gov_dates_partial.html",
        ctx,
    )
