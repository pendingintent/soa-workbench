"""amendments: api (split from amendments.py)."""

from ._shared import (
    AMENDMENT_IMPACT_TYPE_CODELIST,
    AMENDMENT_REASON_CODELIST,
    DocumentContentReferenceCreate,
    GEOGRAPHIC_SCOPE_TYPE_CODELIST,
    GOVERNANCE_DATE_TYPE_CODELIST,
    GeographicScopeCreate,
    GovernanceDateCreate,
    HTTPException,
    JSONResponse,
    StudyAmendmentCreate,
    StudyAmendmentImpactCreate,
    StudyAmendmentReasonCreate,
    StudyAmendmentUpdate,
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
    router,
    soa_exists,
)
from .helpers import (
    _amendment_row,
    _insert_code,
    _next_amendment_uid,
    _next_change_uid,
    _next_enrollment_uid,
    _next_gov_date_uid,
    _next_impact_uid,
    _next_reason_uid,
    _next_ref_uid,
    _next_scope_uid,
)


@router.post(
    "/soa/{soa_id}/freeze/{freeze_id}/amendment",
    status_code=201,
)
def create_amendment(soa_id: int, freeze_id: int, body: StudyAmendmentCreate):
    if not soa_exists(soa_id):
        raise HTTPException(404, "SOA not found")
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
        raise HTTPException(409, "Amendment already exists for this freeze")

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

    after = {
        "amendment_uid": amendment_uid,
        "name": body.name,
        "number": body.number,
        "summary": body.summary,
    }
    _record_amendment_audit(soa_id, "create", amendment_id, after=after)
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
    return JSONResponse(
        {"id": amendment_id, "amendment_uid": amendment_uid},
        status_code=201,
    )


@router.get("/soa/{soa_id}/freeze/{freeze_id}/amendment")
def get_amendment(soa_id: int, freeze_id: int):
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
        raise HTTPException(404, "Amendment not found")
    conn = _connect()
    cur = conn.cursor()
    data = _amendment_row(cur, soa_id, row[0])
    conn.close()
    return JSONResponse(data)


@router.patch("/soa/{soa_id}/amendment/{amendment_id}")
def update_amendment(soa_id: int, amendment_id: int, body: StudyAmendmentUpdate):
    if not soa_exists(soa_id):
        raise HTTPException(404, "SOA not found")
    conn = _connect()
    cur = conn.cursor()
    before = _amendment_row(cur, soa_id, amendment_id)
    if not before:
        conn.close()
        raise HTTPException(404, "Amendment not found")
    fields = body.model_dump(exclude_none=True)
    if not fields:
        conn.close()
        return JSONResponse(before)
    set_clause = ", ".join(f"{k}=?" for k in fields)
    cur.execute(
        f"UPDATE study_amendment SET {set_clause} WHERE id=? AND soa_id=?",
        (*fields.values(), amendment_id, soa_id),
    )
    conn.commit()
    after = _amendment_row(cur, soa_id, amendment_id)
    conn.close()
    _record_amendment_audit(soa_id, "update", amendment_id, before=before, after=after)
    return JSONResponse(after)


@router.delete("/soa/{soa_id}/amendment/{amendment_id}", status_code=204)
def delete_amendment(soa_id: int, amendment_id: int):
    if not soa_exists(soa_id):
        raise HTTPException(404, "SOA not found")
    conn = _connect()
    cur = conn.cursor()
    before = _amendment_row(cur, soa_id, amendment_id)
    if not before:
        conn.close()
        raise HTTPException(404, "Amendment not found")
    amendment_uid = before["amendment_uid"]

    # 1. Collect impact code_uids BEFORE deleting impacts
    cur.execute(
        "SELECT type_code_uid FROM study_amendment_impact "
        "WHERE soa_id=? AND amendment_uid=?",
        (soa_id, amendment_uid),
    )
    impact_code_uids = [row[0] for row in cur.fetchall()]

    # 2. Collect reason code_uids BEFORE deleting reasons
    cur.execute(
        "SELECT code_uid FROM study_amendment_reason "
        "WHERE soa_id=? AND amendment_uid=?",
        (soa_id, amendment_uid),
    )
    reason_code_uids = [row[0] for row in cur.fetchall()]

    # 3. Delete document_content_reference rows for all changes
    cur.execute(
        "SELECT change_uid FROM study_change WHERE soa_id=? AND amendment_uid=?",
        (soa_id, amendment_uid),
    )
    for (cu,) in cur.fetchall():
        cur.execute(
            "DELETE FROM document_content_reference WHERE soa_id=? AND change_uid=?",
            (soa_id, cu),
        )

    # 4. Delete changes
    cur.execute(
        "DELETE FROM study_change WHERE soa_id=? AND amendment_uid=?",
        (soa_id, amendment_uid),
    )

    # 5. Delete impacts
    cur.execute(
        "DELETE FROM study_amendment_impact WHERE soa_id=? AND amendment_uid=?",
        (soa_id, amendment_uid),
    )

    # 6. Delete impact code_association rows
    for cu in impact_code_uids:
        cur.execute(
            "DELETE FROM code_association WHERE soa_id=? AND code_uid=?",
            (soa_id, cu),
        )

    # 7. Delete reasons
    cur.execute(
        "DELETE FROM study_amendment_reason WHERE soa_id=? AND amendment_uid=?",
        (soa_id, amendment_uid),
    )

    # 8. Delete reason code_association rows
    for cu in reason_code_uids:
        cur.execute(
            "DELETE FROM code_association WHERE soa_id=? AND code_uid=?",
            (soa_id, cu),
        )

    # 9. Delete the amendment itself
    cur.execute(
        "DELETE FROM study_amendment WHERE id=? AND soa_id=?",
        (amendment_id, soa_id),
    )
    conn.commit()
    conn.close()
    _record_amendment_audit(soa_id, "delete", amendment_id, before=before)


@router.post(
    "/soa/{soa_id}/amendment/{amendment_id}/reasons",
    status_code=201,
)
def add_secondary_reason(
    soa_id: int, amendment_id: int, body: StudyAmendmentReasonCreate
):
    if not soa_exists(soa_id):
        raise HTTPException(404, "SOA not found")
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
        after={
            "reason_uid": reason_uid,
            "role": "secondary",
            "code": body.code,
        },
    )
    return JSONResponse({"id": reason_id, "reason_uid": reason_uid}, status_code=201)


@router.delete(
    "/soa/{soa_id}/amendment/{amendment_id}/reason/{reason_id}",
    status_code=204,
)
def delete_secondary_reason(soa_id: int, amendment_id: int, reason_id: int):
    if not soa_exists(soa_id):
        raise HTTPException(404, "SOA not found")
    conn = _connect()
    cur = conn.cursor()
    cur.execute(
        "SELECT id,reason_uid,role,code_uid,other_reason "
        "FROM study_amendment_reason WHERE id=? AND soa_id=?",
        (reason_id, soa_id),
    )
    row = cur.fetchone()
    if not row:
        conn.close()
        raise HTTPException(404, "Reason not found")
    if row[2] == "primary":
        conn.close()
        raise HTTPException(400, "Cannot delete primary reason")
    before = {
        "reason_uid": row[1],
        "role": row[2],
        "code_uid": row[3],
    }
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


@router.post(
    "/soa/{soa_id}/amendment/{amendment_id}/impacts",
    status_code=201,
)
def add_impact(soa_id: int, amendment_id: int, body: StudyAmendmentImpactCreate):
    if not soa_exists(soa_id):
        raise HTTPException(404, "SOA not found")
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
        after={"impact_uid": impact_uid, "type_code": body.type_code},
    )
    return JSONResponse({"id": impact_id, "impact_uid": impact_uid}, status_code=201)


@router.delete(
    "/soa/{soa_id}/amendment/{amendment_id}/impact/{impact_id}",
    status_code=204,
)
def delete_impact(soa_id: int, amendment_id: int, impact_id: int):
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


@router.post(
    "/soa/{soa_id}/amendment/{amendment_id}/changes",
    status_code=201,
)
def add_change(soa_id: int, amendment_id: int, body: StudyChangeCreate):
    if not soa_exists(soa_id):
        raise HTTPException(404, "SOA not found")
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
    return JSONResponse({"id": change_id, "change_uid": change_uid}, status_code=201)


@router.delete(
    "/soa/{soa_id}/amendment/{amendment_id}/change/{change_id}",
    status_code=204,
)
def delete_change(soa_id: int, amendment_id: int, change_id: int):
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


@router.post(
    "/soa/{soa_id}/change/{change_id}/sections",
    status_code=201,
)
def add_section(soa_id: int, change_id: int, body: DocumentContentReferenceCreate):
    if not soa_exists(soa_id):
        raise HTTPException(404, "SOA not found")
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
    change_uid = row[0]
    ref_uid = _next_ref_uid(cur, soa_id)
    cur.execute(
        "INSERT INTO document_content_reference "
        "(soa_id,change_uid,ref_uid,section_number,section_title,"
        "applies_to_id) VALUES (?,?,?,?,?,?)",
        (
            soa_id,
            change_uid,
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
        after={"ref_uid": ref_uid, "section_number": body.section_number},
    )
    return JSONResponse({"id": ref_id, "ref_uid": ref_uid}, status_code=201)


@router.delete(
    "/soa/{soa_id}/change/{change_id}/section/{ref_id}",
    status_code=204,
)
def delete_section(soa_id: int, change_id: int, ref_id: int):
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


@router.post(
    "/soa/{soa_id}/amendment/{amendment_id}/geographic-scopes",
    status_code=201,
)
def add_geographic_scope(soa_id: int, amendment_id: int, body: GeographicScopeCreate):
    if not soa_exists(soa_id):
        raise HTTPException(404, "SOA not found")
    conn = _connect()
    cur = conn.cursor()
    am = _amendment_row(cur, soa_id, amendment_id)
    if not am:
        conn.close()
        raise HTTPException(404, "Amendment not found")
    scope_uid = _next_scope_uid(cur, soa_id)
    type_code_uid = _insert_code(
        cur, soa_id, body.type_code, GEOGRAPHIC_SCOPE_TYPE_CODELIST
    )
    cur.execute(
        "INSERT INTO amendment_geographic_scope "
        "(soa_id,amendment_uid,scope_uid,type_code_uid) VALUES (?,?,?,?)",
        (soa_id, am["amendment_uid"], scope_uid, type_code_uid),
    )
    scope_id = cur.lastrowid
    conn.commit()
    conn.close()
    _record_geo_scope_audit(soa_id, "create", scope_id, after={"scope_uid": scope_uid})
    return JSONResponse({"id": scope_id, "scope_uid": scope_uid}, status_code=201)


@router.delete(
    "/soa/{soa_id}/amendment/{amendment_id}/geographic-scope/{scope_id}",
    status_code=204,
)
def delete_geographic_scope(soa_id: int, amendment_id: int, scope_id: int):
    if not soa_exists(soa_id):
        raise HTTPException(404, "SOA not found")
    conn = _connect()
    cur = conn.cursor()
    cur.execute(
        "SELECT id,scope_uid,type_code_uid FROM amendment_geographic_scope "
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
    cur.execute(
        "DELETE FROM amendment_geographic_scope WHERE id=? AND soa_id=?",
        (scope_id, soa_id),
    )
    conn.commit()
    conn.close()
    _record_geo_scope_audit(soa_id, "delete", scope_id, before=before)


@router.post(
    "/soa/{soa_id}/amendment/{amendment_id}/enrollments",
    status_code=201,
)
def add_enrollment(soa_id: int, amendment_id: int, body: SubjectEnrollmentCreate):
    if not soa_exists(soa_id):
        raise HTTPException(404, "SOA not found")
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
            raise HTTPException(400, "for_scope_uid not found on this amendment")
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
    return JSONResponse(
        {"id": enrollment_id, "enrollment_uid": enrollment_uid}, status_code=201
    )


@router.delete(
    "/soa/{soa_id}/amendment/{amendment_id}/enrollment/{enrollment_id}",
    status_code=204,
)
def delete_enrollment(soa_id: int, amendment_id: int, enrollment_id: int):
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


@router.post(
    "/soa/{soa_id}/amendment/{amendment_id}/governance-dates",
    status_code=201,
)
def add_governance_date(soa_id: int, amendment_id: int, body: GovernanceDateCreate):
    if not soa_exists(soa_id):
        raise HTTPException(404, "SOA not found")
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
    for scope_uid in body.scope_uids:
        cur.execute(
            "INSERT INTO governance_date_geographic_scope "
            "(soa_id,date_uid,scope_uid) VALUES (?,?,?)",
            (soa_id, date_uid, scope_uid),
        )
    conn.commit()
    conn.close()
    _record_gov_date_audit(
        soa_id,
        "create",
        date_id,
        after={"date_uid": date_uid, "name": body.name},
    )
    return JSONResponse({"id": date_id, "date_uid": date_uid}, status_code=201)


@router.delete(
    "/soa/{soa_id}/amendment/{amendment_id}/governance-date/{date_id}",
    status_code=204,
)
def delete_governance_date(soa_id: int, amendment_id: int, date_id: int):
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
