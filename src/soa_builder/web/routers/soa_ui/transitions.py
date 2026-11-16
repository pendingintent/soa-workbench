"""soa_ui: transitions (split from soa_ui.py)."""

from ._shared import (
    Form,
    HTMLResponse,
    HTTPException,
    Optional,
    Request,
    _connect,
    _record_visit_audit,
    router,
    soa_exists,
)


# UI endpoint for associating a Transition Start Rule with Visit/Encounter (visit.transitionStartRule)
@router.post(
    "/ui/soa/{soa_id}/set_visit_transition_start_rule", response_class=HTMLResponse
)
def ui_set_visit_transition_start_rule(
    request: Request,
    soa_id: int,
    visit_id: int = Form(...),
    visit_transition_start_rule_uid: str = Form(""),
):
    """Form handler for associating a Transition Start Rule with a Visit/Encounter"""
    if not soa_exists(soa_id):
        raise HTTPException(404, "SOA not found")

    new_uid = (visit_transition_start_rule_uid or "").strip() or None

    conn = _connect()
    cur = conn.cursor()
    cur.execute(
        "SELECT id,name,label,order_index,encounter_uid,description,transitionStartRule FROM visit WHERE id=? AND soa_id=?",
        (visit_id, soa_id),
    )
    row = cur.fetchone()
    if not row:
        conn.close()
        raise HTTPException(404, "Visit not found")

    before = {
        "id": row[0],
        "name": row[1],
        "label": row[2],
        "order_index": row[3],
        "encounter_uid": row[4],
        "description": row[5],
        "transitionStartRule": row[6],
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
        "UPDATE visit SET transitionStartRule=? WHERE id=? AND soa_id=?",
        (new_uid, visit_id, soa_id),
    )
    conn.commit()

    cur.execute(
        "SELECT id,name,label,order_index,encounter_uid,description,transitionStartRule FROM visit WHERE id=? AND soa_id=?",
        (
            visit_id,
            soa_id,
        ),
    )
    r = cur.fetchone()
    after = {
        "id": r[0],
        "name": r[1],
        "label": r[2],
        "order_index": r[3],
        "encounter_uid": r[4],
        "description": r[5],
        "transitionStartRule": r[6],
    }
    updated_fields = [
        f
        for f in ["transitionStartRule"]
        if (before.get(f) or None) != (after.get(f) or None)
    ]
    _record_visit_audit(
        soa_id,
        "update",
        visit_id,
        before=before,
        after={**after, "updated_fields": updated_fields},
    )
    conn.close()
    return HTMLResponse(
        f"<script>window.location='/ui/soa/{int(soa_id)}/edit';</script>"
    )


# UI endpoint for associating a Transition End Rule with Visit/Encounter (visit.transitionEndRule)
@router.post(
    "/ui/soa/{soa_id}/set_visit_transition_end_rule", response_class=HTMLResponse
)
def ui_set_transition_end_rule(
    request: Request,
    soa_id: int,
    visit_id: int = Form(...),
    visit_transition_end_rule_uid: str = Form(""),
):
    """Form Handler for associating a Transition End Rule with a Visit/Encounter"""
    if not soa_exists(soa_id):
        raise HTTPException(404, "SOA not found")

    new_uid = (visit_transition_end_rule_uid or "").strip() or None

    conn = _connect()
    cur = conn.cursor()
    cur.execute(
        "SELECT id,name,label,order_index,encounter_uid,description,transitionEndRule FROM visit WHERE id=? AND soa_id=?",
        (visit_id, soa_id),
    )
    row = cur.fetchone()
    if not row:
        conn.close()
        raise HTTPException(404, "Visit not found")

    before = {
        "id": row[0],
        "name": row[1],
        "label": row[2],
        "order_index": row[3],
        "encounter_uid": row[4],
        "description": row[5],
        "transitionEndRule": row[6],
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
        "UPDATE visit SET transitionEndRule=? WHERE id=? AND soa_id=?",
        (new_uid, visit_id, soa_id),
    )
    conn.commit()

    cur.execute(
        "SELECT id,name,label,order_index,encounter_uid,description,transitionEndRule FROM visit WHERE id=? AND soa_id=?",
        (
            visit_id,
            soa_id,
        ),
    )
    r = cur.fetchone()
    after = {
        "id": r[0],
        "name": r[1],
        "label": r[2],
        "order_index": r[3],
        "encounter_uid": r[4],
        "description": r[5],
        "transitionEndRule": r[6],
    }
    updated_fields = [
        f
        for f in ["transitionEndRule"]
        if (before.get(f) or None) != (after.get(f) or None)
    ]
    _record_visit_audit(
        soa_id,
        "update",
        visit_id,
        before=before,
        after={**after, "updated_fields": updated_fields},
    )
    conn.close()
    return HTMLResponse(
        f"<script>window.location='/ui/soa/{int(soa_id)}/edit';</script>"
    )


# UI endpoint for associating a Timing with Visit/Encounter (visit.scheduledAtId)
@router.post("/ui/soa/{soa_id}/set_timing", response_class=HTMLResponse)
def ui_set_timing(
    request: Request,
    soa_id: int,
    visit_id: int = Form(...),
    timing_id: str = Form(""),
):
    """Form handler for associating a Timing with a Visit/Encounter"""
    if not soa_exists(soa_id):
        raise HTTPException(404, "SOA not found")
    # Determing timing name
    raw_val = (timing_id or "").strip()
    parsed_timing: Optional[int] = None
    if raw_val:
        if raw_val.isdigit():
            parsed_timing = int(raw_val)
        else:
            raise HTTPException(400, "Invalid timing_id value")
    conn = _connect()
    cur = conn.cursor()
    cur.execute(
        "SELECT id,name,label,order_index,encounter_uid,description,scheduledAtId FROM visit WHERE id=? AND soa_id=?",
        (visit_id, soa_id),
    )
    row = cur.fetchone()
    if not row:
        conn.close()
        raise HTTPException(404, "Visit not found")
    before = {
        "id": row[0],
        "name": row[1],
        "label": row[2],
        "order_index": row[3],
        "encounter_uid": row[4],
        "description": row[5],
        "scheduledAtId": row[6],
    }
    if parsed_timing is not None:
        cur.execute(
            "SELECT 1 FROM timing WHERE id=? AND soa_id=?",
            (parsed_timing, soa_id),
        )
        if not cur.fetchone():
            conn.close()
            raise HTTPException(400, "Invalid timing_id for this SOA")
    cur.execute(
        "UPDATE visit SET scheduledAtId=? WHERE id=?",
        (parsed_timing, visit_id),
    )
    conn.commit()
    # Fetch after record audit
    cur.execute(
        "SELECT id,name,label,order_index,encounter_uid,description,scheduledAtId FROM visit WHERE id=? AND soa_id=?",
        (visit_id, soa_id),
    )
    r = cur.fetchone()
    after = {
        "id": r[0],
        "name": r[1],
        "label": r[2],
        "order_index": r[3],
        "encounter_uid": r[4],
        "description": r[5],
        "scheduledAtId": r[6],
    }
    updated_fields = [
        f
        for f in ["scheduledAtId"]
        if (before.get(f) or None) != (after.get(f) or None)
    ]
    _record_visit_audit(
        soa_id,
        "update",
        visit_id,
        before=before,
        after={**after, "updated_fields": updated_fields},
    )
    conn.close()
    return HTMLResponse(
        f"<script>window.location='/ui/soa/{int(soa_id)}/edit';</script>"
    )
