"""soa_ui: editor (split from soa_ui.py)."""

from ...errors import REMOTE_PAYLOAD_ERRORS
from ._shared import (
    Form,
    HTMLResponse,
    HTTPException,
    Optional,
    Request,
    _SOA_CASCADE_TABLES,
    _concept_cache,
    _connect,
    _fetch_arms_for_edit,
    _fetch_matrix,
    _get_cdisc_api_key,
    _get_concepts_override,
    _get_countries_options,
    _get_next_code_uid,
    _get_org_type_options,
    _get_role_type_options,
    _list_organizations,
    _list_roles,
    _list_study_cells,
    _table_has_columns,
    concepts_service,
    datetime,
    get_encounter_id,
    get_epoch_uid,
    get_schedule_timeline,
    get_scheduled_activity_instance,
    instances_router,
    iso_duration_to_days,
    load_epoch_type_map,
    load_epoch_type_options,
    os,
    router,
    soa_exists,
    templates,
    timezone,
)


# API endpoint for displaying the index page
@router.get("/", response_class=HTMLResponse)
def ui_index(request: Request):
    """Render home page for the SoA Workbench."""
    conn = _connect()
    cur = conn.cursor()
    cur.execute(
        "SELECT id,name,created_at,study_id,study_label,study_description FROM soa ORDER BY id DESC"
    )
    rows = cur.fetchall()
    conn.close()
    return templates.TemplateResponse(
        request,
        "index.html",
        {
            "soas": [
                {
                    "id": r[0],
                    "name": r[1],
                    "created_at": r[2],
                    "study_id": r[3],
                    "study_label": r[4],
                    "study_description": r[5],
                }
                for r in rows
            ],
        },
    )


# API endpoint for displaying the help page
@router.get("/ui/help", response_class=HTMLResponse)
def ui_help(request: Request):
    """Render the help page for the SOA Workbench."""
    return templates.TemplateResponse(
        request,
        "help.html",
        {},
    )


# UI endpoint for the USDM logical model explorer
@router.get("/ui/usdm-model-explorer", response_class=HTMLResponse)
def ui_usdm_model_explorer(request: Request):
    """Render the USDM logical model explorer page."""
    return templates.TemplateResponse(
        request,
        "usdm_model_explorer.html",
        {},
    )


# UI endpoint for creating a new Study/SOA
@router.post("/ui/soa/create", response_class=HTMLResponse)
def ui_create_soa(
    request: Request,
    name: str = Form(...),
    study_id: Optional[str] = Form(None),
    study_label: Optional[str] = Form(None),
    study_description: Optional[str] = Form(None),
):
    """Create a new SoA."""
    conn = _connect()
    cur = conn.cursor()
    # Uniqueness check
    if study_id and study_id.strip():
        cur.execute("SELECT 1 FROM soa WHERE study_id=?", (study_id.strip(),))
        if cur.fetchone():
            conn.close()
            return templates.TemplateResponse(
                request,
                "_alert_redirect.html",
                {"message": "study_id already exists", "url": "/"},
            )
    cur.execute(
        "INSERT INTO soa (name, created_at, study_id, study_label, study_description) VALUES (?,?,?,?,?)",
        (
            name,
            datetime.now(timezone.utc).isoformat(),
            (study_id or "").strip() or None,
            (study_label or "").strip() or None,
            (study_description or "").strip() or None,
        ),
    )
    sid = cur.lastrowid
    conn.commit()
    conn.close()
    return HTMLResponse(f"<script>window.location='/ui/soa/{sid}/edit';</script>")


# UI endpoint for updating the metadata of a Study/SOA
@router.post("/ui/soa/{soa_id}/update_meta", response_class=HTMLResponse)
def ui_update_meta(
    request: Request,
    soa_id: int,
    name: Optional[str] = Form(None),
    study_id: Optional[str] = Form(None),
    study_label: Optional[str] = Form(None),
    study_description: Optional[str] = Form(None),
):
    """Update the metadata for an SoA."""
    if not soa_exists(soa_id):
        raise HTTPException(404, "SOA not found")
    conn = _connect()
    cur = conn.cursor()
    cur.execute("SELECT study_id FROM soa WHERE id=?", (soa_id,))
    row = cur.fetchone()
    current_study_id = row[0] if row else None
    proposed = (study_id or "").strip()
    if proposed == "" and current_study_id:
        new_study_id = current_study_id  # preserve existing
    else:
        new_study_id = proposed or None
    if new_study_id:
        cur.execute(
            "SELECT id FROM soa WHERE study_id=? AND id<>?", (new_study_id, soa_id)
        )
        if cur.fetchone():
            conn.close()
            return templates.TemplateResponse(
                request,
                "_alert_redirect.html",
                {"message": "study_id already exists", "url": f"/ui/soa/{soa_id}/edit"},
            )
    if not current_study_id and not new_study_id:
        conn.close()
        return templates.TemplateResponse(
            request,
            "_alert_redirect.html",
            {"message": "study_id is required", "url": f"/ui/soa/{soa_id}/edit"},
        )
    cur.execute(
        "UPDATE soa SET name=?, study_id=?, study_label=?,"
        " study_description=? WHERE id=?",
        (
            (name or "").strip() or None,
            new_study_id,
            (study_label or "").strip() or None,
            (study_description or "").strip() or None,
            soa_id,
        ),
    )
    conn.commit()
    conn.close()
    if request.headers.get("HX-Request") == "true":
        return HTMLResponse("", headers={"HX-Redirect": f"/ui/soa/{soa_id}/edit"})
    return HTMLResponse(
        f"<script>window.location='/ui/soa/{int(soa_id)}/edit';</script>"
    )


# UI endpoint for rendering SOA edit page
@router.get("/ui/soa/{soa_id}/edit", response_class=HTMLResponse)
def ui_edit(request: Request, soa_id: int):
    """Render edit HTML page for an SoA."""
    if not soa_exists(soa_id):
        raise HTTPException(404, "SOA not found")
    instances, activities, cells = _fetch_matrix(soa_id)
    # Epochs list
    conn_ep = _connect()
    cur_ep = conn_ep.cursor()
    cur_ep.execute(
        "SELECT id,name,order_index,epoch_seq,epoch_label,epoch_description FROM epoch WHERE soa_id=? ORDER BY order_index",
        (soa_id,),
    )
    epochs = [
        dict(
            id=r[0],
            name=r[1],
            order_index=r[2],
            epoch_seq=r[3],
            epoch_label=r[4],
            epoch_description=r[5],
        )
        for r in cur_ep.fetchall()
    ]
    conn_ep.close()
    # Elements list
    conn_el = _connect()
    cur_el = conn_el.cursor()
    cur_el.execute(
        "SELECT id,name,label,description,testrl,teenrl,order_index,created_at,element_id FROM element WHERE soa_id=? ORDER BY order_index",
        (soa_id,),
    )
    elements = [
        dict(
            id=r[0],
            name=r[1],
            label=r[2],
            description=r[3],
            testrl=r[4],
            teenrl=r[5],
            order_index=r[6],
            created_at=r[7],
            element_id=r[8],
        )
        for r in cur_el.fetchall()
    ]
    conn_el.close()
    # No pagination: use all activities
    activities_page = activities
    # Build cell lookup
    cell_map = {(c["instance_id"], c["activity_id"]): c["status"] for c in cells}
    superscript_map = {
        (c["instance_id"], c["activity_id"]): c.get("superscript") for c in cells
    }
    concepts = concepts_service.fetch_biomedical_concepts()
    activity_ids = [a["id"] for a in activities_page]
    activity_concepts = {}
    if activity_ids:
        conn = _connect()
        cur = conn.cursor()
        placeholders = ",".join("?" for _ in activity_ids)
        if _table_has_columns(cur, "activity_concept", ("soa_id",)):
            cur.execute(
                f"SELECT activity_id, concept_code, concept_title FROM activity_concept WHERE soa_id=? AND activity_id IN ({placeholders})",
                [soa_id] + activity_ids,
            )
        else:
            cur.execute(
                f"SELECT activity_id, concept_code, concept_title FROM activity_concept WHERE activity_id IN ({placeholders})",
                activity_ids,
            )
        for aid, code, title in cur.fetchall():
            activity_concepts.setdefault(aid, []).append({"code": code, "title": title})
        conn.close()
    # Fetch per-activity surrogate mappings for the matrix view
    conn = _connect()
    cur = conn.cursor()
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
        aid, sur_id, sur_uid, sur_name, sur_label = row
        activity_surrogates.setdefault(aid, []).append(
            {
                "id": sur_id,
                "surrogate_uid": sur_uid,
                "name": sur_name,
                "label": sur_label,
            }
        )
    conn.close()
    concepts_diag = {
        "count": len(_concept_cache.get("data") or []),
        "last_status": _concept_cache.get("last_status"),
        "last_error": _concept_cache.get("last_error"),
        "api_key_present": bool(_get_cdisc_api_key()),
        "override_present": bool(_get_concepts_override()),
        "skip_remote": os.environ.get("CDISC_SKIP_REMOTE") == "1",
    }
    fetched_at = _concept_cache.get("fetched_at")
    last_fetch_iso = None
    last_fetch_relative = None
    if fetched_at:
        dt = datetime.fromtimestamp(fetched_at, tz=timezone.utc)
        last_fetch_iso = dt.isoformat()
        # Simple relative string (seconds/minutes/hours)
        delta = datetime.now(timezone.utc) - dt
        secs = int(delta.total_seconds())
        if secs < 60:
            last_fetch_relative = f"{secs}s ago"
        elif secs < 3600:
            last_fetch_relative = f"{secs // 60}m ago"
        else:
            last_fetch_relative = f"{secs // 3600}h ago"
    # Study metadata for edit form
    conn_meta = _connect()
    cur_meta = conn_meta.cursor()
    cur_meta.execute(
        "SELECT study_id, study_label, study_description, name, created_at FROM soa WHERE id=?",
        (soa_id,),
    )
    meta_row = cur_meta.fetchone()
    conn_meta.close()
    if meta_row:
        study_id_val, study_label_val, study_desc_val, soa_name_val, created_at_val = (
            meta_row
        )
    else:
        study_id_val, study_label_val, study_desc_val, soa_name_val, created_at_val = (
            None,
            None,
            None,
            f"SOA {soa_id}",
            None,
        )
    study_meta = {
        "study_id": study_id_val,
        "study_label": study_label_val,
        "study_description": study_desc_val,
        "soa_name": soa_name_val,
        "created_at": created_at_val,
    }
    # Compute next Code_N using a fresh cursor
    conn_codes = _connect()
    cur_codes = conn_codes.cursor()
    # Precompute next Code_N if needed for UI defaults (currently not displayed)
    _ = _get_next_code_uid(cur_codes, soa_id)
    conn_codes.close()
    # Study Titles for the metadata card
    from ...routers.study_titles import (
        _get_title_type_options,
        _list_titles,
    )
    from ...routers.study_identifiers import (
        _list_identifiers as _list_study_identifiers,
        _list_orgs as _list_identifier_orgs,
    )

    study_titles = _list_titles(soa_id)
    title_type_options = _get_title_type_options()
    study_identifiers = _list_study_identifiers(soa_id)
    identifier_orgs = _list_identifier_orgs(soa_id)
    # Load Protocol Terminology (C174222) options from CDISC Library
    from ...utils import get_protocol_ct_codelist_map as _get_protocol_ct_codelist_map

    c174222_map = _get_protocol_ct_codelist_map("C174222")
    protocol_terminology_C174222 = [
        {"cdisc_submission_value": sv}
        for sv in sorted({v for v in c174222_map.values() if v})
    ]
    # Build mapping code_uid -> submission value (Arm Type C174222)
    conn_map = _connect()
    cur_map = conn_map.cursor()
    cur_map.execute(
        "SELECT code_uid, code FROM code_association "
        "WHERE soa_id=? AND codelist_code='C174222'",
        (soa_id,),
    )
    code_to_submission = {
        row[0]: c174222_map.get(row[1], "") for row in cur_map.fetchall()
    }
    conn_map.close()
    submission_values = {
        opt.get("cdisc_submission_value") or "" for opt in protocol_terminology_C174222
    }

    # DDF Terminology options for Arm Data Origin Type (C188727) from CDISC Library
    from ...utils import get_ddf_ct_codelist_map as _get_ddf_ct_codelist_map

    c188727_map = _get_ddf_ct_codelist_map("C188727")
    ddf_terminology_C188727 = [
        {"cdisc_submission_value": sv}
        for sv in sorted({v for v in c188727_map.values() if v})
    ]
    # Build mapping code_uid -> submission value (Arm dataOriginType C188727)
    conn_ddf_map = _connect()
    cur_ddf_map = conn_ddf_map.cursor()
    cur_ddf_map.execute(
        "SELECT code_uid, code FROM code_association "
        "WHERE soa_id=? AND codelist_code='C188727'",
        (soa_id,),
    )
    ddf_code_to_submission = {
        row[0]: c188727_map.get(row[1], "") for row in cur_ddf_map.fetchall()
    }
    conn_ddf_map.close()
    ddf_submission_values = {
        ddf_opt.get("cdisc_submission_value") or ""
        for ddf_opt in ddf_terminology_C188727
    }

    base_arms = _fetch_arms_for_edit(soa_id)
    arms_enriched = []
    for a in base_arms:
        arm_type = a.get("type")
        type_display = code_to_submission.get(arm_type)
        data_origin_type = a.get("data_origin_type")
        data_origin_type_display = ddf_code_to_submission.get(data_origin_type)
        if type_display is None and arm_type:
            type_display = arm_type if arm_type in submission_values else None
        if data_origin_type_display is None and data_origin_type:
            data_origin_type_display = (
                data_origin_type if data_origin_type in ddf_submission_values else None
            )
        arms_enriched.append(
            {
                **a,
                "type_display": type_display,
                "data_origin_type_display": data_origin_type_display,
            }
        )
    # Enrich epochs using API-only map: code -> submissionValue
    # Resolve stored epoch.type (code_uid) to terminology code via code table, then map to submissionValue.
    code_map: dict[int, str] = {}
    conn_em = _connect()
    cur_em = conn_em.cursor()
    cur_em.execute(
        "SELECT e.id, c.code FROM epoch e LEFT JOIN code_association c ON c.code_uid = e.type AND c.soa_id = e.soa_id WHERE e.soa_id=?",
        (soa_id,),
    )
    for eid, code in cur_em.fetchall():
        if eid is not None and code:
            code_map[eid] = code
    conn_em.close()
    try:
        code_to_submission = load_epoch_type_map(force=False) or {}
    except REMOTE_PAYLOAD_ERRORS:
        code_to_submission = {}
    epochs = [
        {
            **e,
            "epoch_type_submission_value": code_to_submission.get(
                code_map.get(e.get("id"), ""), None
            ),
        }
        for e in epochs
    ]

    # Epoch Type options (C99079) must come from CDISC API only
    epoch_type_options = load_epoch_type_options(force=False) or []
    study_cells = _list_study_cells(soa_id)

    # Transition Rules list
    conn_tr = _connect()
    cur_tr = conn_tr.cursor()
    cur_tr.execute(
        "SELECT transition_rule_uid,name,label,description,text,order_index,created_at FROM transition_rule WHERE soa_id=? ORDER BY order_index",
        (soa_id,),
    )
    transition_rules = [
        dict(
            transition_rule_uid=r[0],
            name=r[1],
            label=r[2],
            description=r[3],
            text=r[4],
            order_index=r[5],
            created_at=r[6],
        )
        for r in cur_tr.fetchall()
    ]
    conn_tr.close()

    # Load Timings for dropdown
    conn_tm = _connect()
    cur_tm = conn_tm.cursor()
    cur_tm.execute(
        "SELECT id,name FROM timing WHERE soa_id=? ORDER BY id",
        (soa_id,),
    )
    timings = [{"id": r[0], "name": r[1]} for r in cur_tm.fetchall()]
    conn_tm.close()

    # Load instances
    conn_inst = _connect()
    cur_inst = conn_inst.cursor()
    cur_inst.execute(
        """
        SELECT i.id,i.name,i.instance_uid,i.label,i.member_of_timeline,st.name AS timeline_name,st.label AS timeline_label,
        v.id AS encounter_id,v.name AS encounter_name,v.label AS encounter_label,v.superscript AS encounter_superscript,
        e.id AS epoch_id,e.name AS epoch_name,e.epoch_label as epoch_label,e.superscript AS epoch_superscript,
        tm.window_label,tm.label AS timing_label,tm.name AS timing_name,tm.value AS study_day
        FROM instances i
        LEFT JOIN schedule_timelines st ON st.schedule_timeline_uid = i.member_of_timeline AND st.soa_id = i.soa_id
        LEFT JOIN visit v ON v.encounter_uid = i.encounter_uid AND v.soa_id = i.soa_id
        LEFT JOIN epoch e ON e.epoch_uid = i.epoch_uid AND e.soa_id = i.soa_id
        LEFT JOIN timing tm ON tm.id = v.scheduledAtId AND tm.soa_id = v.soa_id
        WHERE i.soa_id=?
        ORDER BY COALESCE(i.member_of_timeline, 'zzz'), i.order_index, i.id
            """,
        (soa_id,),
    )
    instances = [
        {
            "id": r[0],
            "name": r[1],
            "instance_uid": r[2],
            "label": r[3],
            "member_of_timeline": r[4],
            "timeline_name": r[5],
            "timeline_label": r[6],
            "encounter_id": r[7],
            "encounter_name": r[8],
            "encounter_label": r[9],
            "encounter_superscript": r[10],
            "epoch_id": r[11],
            "epoch_name": r[12],
            "epoch_label": r[13],
            "epoch_superscript": r[14],
            "window_label": r[15],
            "timing_label": r[16],
            "timing_name": r[17],
            "study_day": iso_duration_to_days(r[18]),
        }
        for r in cur_inst.fetchall()
    ]
    cur_inst.close()

    # Load Schedule Timelines for timeline selector
    conn_tl = _connect()
    cur_tl = conn_tl.cursor()
    cur_tl.execute(
        """
        SELECT schedule_timeline_uid,name,main_timeline
        FROM schedule_timelines
        WHERE soa_id=?
        ORDER BY main_timeline DESC, name
        """,
        (soa_id,),
    )
    timelines = [
        {
            "schedule_timeline_uid": r[0],
            "name": r[1],
            "main_timeline": bool(r[2]),
        }
        for r in cur_tl.fetchall()
    ]
    conn_tl.close()

    # Group instances by timeline
    instances_by_timeline = {}
    for inst in instances:
        timeline_key = inst.get("member_of_timeline") or "unassigned"
        if timeline_key not in instances_by_timeline:
            instances_by_timeline[timeline_key] = []
        instances_by_timeline[timeline_key].append(inst)

    # Activities per timeline: an activity is shown in timeline T's matrix
    # if any matrix_cells row connects it to an instance whose
    # member_of_timeline == T. The instance->timeline link (set on the
    # study_timing page) is the authoritative criterion.
    instance_timeline = {
        inst["id"]: (inst.get("member_of_timeline") or "unassigned")
        for inst in instances
    }
    activity_ids_by_timeline: dict = {tl: set() for tl in instances_by_timeline.keys()}
    for c in cells:
        tl = instance_timeline.get(c["instance_id"])
        if tl is None or tl not in activity_ids_by_timeline:
            continue
        activity_ids_by_timeline[tl].add(c["activity_id"])
    scheduled_activity_ids = (
        set().union(*activity_ids_by_timeline.values())
        if activity_ids_by_timeline
        else set()
    )
    unscheduled_activity_ids = {
        a["id"] for a in activities_page if a["id"] not in scheduled_activity_ids
    }
    for tl in activity_ids_by_timeline:
        activity_ids_by_timeline[tl] |= unscheduled_activity_ids
    activities_by_timeline: dict = {
        tl: [a for a in activities_page if a["id"] in ids]
        for tl, ids in activity_ids_by_timeline.items()
    }

    # Determine default timeline (main_timeline or first available)
    default_timeline = None
    for tl in timelines:
        if tl["main_timeline"]:
            default_timeline = tl["schedule_timeline_uid"]
            break
    if not default_timeline and timelines:
        default_timeline = timelines[0]["schedule_timeline_uid"]

    # If no default timeline found or no timelines exist, check if there are unassigned instances
    if not default_timeline and "unassigned" in instances_by_timeline:
        default_timeline = "unassigned"

    # Load footnotes for display below matrix
    conn_fn = _connect()
    cur_fn = conn_fn.cursor()
    cur_fn.execute(
        "SELECT id,soa_id,footnote_uid,name,label,description,text,dictionary_uid FROM footnote WHERE soa_id=? ORDER BY id",
        (soa_id,),
    )
    footnotes = [
        dict(
            id=r[0],
            soa_id=r[1],
            footnote_uid=r[2],
            name=r[3],
            label=r[4],
            description=r[5],
            text=r[6],
            dictionary_uid=r[7],
        )
        for r in cur_fn.fetchall()
    ]
    conn_fn.close()

    instances_crud = instances_router.list_instances(soa_id)
    encounter_options = get_encounter_id(soa_id)
    epoch_options = get_epoch_uid(soa_id)
    schedule_timelines_options = get_schedule_timeline(soa_id)
    instance_options = get_scheduled_activity_instance(soa_id)

    return templates.TemplateResponse(
        request,
        "edit.html",
        {
            "soa_id": soa_id,
            "epochs": epochs,
            "instances": instances,
            "instances_crud": instances_crud,
            "encounter_options": encounter_options,
            "epoch_options": epoch_options,
            "schedule_timelines_options": schedule_timelines_options,
            "instance_options": instance_options,
            "activities": activities_page,
            "elements": elements,
            "arms": arms_enriched,
            "cell_map": cell_map,
            "concepts": concepts,
            "activity_concepts": activity_concepts,
            "activity_surrogates": activity_surrogates,
            "concepts_empty": len(concepts) == 0,
            "concepts_diag": concepts_diag,
            "concepts_last_fetch_iso": last_fetch_iso,
            "concepts_last_fetch_relative": last_fetch_relative,
            **study_meta,
            "protocol_terminology_C174222": protocol_terminology_C174222,
            "ddf_terminology_C188727": ddf_terminology_C188727,
            # Epoch Type options (C99079)
            "epoch_type_options": epoch_type_options,
            # Study Cells
            "study_cells": study_cells,
            "transition_rules": transition_rules,
            "timings": timings,
            "timelines": timelines,
            "instances_by_timeline": instances_by_timeline,
            "activities_by_timeline": activities_by_timeline,
            "default_timeline": default_timeline,
            "footnotes": footnotes,
            "superscript_map": superscript_map,
            "study_titles": study_titles,
            "title_type_options": title_type_options,
            "study_identifiers": study_identifiers,
            "orgs": identifier_orgs,
            "organizations": _list_organizations(soa_id),
            "org_type_options": _get_org_type_options(),
            "countries_options": _get_countries_options(),
            "roles": _list_roles(soa_id),
            "role_type_options": _get_role_type_options(),
        },
    )


@router.post("/ui/soa/{soa_id}/delete", response_class=HTMLResponse)
def ui_delete_soa(
    request: Request,
    soa_id: int,
    confirm_study_id: str = Form(...),
):
    """Permanently delete a study and all its related records."""
    conn = _connect()
    cur = conn.cursor()
    cur.execute("SELECT study_id, name FROM soa WHERE id=?", (soa_id,))
    row = cur.fetchone()
    conn.close()

    if not row:
        raise HTTPException(404, "SOA not found")

    study_id_db, name_db = row[0], row[1]
    # Studies without a study_id (e.g. imported bundles) confirm by name
    if study_id_db is None:
        if confirm_study_id != name_db:
            raise HTTPException(
                400,
                f"Study name '{confirm_study_id}' does not match.",
            )
    elif study_id_db != confirm_study_id:
        raise HTTPException(
            400,
            f"Study ID '{confirm_study_id}' does not match.",
        )

    conn = _connect()
    cur = conn.cursor()
    for table in _SOA_CASCADE_TABLES:
        cur.execute(f"DELETE FROM {table} WHERE soa_id=?", (soa_id,))
    cur.execute("DELETE FROM soa WHERE id=?", (soa_id,))
    conn.commit()
    conn.close()

    return HTMLResponse("<script>window.location='/';</script>")
