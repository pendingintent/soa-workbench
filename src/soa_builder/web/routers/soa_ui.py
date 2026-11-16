"""HTMX UI for editing a SoA (moved from app.py)."""

from ..services import concepts as concepts_service
from ..audit import _record_element_audit
from fastapi import APIRouter
from ..initialize_database import _connect
from ..routers import activities as activities_router
from ..routers import elements as elements_router
from ..routers import instances as instances_router
from ..routers.organizations import _get_countries_options
from ..routers.organizations import _get_org_type_options
from ..routers.organizations import _list_organizations
from ..routers.roles import _get_role_type_options
from ..routers.roles import _list_roles
from ..schemas import CellCreate
from ..schemas import ElementCreate
from ..schemas import ElementUpdate
from ..utils import find_visit_cell
from ..utils import get_cdisc_api_key as _get_cdisc_api_key
from ..utils import get_concepts_override as _get_concepts_override
from ..utils import get_encounter_id
from ..utils import get_epoch_uid
from ..utils import get_next_code_uid as _get_next_code_uid
from ..utils import get_next_concept_uid as _get_next_concept_uid
from ..utils import get_schedule_timeline
from ..utils import get_scheduled_activity_instance
from ..utils import iso_duration_to_days
from ..utils import load_epoch_type_map
from ..utils import load_epoch_type_options
from ..utils import soa_exists
from ..utils import table_has_columns as _table_has_columns
from datetime import datetime
from datetime import timezone
from fastapi import BackgroundTasks
from fastapi import Form
from fastapi import HTTPException
from fastapi import Request
from fastapi.responses import HTMLResponse
from typing import List
from typing import Optional
import html as _html
import os
from ..audit import _record_activity_audit
from ..audit import _record_reorder_audit
from ..audit import _record_visit_audit
from ..routers.soa_core import delete_activity
from ..routers.soa_core import set_cell
from ..services.concepts import _cleanup_orphaned_concept_rows
from ..services.concepts import _concept_cache
from ..services.concepts import _enrich_biomedical_concept_bg
from ..services.concepts import _enrich_code_bg
from ..services.concepts import _get_activity_concepts
from ..services.concepts import _get_activity_surrogates
from ..services.concepts import _get_bc_categories_for_cell
from ..services.concepts import _get_concept_groups_for_cell
from ..services.concepts import _upsert_biomedical_concept
from ..services.matrix import _fetch_arms_for_edit
from ..services.matrix import _fetch_matrix
from ..services.matrix import _list_study_cells
from ..services.matrix import _render_activity_td
from ..services.matrix import _render_cell_td
from ..services.matrix import _render_encounter_th
from ..services.matrix import _render_epoch_th
from ..templating import templates

router = APIRouter()


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
    conn = _connect()
    cur = conn.cursor()
    cur.execute(
        "SELECT status,id FROM matrix_cells WHERE soa_id=? AND instance_id=? AND activity_id=?",
        (soa_id, instance_id, activity_id),
    )
    row = cur.fetchone()
    if row:
        cur.execute("DELETE FROM matrix_cells WHERE id=?", (row[1],))
        conn.commit()
        conn.close()
        return HTMLResponse(_render_cell_td(soa_id, instance_id, activity_id, "", None))
    else:
        cur.execute(
            "INSERT INTO matrix_cells (soa_id, instance_id, activity_id, status) VALUES (?,?,?,?)",
            (soa_id, instance_id, activity_id, "X"),
        )
        conn.commit()
        conn.close()
        return HTMLResponse(
            _render_cell_td(soa_id, instance_id, activity_id, "X", None)
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
            return HTMLResponse(
                "<script>alert('study_id already exists');window.location='/'</script>"
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
            return HTMLResponse(
                "<script>alert('study_id already exists');window.location='/ui/soa/%d/edit';</script>"
                % soa_id
            )
    if not current_study_id and not new_study_id:
        conn.close()
        return HTMLResponse(
            "<script>alert('study_id is required');window.location='/ui/soa/%d/edit';</script>"
            % soa_id
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
    from ..routers.study_titles import (
        _get_title_type_options,
        _list_titles,
    )
    from ..routers.study_identifiers import (
        _list_identifiers as _list_study_identifiers,
        _list_orgs as _list_identifier_orgs,
    )

    study_titles = _list_titles(soa_id)
    title_type_options = _get_title_type_options()
    study_identifiers = _list_study_identifiers(soa_id)
    identifier_orgs = _list_identifier_orgs(soa_id)
    # Load Protocol Terminology (C174222) options from CDISC Library
    from ..utils import get_protocol_ct_codelist_map as _get_protocol_ct_codelist_map

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
    from ..utils import get_ddf_ct_codelist_map as _get_ddf_ct_codelist_map

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
    except Exception:
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
        elements_router.create_element(soa_id, payload)
    except Exception:
        pass

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
    except Exception:
        # Let redirect proceed; detailed errors will appear in API logs
        pass

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
        cell_html = (
            f'<td hx-post="/ui/soa/{soa_id}/toggle_cell" '
            f'hx-vals=\'{{"visit_id": {int(visit_id)}, "activity_id": {activity_id}}}\' '
            f'hx-swap="outerHTML" class="cell">{current}</td>'
        )
    return HTMLResponse(cell_html)


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
    sup_val = _html.escape(row[0] or "", quote=True)
    html = (
        f'<td class="cell cell-editing" style="background:#fffde7;min-width:70px;">'
        f"X"
        f'<form style="display:inline;"'
        f' hx-post="/ui/soa/{soa_id}/cell_superscript/{instance_id}/{activity_id}"'
        f' hx-swap="outerHTML" hx-target="closest td">'
        f'<input name="superscript" value="{sup_val}" size="5"'
        f' style="width:45px;font-size:0.8em;" autofocus />'
        f'<button type="submit" onclick="event.stopPropagation()">&#10003;</button>'
        f"</form>"
        f'<span hx-get="/ui/soa/{soa_id}/cell_superscript_view/{instance_id}/{activity_id}"'
        f' hx-swap="outerHTML" hx-target="closest td"'
        f' onclick="event.stopPropagation()" style="cursor:pointer;">&#10005;</span>'
        f"</td>"
    )
    return HTMLResponse(html)


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
    sup_val = _html.escape(row[0] or "", quote=True)
    html = (
        f'<td class="cell-editing" style="background:#fffde7;">'
        f'<form style="display:inline;"'
        f' hx-post="/ui/soa/{soa_id}/activity_superscript/{activity_id}"'
        f' hx-swap="outerHTML" hx-target="closest td">'
        f'<input name="superscript" value="{sup_val}" size="5"'
        f' style="width:45px;font-size:0.8em;" autofocus />'
        f'<button type="submit" onclick="event.stopPropagation()">&#10003;</button>'
        f"</form>"
        f'<span hx-get="/ui/soa/{soa_id}/activity_superscript_view/{activity_id}"'
        f' hx-swap="outerHTML" hx-target="closest td"'
        f' onclick="event.stopPropagation()" style="cursor:pointer;">&#10005;</span>'
        f"</td>"
    )
    return HTMLResponse(html)


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
    sup_val = _html.escape(row[0] or "", quote=True)
    html = (
        f'<th class="cell-editing" style="background:#fffde7;">'
        f'<form style="display:inline;"'
        f' hx-post="/ui/soa/{soa_id}/encounter_superscript/{encounter_id}"'
        f' hx-swap="outerHTML" hx-target="closest th">'
        f'<input name="superscript" value="{sup_val}" size="5"'
        f' style="width:45px;font-size:0.8em;" autofocus />'
        f'<button type="submit" onclick="event.stopPropagation()">&#10003;</button>'
        f"</form>"
        f'<span hx-get="/ui/soa/{soa_id}/encounter_superscript_view/{encounter_id}"'
        f' hx-swap="outerHTML" hx-target="closest th"'
        f' onclick="event.stopPropagation()" style="cursor:pointer;">&#10005;</span>'
        f"</th>"
    )
    return HTMLResponse(html)


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
    sup_val = _html.escape(row[0] or "", quote=True)
    colspan_safe = _html.escape(str(colspan), quote=True)
    html = (
        f'<th class="cell-editing" style="background:#fffde7;" colspan="{colspan_safe}">'
        f'<form style="display:inline;"'
        f' hx-post="/ui/soa/{soa_id}/epoch_superscript/{epoch_id}"'
        f' hx-swap="outerHTML" hx-target="closest th">'
        f'<input type="hidden" name="colspan" value="{colspan_safe}" />'
        f'<input name="superscript" value="{sup_val}" size="5"'
        f' style="width:45px;font-size:0.8em;" autofocus />'
        f'<button type="submit" onclick="event.stopPropagation()">&#10003;</button>'
        f"</form>"
        f'<span hx-get="/ui/soa/{soa_id}/epoch_superscript_view/{epoch_id}?colspan={colspan_safe}"'
        f' hx-swap="outerHTML" hx-target="closest th"'
        f' onclick="event.stopPropagation()" style="cursor:pointer;">&#10005;</span>'
        f"</th>"
    )
    return HTMLResponse(html)


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


_SOA_CASCADE_TABLES = [
    "matrix_cells",
    "activity_concept",
    "activity_concept_dss",
    "activity_surrogate",
    "activity_audit",
    "activity",
    "alias_code",
    "amendment_geographic_scope",
    "amendment_geographic_scope_audit",
    "amendment_governance_date",
    "amendment_governance_date_audit",
    "amendment_subject_enrollment",
    "amendment_subject_enrollment_audit",
    "arm_audit",
    "arm",
    "bcp_response_code",
    "biomedical_concept_property",
    "biomedical_concept_surrogate_audit",
    "biomedical_concept_surrogate",
    "biomedical_concept_audit",
    "biomedical_concept",
    "code_association",
    "code",
    "condition_assignment",
    "decision_instances",
    "document_content_reference_audit",
    "document_content_reference",
    "element_audit",
    "element",
    "endpoint_audit",
    "endpoint",
    "epoch_audit",
    "epoch",
    "footnote_audit",
    "footnote",
    "governance_date_geographic_scope",
    "instance_audit",
    "instances",
    "objective_audit",
    "objective",
    "reorder_audit",
    "rollback_audit",
    "schedule_timelines_audit",
    "schedule_timelines",
    "soa_freeze",
    "study_amendment_impact_audit",
    "study_amendment_impact",
    "study_amendment_reason_audit",
    "study_amendment_reason",
    "study_amendment_audit",
    "study_amendment",
    "study_cell_audit",
    "study_cell",
    "study_change_audit",
    "study_change",
    "timing_audit",
    "timing",
    "transition_rule_audit",
    "transition_rule",
    "visit_audit",
    "visit",
]


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
