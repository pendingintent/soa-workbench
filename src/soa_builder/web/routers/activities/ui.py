"""activities: ui (split from activities.py)."""

from ...errors import REMOTE_PAYLOAD_ERRORS
from ._shared import (
    ActivityCreate,
    Form,
    HTMLResponse,
    HTTPException,
    RedirectResponse,
    Request,
    _connect,
    _get_cdisc_api_key,
    _get_cdisc_crf_api_key,
    _get_cdisc_crf_subscription_key,
    _html,
    _record_activity_audit,
    _table_has_columns,
    json,
    os,
    soa_exists,
    templates,
    ui_router,
)
from .helpers import _reindex_activities, _render_crf_cell, _render_dss_cell
from .api import add_activity


@ui_router.get("/ui/soa/{soa_id}/activities", response_class=HTMLResponse)
def ui_list_activities(request: Request, soa_id: int):
    if not soa_exists(soa_id):
        raise HTTPException(404, "SOA not found")

    conn = _connect()
    cur = conn.cursor()
    cur.execute(
        "SELECT id,name,order_index,activity_uid,label,description FROM activity WHERE soa_id=? ORDER BY order_index",
        (soa_id,),
    )
    activities = [
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

    # Fetch activity concepts for all activities in this SOA
    activity_concepts: dict = {}
    has_group = _table_has_columns(cur, "activity_concept", ("concept_group_uid",))
    has_category = _table_has_columns(cur, "activity_concept", ("bc_category_name",))
    if _table_has_columns(cur, "activity_concept", ("soa_id",)):
        if has_group and has_category:
            cur.execute(
                "SELECT ac.activity_id, ac.concept_code, ac.concept_title, "
                "ac.concept_group_uid, cg.name, ac.bc_category_name "
                "FROM activity_concept ac "
                "LEFT JOIN concept_group cg "
                "ON cg.concept_group_uid=ac.concept_group_uid "
                "WHERE ac.soa_id=? "
                "ORDER BY ac.bc_category_name NULLS LAST, "
                "ac.concept_group_uid NULLS LAST, ac.id",
                (soa_id,),
            )
        elif has_group:
            cur.execute(
                "SELECT ac.activity_id, ac.concept_code, ac.concept_title, "
                "ac.concept_group_uid, cg.name, NULL "
                "FROM activity_concept ac "
                "LEFT JOIN concept_group cg "
                "ON cg.concept_group_uid=ac.concept_group_uid "
                "WHERE ac.soa_id=? "
                "ORDER BY ac.concept_group_uid NULLS LAST, ac.id",
                (soa_id,),
            )
        else:
            cur.execute(
                "SELECT activity_id, concept_code, concept_title, NULL, NULL, NULL "
                "FROM activity_concept WHERE soa_id=?",
                (soa_id,),
            )
    else:
        activity_ids = [a["id"] for a in activities]
        if activity_ids:
            placeholders = ",".join("?" * len(activity_ids))
            cur.execute(
                f"SELECT activity_id, concept_code, concept_title, NULL, NULL, NULL "
                f"FROM activity_concept WHERE activity_id IN ({placeholders})",
                activity_ids,
            )
        else:
            cur.execute("SELECT 1 WHERE 0")  # no-op
    for row in cur.fetchall():
        aid, code, title = row[0], row[1], row[2]
        concept_group_uid = row[3] if has_group else None
        group_name = row[4] if has_group else None
        bc_category_name = row[5] if has_category else None
        activity_concepts.setdefault(aid, []).append(
            {
                "code": code,
                "title": title,
                "concept_group_uid": concept_group_uid,
                "group_name": group_name,
                "bc_category_name": bc_category_name,
                "assigned_dss": [],
                "assigned_crf": [],
            }
        )

    # Fetch DSS assignments from the new many-to-many table
    cur.execute(
        "SELECT activity_id, concept_code, id, dss_title, dss_href, dss_display"
        " FROM activity_concept_dss WHERE soa_id=?",
        (soa_id,),
    )
    for aid, code, row_id, dss_title, dss_href, dss_display in cur.fetchall():
        for ac in activity_concepts.get(aid, []):
            if ac["code"] == code:
                ac["assigned_dss"].append(
                    {
                        "id": row_id,
                        "dss_title": dss_title,
                        "dss_href": dss_href,
                        "dss_display": dss_display or dss_title,
                    }
                )
                break

    # Fetch CRF assignments
    cur.execute(
        "SELECT activity_id, concept_code, id, crf_title, crf_href"
        " FROM activity_concept_crf WHERE soa_id=?",
        (soa_id,),
    )
    for aid, code, row_id, crf_title, crf_href in cur.fetchall():
        for ac in activity_concepts.get(aid, []):
            if ac["code"] == code:
                ac["assigned_crf"].append(
                    {
                        "id": row_id,
                        "crf_title": crf_title,
                        "crf_href": crf_href,
                    }
                )
                break

    # Fetch concept groups globally (for the dropdown in concepts_cell),
    # organized into CDISC-scheme + Custom Concept Groups sections.
    from ...services.concepts import _get_concept_group_sections

    concept_groups = _get_concept_group_sections()
    conn.close()

    # Fetch CDISC BC categories list (for the category dropdown in concepts_cell)
    from ...services.concepts import fetch_biomedical_concept_categories as _fetch_cats

    bc_categories_list = _fetch_cats()

    # Fetch biomedical concepts list (lazy import to avoid circular dependency)
    from ...services.concepts import fetch_biomedical_concepts as _app_fetch_concepts

    concepts = _app_fetch_concepts()

    # Fetch surrogates for this SOA
    conn = _connect()
    cur = conn.cursor()
    cur.execute(
        "SELECT id, surrogate_uid, name, label, description, reference FROM biomedical_concept_surrogate WHERE soa_id=? ORDER BY id",
        (soa_id,),
    )
    surrogates = [
        {
            "id": r[0],
            "surrogate_uid": r[1],
            "name": r[2],
            "label": r[3],
            "description": r[4],
            "reference": r[5],
        }
        for r in cur.fetchall()
    ]

    # Per-activity surrogate mappings: activity_id -> [surrogate dicts]
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
        activity_id, sur_id, sur_uid, sur_name, sur_label = row
        activity_surrogates.setdefault(activity_id, []).append(
            {
                "id": sur_id,
                "surrogate_uid": sur_uid,
                "name": sur_name,
                "label": sur_label,
            }
        )

    cur.execute(
        "SELECT study_id, study_label, study_description, name, created_at FROM soa WHERE id=?",
        (soa_id,),
    )
    meta_row = cur.fetchone()
    conn.close()
    study_id, study_label, study_description, study_name, study_created_at = meta_row

    return templates.TemplateResponse(
        request,
        "activities.html",
        {
            "request": request,
            "soa_id": soa_id,
            "activities": activities,
            "activity_concepts": activity_concepts,
            "concepts": concepts,
            "surrogates": surrogates,
            "activity_surrogates": activity_surrogates,
            "concept_groups": concept_groups,
            "bc_categories_list": bc_categories_list,
            "study_id": study_id,
            "study_label": study_label,
            "study_description": study_description,
            "study_name": study_name,
        },
    )


@ui_router.post("/ui/soa/{soa_id}/activities/concepts_refresh")
def ui_refresh_concepts_activities(request: Request, soa_id: int):
    """Refresh biomedical concepts cache, then redirect back to activities page."""
    if not soa_exists(soa_id):
        raise HTTPException(404, "SOA not found")
    from ...services.concepts import fetch_biomedical_concepts as _app_fetch_concepts

    _app_fetch_concepts(force=True)
    redirect_url = f"/ui/soa/{int(soa_id)}/activities"
    if request.headers.get("HX-Request") == "true":
        return HTMLResponse("", headers={"HX-Redirect": redirect_url})
    return RedirectResponse(url=redirect_url, status_code=303)


@ui_router.post("/ui/soa/{soa_id}/activities/create")
def ui_create_activity(
    request: Request,
    soa_id: int,
    name: str | None = Form(None),
    label: str | None = Form(None),
    description: str | None = Form(None),
):
    if not soa_exists(soa_id):
        raise HTTPException(404, "SOA not found")
    payload = ActivityCreate(name=name or "", label=label, description=description)
    add_activity(soa_id, payload)
    return RedirectResponse(url=f"/ui/soa/{int(soa_id)}/activities", status_code=303)


@ui_router.post("/ui/soa/{soa_id}/activities/{activity_id}/delete")
def ui_delete_activity_page(request: Request, soa_id: int, activity_id: int):
    if not soa_exists(soa_id):
        raise HTTPException(404, "SOA not found")
    conn = _connect()
    cur = conn.cursor()
    cur.execute(
        "SELECT id,name,order_index FROM activity WHERE id=? AND soa_id=?",
        (activity_id, soa_id),
    )
    row = cur.fetchone()
    if not row:
        conn.close()
        raise HTTPException(404, "Activity not found")
    before = {"id": row[0], "name": row[1], "order_index": row[2]}
    # Capture concept pairs before deleting for cascade cleanup
    ac_has_conceptuid = _table_has_columns(cur, "activity_concept", ("concept_uid",))
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
    old_pairs = cur.fetchall()
    cur.execute(
        "DELETE FROM matrix_cells WHERE soa_id=? AND activity_id=?",
        (soa_id, activity_id),
    )
    cur.execute(
        "DELETE FROM activity_concept WHERE activity_id=? AND soa_id=?",
        (activity_id, soa_id),
    )
    from ...services.concepts import _cleanup_orphaned_concept_rows

    _cleanup_orphaned_concept_rows(cur, soa_id, old_pairs)
    cur.execute("DELETE FROM activity WHERE id=?", (activity_id,))
    conn.commit()
    conn.close()
    _reindex_activities(soa_id)
    _record_activity_audit(soa_id, "delete", activity_id, before=before, after=None)
    return RedirectResponse(url=f"/ui/soa/{int(soa_id)}/activities", status_code=303)


@ui_router.get(
    "/ui/soa/{soa_id}/activity/{activity_id}/concept/{concept_code}/dss/options",
    response_class=HTMLResponse,
)
def ui_dss_options(
    soa_id: int,
    activity_id: int,
    concept_code: str,
):
    """Return <option> elements for unassigned DSS for a concept (lazy load)."""
    from ...services.concepts import fetch_sdtm_specializations as _app_fetch_dss

    available = _app_fetch_dss(code=concept_code)

    conn = _connect()
    cur = conn.cursor()
    cur.execute(
        "SELECT dss_title FROM activity_concept_dss"
        " WHERE soa_id=? AND activity_id=? AND concept_code=?",
        (soa_id, activity_id, concept_code),
    )
    assigned_ids = {r[0] for r in cur.fetchall()}
    conn.close()

    options = ['<option value="" disabled selected>Select DSS...</option>']
    for d in available:
        dss_id = d["href"].rstrip("/").split("/")[-1]
        if dss_id not in assigned_ids:
            # value = dss_id||href||display_title
            value = f"{dss_id}||{d['href']}||{d['title']}"
            title_escaped = _html.escape(str(d["title"]), quote=True)
            value_escaped = _html.escape(value, quote=True)
            options.append(f'<option value="{value_escaped}">{title_escaped}</option>')
    return HTMLResponse("\n".join(options))


@ui_router.post(
    "/ui/soa/{soa_id}/activity/{activity_id}/concept/{concept_code}/dss",
    response_class=HTMLResponse,
)
def ui_save_dss_assignment(
    request: Request,
    soa_id: int,
    activity_id: int,
    concept_code: str,
    dss_selection: str = Form(""),
):
    """Add a DSS assignment for a specific concept on an activity."""
    if not soa_exists(soa_id):
        raise HTTPException(404, "SOA not found")

    conn = _connect()
    cur = conn.cursor()
    cur.execute(
        "SELECT 1 FROM activity WHERE id=? AND soa_id=?",
        (activity_id, soa_id),
    )
    if not cur.fetchone():
        conn.close()
        raise HTTPException(404, "Activity not found")

    # Parse selection value (datasetSpecializationId||href||display_title)
    selection = dss_selection.strip()
    if not selection or "||" not in selection:
        conn.close()
        return _render_dss_cell(request, soa_id, activity_id)
    parts = selection.split("||", 2)
    new_title = parts[0]
    new_href = parts[1] if len(parts) > 1 else ""
    new_display = parts[2] if len(parts) > 2 else new_title
    if not new_title or not new_href:
        conn.close()
        return _render_dss_cell(request, soa_id, activity_id)

    # Prevent duplicate assignments
    cur.execute(
        "SELECT id FROM activity_concept_dss"
        " WHERE soa_id=? AND activity_id=? AND concept_code=? AND dss_title=?",
        (soa_id, activity_id, concept_code, new_title),
    )
    if cur.fetchone():
        conn.close()
        return _render_dss_cell(request, soa_id, activity_id)

    cur.execute(
        "INSERT INTO activity_concept_dss"
        " (soa_id, activity_id, concept_code, dss_title, dss_href, dss_display)"
        " VALUES (?, ?, ?, ?, ?, ?)",
        (soa_id, activity_id, concept_code, new_title, new_href, new_display),
    )
    conn.commit()
    conn.close()

    _record_activity_audit(
        soa_id,
        "add_dss",
        activity_id,
        before=None,
        after={
            "concept_code": concept_code,
            "dss_title": new_title,
            "dss_href": new_href,
        },
    )

    return _render_dss_cell(request, soa_id, activity_id)


@ui_router.post(
    "/ui/soa/{soa_id}/activity/{activity_id}"
    "/concept/{concept_code}/dss/{dss_row_id}/delete",
    response_class=HTMLResponse,
)
def ui_delete_dss_assignment(
    request: Request,
    soa_id: int,
    activity_id: int,
    concept_code: str,
    dss_row_id: int,
):
    """Remove a single DSS assignment from activity_concept_dss."""
    if not soa_exists(soa_id):
        raise HTTPException(404, "SOA not found")

    conn = _connect()
    cur = conn.cursor()

    # Capture the row before deletion for audit
    cur.execute(
        "SELECT dss_title, dss_href FROM activity_concept_dss WHERE id=? AND soa_id=?",
        (dss_row_id, soa_id),
    )
    row = cur.fetchone()
    if not row:
        conn.close()
        return _render_dss_cell(request, soa_id, activity_id)
    old_title, old_href = row

    cur.execute(
        "DELETE FROM activity_concept_dss WHERE id=? AND soa_id=?",
        (dss_row_id, soa_id),
    )

    conn.commit()
    conn.close()

    _record_activity_audit(
        soa_id,
        "remove_dss",
        activity_id,
        before={
            "concept_code": concept_code,
            "dss_title": old_title,
            "dss_href": old_href,
        },
        after=None,
    )

    return _render_dss_cell(request, soa_id, activity_id)


@ui_router.get(
    "/ui/soa/{soa_id}/activity/{activity_id}/dss_cell",
    response_class=HTMLResponse,
)
def ui_get_dss_cell(request: Request, soa_id: int, activity_id: int):
    """Return the DSS cell partial for an activity."""
    if not soa_exists(soa_id):
        raise HTTPException(404, "SOA not found")
    return _render_dss_cell(request, soa_id, activity_id)


@ui_router.get(
    "/ui/soa/{soa_id}/dss/detail",
    response_class=HTMLResponse,
)
def ui_dss_detail(request: Request, soa_id: int, href: str = "", title: str = ""):
    """Detail page for a single DSS, fetched by href."""
    if not soa_exists(soa_id):
        raise HTTPException(404, "SOA not found")

    import requests as _requests

    api_key = _get_cdisc_api_key()
    subscription_key = os.environ.get("CDISC_SUBSCRIPTION_KEY")
    unified_key = subscription_key or api_key
    headers: dict = {}
    if unified_key:
        headers["Ocp-Apim-Subscription-Key"] = unified_key
    if api_key:
        headers["Authorization"] = f"Bearer {api_key}"
        headers["api-key"] = api_key

    from urllib.parse import urlparse as _urlparse, urlunparse as _urlunparse

    _bc_base = os.environ.get(
        "CDISC_BC_API_BASE_URL",
        "https://api.library.cdisc.org/api/cosmos/v2",
    )
    _p = _urlparse(_bc_base)

    status = None
    error = None
    pretty_json = None
    raw_text_snippet = None
    data = None
    # Only CDISC Library URLs are fetched (SSRF guard); anything else is
    # reported on the page without a request.
    href_is_valid = False
    if href:
        _parsed_href = _urlparse(href)
        if _parsed_href.netloc != _p.netloc or _parsed_href.scheme not in (
            "http",
            "https",
        ):
            error = "Invalid href: only CDISC Library API URLs are permitted."
        else:
            href_is_valid = True
    if href_is_valid:
        _safe_url = _urlunparse(
            (
                _p.scheme,
                _p.netloc,
                _parsed_href.path,
                _parsed_href.params,
                _parsed_href.query,
                "",
            )
        )
        try:
            resp = _requests.get(
                _safe_url,
                headers=headers,
                timeout=int(os.environ.get("CDISC_REQUEST_TIMEOUT", "15")),
                allow_redirects=False,
            )
            status = resp.status_code
            raw_text_snippet = resp.text[:500]
            if resp.status_code == 200:
                try:
                    data = resp.json()
                except ValueError:
                    error = "200 OK but response was not valid JSON"
                    data = None
                if data is not None:
                    try:
                        pretty_json = json.dumps(data, indent=2, sort_keys=True)
                    except TypeError:
                        pretty_json = json.dumps(data, indent=2)
            else:
                error = f"HTTP {resp.status_code} retrieving specialization"
        except REMOTE_PAYLOAD_ERRORS as e:
            error = f"Fetch error: {e}"[:300]
    elif not href:
        error = "No href provided."

    # Extract variables list and summary fields for structured display
    variables = []
    summary = {}
    if data and isinstance(data, dict):
        variables = data.get("variables", [])
        for key in (
            "datasetSpecializationId",
            "domain",
            "shortName",
            "source",
            "sdtmigStartVersion",
            "sdtmigEndVersion",
        ):
            val = data.get(key)
            if val is not None and val != "":
                summary[key] = val
        # Nested objects: check top-level and _links
        links = data.get("_links", {})
        pbc = data.get("parentBiomedicalConcept") or links.get(
            "parentBiomedicalConcept"
        )
        if isinstance(pbc, dict):
            parts = []
            if pbc.get("shortName"):
                parts.append(pbc["shortName"])
            elif pbc.get("title"):
                parts.append(pbc["title"])
            if pbc.get("conceptId"):
                parts.append(f"({pbc['conceptId']})")
            if parts:
                summary["parentBiomedicalConcept"] = " ".join(parts)
        ppkg = data.get("parentPackage") or links.get("parentPackage")
        if isinstance(ppkg, dict):
            parts = []
            if ppkg.get("name"):
                parts.append(ppkg["name"])
            elif ppkg.get("title"):
                parts.append(ppkg["title"])
            if ppkg.get("type"):
                parts.append(f"[{ppkg['type']}]")
            if parts:
                summary["parentPackage"] = " ".join(parts)
            ppkg_href = ppkg.get("href", "")
            if ppkg_href and not ppkg_href.startswith("http"):
                ppkg_href = f"https://api.library.cdisc.org{ppkg_href}"
            if ppkg_href:
                summary["parentPackageHref"] = ppkg_href

    return templates.TemplateResponse(
        request,
        "sdtm_specialization_detail.html",
        {
            "index": 0,
            "title": title or "(untitled)",
            "href": href,
            "status": status,
            "error": error,
            "pretty_json": pretty_json,
            "raw_text_snippet": raw_text_snippet,
            "missing_key": unified_key is None,
            "total": 1,
            "back_url": f"/ui/soa/{soa_id}/activities",
            "summary": summary,
            "variables": variables,
        },
    )


@ui_router.get(
    "/ui/soa/{soa_id}/activity/{activity_id}/concept/{concept_code}/crf/options",
    response_class=HTMLResponse,
)
def ui_crf_options(
    soa_id: int,
    activity_id: int,
    concept_code: str,
):
    """Return <option> elements for unassigned CRF specializations (lazy load)."""
    from ...services.concepts import fetch_crf_specializations as _app_fetch_crf

    available = _app_fetch_crf(code=concept_code)

    conn = _connect()
    cur = conn.cursor()
    cur.execute(
        "SELECT crf_href FROM activity_concept_crf"
        " WHERE soa_id=? AND activity_id=? AND concept_code=?",
        (soa_id, activity_id, concept_code),
    )
    assigned_hrefs = {r[0] for r in cur.fetchall()}
    conn.close()

    options = ['<option value="" disabled selected>Select CRF...</option>']
    for d in available:
        href = d.get("href") or ""
        if href not in assigned_hrefs:
            title = d.get("title") or href
            value = f"{title}||{href}"
            title_escaped = _html.escape(str(title), quote=True)
            value_escaped = _html.escape(value, quote=True)
            options.append(f'<option value="{value_escaped}">{title_escaped}</option>')
    return HTMLResponse("\n".join(options))


@ui_router.post(
    "/ui/soa/{soa_id}/activity/{activity_id}/concept/{concept_code}/crf",
    response_class=HTMLResponse,
)
def ui_save_crf_assignment(
    request: Request,
    soa_id: int,
    activity_id: int,
    concept_code: str,
    crf_selection: str = Form(""),
):
    """Add a CRF specialization assignment for a concept on an activity."""
    if not soa_exists(soa_id):
        raise HTTPException(404, "SOA not found")

    conn = _connect()
    cur = conn.cursor()
    cur.execute(
        "SELECT 1 FROM activity WHERE id=? AND soa_id=?",
        (activity_id, soa_id),
    )
    if not cur.fetchone():
        conn.close()
        raise HTTPException(404, "Activity not found")

    selection = crf_selection.strip()
    if not selection or "||" not in selection:
        conn.close()
        return _render_crf_cell(request, soa_id, activity_id)
    parts = selection.split("||", 1)
    new_title = parts[0]
    new_href = parts[1] if len(parts) > 1 else ""
    if not new_title or not new_href:
        conn.close()
        return _render_crf_cell(request, soa_id, activity_id)

    # Prevent duplicate assignments (guard on href)
    cur.execute(
        "SELECT id FROM activity_concept_crf"
        " WHERE soa_id=? AND activity_id=? AND concept_code=? AND crf_href=?",
        (soa_id, activity_id, concept_code, new_href),
    )
    if cur.fetchone():
        conn.close()
        return _render_crf_cell(request, soa_id, activity_id)

    cur.execute(
        "INSERT INTO activity_concept_crf"
        " (soa_id, activity_id, concept_code, crf_title, crf_href)"
        " VALUES (?, ?, ?, ?, ?)",
        (soa_id, activity_id, concept_code, new_title, new_href),
    )
    conn.commit()
    conn.close()

    _record_activity_audit(
        soa_id,
        "add_crf",
        activity_id,
        before=None,
        after={
            "concept_code": concept_code,
            "crf_title": new_title,
            "crf_href": new_href,
        },
    )

    return _render_crf_cell(request, soa_id, activity_id)


@ui_router.post(
    "/ui/soa/{soa_id}/activity/{activity_id}"
    "/concept/{concept_code}/crf/{crf_row_id}/delete",
    response_class=HTMLResponse,
)
def ui_delete_crf_assignment(
    request: Request,
    soa_id: int,
    activity_id: int,
    concept_code: str,
    crf_row_id: int,
):
    """Remove a single CRF specialization assignment."""
    if not soa_exists(soa_id):
        raise HTTPException(404, "SOA not found")

    conn = _connect()
    cur = conn.cursor()

    cur.execute(
        "SELECT crf_title, crf_href FROM activity_concept_crf WHERE id=? AND soa_id=?",
        (crf_row_id, soa_id),
    )
    row = cur.fetchone()
    if not row:
        conn.close()
        return _render_crf_cell(request, soa_id, activity_id)
    old_title, old_href = row

    cur.execute(
        "DELETE FROM activity_concept_crf WHERE id=? AND soa_id=?",
        (crf_row_id, soa_id),
    )
    conn.commit()
    conn.close()

    _record_activity_audit(
        soa_id,
        "remove_crf",
        activity_id,
        before={
            "concept_code": concept_code,
            "crf_title": old_title,
            "crf_href": old_href,
        },
        after=None,
    )

    return _render_crf_cell(request, soa_id, activity_id)


@ui_router.get(
    "/ui/soa/{soa_id}/activity/{activity_id}/crf_cell",
    response_class=HTMLResponse,
)
def ui_get_crf_cell(request: Request, soa_id: int, activity_id: int):
    """Return the CRF cell partial for an activity."""
    if not soa_exists(soa_id):
        raise HTTPException(404, "SOA not found")
    return _render_crf_cell(request, soa_id, activity_id)


@ui_router.get(
    "/ui/soa/{soa_id}/activity/{activity_id}/crf/detail",
    response_class=HTMLResponse,
)
def ui_crf_detail_from_activity(
    request: Request,
    soa_id: int,
    activity_id: int,
    href: str = "",
    title: str = "",
):
    """Detail page for a single CRF specialization, linked from the activities page."""
    if not soa_exists(soa_id):
        raise HTTPException(404, "SOA not found")

    import requests as _requests
    import json as _json

    api_key = _get_cdisc_crf_api_key()
    subscription_key = _get_cdisc_crf_subscription_key()
    unified_key = subscription_key or api_key
    headers: dict = {}
    if unified_key:
        headers["Ocp-Apim-Subscription-Key"] = unified_key
    if api_key:
        headers["Authorization"] = f"Bearer {api_key}"
        headers["api-key"] = api_key

    from urllib.parse import urlparse as _urlparse, urlunparse as _urlunparse

    _crf_base = os.environ.get(
        "CDISC_CRF_API_BASE_URL",
    ) or os.environ.get(
        "CDISC_BC_API_BASE_URL",
        "https://api.library.cdisc.org/api/cosmos/v2",
    )
    _p = _urlparse(_crf_base)

    spec_data = None
    spec_json = None
    error = None
    status = None
    if href:
        _parsed_href = _urlparse(href)
        if _parsed_href.netloc != _p.netloc or _parsed_href.scheme not in (
            "http",
            "https",
        ):
            error = "Invalid href: only CDISC Library API URLs are permitted."
        else:
            _safe_url = _urlunparse(
                (
                    _p.scheme,
                    _p.netloc,
                    _parsed_href.path,
                    _parsed_href.params,
                    _parsed_href.query,
                    "",
                )
            )
            try:
                resp = _requests.get(
                    _safe_url,
                    headers=headers,
                    timeout=int(os.environ.get("CDISC_REQUEST_TIMEOUT", "15")),
                    allow_redirects=False,
                )
                status = resp.status_code
                if resp.status_code == 200:
                    try:
                        spec_data = resp.json()
                        spec_json = _json.dumps(spec_data, indent=2, sort_keys=True)
                    except ValueError:
                        error = "200 OK but response was not valid JSON"
                else:
                    error = f"HTTP {resp.status_code} retrieving CRF specialization"
            except REMOTE_PAYLOAD_ERRORS as e:
                error = f"Fetch error: {e}"[:300]
    else:
        error = "No href provided."

    return templates.TemplateResponse(
        request,
        "crf_specialization_detail.html",
        {
            "title": title or "(untitled)",
            "href": href,
            "status": status,
            "error": error,
            "spec_data": spec_data,
            "spec_json": spec_json,
            "missing_key": unified_key is None,
            "back_url": f"/ui/soa/{soa_id}/activities",
        },
    )
