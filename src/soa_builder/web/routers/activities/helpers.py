"""activities: helpers (split from activities.py)."""

from ._shared import (
    HTMLResponse,
    _ACT_CONCEPT_CACHE,
    _ACT_CONCEPT_TTL,
    _connect,
    _table_has_columns,
    json,
    logger,
    os,
    templates,
    time,
)


def fetch_biomedical_concepts(force: bool = False):
    override_json = os.environ.get("CDISC_CONCEPTS_JSON")
    if override_json:
        try:
            data = json.loads(override_json)
            # Normalize data into an iterable list of dicts
            if isinstance(data, dict):
                # Common patterns: {'items': [...]}, {'concepts': [...]}, or direct properties
                candidate_lists = []
                for key in ["items", "concepts", "data"]:
                    val = data.get(key)
                    if isinstance(val, list):
                        candidate_lists.append(val)
                if candidate_lists:
                    iterable = candidate_lists[0]
                else:
                    # Fallback: treat dict values; filter only list of dicts or single dicts
                    vals = []
                    for v in data.values():
                        if isinstance(v, list):
                            vals.extend([x for x in v if isinstance(x, dict)])
                        elif isinstance(v, dict):
                            vals.append(v)
                    iterable = vals
            elif isinstance(data, list):
                iterable = data
            else:
                iterable = []
            concepts = []
            for c in iterable:
                if not isinstance(c, dict):
                    continue
                code = c.get("code") or c.get("concept_code")
                title = c.get("title") or c.get("concept_title") or code
                if code:
                    concepts.append({"code": code, "title": title})
            return concepts
        except Exception as e:
            logger.debug("fetch_biomedical_concepts override JSON parse failed: %s", e)
            return []
    now = time.time()
    if (
        not force
        and _ACT_CONCEPT_CACHE["data"]
        and now - _ACT_CONCEPT_CACHE["fetched_at"] < _ACT_CONCEPT_TTL
    ):
        return _ACT_CONCEPT_CACHE["data"]
    # Remote fetch intentionally omitted here to prevent dependency & circular import; return empty list (titles fallback to codes)
    _ACT_CONCEPT_CACHE["data"] = []
    _ACT_CONCEPT_CACHE["fetched_at"] = now
    return []


def _next_activity_uid(cur, soa_id: int) -> str:
    """Return the next Activity_N UID, never reusing a deleted one.

    Scans both the live table and the audit trail so deleted UIDs are
    never recycled — matching the pattern used by _next_study_cell_uid.
    """
    max_n = 0
    cur.execute("SELECT activity_uid FROM activity WHERE soa_id=?", (soa_id,))
    for (uid,) in cur.fetchall():
        if isinstance(uid, str) and uid.startswith("Activity_"):
            try:
                n = int(uid.split("_")[-1])
                if n > max_n:
                    max_n = n
            except (ValueError, IndexError):
                pass
    cur.execute(
        "SELECT before_json, after_json FROM activity_audit WHERE soa_id=?",
        (soa_id,),
    )
    for before_raw, after_raw in cur.fetchall():
        for raw in (before_raw, after_raw):
            if not raw:
                continue
            try:
                uid = json.loads(raw).get("activity_uid", "")
                if isinstance(uid, str) and uid.startswith("Activity_"):
                    n = int(uid.split("_")[-1])
                    if n > max_n:
                        max_n = n
            except Exception:
                pass
    return f"Activity_{max_n + 1}"


def _reindex_activities(soa_id: int):
    """Re-number order_index after a delete. activity_uid is immutable and never changed."""
    conn = _connect()
    cur = conn.cursor()
    cur.execute(
        "SELECT id FROM activity WHERE soa_id=? ORDER BY order_index", (soa_id,)
    )
    ids = [r[0] for r in cur.fetchall()]
    for idx, _id in enumerate(ids, start=1):
        cur.execute("UPDATE activity SET order_index=? WHERE id=?", (idx, _id))
    conn.commit()
    conn.close()


def _render_dss_cell(request, soa_id, activity_id):
    """Helper: render the dss_cell.html partial for a single activity."""
    conn = _connect()
    cur = conn.cursor()

    # Fetch concept rows for this activity
    if _table_has_columns(cur, "activity_concept", ("soa_id",)):
        cur.execute(
            "SELECT concept_code, concept_title"
            " FROM activity_concept WHERE activity_id=? AND soa_id=?",
            (activity_id, soa_id),
        )
    else:
        cur.execute(
            "SELECT concept_code, concept_title"
            " FROM activity_concept WHERE activity_id=?",
            (activity_id,),
        )
    concepts_list = [
        {"code": r[0], "title": r[1], "assigned_dss": [], "assigned_crf": []}
        for r in cur.fetchall()
    ]

    # Fetch DSS assignments from the new table
    concept_index = {c["code"]: c for c in concepts_list}
    cur.execute(
        "SELECT concept_code, id, dss_title, dss_href"
        " FROM activity_concept_dss"
        " WHERE soa_id=? AND activity_id=?",
        (soa_id, activity_id),
    )
    cur.execute(
        "SELECT concept_code, id, dss_title, dss_href, dss_display"
        " FROM activity_concept_dss"
        " WHERE soa_id=? AND activity_id=?",
        (soa_id, activity_id),
    )
    for code, row_id, dss_title, dss_href, dss_display in cur.fetchall():
        if code in concept_index:
            concept_index[code]["assigned_dss"].append(
                {
                    "id": row_id,
                    "dss_title": dss_title,
                    "dss_href": dss_href,
                    "dss_display": dss_display or dss_title,
                }
            )
    conn.close()

    activity_concepts = {activity_id: concepts_list}
    html = templates.get_template("dss_cell.html").render(
        request=request,
        soa_id=soa_id,
        activity_id=activity_id,
        activity_concepts=activity_concepts,
    )
    return HTMLResponse(html)


def _render_crf_cell(request, soa_id, activity_id):
    """Helper: render the crf_cell.html partial for a single activity."""
    conn = _connect()
    cur = conn.cursor()

    if _table_has_columns(cur, "activity_concept", ("soa_id",)):
        cur.execute(
            "SELECT concept_code, concept_title"
            " FROM activity_concept WHERE activity_id=? AND soa_id=?",
            (activity_id, soa_id),
        )
    else:
        cur.execute(
            "SELECT concept_code, concept_title"
            " FROM activity_concept WHERE activity_id=?",
            (activity_id,),
        )
    concepts_list = [
        {"code": r[0], "title": r[1], "assigned_crf": []} for r in cur.fetchall()
    ]

    concept_index = {c["code"]: c for c in concepts_list}
    cur.execute(
        "SELECT concept_code, id, crf_title, crf_href"
        " FROM activity_concept_crf"
        " WHERE soa_id=? AND activity_id=?",
        (soa_id, activity_id),
    )
    for code, row_id, crf_title, crf_href in cur.fetchall():
        if code in concept_index:
            concept_index[code]["assigned_crf"].append(
                {
                    "id": row_id,
                    "crf_title": crf_title,
                    "crf_href": crf_href,
                }
            )
    conn.close()

    activity_concepts = {activity_id: concepts_list}
    html = templates.get_template("crf_cell.html").render(
        request=request,
        soa_id=soa_id,
        activity_id=activity_id,
        activity_concepts=activity_concepts,
    )
    return HTMLResponse(html)
