"""_freeze_helpers: rollback (split from _freeze_helpers.py)."""

from ._shared import (
    ACTIVITY_ID_CHILD_TABLES,
    HTTPException,
    Optional,
    _BC_CODE_CHAIN_TABLES,
    _connect,
    _get_next_concept_uid,
    _table_has_columns,
    datetime,
    reserve_decision_uid_floor,
    reserve_encounter_uid_floor,
    reserve_instance_uid_floor,
    reserve_timeline_uid_floor,
    timezone,
)
from .capture import _table_columns
from .store import _create_freeze, _get_freeze


def _safety_freeze_label(soa_id: int, target_label: Optional[str]) -> str:
    """Unique label for the automatic pre-rollback freeze."""
    conn = _connect()
    cur = conn.cursor()
    cur.execute("SELECT version_label FROM soa_freeze WHERE soa_id=?", (soa_id,))
    existing = {r[0] for r in cur.fetchall()}
    conn.close()
    base = f"pre-rollback-to-{target_label or 'unnamed'}"
    label, n = base, 2
    while label in existing:
        label = f"{base}-{n}"
        n += 1
    return label


def _code_uids_referenced_elsewhere(cur, soa_id: int) -> set:
    """Return code_uids referenced by tables outside the BC chain.

    Scans every ``*code_uid`` column so code rows owned by entities
    rollback never restores (titles, interventions, roles, ...) stay
    put, including ones added by future tables.
    """
    skip = {"code", "alias_code", "code_association", *_BC_CODE_CHAIN_TABLES}
    cur.execute(
        "SELECT name FROM sqlite_master WHERE type='table'"
        " AND name NOT LIKE '%\\_audit' ESCAPE '\\'"
    )
    tables = [r[0] for r in cur.fetchall() if r[0] not in skip]
    referenced: set = set()
    for table in tables:
        cols = _table_columns(cur, table)
        if "soa_id" not in cols:
            continue
        for col in cols:
            if col.endswith("code_uid"):
                cur.execute(
                    f"SELECT {col} FROM {table} WHERE soa_id=? AND {col} IS NOT NULL",
                    (soa_id,),
                )
                referenced.update(r[0] for r in cur.fetchall())
    return referenced


def _delete_bc_code_chain(cur, soa_id: int) -> None:
    """Delete the alias_code/code rows behind the BC tables only.

    Rollback rebuilds biomedical concepts (and their alias and
    standard codes) from the snapshot's activity concepts. Codes that
    other entities point at are kept.
    """
    alias_uids: set = set()
    for table in _BC_CODE_CHAIN_TABLES:
        if not _table_has_columns(cur, table, ("code",)):
            continue
        cur.execute(
            f"SELECT code FROM {table} WHERE soa_id=? AND code IS NOT NULL",
            (soa_id,),
        )
        alias_uids.update(r[0] for r in cur.fetchall())
    if not alias_uids:
        return
    alias_list = sorted(alias_uids)
    ph = ",".join("?" * len(alias_list))
    cur.execute(
        f"SELECT standard_code FROM alias_code"
        f" WHERE soa_id=? AND alias_code_uid IN ({ph})"
        f" AND standard_code IS NOT NULL",
        [soa_id] + alias_list,
    )
    code_uids = {r[0] for r in cur.fetchall()}
    code_uids -= _code_uids_referenced_elsewhere(cur, soa_id)
    cur.execute(
        f"DELETE FROM alias_code WHERE soa_id=? AND alias_code_uid IN ({ph})",
        [soa_id] + alias_list,
    )
    if code_uids:
        code_list = sorted(code_uids)
        cph = ",".join("?" * len(code_list))
        cur.execute(
            f"DELETE FROM code WHERE soa_id=? AND code_uid IN ({cph})",
            [soa_id] + code_list,
        )


def _remap_activity_children(cur, soa_id: int, activity_id_map: dict) -> int:
    """Re-point rows keyed on old activity ids; drop unmapped ones.

    Returns the number of rows dropped. Updates go row by row through
    the child's own id, so an old id can never collide with a new one.
    """
    dropped = 0
    for table in ACTIVITY_ID_CHILD_TABLES:
        if not _table_has_columns(cur, table, ("id", "activity_id", "soa_id")):
            continue
        cur.execute(
            f"SELECT id, activity_id FROM {table} WHERE soa_id=?",
            (soa_id,),
        )
        for row_id, old_activity_id in cur.fetchall():
            new_activity_id = activity_id_map.get(old_activity_id)
            if new_activity_id is None:
                cur.execute(f"DELETE FROM {table} WHERE id=?", (row_id,))
                dropped += 1
            else:
                cur.execute(
                    f"UPDATE {table} SET activity_id=? WHERE id=?",
                    (new_activity_id, row_id),
                )
    return dropped


def _restore_rows(cur, soa_id: int, table: str, rows, enabled: bool) -> int:
    """Insert snapshot ``rows`` for ``soa_id`` into ``table`` using the
    columns they share with the live table (id/soa_id excluded)."""
    if not enabled:
        return 0
    table_cols = _table_columns(cur, table)
    written = 0
    for row in rows:
        keys = [k for k in row if k not in ("id", "soa_id") and k in table_cols]
        cur.execute(
            f"INSERT INTO {table} (soa_id,{','.join(keys)}) "
            f"VALUES ({','.join('?' * (len(keys) + 1))})",
            [soa_id] + [row[k] for k in keys],
        )
        written += 1
    return written


def _rollback_freeze(soa_id: int, freeze_id: int) -> dict:
    # Lazy import to avoid circular dependency with app.py
    from ...services.concepts import _upsert_biomedical_concept

    freeze = _get_freeze(soa_id, freeze_id)
    if not freeze:
        raise HTTPException(404, "Freeze not found")
    snap = freeze["snapshot"]
    if snap.get("soa_id") != soa_id:
        raise HTTPException(400, "Snapshot SoA mismatch")
    # Safety net: freeze the current state first so the rollback can be
    # undone. Any failure here aborts before anything is deleted.
    safety_id, safety_label = _create_freeze(
        soa_id, _safety_freeze_label(soa_id, freeze.get("version_label"))
    )
    visits = snap.get("visits", [])
    activities = snap.get("activities", [])
    cells = snap.get("cells", [])
    elements = snap.get("elements", [])
    concepts_map = snap.get("activity_concepts", {}) or {}
    snap_epochs = snap.get("epochs", []) or []
    snap_code_assocs = snap.get("code_associations", []) or []
    snap_arms = snap.get("arms", []) or []
    snap_timings = snap.get("timings", []) or []
    snap_objectives = snap.get("objectives", []) or []
    snap_endpoints = snap.get("endpoints", []) or []
    snap_encounters = snap.get("encounters_full")
    snap_instances = snap.get("instances")
    snap_timelines = snap.get("schedule_timelines")
    snap_decisions = snap.get("decision_instances")
    # Freezes taken before migration m0118 have no criteria key; their
    # rollback leaves the live criteria alone.
    snap_eligibility = snap.get("eligibility_criteria")
    snap_study_cells = snap.get("study_cells")
    restore_encounters = snap_encounters is not None
    restore_instances = snap_instances is not None
    restore_timelines = snap_timelines is not None
    restore_decisions = snap_decisions is not None
    restore_eligibility = snap_eligibility is not None
    # Study cells need element_uid (NOT NULL) and elements that kept
    # their element_id; only newer captures provide both.
    restore_study_cells = (
        snap_study_cells is not None
        and (snap.get("capture_version") or 1) >= 2
        and all(c.get("element_uid") for c in snap_study_cells)
    )
    instance_axis = any(v.get("instance_uid") for v in visits)
    # Legacy snapshots carry real visits (no instance_uid) under
    # "visits" and are replayed into the visit table as before.
    legacy_visits = not instance_axis and not restore_encounters
    conn = _connect()
    cur = conn.cursor()
    restore_eligibility = restore_eligibility and _table_has_columns(
        cur, "eligibility_criterion", ("criterion_uid",)
    )
    # Rollback re-creates snapshot UIDs and drops post-freeze ones. Keep
    # the UID counters past both so a removed UID is never reissued.
    # Must run before the deletes below (the seed reads live rows).
    reserve_encounter_uid_floor(
        cur,
        soa_id,
        [
            e.get("encounter_uid")
            for e in (snap_encounters or [])
            if e.get("encounter_uid")
        ],
    )
    reserve_instance_uid_floor(
        cur,
        soa_id,
        [
            i.get("instance_uid")
            for i in (snap_instances or [])
            if i.get("instance_uid")
        ],
    )
    reserve_timeline_uid_floor(
        cur,
        soa_id,
        [
            t.get("schedule_timeline_uid")
            for t in (snap_timelines or [])
            if t.get("schedule_timeline_uid")
        ],
    )
    reserve_decision_uid_floor(
        cur,
        soa_id,
        [
            d.get("instance_uid")
            for d in (snap_decisions or [])
            if d.get("instance_uid")
        ],
    )
    cur.execute("DELETE FROM matrix_cells WHERE soa_id=?", (soa_id,))
    cur.execute(
        "DELETE FROM activity_concept WHERE activity_id IN"
        " (SELECT id FROM activity WHERE soa_id=? )",
        (soa_id,),
    )
    # Only the BC chain's alias/standard codes are reset; codes owned by
    # entities rollback does not restore (titles, roles, ...) must stay.
    # Must run before the BC tables are emptied (it reads their codes).
    _delete_bc_code_chain(cur, soa_id)
    cur.execute("DELETE FROM biomedical_concept WHERE soa_id=?", (soa_id,))
    # Cascade: BCPs/RCs are rebuilt on the next populate; wipe them here so
    # they do not orphan when biomedical_concept/alias_code/code are reset.
    cur.execute("DELETE FROM bcp_response_code WHERE soa_id=?", (soa_id,))
    cur.execute("DELETE FROM biomedical_concept_property WHERE soa_id=?", (soa_id,))
    if _table_has_columns(cur, "objective", ("id",)):
        cur.execute("DELETE FROM objective WHERE soa_id=?", (soa_id,))
    if _table_has_columns(cur, "endpoint", ("id",)):
        cur.execute("DELETE FROM endpoint WHERE soa_id=?", (soa_id,))
    if _table_has_columns(cur, "timing", ("id",)):
        cur.execute("DELETE FROM timing WHERE soa_id=?", (soa_id,))
    if _table_has_columns(cur, "arm", ("id",)):
        cur.execute("DELETE FROM arm WHERE soa_id=?", (soa_id,))
    if _table_has_columns(cur, "epoch", ("id",)):
        cur.execute("DELETE FROM epoch WHERE soa_id=?", (soa_id,))
    cur.execute("DELETE FROM code_association WHERE soa_id=?", (soa_id,))
    # Live ids can differ from the snapshot's (e.g. after an earlier
    # rollback), so child rows are re-pointed through activity_uid.
    cur.execute("SELECT id, activity_uid FROM activity WHERE soa_id=?", (soa_id,))
    live_activity_uid = {r[0]: r[1] for r in cur.fetchall()}
    cur.execute("DELETE FROM activity WHERE soa_id=?", (soa_id,))
    # Encounters live in the visit table. Instance-axis snapshots keep
    # instance rows under "visits", so only delete encounters when
    # there is something valid to restore them from.
    if restore_encounters or legacy_visits:
        cur.execute("DELETE FROM visit WHERE soa_id=?", (soa_id,))
    if restore_instances:
        cur.execute("DELETE FROM instances WHERE soa_id=?", (soa_id,))
    if restore_timelines:
        cur.execute("DELETE FROM schedule_timelines WHERE soa_id=?", (soa_id,))
    if restore_decisions:
        cur.execute("DELETE FROM decision_instances WHERE soa_id=?", (soa_id,))
    if restore_eligibility:
        cur.execute("DELETE FROM eligibility_criterion WHERE soa_id=?", (soa_id,))
    if restore_study_cells:
        cur.execute("DELETE FROM study_cell WHERE soa_id=?", (soa_id,))
    cur.execute("DELETE FROM element WHERE soa_id=?", (soa_id,))
    for ca in snap_code_assocs:
        stored_code = ca.get("code")
        cur.execute(
            "INSERT INTO code_association"
            " (soa_id, code_uid, codelist_table, codelist_code, code, decode)"
            " VALUES (?,?,?,?,?,?)",
            (
                soa_id,
                ca.get("code_uid"),
                ca.get("codelist_table"),
                ca.get("codelist_code"),
                stored_code,
                ca.get("decode") or stored_code,
            ),
        )
    if _table_has_columns(cur, "epoch", ("type", "epoch_uid")):
        for ep in sorted(snap_epochs, key=lambda x: x.get("order_index") or 0):
            cur.execute(
                "INSERT INTO epoch"
                " (soa_id,name,order_index,epoch_seq,epoch_label,"
                "epoch_description,type,epoch_uid) VALUES (?,?,?,?,?,?,?,?)",
                (
                    soa_id,
                    ep.get("name"),
                    ep.get("order_index"),
                    ep.get("epoch_seq"),
                    ep.get("epoch_label"),
                    ep.get("epoch_description"),
                    ep.get("type"),
                    ep.get("epoch_uid"),
                ),
            )
    if _table_has_columns(cur, "arm", ("arm_uid",)):
        for a in sorted(snap_arms, key=lambda x: x.get("order_index") or 0):
            cur.execute(
                "INSERT INTO arm"
                " (soa_id,name,label,description,type,data_origin_type,"
                "order_index,arm_uid) VALUES (?,?,?,?,?,?,?,?)",
                (
                    soa_id,
                    a.get("name"),
                    a.get("label"),
                    a.get("description"),
                    a.get("type"),
                    a.get("data_origin_type"),
                    a.get("order_index"),
                    a.get("arm_uid"),
                ),
            )
    if _table_has_columns(cur, "timing", ("timing_uid",)):
        for t in sorted(snap_timings, key=lambda x: x.get("order_index") or 0):
            cur.execute(
                "INSERT INTO timing"
                " (soa_id,timing_uid,name,label,description,type,value,"
                "value_label,relative_to_from,"
                "relative_from_schedule_instance,"
                "relative_to_schedule_instance,window_label,window_upper,"
                "window_lower,order_index,member_of_timeline)"
                " VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
                (
                    soa_id,
                    t.get("timing_uid"),
                    t.get("name"),
                    t.get("label"),
                    t.get("description"),
                    t.get("type"),
                    t.get("value"),
                    t.get("value_label"),
                    t.get("relative_to_from"),
                    t.get("relative_from_schedule_instance"),
                    t.get("relative_to_schedule_instance"),
                    t.get("window_label"),
                    t.get("window_upper"),
                    t.get("window_lower"),
                    t.get("order_index"),
                    t.get("member_of_timeline"),
                ),
            )
    if _table_has_columns(cur, "objective", ("objective_uid",)):
        for o in sorted(snap_objectives, key=lambda x: x.get("order_index") or 0):
            cur.execute(
                "INSERT INTO objective"
                " (soa_id,objective_uid,name,label,description,text,"
                "level_code_uid,order_index) VALUES (?,?,?,?,?,?,?,?)",
                (
                    soa_id,
                    o.get("objective_uid"),
                    o.get("name"),
                    o.get("label"),
                    o.get("description"),
                    o.get("text"),
                    o.get("level_code_uid"),
                    o.get("order_index"),
                ),
            )
    if _table_has_columns(cur, "endpoint", ("endpoint_uid",)):
        for e in sorted(snap_endpoints, key=lambda x: x.get("order_index") or 0):
            cur.execute(
                "INSERT INTO endpoint"
                " (soa_id,endpoint_uid,objective_uid,name,label,"
                "description,text,purpose,level_code_uid,order_index)"
                " VALUES (?,?,?,?,?,?,?,?,?,?)",
                (
                    soa_id,
                    e.get("endpoint_uid"),
                    e.get("objective_uid"),
                    e.get("name"),
                    e.get("label"),
                    e.get("description"),
                    e.get("text"),
                    e.get("purpose"),
                    e.get("level_code_uid"),
                    e.get("order_index"),
                ),
            )
    visit_id_map = {}
    encounters_written = 0
    if restore_encounters:
        visit_cols = _table_columns(cur, "visit")
        for enc in sorted(snap_encounters, key=lambda x: x.get("order_index") or 0):
            cols = ["soa_id"] + [k for k in enc if k != "id" and k in visit_cols]
            vals = [soa_id] + [enc[k] for k in cols[1:]]
            cur.execute(
                f"INSERT INTO visit ({','.join(cols)}) "
                f"VALUES ({','.join('?' * len(cols))})",
                vals,
            )
            visit_id_map[enc.get("id")] = cur.lastrowid
            encounters_written += 1
    elif legacy_visits:
        for v in sorted(visits, key=lambda x: x.get("order_index", 0)):
            cur.execute(
                "INSERT INTO visit (soa_id,name,label,order_index) VALUES (?,?,?,?)",
                (
                    soa_id,
                    v.get("name"),
                    v.get("label") or None,
                    v.get("order_index"),
                ),
            )
            visit_id_map[v.get("id")] = cur.lastrowid
            encounters_written += 1
    instances_written = 0
    if restore_instances:
        instance_cols = _table_columns(cur, "instances")
        for inst in snap_instances:
            cols = ["soa_id"] + [k for k in inst if k != "id" and k in instance_cols]
            vals = [soa_id] + [inst[k] for k in cols[1:]]
            cur.execute(
                f"INSERT INTO instances ({','.join(cols)}) "
                f"VALUES ({','.join('?' * len(cols))})",
                vals,
            )
            instances_written += 1
    timelines_written = _restore_rows(
        cur, soa_id, "schedule_timelines", snap_timelines, restore_timelines
    )
    decisions_written = _restore_rows(
        cur, soa_id, "decision_instances", snap_decisions, restore_decisions
    )
    _restore_rows(
        cur, soa_id, "eligibility_criterion", snap_eligibility, restore_eligibility
    )
    study_cells_written = _restore_rows(
        cur, soa_id, "study_cell", snap_study_cells, restore_study_cells
    )
    cur.execute("PRAGMA table_info(activity)")
    activity_cols = {r[1] for r in cur.fetchall()}
    optional_activity_cols = [
        c
        for c in ("activity_uid", "label", "description", "superscript")
        if c in activity_cols
    ]
    activity_id_map = {}
    new_id_by_activity_uid = {}
    for a in sorted(activities, key=lambda x: x.get("order_index", 0)):
        cols = ["soa_id", "name", "order_index"]
        vals = [soa_id, a.get("name"), a.get("order_index")]
        for col in optional_activity_cols:
            if a.get(col) is not None:
                cols.append(col)
                vals.append(a.get(col))
        cur.execute(
            f"INSERT INTO activity ({','.join(cols)}) "
            f"VALUES ({','.join('?' * len(cols))})",
            vals,
        )
        new_id = cur.lastrowid
        activity_id_map[a.get("id")] = new_id
        if a.get("activity_uid"):
            new_id_by_activity_uid[a["activity_uid"]] = new_id
    live_to_new_activity_id = {}
    for live_id, uid in live_activity_uid.items():
        if uid:
            new_id = new_id_by_activity_uid.get(uid)
        else:  # legacy row without a UID: fall back to the snapshot id
            new_id = activity_id_map.get(live_id)
        if new_id is not None:
            live_to_new_activity_id[live_id] = new_id
    _remap_activity_children(cur, soa_id, live_to_new_activity_id)
    # Snapshots store the matrix axis under "visits" (instance rows
    # carrying instance_uid) and cells keyed on instance_id. Instances
    # are not rebuilt by rollback, so map each snapshot instance id to
    # the live row through its immutable instance_uid.
    snap_instance_uid = {
        v.get("id"): v.get("instance_uid") for v in visits if v.get("instance_uid")
    }
    cur.execute(
        "SELECT instance_uid, id FROM instances WHERE soa_id=?",
        (soa_id,),
    )
    live_instance_id = {r[0]: r[1] for r in cur.fetchall()}
    # encounter_uid -> instance id, only where exactly one instance
    # shares the encounter
    cur.execute(
        "SELECT encounter_uid, MIN(id) FROM instances "
        "WHERE soa_id=? AND encounter_uid IS NOT NULL "
        "GROUP BY encounter_uid HAVING COUNT(*) = 1",
        (soa_id,),
    )
    unique_instance_by_encounter = {r[0]: r[1] for r in cur.fetchall()}
    snap_encounter_uid = {
        v.get("id"): v.get("encounter_uid") for v in visits if v.get("encounter_uid")
    }
    restored_instance_cells = set()
    has_cell_superscript = _table_has_columns(cur, "matrix_cells", ("superscript",))
    inserted_cells = 0
    skipped_cells = 0
    skipped_missing_instance = 0
    for c in cells:
        old_aid = c.get("activity_id")
        status = (c.get("status") or "").strip()
        if status == "":
            continue
        aid = activity_id_map.get(old_aid)
        if not aid:
            skipped_cells += 1
            continue
        if c.get("instance_id") is not None:
            inst_uid = snap_instance_uid.get(c.get("instance_id"))
            inst_id = live_instance_id.get(inst_uid)
            if inst_id is None:
                skipped_missing_instance += 1
                skipped_cells += 1
                continue
            cols = ["soa_id", "instance_id", "activity_id", "status"]
            vals = [soa_id, inst_id, aid, status]
            if has_cell_superscript and c.get("superscript") is not None:
                cols.append("superscript")
                vals.append(c.get("superscript"))
            cur.execute(
                f"INSERT INTO matrix_cells ({','.join(cols)}) "
                f"VALUES ({','.join('?' * len(cols))})",
                vals,
            )
            restored_instance_cells.add((soa_id, inst_id, aid))
            inserted_cells += 1
            continue
        # Legacy snapshot: cell keyed on visit_id only
        vid = visit_id_map.get(c.get("visit_id"))
        if vid:
            # Resolve through the snapshot visit's encounter_uid: the
            # restored visit row does not carry it, and the startup
            # backfill will not run again.
            inst_id = unique_instance_by_encounter.get(
                snap_encounter_uid.get(c.get("visit_id"))
            )
            if (soa_id, inst_id, aid) in restored_instance_cells:
                inst_id = None
            if inst_id is not None:
                restored_instance_cells.add((soa_id, inst_id, aid))
            cur.execute(
                "INSERT INTO matrix_cells "
                "(soa_id, visit_id, instance_id, activity_id, status)"
                " VALUES (?,?,?,?,?)",
                (soa_id, vid, inst_id, aid, status),
            )
            inserted_cells += 1
        else:
            skipped_cells += 1
    elements_restored = 0
    element_cols = _table_columns(cur, "element")
    for el in sorted(elements, key=lambda x: x.get("order_index", 0)):
        el_cols = ["soa_id", "name", "label", "description", "testrl"]
        el_cols += ["teenrl", "order_index", "created_at"]
        el_vals = [
            soa_id,
            el.get("name"),
            el.get("label"),
            el.get("description"),
            el.get("testrl"),
            el.get("teenrl"),
            el.get("order_index"),
            datetime.now(timezone.utc).isoformat(),
        ]
        if el.get("element_id") and "element_id" in element_cols:
            el_cols.append("element_id")
            el_vals.append(el.get("element_id"))
        cur.execute(
            f"INSERT INTO element ({','.join(el_cols)}) "
            f"VALUES ({','.join('?' * len(el_cols))})",
            el_vals,
        )
        elements_restored += 1
    inserted_concepts = 0
    for old_aid, concept_list in concepts_map.items():
        new_aid = activity_id_map.get(int(old_aid))
        if not new_aid:
            continue
        cur.execute("SELECT activity_uid FROM activity WHERE id=?", (new_aid,))
        row_uid = cur.fetchone()
        new_activity_uid = row_uid[0] if row_uid else None
        ac_has_soa = _table_has_columns(cur, "activity_concept", ("soa_id",))
        ac_has_actuid = _table_has_columns(cur, "activity_concept", ("activity_uid",))
        ac_has_conceptuid = _table_has_columns(
            cur, "activity_concept", ("concept_uid",)
        )
        for c in concept_list:
            code = c.get("code")
            title = c.get("title") or code
            if not code:
                continue
            concept_uid = (
                _get_next_concept_uid(cur, soa_id) if ac_has_conceptuid else None
            )
            if ac_has_soa and ac_has_actuid:
                if ac_has_conceptuid:
                    cur.execute(
                        "INSERT INTO activity_concept"
                        " (soa_id, activity_id, activity_uid, concept_uid,"
                        " concept_code, concept_title) VALUES (?,?,?,?,?,?)",
                        (
                            soa_id,
                            new_aid,
                            new_activity_uid,
                            concept_uid,
                            code,
                            title,
                        ),
                    )
                else:
                    cur.execute(
                        "INSERT INTO activity_concept"
                        " (soa_id, activity_id, activity_uid, concept_code,"
                        " concept_title) VALUES (?,?,?,?,?)",
                        (soa_id, new_aid, new_activity_uid, code, title),
                    )
            elif ac_has_actuid:
                if ac_has_conceptuid:
                    cur.execute(
                        "INSERT INTO activity_concept"
                        " (activity_id, activity_uid, concept_uid, concept_code,"
                        " concept_title) VALUES (?,?,?,?,?)",
                        (new_aid, new_activity_uid, concept_uid, code, title),
                    )
                else:
                    cur.execute(
                        "INSERT INTO activity_concept"
                        " (activity_id, activity_uid, concept_code, concept_title)"
                        " VALUES (?,?,?,?)",
                        (new_aid, new_activity_uid, code, title),
                    )
            elif ac_has_soa:
                if ac_has_conceptuid:
                    cur.execute(
                        "INSERT INTO activity_concept"
                        " (soa_id, activity_id, concept_uid, concept_code,"
                        " concept_title) VALUES (?,?,?,?,?)",
                        (soa_id, new_aid, concept_uid, code, title),
                    )
                else:
                    cur.execute(
                        "INSERT INTO activity_concept"
                        " (soa_id, activity_id, concept_code, concept_title)"
                        " VALUES (?,?,?,?)",
                        (soa_id, new_aid, code, title),
                    )
            else:
                if ac_has_conceptuid:
                    cur.execute(
                        "INSERT INTO activity_concept"
                        " (activity_id, concept_uid, concept_code, concept_title)"
                        " VALUES (?,?,?,?)",
                        (new_aid, concept_uid, code, title),
                    )
                else:
                    cur.execute(
                        "INSERT INTO activity_concept"
                        " (activity_id, concept_code, concept_title)"
                        " VALUES (?,?,?)",
                        (new_aid, code, title),
                    )
            _upsert_biomedical_concept(cur, soa_id, concept_uid, title, code)
            inserted_concepts += 1
    conn.commit()
    conn.close()
    return {
        "rollback_freeze_id": freeze_id,
        "visits_restored": encounters_written,
        "encounters_restored": encounters_written,
        "encounters_untouched": not (restore_encounters or legacy_visits),
        "instances_restored": instances_written,
        "instances_untouched": not restore_instances,
        "timelines_restored": timelines_written,
        "timelines_untouched": not restore_timelines,
        "decision_instances_restored": decisions_written,
        "decision_instances_untouched": not restore_decisions,
        "study_cells_restored": study_cells_written,
        "study_cells_untouched": not restore_study_cells,
        "safety_freeze_id": safety_id,
        "safety_freeze_label": safety_label,
        "activities_restored": len(activities),
        "cells_restored": inserted_cells,
        "cells_skipped_missing_instance": skipped_missing_instance,
        "cells_skipped": skipped_cells,
        "concept_mappings_restored": inserted_concepts,
        "elements_restored": elements_restored,
        "epochs_restored": len(snap_epochs),
        "arms_restored": len(snap_arms),
        "timings_restored": len(snap_timings),
        "objectives_restored": len(snap_objectives),
        "endpoints_restored": len(snap_endpoints),
        "code_associations_restored": len(snap_code_assocs),
    }


def _record_rollback_audit(soa_id: int, freeze_id: int, stats: dict):
    conn = _connect()
    cur = conn.cursor()
    cur.execute(
        "INSERT INTO rollback_audit"
        " (soa_id, freeze_id, performed_at, visits_restored,"
        " activities_restored, cells_restored, concepts_restored,"
        " elements_restored, cells_skipped, encounters_restored,"
        " instances_restored, timelines_restored,"
        " decision_instances_restored, study_cells_restored,"
        " safety_freeze_id) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
        (
            soa_id,
            freeze_id,
            datetime.now(timezone.utc).isoformat(),
            stats.get("visits_restored"),
            stats.get("activities_restored"),
            stats.get("cells_restored"),
            stats.get("concept_mappings_restored"),
            stats.get("elements_restored"),
            stats.get("cells_skipped"),
            stats.get("encounters_restored"),
            stats.get("instances_restored"),
            stats.get("timelines_restored"),
            stats.get("decision_instances_restored"),
            stats.get("study_cells_restored"),
            stats.get("safety_freeze_id"),
        ),
    )
    conn.commit()
    conn.close()


def _list_rollback_audit(soa_id: int) -> list:
    conn = _connect()
    cur = conn.cursor()
    cur.execute(
        "SELECT id, freeze_id, performed_at, visits_restored,"
        " activities_restored, cells_restored, concepts_restored,"
        " elements_restored, cells_skipped, encounters_restored,"
        " instances_restored, timelines_restored,"
        " decision_instances_restored, study_cells_restored,"
        " safety_freeze_id"
        " FROM rollback_audit WHERE soa_id=? ORDER BY id DESC",
        (soa_id,),
    )
    rows = [
        {
            "id": r[0],
            "freeze_id": r[1],
            "performed_at": r[2],
            "visits_restored": r[3],
            "activities_restored": r[4],
            "cells_restored": r[5],
            "concepts_restored": r[6],
            "elements_restored": r[7],
            "cells_skipped": r[8],
            "encounters_restored": r[9],
            "instances_restored": r[10],
            "timelines_restored": r[11],
            "decision_instances_restored": r[12],
            "study_cells_restored": r[13],
            "safety_freeze_id": r[14],
        }
        for r in cur.fetchall()
    ]
    conn.close()
    return rows


def _rollback_preview(soa_id: int, freeze_id: int) -> dict:
    freeze = _get_freeze(soa_id, freeze_id)
    if not freeze:
        raise HTTPException(404, "Freeze not found")
    snap = freeze["snapshot"]
    visits = snap.get("visits", [])
    activities = snap.get("activities", [])
    cells = [c for c in snap.get("cells", []) if (c.get("status") or "").strip() != ""]
    concepts_map = snap.get("activity_concepts", {}) or {}
    snap_encounters = snap.get("encounters_full")
    snap_instances = snap.get("instances")
    instance_axis = any(v.get("instance_uid") for v in visits)
    if snap_encounters is not None:
        encounters_to_restore = len(snap_encounters)
    elif instance_axis:
        encounters_to_restore = 0  # live encounters are left untouched
    else:
        encounters_to_restore = len(visits)
    return {
        "freeze_id": freeze_id,
        "version_label": freeze.get("version_label"),
        "visits_to_restore": len(visits),
        "encounters_to_restore": encounters_to_restore,
        "instances_to_restore": len(snap_instances or []),
        "timelines_to_restore": len(snap.get("schedule_timelines") or []),
        "decision_instances_to_restore": len(snap.get("decision_instances") or []),
        "study_cells_to_restore": len(snap.get("study_cells") or []),
        "activities_to_restore": len(activities),
        "cells_to_restore": len(cells),
        "concept_mappings_to_restore": sum(len(v) for v in concepts_map.values()),
    }
