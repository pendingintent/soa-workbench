"""Freeze/rollback helpers for SOA snapshots.

Extracted from app.py so the freeze router can import them directly
without the previous lazy-import circularity workaround.
"""

import json
import logging
from datetime import datetime, timezone
from typing import Optional

from fastapi import HTTPException

from ..db import _connect
from ..utils import (
    soa_exists,
    get_next_concept_uid as _get_next_concept_uid,
    table_has_columns as _table_has_columns,
)

logger = logging.getLogger("soa_builder.web.routers._freeze_helpers")


def _capture_code_associations(cur, soa_id: int) -> list:
    if not _table_has_columns(cur, "code_association", ("code_uid",)):
        return []
    cur.execute(
        "SELECT code_uid, codelist_table, codelist_code, code"
        " FROM code_association WHERE soa_id=?",
        (soa_id,),
    )
    return [
        {
            "code_uid": r[0],
            "codelist_table": r[1],
            "codelist_code": r[2],
            "code": r[3],
        }
        for r in cur.fetchall()
    ]


def _capture_arms(cur, soa_id: int) -> list:
    if not _table_has_columns(cur, "arm", ("id",)):
        return []
    cur.execute(
        "SELECT id,name,label,description,type,data_origin_type,"
        "order_index,arm_uid FROM arm WHERE soa_id=? ORDER BY order_index, id",
        (soa_id,),
    )
    return [
        {
            "id": r[0],
            "name": r[1],
            "label": r[2],
            "description": r[3],
            "type": r[4],
            "data_origin_type": r[5],
            "order_index": r[6],
            "arm_uid": r[7],
        }
        for r in cur.fetchall()
    ]


def _capture_timings(cur, soa_id: int) -> list:
    if not _table_has_columns(cur, "timing", ("id",)):
        return []
    cur.execute(
        "SELECT id,timing_uid,name,label,description,type,value,value_label,"
        "relative_to_from,relative_from_schedule_instance,"
        "relative_to_schedule_instance,window_label,window_upper,"
        "window_lower,order_index,member_of_timeline"
        " FROM timing WHERE soa_id=? ORDER BY order_index, id",
        (soa_id,),
    )
    return [
        {
            "id": r[0],
            "timing_uid": r[1],
            "name": r[2],
            "label": r[3],
            "description": r[4],
            "type": r[5],
            "value": r[6],
            "value_label": r[7],
            "relative_to_from": r[8],
            "relative_from_schedule_instance": r[9],
            "relative_to_schedule_instance": r[10],
            "window_label": r[11],
            "window_upper": r[12],
            "window_lower": r[13],
            "order_index": r[14],
            "member_of_timeline": r[15],
        }
        for r in cur.fetchall()
    ]


def _capture_objectives(cur, soa_id: int) -> list:
    if not _table_has_columns(cur, "objective", ("id",)):
        return []
    cur.execute(
        "SELECT id,objective_uid,name,label,description,text,"
        "level_code_uid,order_index"
        " FROM objective WHERE soa_id=? ORDER BY order_index, id",
        (soa_id,),
    )
    return [
        {
            "id": r[0],
            "objective_uid": r[1],
            "name": r[2],
            "label": r[3],
            "description": r[4],
            "text": r[5],
            "level_code_uid": r[6],
            "order_index": r[7],
        }
        for r in cur.fetchall()
    ]


def _capture_endpoints(cur, soa_id: int) -> list:
    if not _table_has_columns(cur, "endpoint", ("id",)):
        return []
    cur.execute(
        "SELECT id,endpoint_uid,objective_uid,name,label,description,"
        "text,purpose,level_code_uid,order_index"
        " FROM endpoint WHERE soa_id=? ORDER BY order_index, id",
        (soa_id,),
    )
    return [
        {
            "id": r[0],
            "endpoint_uid": r[1],
            "objective_uid": r[2],
            "name": r[3],
            "label": r[4],
            "description": r[5],
            "text": r[6],
            "purpose": r[7],
            "level_code_uid": r[8],
            "order_index": r[9],
        }
        for r in cur.fetchall()
    ]


def _capture_encounters_full(cur, soa_id: int) -> list:
    if not _table_has_columns(cur, "visit", ("id",)):
        return []
    base_cols = "id,name,label,description,order_index,encounter_uid,type"
    extra_cols = []
    for col in (
        "environmentalSettings",
        "contactModes",
        "transitionStartRule",
        "transitionEndRule",
        "scheduledAtId",
    ):
        if _table_has_columns(cur, "visit", (col,)):
            extra_cols.append(col)
    select = base_cols
    if extra_cols:
        select += "," + ",".join(extra_cols)
    cur.execute(
        f"SELECT {select} FROM visit WHERE soa_id=? ORDER BY order_index, id",
        (soa_id,),
    )
    col_names = [
        "id",
        "name",
        "label",
        "description",
        "order_index",
        "encounter_uid",
        "type",
    ] + extra_cols
    return [dict(zip(col_names, r)) for r in cur.fetchall()]


def _capture_study_cells(cur, soa_id: int) -> list:
    if not _table_has_columns(cur, "study_cell", ("study_cell_uid",)):
        return []
    cur.execute(
        "SELECT study_cell_uid,arm_uid,epoch_uid,order_index"
        " FROM study_cell WHERE soa_id=? ORDER BY order_index, id",
        (soa_id,),
    )
    return [
        {
            "study_cell_uid": r[0],
            "arm_uid": r[1],
            "epoch_uid": r[2],
            "order_index": r[3],
        }
        for r in cur.fetchall()
    ]


def _capture_schedule_timelines(cur, soa_id: int) -> list:
    if not _table_has_columns(cur, "schedule_timelines", ("schedule_timeline_uid",)):
        return []
    cur.execute(
        "SELECT schedule_timeline_uid,name,label,description,"
        "main_timeline,entry_condition,entry_id,exit_id"
        " FROM schedule_timelines WHERE soa_id=? ORDER BY id",
        (soa_id,),
    )
    return [
        {
            "schedule_timeline_uid": r[0],
            "name": r[1],
            "label": r[2],
            "description": r[3],
            "main_timeline": r[4],
            "entry_condition": r[5],
            "entry_id": r[6],
            "exit_id": r[7],
        }
        for r in cur.fetchall()
    ]


def _capture_instances(cur, soa_id: int) -> list:
    if not _table_has_columns(cur, "instances", ("instance_uid",)):
        return []
    cur.execute(
        "SELECT instance_uid,name,label,description,epoch_uid,"
        "timeline_id,encounter_uid,member_of_timeline"
        " FROM instances WHERE soa_id=? ORDER BY id",
        (soa_id,),
    )
    return [
        {
            "instance_uid": r[0],
            "name": r[1],
            "label": r[2],
            "description": r[3],
            "epoch_uid": r[4],
            "timeline_id": r[5],
            "encounter_uid": r[6],
            "member_of_timeline": r[7],
        }
        for r in cur.fetchall()
    ]


def _capture_decision_instances(cur, soa_id: int) -> list:
    if not _table_has_columns(cur, "decision_instances", ("instance_uid",)):
        return []
    cur.execute(
        "SELECT instance_uid,name,label,description,epoch_uid,"
        "member_of_timeline"
        " FROM decision_instances WHERE soa_id=? ORDER BY id",
        (soa_id,),
    )
    return [
        {
            "instance_uid": r[0],
            "name": r[1],
            "label": r[2],
            "description": r[3],
            "epoch_uid": r[4],
            "member_of_timeline": r[5],
        }
        for r in cur.fetchall()
    ]


def _capture_bc_surrogates(cur, soa_id: int) -> list:
    if not _table_has_columns(cur, "biomedical_concept_surrogate", ("surrogate_uid",)):
        return []
    cur.execute(
        "SELECT surrogate_uid,name,label,description,reference"
        " FROM biomedical_concept_surrogate WHERE soa_id=? ORDER BY id",
        (soa_id,),
    )
    return [
        {
            "surrogate_uid": r[0],
            "name": r[1],
            "label": r[2],
            "description": r[3],
            "reference": r[4],
        }
        for r in cur.fetchall()
    ]


def _capture_biomedical_concepts(cur, soa_id: int) -> list:
    if not _table_has_columns(cur, "biomedical_concept", ("biomedical_concept_uid",)):
        return []
    cur.execute(
        "SELECT bc.biomedical_concept_uid, bc.name, bc.label, bc.code, c.code"
        " FROM biomedical_concept bc"
        " LEFT JOIN alias_code a"
        "   ON bc.code = a.alias_code_uid AND bc.soa_id = a.soa_id"
        " LEFT JOIN code c"
        "   ON a.standard_code = c.code_uid AND a.soa_id = c.soa_id"
        " WHERE bc.soa_id=? ORDER BY bc.id",
        (soa_id,),
    )
    return [
        {
            "biomedical_concept_uid": r[0],
            "name": r[1],
            "label": r[2],
            "code": r[3],
            "concept_id": r[4],
        }
        for r in cur.fetchall()
    ]


def _capture_bc_properties(cur, soa_id: int) -> list:
    if not _table_has_columns(
        cur,
        "biomedical_concept_property",
        ("biomedical_concept_property_uid",),
    ):
        return []
    cur.execute(
        "SELECT biomedical_concept_property_uid,name,label,"
        "isRequired,isEnabled,datatype"
        " FROM biomedical_concept_property WHERE soa_id=? ORDER BY id",
        (soa_id,),
    )
    return [
        {
            "biomedical_concept_property_uid": r[0],
            "name": r[1],
            "label": r[2],
            "isRequired": r[3],
            "isEnabled": r[4],
            "datatype": r[5],
        }
        for r in cur.fetchall()
    ]


def _capture_amendments(cur, soa_id: int) -> list:
    if not _table_has_columns(cur, "study_amendment", ("amendment_uid",)):
        return []
    cur.execute(
        "SELECT amendment_uid,name,number,summary,label,description"
        " FROM study_amendment WHERE soa_id=? ORDER BY id",
        (soa_id,),
    )
    return [
        {
            "amendment_uid": r[0],
            "name": r[1],
            "number": r[2],
            "summary": r[3],
            "label": r[4],
            "description": r[5],
        }
        for r in cur.fetchall()
    ]


def _capture_extension_attributes(cur, soa_id: int) -> list:
    if not _table_has_columns(
        cur, "activity_concept_dss", ("extension_attribute_uid",)
    ):
        return []
    cur.execute(
        "SELECT extension_attribute_uid,concept_code,dss_href,"
        "dss_domain,dss_display"
        " FROM activity_concept_dss WHERE soa_id=?"
        " AND extension_attribute_uid IS NOT NULL ORDER BY id",
        (soa_id,),
    )
    return [
        {
            "extension_attribute_uid": r[0],
            "concept_code": r[1],
            "dss_href": r[2],
            "dss_domain": r[3],
            "dss_display": r[4],
        }
        for r in cur.fetchall()
    ]


def _diff_entity_list(
    l_list: list,
    r_list: list,
    uid_key: str,
    display_fields: list = None,
) -> tuple:
    """Return (added, removed, changed) for two lists of entity dicts.

    uid_key identifies the entity across snapshots.
    display_fields limits which fields are checked for changes.
    """
    l_map = {
        e[uid_key]: e for e in (l_list or []) if isinstance(e, dict) and e.get(uid_key)
    }
    r_map = {
        e[uid_key]: e for e in (r_list or []) if isinstance(e, dict) and e.get(uid_key)
    }
    added = [r_map[k] for k in r_map.keys() - l_map.keys()]
    removed = [l_map[k] for k in l_map.keys() - r_map.keys()]
    changed = []
    for k in r_map.keys() & l_map.keys():
        fields = display_fields or list(set(l_map[k].keys()) | set(r_map[k].keys()))
        field_diffs = {
            f: {"old": l_map[k].get(f), "new": r_map[k].get(f)}
            for f in fields
            if l_map[k].get(f) != r_map[k].get(f)
        }
        if field_diffs:
            changed.append({"uid": k, "changes": field_diffs})
    return added, removed, changed


def _list_freezes(soa_id: int):
    conn = _connect()
    cur = conn.cursor()
    cur.execute(
        "SELECT f.id, f.version_label, f.created_at, a.amendment_uid"
        " FROM soa_freeze f"
        " LEFT JOIN study_amendment a"
        " ON a.freeze_id = f.id AND a.soa_id = f.soa_id"
        " WHERE f.soa_id=? ORDER BY f.id DESC",
        (soa_id,),
    )
    rows = [
        dict(
            id=r[0],
            version_label=r[1],
            created_at=r[2],
            amendment_uid=r[3],
        )
        for r in cur.fetchall()
    ]
    conn.close()
    return rows


def _get_freeze(soa_id: int, freeze_id: int):
    conn = _connect()
    cur = conn.cursor()
    cur.execute(
        "SELECT id, version_label, created_at, snapshot_json"
        " FROM soa_freeze WHERE id=? AND soa_id=?",
        (freeze_id, soa_id),
    )
    row = cur.fetchone()
    conn.close()
    if not row:
        return None
    try:
        snap = json.loads(row[3])
    except Exception:
        snap = {"error": "Corrupt snapshot"}
    return {
        "id": row[0],
        "version_label": row[1],
        "created_at": row[2],
        "snapshot": snap,
    }


def _create_freeze(soa_id: int, version_label: Optional[str]):
    # Lazy imports to avoid circular dependency with app.py
    from ..app import _fetch_matrix

    if not soa_exists(soa_id):
        raise HTTPException(404, "SOA not found")
    conn = _connect()
    cur = conn.cursor()
    cur.execute("SELECT version_label FROM soa_freeze WHERE soa_id=?", (soa_id,))
    existing_labels = {r[0] for r in cur.fetchall()}
    if not version_label or not version_label.strip():
        n = 1
        while f"v{n}" in existing_labels:
            n += 1
        version_label = f"v{n}"
    else:
        version_label = version_label.strip()
    if version_label in existing_labels:
        raise HTTPException(400, "Version label already exists for this SOA")
    cur.execute(
        "SELECT name, created_at, study_id, study_label, study_description"
        " FROM soa WHERE id=?",
        (soa_id,),
    )
    row = cur.fetchone()
    soa_name = row[0] if row else f"SOA {soa_id}"
    study_id_val = row[2] if row else None
    study_label_val = row[3] if row else None
    study_description_val = row[4] if row else None
    visits, activities, cells = _fetch_matrix(soa_id)
    conn2 = _connect()
    cur2 = conn2.cursor()
    cur2.execute(
        "SELECT id,name,order_index,epoch_seq,epoch_label,"
        "epoch_description,type,epoch_uid"
        " FROM epoch WHERE soa_id=? ORDER BY order_index",
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
            type=r[6],
            epoch_uid=r[7],
        )
        for r in cur2.fetchall()
    ]
    conn2.close()
    conn_el = _connect()
    cur_el = conn_el.cursor()
    cur_el.execute(
        "SELECT id,name,label,description,testrl,teenrl,order_index"
        " FROM element WHERE soa_id=? ORDER BY order_index",
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
        )
        for r in cur_el.fetchall()
    ]
    conn_el.close()
    activity_ids = [a["id"] for a in activities]
    concepts_map: dict = {}
    if activity_ids:
        placeholders = ",".join("?" for _ in activity_ids)
        has_uid = _table_has_columns(cur, "activity_concept", ("concept_uid",))
        if _table_has_columns(cur, "activity_concept", ("soa_id",)):
            if has_uid:
                cur.execute(
                    f"SELECT activity_id, concept_code, concept_title, concept_uid"
                    f" FROM activity_concept WHERE soa_id=? AND activity_id IN ({placeholders})",
                    [soa_id] + activity_ids,
                )
            else:
                cur.execute(
                    f"SELECT activity_id, concept_code, concept_title, NULL as concept_uid"
                    f" FROM activity_concept WHERE soa_id=? AND activity_id IN ({placeholders})",
                    [soa_id] + activity_ids,
                )
        else:
            if has_uid:
                cur.execute(
                    f"SELECT activity_id, concept_code, concept_title, concept_uid"
                    f" FROM activity_concept WHERE activity_id IN ({placeholders})",
                    activity_ids,
                )
            else:
                cur.execute(
                    f"SELECT activity_id, concept_code, concept_title, NULL as concept_uid"
                    f" FROM activity_concept WHERE activity_id IN ({placeholders})",
                    activity_ids,
                )
        for aid, code, title, cuid in cur.fetchall():
            entry = {"code": code, "title": title}
            if cuid:
                entry["uid"] = cuid
            concepts_map.setdefault(aid, []).append(entry)
    code_associations = _capture_code_associations(cur, soa_id)
    arms = _capture_arms(cur, soa_id)
    timings = _capture_timings(cur, soa_id)
    objectives = _capture_objectives(cur, soa_id)
    endpoints = _capture_endpoints(cur, soa_id)
    encounters_full = _capture_encounters_full(cur, soa_id)
    study_cells = _capture_study_cells(cur, soa_id)
    schedule_timelines = _capture_schedule_timelines(cur, soa_id)
    instances = _capture_instances(cur, soa_id)
    decision_instances = _capture_decision_instances(cur, soa_id)
    bc_surrogates = _capture_bc_surrogates(cur, soa_id)
    biomedical_concepts = _capture_biomedical_concepts(cur, soa_id)
    bc_properties = _capture_bc_properties(cur, soa_id)
    amendments = _capture_amendments(cur, soa_id)
    extension_attributes = _capture_extension_attributes(cur, soa_id)
    snapshot = {
        "soa_id": soa_id,
        "soa_name": soa_name,
        "study_id": study_id_val,
        "study_label": study_label_val,
        "study_description": study_description_val,
        "version_label": version_label,
        "frozen_at": datetime.now(timezone.utc).isoformat(),
        "epochs": epochs,
        "elements": elements,
        "visits": visits,
        "activities": activities,
        "cells": cells,
        "activity_concepts": concepts_map,
        "code_associations": code_associations,
        "arms": arms,
        "timings": timings,
        "objectives": objectives,
        "endpoints": endpoints,
        "encounters_full": encounters_full,
        "study_cells": study_cells,
        "schedule_timelines": schedule_timelines,
        "instances": instances,
        "decision_instances": decision_instances,
        "bc_surrogates": bc_surrogates,
        "biomedical_concepts": biomedical_concepts,
        "bc_properties": bc_properties,
        "amendments": amendments,
        "extension_attributes": extension_attributes,
    }
    snap_json = json.dumps(snapshot)
    cur.execute(
        "INSERT INTO soa_freeze (soa_id, version_label, created_at, snapshot_json)"
        " VALUES (?,?,?,?)",
        (
            soa_id,
            version_label,
            datetime.now(timezone.utc).isoformat(),
            snap_json,
        ),
    )
    fid = cur.lastrowid
    conn.commit()
    conn.close()
    return fid, version_label


def _diff_freezes(soa_id: int, left_id: int, right_id: int):
    return _diff_freezes_limited(soa_id, left_id, right_id, limit=None)


def _diff_freezes_limited(
    soa_id: int, left_id: int, right_id: int, limit: Optional[int]
):
    left = _get_freeze(soa_id, left_id)
    right = _get_freeze(soa_id, right_id)
    if not left or not right:
        raise HTTPException(404, "Freeze not found")
    l_snap = left["snapshot"]
    r_snap = right["snapshot"]
    l_vis = {
        str(v["id"]): v
        for v in l_snap.get("visits", [])
        if isinstance(v, dict) and "id" in v
    }
    r_vis = {
        str(v["id"]): v
        for v in r_snap.get("visits", [])
        if isinstance(v, dict) and "id" in v
    }
    visits_added_all = [r_vis[k] for k in r_vis.keys() - l_vis.keys()]
    visits_removed_all = [l_vis[k] for k in l_vis.keys() - r_vis.keys()]
    l_act = {
        str(a["id"]): a
        for a in l_snap.get("activities", [])
        if isinstance(a, dict) and "id" in a
    }
    r_act = {
        str(a["id"]): a
        for a in r_snap.get("activities", [])
        if isinstance(a, dict) and "id" in a
    }
    acts_added_all = [r_act[k] for k in r_act.keys() - l_act.keys()]
    acts_removed_all = [l_act[k] for k in l_act.keys() - r_act.keys()]

    def _cell_key(cell: dict):
        if not isinstance(cell, dict):
            return None
        activity_id = cell.get("activity_id")
        if activity_id is None:
            return None
        if cell.get("instance_id") is not None:
            return ("instance", int(cell["instance_id"]), int(activity_id))
        if cell.get("visit_id") is not None:
            return ("visit", int(cell["visit_id"]), int(activity_id))
        return None

    def _normalize_cell(cell: dict) -> dict:
        axis_type = (
            "instance"
            if cell.get("instance_id") is not None
            else "visit"
            if cell.get("visit_id") is not None
            else None
        )
        axis_id = None
        if axis_type == "instance":
            axis_id = cell.get("instance_id")
        elif axis_type == "visit":
            axis_id = cell.get("visit_id")
        return {
            "axis_type": axis_type,
            "axis_id": axis_id,
            "instance_id": cell.get("instance_id"),
            "visit_id": cell.get("visit_id"),
            "activity_id": cell.get("activity_id"),
            "status": cell.get("status"),
        }

    def _build_cell_map(snapshot_cells):
        mapped = {}
        for raw in snapshot_cells or []:
            key = _cell_key(raw)
            if not key:
                continue
            mapped[key] = _normalize_cell(raw)
        return mapped

    l_cells = _build_cell_map(l_snap.get("cells", []))
    r_cells = _build_cell_map(r_snap.get("cells", []))
    cells_added_all = [r_cells[k] for k in r_cells.keys() - l_cells.keys()]
    cells_removed_all = [l_cells[k] for k in l_cells.keys() - r_cells.keys()]
    cells_changed_all = []
    for k in r_cells.keys() & l_cells.keys():
        if r_cells[k].get("status") != l_cells[k].get("status"):
            cells_changed_all.append(
                {
                    "axis_type": l_cells[k].get("axis_type"),
                    "axis_id": l_cells[k].get("axis_id"),
                    "visit_id": l_cells[k].get("visit_id"),
                    "instance_id": l_cells[k].get("instance_id"),
                    "activity_id": l_cells[k].get("activity_id"),
                    "old_status": l_cells[k].get("status"),
                    "new_status": r_cells[k].get("status"),
                }
            )
    l_concepts_map = l_snap.get("activity_concepts", {}) or {}
    r_concepts_map = r_snap.get("activity_concepts", {}) or {}
    concept_changes_all = []
    all_aids = set(map(str, l_concepts_map.keys())) | set(
        map(str, r_concepts_map.keys())
    )

    def _get_concept_list(m, key):
        if key in m:
            return m[key] or []
        if key.isdigit() and int(key) in m:
            return m[int(key)] or []
        return []

    for aid in all_aids:
        la = _get_concept_list(l_concepts_map, aid)
        ra = _get_concept_list(r_concepts_map, aid)
        l_set = {c["code"] for c in la if isinstance(c, dict)}
        r_set = {c["code"] for c in ra if isinstance(c, dict)}
        added = sorted(list(r_set - l_set))
        removed = sorted(list(l_set - r_set))
        title_changes = []
        for code in sorted(list(l_set & r_set)):
            l_title = next((c["title"] for c in la if c.get("code") == code), None)
            r_title = next((c["title"] for c in ra if c.get("code") == code), None)
            if l_title is not None and r_title is not None and l_title != r_title:
                title_changes.append(
                    {"code": code, "old_title": l_title, "new_title": r_title}
                )
        titles = {
            c["code"]: c.get("title") or c["code"]
            for c in la + ra
            if isinstance(c, dict) and c.get("code")
        }
        if added or removed or title_changes:
            concept_changes_all.append(
                {
                    "activity_id": aid,
                    "activity_name": (
                        r_act.get(aid, {}).get("name")
                        or l_act.get(aid, {}).get("name")
                        or f"Activity {aid}"
                    ),
                    "added": added,
                    "removed": removed,
                    "title_changes": title_changes,
                    "titles": titles,
                }
            )

    # --- Per-entity diffs for all USDM entity classes ---
    _ENTITY_SPECS = [
        (
            "epochs",
            "epoch_uid",
            ["name", "epoch_label", "epoch_description", "type"],
        ),
        (
            "arms",
            "arm_uid",
            ["name", "label", "description", "type"],
        ),
        (
            "elements",
            "name",
            ["label", "description", "testrl", "teenrl"],
        ),
        (
            "timings",
            "timing_uid",
            ["name", "value", "window_lower", "window_upper", "type"],
        ),
        (
            "objectives",
            "objective_uid",
            ["text", "level_code_uid"],
        ),
        (
            "endpoints",
            "endpoint_uid",
            ["text", "objective_uid", "level_code_uid"],
        ),
        (
            "encounters_full",
            "encounter_uid",
            ["name", "label", "type", "contactModes"],
        ),
        (
            "study_cells",
            "study_cell_uid",
            ["arm_uid", "epoch_uid"],
        ),
        (
            "schedule_timelines",
            "schedule_timeline_uid",
            ["name", "main_timeline", "entry_condition"],
        ),
        (
            "instances",
            "instance_uid",
            ["name", "epoch_uid", "encounter_uid", "member_of_timeline"],
        ),
        (
            "decision_instances",
            "instance_uid",
            ["name", "epoch_uid", "member_of_timeline"],
        ),
        (
            "bc_surrogates",
            "surrogate_uid",
            ["name", "label", "reference"],
        ),
        (
            "biomedical_concepts",
            "biomedical_concept_uid",
            ["name", "label", "code", "concept_id"],
        ),
        (
            "bc_properties",
            "biomedical_concept_property_uid",
            ["name", "isRequired", "isEnabled", "datatype"],
        ),
        (
            "amendments",
            "amendment_uid",
            ["name", "number", "summary"],
        ),
        (
            "extension_attributes",
            "extension_attribute_uid",
            ["dss_href", "dss_domain", "dss_display"],
        ),
    ]

    entity_diffs_all = {}
    for snap_key, uid_key, fields in _ENTITY_SPECS:
        l_ents = l_snap.get(snap_key, []) or []
        r_ents = r_snap.get(snap_key, []) or []
        ea, er, ec = _diff_entity_list(l_ents, r_ents, uid_key, fields)
        entity_diffs_all[snap_key] = {
            "added": ea,
            "removed": er,
            "changed": ec,
            "data_unavailable": snap_key not in l_snap or snap_key not in r_snap,
        }

    def _truncate(lst):
        if limit and limit > 0 and len(lst) > limit:
            return lst[:limit], True
        return lst, False

    visits_added, visits_added_trunc = _truncate(visits_added_all)
    visits_removed, visits_removed_trunc = _truncate(visits_removed_all)
    acts_added, acts_added_trunc = _truncate(acts_added_all)
    acts_removed, acts_removed_trunc = _truncate(acts_removed_all)
    cells_added, cells_added_trunc = _truncate(cells_added_all)
    cells_removed, cells_removed_trunc = _truncate(cells_removed_all)
    cells_changed, cells_changed_trunc = _truncate(cells_changed_all)
    concept_changes, concept_changes_trunc = _truncate(concept_changes_all)

    entity_diffs = {}
    entity_meta = {}
    for snap_key, _uid_key, _fields in _ENTITY_SPECS:
        raw = entity_diffs_all[snap_key]
        ea, ea_trunc = _truncate(raw["added"])
        er, er_trunc = _truncate(raw["removed"])
        ec, ec_trunc = _truncate(raw["changed"])
        entity_diffs[snap_key] = {
            "added": ea,
            "removed": er,
            "changed": ec,
            "data_unavailable": raw["data_unavailable"],
        }
        entity_meta[snap_key] = {
            "added_total": len(raw["added"]),
            "removed_total": len(raw["removed"]),
            "changed_total": len(raw["changed"]),
            "added_truncated": ea_trunc,
            "removed_truncated": er_trunc,
            "changed_truncated": ec_trunc,
        }

    meta = {
        "limit": limit,
        "visits": {
            "added_total": len(visits_added_all),
            "removed_total": len(visits_removed_all),
            "added_truncated": visits_added_trunc,
            "removed_truncated": visits_removed_trunc,
        },
        "activities": {
            "added_total": len(acts_added_all),
            "removed_total": len(acts_removed_all),
            "added_truncated": acts_added_trunc,
            "removed_truncated": acts_removed_trunc,
        },
        "cells": {
            "added_total": len(cells_added_all),
            "removed_total": len(cells_removed_all),
            "changed_total": len(cells_changed_all),
            "added_truncated": cells_added_trunc,
            "removed_truncated": cells_removed_trunc,
            "changed_truncated": cells_changed_trunc,
        },
        "concepts": {
            "changes_total": len(concept_changes_all),
            "changes_truncated": concept_changes_trunc,
        },
        "entities": entity_meta,
    }
    return {
        "left": {
            "id": left["id"],
            "label": left["version_label"],
            "created_at": left["created_at"],
        },
        "right": {
            "id": right["id"],
            "label": right["version_label"],
            "created_at": right["created_at"],
        },
        "visits": {"added": visits_added, "removed": visits_removed},
        "activities": {"added": acts_added, "removed": acts_removed},
        "cells": {
            "added": cells_added,
            "removed": cells_removed,
            "changed": cells_changed,
        },
        "concepts": concept_changes,
        "entities": entity_diffs,
        "meta": meta,
    }


def _rollback_freeze(soa_id: int, freeze_id: int) -> dict:
    # Lazy import to avoid circular dependency with app.py
    from ..app import _upsert_biomedical_concept

    freeze = _get_freeze(soa_id, freeze_id)
    if not freeze:
        raise HTTPException(404, "Freeze not found")
    snap = freeze["snapshot"]
    if snap.get("soa_id") != soa_id:
        raise HTTPException(400, "Snapshot SoA mismatch")
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
    conn = _connect()
    cur = conn.cursor()
    cur.execute("DELETE FROM matrix_cells WHERE soa_id=?", (soa_id,))
    cur.execute(
        "DELETE FROM activity_concept WHERE activity_id IN"
        " (SELECT id FROM activity WHERE soa_id=? )",
        (soa_id,),
    )
    cur.execute("DELETE FROM biomedical_concept WHERE soa_id=?", (soa_id,))
    # Cascade: BCPs/RCs are rebuilt on the next populate; wipe them here so
    # they do not orphan when biomedical_concept/alias_code/code are reset.
    cur.execute("DELETE FROM bcp_response_code WHERE soa_id=?", (soa_id,))
    cur.execute("DELETE FROM biomedical_concept_property WHERE soa_id=?", (soa_id,))
    cur.execute("DELETE FROM alias_code WHERE soa_id=?", (soa_id,))
    cur.execute("DELETE FROM code WHERE soa_id=?", (soa_id,))
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
    cur.execute("DELETE FROM activity WHERE soa_id=?", (soa_id,))
    cur.execute("DELETE FROM visit WHERE soa_id=?", (soa_id,))
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
        new_id = cur.lastrowid
        visit_id_map[v.get("id")] = new_id
    activity_id_map = {}
    for a in sorted(activities, key=lambda x: x.get("order_index", 0)):
        cur.execute(
            "INSERT INTO activity (soa_id,name,order_index) VALUES (?,?,?)",
            (soa_id, a.get("name"), a.get("order_index")),
        )
        new_id = cur.lastrowid
        activity_id_map[a.get("id")] = new_id
    inserted_cells = 0
    for c in cells:
        old_vid = c.get("visit_id")
        old_aid = c.get("activity_id")
        status = c.get("status", "").strip()
        if status == "":
            continue
        vid = visit_id_map.get(old_vid)
        aid = activity_id_map.get(old_aid)
        if vid and aid:
            cur.execute(
                "INSERT INTO matrix_cells (soa_id, visit_id, activity_id, status)"
                " VALUES (?,?,?,?)",
                (soa_id, vid, aid, status),
            )
            inserted_cells += 1
    elements_restored = 0
    for el in sorted(elements, key=lambda x: x.get("order_index", 0)):
        cur.execute(
            "INSERT INTO element"
            " (soa_id,name,label,description,testrl,teenrl,order_index,created_at)"
            " VALUES (?,?,?,?,?,?,?,?)",
            (
                soa_id,
                el.get("name"),
                el.get("label"),
                el.get("description"),
                el.get("testrl"),
                el.get("teenrl"),
                el.get("order_index"),
                datetime.now(timezone.utc).isoformat(),
            ),
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
        "visits_restored": len(visits),
        "activities_restored": len(activities),
        "cells_restored": inserted_cells,
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
        " elements_restored) VALUES (?,?,?,?,?,?,?,?)",
        (
            soa_id,
            freeze_id,
            datetime.now(timezone.utc).isoformat(),
            stats.get("visits_restored"),
            stats.get("activities_restored"),
            stats.get("cells_restored"),
            stats.get("concept_mappings_restored"),
            stats.get("elements_restored"),
        ),
    )
    conn.commit()
    conn.close()


def _list_rollback_audit(soa_id: int) -> list:
    conn = _connect()
    cur = conn.cursor()
    cur.execute(
        "SELECT id, freeze_id, performed_at, visits_restored,"
        " activities_restored, cells_restored, concepts_restored"
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
        }
        for r in cur.fetchall()
    ]
    conn.close()
    return rows


def _delete_freeze(soa_id: int, freeze_id: int) -> bool:
    """Delete a freeze row. Returns True if a row was deleted, False otherwise."""
    conn = _connect()
    cur = conn.cursor()
    cur.execute(
        "DELETE FROM soa_freeze WHERE id=? AND soa_id=?",
        (freeze_id, soa_id),
    )
    deleted = cur.rowcount > 0
    conn.commit()
    conn.close()
    return deleted


def _rollback_preview(soa_id: int, freeze_id: int) -> dict:
    freeze = _get_freeze(soa_id, freeze_id)
    if not freeze:
        raise HTTPException(404, "Freeze not found")
    snap = freeze["snapshot"]
    visits = snap.get("visits", [])
    activities = snap.get("activities", [])
    cells = [c for c in snap.get("cells", []) if c.get("status", "").strip() != ""]
    concepts_map = snap.get("activity_concepts", {}) or {}
    return {
        "freeze_id": freeze_id,
        "version_label": freeze.get("version_label"),
        "visits_to_restore": len(visits),
        "activities_to_restore": len(activities),
        "cells_to_restore": len(cells),
        "concept_mappings_to_restore": sum(len(v) for v in concepts_map.values()),
    }
