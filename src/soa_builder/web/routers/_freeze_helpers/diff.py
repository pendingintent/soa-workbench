"""_freeze_helpers: diff (split from _freeze_helpers.py)."""

from ._shared import (
    HTTPException,
    Optional,
)
from .store import _get_freeze


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
