"""_freeze_helpers: store (split from _freeze_helpers.py)."""

from ...errors import MALFORMED_JSON_ERRORS
from ._shared import (
    CAPTURE_VERSION,
    HTTPException,
    Optional,
    _connect,
    _table_has_columns,
    datetime,
    json,
    soa_exists,
    timezone,
)
from .capture import (
    _capture_amendments,
    _capture_arms,
    _capture_bc_properties,
    _capture_bc_surrogates,
    _capture_biomedical_concepts,
    _capture_code_associations,
    _capture_decision_instances,
    _capture_eligibility_criteria,
    _capture_encounters_full,
    _capture_endpoints,
    _capture_extension_attributes,
    _capture_instances,
    _capture_objectives,
    _capture_schedule_timelines,
    _capture_study_cells,
    _capture_timings,
)


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
    except MALFORMED_JSON_ERRORS:
        snap = {"error": "Corrupt snapshot"}
    return {
        "id": row[0],
        "version_label": row[1],
        "created_at": row[2],
        "snapshot": snap,
    }


def _create_freeze(soa_id: int, version_label: Optional[str]):
    # Lazy imports to avoid circular dependency with app.py
    from ...services.matrix import _fetch_matrix

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
    has_element_id = _table_has_columns(cur_el, "element", ("element_id",))
    cur_el.execute(
        "SELECT id,name,label,description,testrl,teenrl,order_index"
        + (",element_id" if has_element_id else "")
        + " FROM element WHERE soa_id=? ORDER BY order_index",
        (soa_id,),
    )
    elements = []
    for r in cur_el.fetchall():
        el = dict(
            id=r[0],
            name=r[1],
            label=r[2],
            description=r[3],
            testrl=r[4],
            teenrl=r[5],
            order_index=r[6],
        )
        if has_element_id:
            el["element_id"] = r[7]
        elements.append(el)
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
    eligibility_criteria = _capture_eligibility_criteria(cur, soa_id)
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
        "capture_version": CAPTURE_VERSION,
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
        "eligibility_criteria": eligibility_criteria,
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
