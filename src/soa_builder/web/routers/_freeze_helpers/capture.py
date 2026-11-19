"""_freeze_helpers: capture (split from _freeze_helpers.py)."""

from ._shared import (
    _table_has_columns,
)


def _table_columns(cur, table: str) -> set:
    """Return the set of column names for ``table``."""
    cur.execute(f"PRAGMA table_info({table})")
    return {row[1] for row in cur.fetchall()}


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
        "SELECT study_cell_uid,arm_uid,epoch_uid,order_index,element_uid"
        " FROM study_cell WHERE soa_id=? ORDER BY order_index, id",
        (soa_id,),
    )
    return [
        {
            "study_cell_uid": r[0],
            "arm_uid": r[1],
            "epoch_uid": r[2],
            "order_index": r[3],
            "element_uid": r[4],
        }
        for r in cur.fetchall()
    ]


_TIMELINE_CAPTURE_COLUMNS = (
    "schedule_timeline_uid",
    "name",
    "label",
    "description",
    "main_timeline",
    "entry_condition",
    "entry_id",
    "exit_id",
    "order_index",
)
# Added by migration m0116; captured only when the columns exist.
_TIMELINE_DURATION_COLUMNS = (
    "planned_duration_text",
    "planned_duration_value",
    "planned_duration_unit_code",
    "planned_duration_unit_decode",
    "planned_duration_unit_version",
    "planned_duration_will_vary",
    "planned_duration_reason",
)


def _capture_schedule_timelines(cur, soa_id: int) -> list:
    if not _table_has_columns(cur, "schedule_timelines", ("schedule_timeline_uid",)):
        return []
    columns = list(_TIMELINE_CAPTURE_COLUMNS)
    if _table_has_columns(cur, "schedule_timelines", _TIMELINE_DURATION_COLUMNS):
        columns += _TIMELINE_DURATION_COLUMNS
    cur.execute(
        f"SELECT {','.join(columns)}"
        " FROM schedule_timelines WHERE soa_id=? ORDER BY id",
        (soa_id,),
    )
    return [dict(zip(columns, row)) for row in cur.fetchall()]


def _capture_instances(cur, soa_id: int) -> list:
    if not _table_has_columns(cur, "instances", ("instance_uid",)):
        return []
    cur.execute(
        "SELECT instance_uid,name,label,description,epoch_uid,"
        "timeline_id,encounter_uid,member_of_timeline,"
        "default_condition_uid,timeline_exit_id,order_index"
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
            "default_condition_uid": r[8],
            "timeline_exit_id": r[9],
            "order_index": r[10],
        }
        for r in cur.fetchall()
    ]


def _capture_decision_instances(cur, soa_id: int) -> list:
    if not _table_has_columns(cur, "decision_instances", ("instance_uid",)):
        return []
    cur.execute(
        "SELECT instance_uid,name,label,description,epoch_uid,"
        "member_of_timeline,default_condition_uid,order_index"
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
            "default_condition_uid": r[6],
            "order_index": r[7],
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


_ELIGIBILITY_COLUMNS = (
    "criterion_uid",
    "item_uid",
    "name",
    "label",
    "description",
    "identifier",
    "category_code",
    "category_decode",
    "category_version",
    "text",
    "order_index",
    "created_at",
)


def _capture_eligibility_criteria(cur, soa_id: int) -> list:
    """Eligibility criteria rows (migration m0118); [] before it ran."""
    if not _table_has_columns(cur, "eligibility_criterion", ("criterion_uid",)):
        return []
    cur.execute(
        f"SELECT {','.join(_ELIGIBILITY_COLUMNS)} FROM eligibility_criterion"
        " WHERE soa_id=? ORDER BY order_index, id",
        (soa_id,),
    )
    return [dict(zip(_ELIGIBILITY_COLUMNS, row)) for row in cur.fetchall()]
