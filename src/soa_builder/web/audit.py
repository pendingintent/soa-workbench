"""Audit trail recorders: one row per entity create/update/delete.

Every ``_record_<entity>_audit`` keeps its historical name and signature
and delegates to ``_record_audit``. Recording an audit row must never
break the request that triggered it, so failures are swallowed, but
they are logged at ERROR and counted: ``tests/conftest.py`` fails any
test that leaves the counter raised.
"""

import json
import logging
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

from .db import _connect

logger = logging.getLogger("soa_builder.audit")

_failures = 0


def audit_failure_count() -> int:
    """Number of audit rows that failed to record in this process."""
    return _failures


def reset_audit_failures() -> None:
    global _failures
    _failures = 0


def _note_failure(label: str, exc: Exception) -> None:
    global _failures
    _failures += 1
    logger.error("Failed recording %s audit: %s", label, exc)


def _now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _dump(value: Optional[Dict[str, Any]]) -> Optional[str]:
    """JSON for before/after; empty or missing values are stored as NULL."""
    return json.dumps(value) if value else None


def _record_audit(
    table: str,
    entity_col: str,
    label: str,
    soa_id: int,
    action: str,
    entity_id: Optional[int],
    before: Optional[Dict[str, Any]] = None,
    after: Optional[Dict[str, Any]] = None,
    cur=None,
) -> None:
    """Insert one audit row into ``table``.

    With ``cur`` the row joins the caller's transaction (the caller
    commits or rolls back); otherwise it is written and committed on its
    own connection.
    """
    sql = (
        f"INSERT INTO {table} (soa_id, {entity_col}, action, before_json,"
        " after_json, performed_at) VALUES (?,?,?,?,?,?)"
    )
    values = (soa_id, entity_id, action, _dump(before), _dump(after), _now())
    try:
        if cur is not None:
            cur.execute(sql, values)
            return
        conn = _connect()
        try:
            conn.execute(sql, values)
            conn.commit()
        finally:
            conn.close()
    except Exception as exc:  # audit must never break the request
        _note_failure(label, exc)


def _record_reorder_audit(
    soa_id: int,
    entity_type: str,
    old_order: List[int],
    new_order: List[int],
):
    try:
        conn = _connect()
        try:
            conn.execute(
                "INSERT INTO reorder_audit (soa_id, entity_type, old_order_json,"
                " new_order_json, performed_at) VALUES (?,?,?,?,?)",
                (
                    soa_id,
                    entity_type,
                    json.dumps(old_order),
                    json.dumps(new_order),
                    _now(),
                ),
            )
            conn.commit()
        finally:
            conn.close()
    except Exception as exc:  # audit must never break the request
        _note_failure("reorder", exc)


def _record_arm_audit(
    soa_id: int,
    action: str,
    arm_id: Optional[int],
    before: Optional[Dict[str, Any]] = None,
    after: Optional[Dict[str, Any]] = None,
):
    _record_audit("arm_audit", "arm_id", "arm", soa_id, action, arm_id, before, after)


def _record_element_audit(
    soa_id: int,
    action: str,
    element_id: Optional[int],
    before: Optional[Dict[str, Any]] = None,
    after: Optional[Dict[str, Any]] = None,
):
    _record_audit(
        "element_audit",
        "element_id",
        "element",
        soa_id,
        action,
        element_id,
        before,
        after,
    )


def _record_visit_audit(
    soa_id: int,
    action: str,
    visit_id: Optional[int],
    before: Optional[Dict[str, Any]] = None,
    after: Optional[Dict[str, Any]] = None,
):
    _record_audit(
        "visit_audit", "visit_id", "visit", soa_id, action, visit_id, before, after
    )


def _record_activity_audit(
    soa_id: int,
    action: str,
    activity_id: Optional[int],
    before: Optional[Dict[str, Any]] = None,
    after: Optional[Dict[str, Any]] = None,
):
    _record_audit(
        "activity_audit",
        "activity_id",
        "activity",
        soa_id,
        action,
        activity_id,
        before,
        after,
    )


def _record_study_cell_audit(
    soa_id: int,
    action: str,
    study_cell_id: Optional[int],
    before: Optional[Dict[str, Any]] = None,
    after: Optional[Dict[str, Any]] = None,
):
    _record_audit(
        "study_cell_audit",
        "study_cell_id",
        "study_cell",
        soa_id,
        action,
        study_cell_id,
        before,
        after,
    )


def _record_timing_audit(
    soa_id: int,
    action: str,
    timing_id: Optional[int],
    before: Optional[Dict[str, Any]] = None,
    after: Optional[Dict[str, Any]] = None,
):
    _record_audit(
        "timing_audit", "timing_id", "timing", soa_id, action, timing_id, before, after
    )


def _record_schedule_timeline_audit(
    soa_id: int,
    action: str,
    schedule_timeline_id: Optional[int],
    before: Optional[Dict[str, Any]] = None,
    after: Optional[Dict[str, Any]] = None,
):
    _record_audit(
        "schedule_timelines_audit",
        "schedule_timeline_id",
        "schedule timeline",
        soa_id,
        action,
        schedule_timeline_id,
        before,
        after,
    )


def _record_instance_audit(
    soa_id: int,
    action: str,
    instance_id: Optional[int],
    before: Optional[Dict[str, Any]] = None,
    after: Optional[Dict[str, Any]] = None,
):
    _record_audit(
        "instance_audit",
        "instance_id",
        "instance",
        soa_id,
        action,
        instance_id,
        before,
        after,
    )


def _record_decision_instance_audit(
    soa_id: int,
    action: str,
    decision_instance_id: Optional[int],
    before: Optional[Dict[str, Any]] = None,
    after: Optional[Dict[str, Any]] = None,
):
    _record_audit(
        "decision_instance_audit",
        "decision_instance_id",
        "decision_instance",
        soa_id,
        action,
        decision_instance_id,
        before,
        after,
    )


def _record_condition_assignment_audit(
    soa_id: int,
    action: str,
    condition_assignment_id: Optional[int],
    before: Optional[Dict[str, Any]] = None,
    after: Optional[Dict[str, Any]] = None,
):
    _record_audit(
        "condition_assignment_audit",
        "condition_assignment_id",
        "condition_assignment",
        soa_id,
        action,
        condition_assignment_id,
        before,
        after,
    )


def _record_transition_rule_audit(
    soa_id: int,
    action: str,
    transition_rule_id: Optional[int],
    before: Optional[Dict[str, Any]] = None,
    after: Optional[Dict[str, Any]] = None,
):
    _record_audit(
        "transition_rule_audit",
        "transition_rule_id",
        "transition rule",
        soa_id,
        action,
        transition_rule_id,
        before,
        after,
    )


def _record_biomedical_concept_audit(
    soa_id: int,
    action: str,
    biomedical_concept_id: Optional[int],
    before: Optional[Dict[str, Any]] = None,
    after: Optional[Dict[str, Any]] = None,
    cur=None,
):
    _record_audit(
        "biomedical_concept_audit",
        "biomedical_concept_id",
        "biomedical_concept",
        soa_id,
        action,
        biomedical_concept_id,
        before,
        after,
        cur=cur,
    )


def _record_bc_surrogate_audit(
    soa_id: int,
    action: str,
    surrogate_id: Optional[int],
    before: Optional[Dict[str, Any]] = None,
    after: Optional[Dict[str, Any]] = None,
):
    _record_audit(
        "biomedical_concept_surrogate_audit",
        "surrogate_id",
        "bc_surrogate",
        soa_id,
        action,
        surrogate_id,
        before,
        after,
    )


def _record_footnote_audit(
    soa_id: int,
    action: str,
    footnote_id: Optional[int],
    before: Optional[Dict[str, Any]] = None,
    after: Optional[Dict[str, Any]] = None,
):
    _record_audit(
        "footnote_audit",
        "footnote_id",
        "footnote",
        soa_id,
        action,
        footnote_id,
        before,
        after,
    )


def _record_objective_audit(
    soa_id: int,
    action: str,
    objective_id: Optional[int],
    before: Optional[Dict[str, Any]] = None,
    after: Optional[Dict[str, Any]] = None,
):
    _record_audit(
        "objective_audit",
        "objective_id",
        "objective",
        soa_id,
        action,
        objective_id,
        before,
        after,
    )


def _record_endpoint_audit(
    soa_id: int,
    action: str,
    endpoint_id: Optional[int],
    before: Optional[Dict[str, Any]] = None,
    after: Optional[Dict[str, Any]] = None,
):
    _record_audit(
        "endpoint_audit",
        "endpoint_id",
        "endpoint",
        soa_id,
        action,
        endpoint_id,
        before,
        after,
    )


def _record_amendment_audit(
    soa_id: int,
    action: str,
    amendment_id: Optional[int],
    before: Optional[Dict[str, Any]] = None,
    after: Optional[Dict[str, Any]] = None,
):
    _record_audit(
        "study_amendment_audit",
        "amendment_id",
        "amendment",
        soa_id,
        action,
        amendment_id,
        before,
        after,
    )


def _record_reason_audit(
    soa_id: int,
    action: str,
    reason_id: Optional[int],
    before: Optional[Dict[str, Any]] = None,
    after: Optional[Dict[str, Any]] = None,
):
    _record_audit(
        "study_amendment_reason_audit",
        "reason_id",
        "reason",
        soa_id,
        action,
        reason_id,
        before,
        after,
    )


def _record_impact_audit(
    soa_id: int,
    action: str,
    impact_id: Optional[int],
    before: Optional[Dict[str, Any]] = None,
    after: Optional[Dict[str, Any]] = None,
):
    _record_audit(
        "study_amendment_impact_audit",
        "impact_id",
        "impact",
        soa_id,
        action,
        impact_id,
        before,
        after,
    )


def _record_change_audit(
    soa_id: int,
    action: str,
    change_id: Optional[int],
    before: Optional[Dict[str, Any]] = None,
    after: Optional[Dict[str, Any]] = None,
):
    _record_audit(
        "study_change_audit",
        "change_id",
        "change",
        soa_id,
        action,
        change_id,
        before,
        after,
    )


def _record_ref_audit(
    soa_id: int,
    action: str,
    ref_id: Optional[int],
    before: Optional[Dict[str, Any]] = None,
    after: Optional[Dict[str, Any]] = None,
):
    _record_audit(
        "document_content_reference_audit",
        "ref_id",
        "ref",
        soa_id,
        action,
        ref_id,
        before,
        after,
    )


def _record_geo_scope_audit(
    soa_id: int,
    action: str,
    scope_id: Optional[int],
    before: Optional[Dict[str, Any]] = None,
    after: Optional[Dict[str, Any]] = None,
):
    _record_audit(
        "amendment_geographic_scope_audit",
        "scope_id",
        "geo_scope",
        soa_id,
        action,
        scope_id,
        before,
        after,
    )


def _record_enrollment_audit(
    soa_id: int,
    action: str,
    enrollment_id: Optional[int],
    before: Optional[Dict[str, Any]] = None,
    after: Optional[Dict[str, Any]] = None,
):
    _record_audit(
        "amendment_subject_enrollment_audit",
        "enrollment_id",
        "enrollment",
        soa_id,
        action,
        enrollment_id,
        before,
        after,
    )


def _record_organization_audit(
    soa_id: int,
    action: str,
    org_id: Optional[int],
    before: Optional[Dict[str, Any]] = None,
    after: Optional[Dict[str, Any]] = None,
):
    _record_audit(
        "organization_audit",
        "org_id",
        "organization",
        soa_id,
        action,
        org_id,
        before,
        after,
    )


def _record_role_audit(
    soa_id: int,
    action: str,
    role_id: Optional[int],
    before: Optional[Dict[str, Any]] = None,
    after: Optional[Dict[str, Any]] = None,
):
    _record_audit(
        "role_audit", "role_id", "role", soa_id, action, role_id, before, after
    )


def _record_gov_date_audit(
    soa_id: int,
    action: str,
    date_id: Optional[int],
    before: Optional[Dict[str, Any]] = None,
    after: Optional[Dict[str, Any]] = None,
):
    _record_audit(
        "amendment_governance_date_audit",
        "date_id",
        "gov_date",
        soa_id,
        action,
        date_id,
        before,
        after,
    )


def _record_study_title_audit(
    soa_id: int,
    action: str,
    title_id: Optional[int],
    before: Optional[Dict[str, Any]] = None,
    after: Optional[Dict[str, Any]] = None,
):
    _record_audit(
        "study_title_audit",
        "title_id",
        "study_title",
        soa_id,
        action,
        title_id,
        before,
        after,
    )


def _record_study_intervention_audit(
    soa_id: int,
    action: str,
    intervention_id: Optional[int],
    before: Optional[Dict[str, Any]] = None,
    after: Optional[Dict[str, Any]] = None,
):
    _record_audit(
        "study_intervention_audit",
        "intervention_id",
        "study_intervention",
        soa_id,
        action,
        intervention_id,
        before,
        after,
    )


def _record_estimand_audit(
    soa_id: int,
    action: str,
    estimand_id: Optional[int],
    before: Optional[Dict[str, Any]] = None,
    after: Optional[Dict[str, Any]] = None,
):
    _record_audit(
        "estimand_audit",
        "estimand_id",
        "estimand",
        soa_id,
        action,
        estimand_id,
        before,
        after,
    )


def _record_indication_audit(
    soa_id: int,
    action: str,
    indication_id: Optional[int],
    before: Optional[Dict[str, Any]] = None,
    after: Optional[Dict[str, Any]] = None,
):
    _record_audit(
        "indication_audit",
        "indication_id",
        "indication",
        soa_id,
        action,
        indication_id,
        before,
        after,
    )


def _record_person_audit(
    soa_id: int,
    action: str,
    person_id: Optional[int],
    before: Optional[Dict[str, Any]] = None,
    after: Optional[Dict[str, Any]] = None,
):
    _record_audit(
        "person_audit", "person_id", "person", soa_id, action, person_id, before, after
    )


def _record_study_identifier_audit(
    soa_id: int,
    action: str,
    si_id: Optional[int],
    before: Optional[Dict[str, Any]] = None,
    after: Optional[Dict[str, Any]] = None,
):
    _record_audit(
        "study_identifier_audit",
        "si_id",
        "study_identifier",
        soa_id,
        action,
        si_id,
        before,
        after,
    )
