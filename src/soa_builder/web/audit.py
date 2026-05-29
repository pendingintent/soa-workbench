import json
import logging
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

from .db import _connect

logger = logging.getLogger("soa_builder.audit")


def _record_arm_audit(
    soa_id: int,
    action: str,
    arm_id: int | None,
    before: Optional[Dict[str, Any]] = None,
    after: Optional[Dict[str, Any]] = None,
):
    try:
        conn = _connect()
        cur = conn.cursor()
        cur.execute(
            "INSERT INTO arm_audit (soa_id, arm_id, action, before_json, after_json, performed_at) VALUES (?,?,?,?,?,?)",
            (
                soa_id,
                arm_id,
                action,
                json.dumps(before) if before else None,
                json.dumps(after) if after else None,
                datetime.now(timezone.utc).isoformat(),
            ),
        )
        conn.commit()
        conn.close()
    except Exception as e:
        logger.warning("Failed recording arm audit: %s", e)


def _record_element_audit(
    soa_id: int,
    action: str,
    element_id: int | None,
    before: Optional[Dict[str, Any]] = None,
    after: Optional[Dict[str, Any]] = None,
):
    try:
        conn = _connect()
        cur = conn.cursor()
        cur.execute(
            "INSERT INTO element_audit (soa_id, element_id, action, before_json, after_json, performed_at) VALUES (?,?,?,?,?,?)",
            (
                soa_id,
                element_id,
                action,
                json.dumps(before) if before else None,
                json.dumps(after) if after else None,
                datetime.now(timezone.utc).isoformat(),
            ),
        )
        conn.commit()
        conn.close()
    except Exception as e:
        logger.warning("Failed recording element audit: %s", e)


def _record_reorder_audit(
    soa_id: int,
    entity_type: str,
    old_order: List[int],
    new_order: List[int],
):
    try:
        conn = _connect()
        cur = conn.cursor()
        cur.execute(
            "INSERT INTO reorder_audit (soa_id, entity_type, old_order_json, new_order_json, performed_at) VALUES (?,?,?,?,?)",
            (
                soa_id,
                entity_type,
                json.dumps(old_order),
                json.dumps(new_order),
                datetime.now(timezone.utc).isoformat(),
            ),
        )
        conn.commit()
        conn.close()
    except Exception as e:
        logger.warning("Failed recording reorder audit: %s", e)


def _record_visit_audit(
    soa_id: int,
    action: str,
    visit_id: Optional[int],
    before: Optional[Dict[str, Any]] = None,
    after: Optional[Dict[str, Any]] = None,
):
    try:
        conn = _connect()
        cur = conn.cursor()
        cur.execute(
            "INSERT INTO visit_audit (soa_id, visit_id, action, before_json, after_json, performed_at) VALUES (?,?,?,?,?,?)",
            (
                soa_id,
                visit_id,
                action,
                json.dumps(before) if before else None,
                json.dumps(after) if after else None,
                datetime.now(timezone.utc).isoformat(),
            ),
        )
        conn.commit()
        conn.close()
    except Exception as e:
        logger.warning("Failed recording visit audit: %s", e)


def _record_activity_audit(
    soa_id: int,
    action: str,
    activity_id: Optional[int],
    before: Optional[Dict[str, Any]] = None,
    after: Optional[Dict[str, Any]] = None,
):
    try:
        conn = _connect()
        cur = conn.cursor()
        cur.execute(
            "INSERT INTO activity_audit (soa_id, activity_id, action, before_json, after_json, performed_at) VALUES (?,?,?,?,?,?)",
            (
                soa_id,
                activity_id,
                action,
                json.dumps(before) if before else None,
                json.dumps(after) if after else None,
                datetime.now(timezone.utc).isoformat(),
            ),
        )
        conn.commit()
        conn.close()
    except Exception as e:
        logger.warning("Failed recording activity audit: %s", e)


def _record_study_cell_audit(
    soa_id: int,
    action: str,
    study_cell_id: int | None,
    before: Optional[Dict[str, Any]] = None,
    after: Optional[Dict[str, Any]] = None,
):
    try:
        conn = _connect()
        cur = conn.cursor()
        cur.execute(
            "INSERT INTO study_cell_audit (soa_id, study_cell_id, action, before_json, after_json, performed_at) VALUES (?,?,?,?,?,?)",
            (
                soa_id,
                study_cell_id,
                action,
                json.dumps(before) if before else None,
                json.dumps(after) if after else None,
                datetime.now(timezone.utc).isoformat(),
            ),
        )
        conn.commit()
        conn.close()
    except Exception as e:
        logger.warning("Failed recording study_cell audit: %s", e)


def _record_timing_audit(
    soa_id: int,
    action: str,
    timing_id: int | None,
    before: Optional[Dict[str, Any]] = None,
    after: Optional[Dict[str, Any]] = None,
):
    try:
        conn = _connect()
        cur = conn.cursor()
        cur.execute(
            "INSERT INTO timing_audit (soa_id, timing_id, action, before_json, after_json, performed_at) VALUES (?,?,?,?,?,?)",
            (
                soa_id,
                timing_id,
                action,
                json.dumps(before) if before else None,
                json.dumps(after) if after else None,
                datetime.now(timezone.utc).isoformat(),
            ),
        )
        conn.commit()
        conn.close()
    except Exception as e:
        logger.warning("Failed recording timing audit: %s", e)


def _record_schedule_timeline_audit(
    soa_id: int,
    action: str,
    schedule_timeline_id: int | None,
    before: Optional[Dict[str, Any]] = None,
    after: Optional[Dict[str, Any]] = None,
):
    try:
        conn = _connect()
        cur = conn.cursor()
        cur.execute(
            "INSERT into schedule_timelines_audit (soa_id,schedule_timeline_id,action,before_json,after_json,performed_at) VALUES (?,?,?,?,?,?)",
            (
                soa_id,
                schedule_timeline_id,
                action,
                json.dumps(before) if before else None,
                json.dumps(after) if after else None,
                datetime.now(timezone.utc).isoformat(),
            ),
        )
        conn.commit()
        conn.close()
    except Exception as e:
        logger.warning("Failed recording schedule timeline audit: %s", e)


def _record_instance_audit(
    soa_id: int,
    action: str,
    instance_id: int | None,
    before: Optional[Dict[str, Any]] = None,
    after: Optional[Dict[str, Any]] = None,
):
    try:
        conn = _connect()
        cur = conn.cursor()
        cur.execute(
            "INSERT INTO instance_audit (soa_id, instance_id, action, before_json, after_json, performed_at) VALUES (?,?,?,?,?,?)",
            (
                soa_id,
                instance_id,
                action,
                json.dumps(before) if before else None,
                json.dumps(after) if after else None,
                datetime.now(timezone.utc).isoformat(),
            ),
        )
        conn.commit()
        conn.close()
    except Exception as e:
        logger.warning("Failed recording instance audit: %s", e)


def _record_decision_instance_audit(
    soa_id: int,
    action: str,
    decision_instance_id: int | None,
    before: Optional[Dict[str, Any]] = None,
    after: Optional[Dict[str, Any]] = None,
):
    try:
        conn = _connect()
        cur = conn.cursor()
        cur.execute(
            "INSERT INTO decision_instance_audit (soa_id, decision_instance_id, action, before_json, after_json, performed_at) VALUES (?,?,?,?,?,?)",
            (
                soa_id,
                decision_instance_id,
                action,
                json.dumps(before) if before else None,
                json.dumps(after) if after else None,
                datetime.now(timezone.utc).isoformat(),
            ),
        )
        conn.commit()
        conn.close()
    except Exception as e:
        logger.warning("Failed recording decision_instance audit: %s", e)


def _record_condition_assignment_audit(
    soa_id: int,
    action: str,
    condition_assignment_id: int | None,
    before: Optional[Dict[str, Any]] = None,
    after: Optional[Dict[str, Any]] = None,
):
    try:
        conn = _connect()
        cur = conn.cursor()
        cur.execute(
            "INSERT INTO condition_assignment_audit (soa_id, condition_assignment_id, action, before_json, after_json, performed_at) VALUES (?,?,?,?,?,?)",
            (
                soa_id,
                condition_assignment_id,
                action,
                json.dumps(before) if before else None,
                json.dumps(after) if after else None,
                datetime.now(timezone.utc).isoformat(),
            ),
        )
        conn.commit()
        conn.close()
    except Exception as e:
        logger.warning("Failed recording condition_assignment audit: %s", e)


# Transition Rule Audit
def _record_transition_rule_audit(
    soa_id: int,
    action: str,
    transition_rule_id: Optional[int],
    before: Optional[dict] = None,
    after: Optional[dict] = None,
):
    try:
        conn = _connect()
        cur = conn.cursor()
        cur.execute(
            "INSERT INTO transition_rule_audit (soa_id, transition_rule_id, action, before_json, after_json, performed_at) "
            "VALUES (?,?,?,?,?,?)",
            (
                soa_id,
                transition_rule_id,
                action,
                json.dumps(before) if before else None,
                json.dumps(after) if after else None,
                datetime.now(timezone.utc).isoformat(),
            ),
        )
        conn.commit()
        conn.close()
    except Exception as e:
        logger.warning("Failed recording transition rule audit: %s", e)


def _record_biomedical_concept_audit(
    soa_id: int,
    action: str,
    biomedical_concept_id: Optional[int],
    before: Optional[Dict[str, Any]] = None,
    after: Optional[Dict[str, Any]] = None,
    cur=None,
):
    try:
        own_conn = cur is None
        if own_conn:
            conn = _connect()
            cur = conn.cursor()
        cur.execute(
            "INSERT INTO biomedical_concept_audit"
            " (soa_id, biomedical_concept_id, action, before_json, after_json, performed_at)"
            " VALUES (?,?,?,?,?,?)",
            (
                soa_id,
                biomedical_concept_id,
                action,
                json.dumps(before) if before else None,
                json.dumps(after) if after else None,
                datetime.now(timezone.utc).isoformat(),
            ),
        )
        if own_conn:
            conn.commit()
            conn.close()
    except Exception as e:
        logger.warning("Failed recording biomedical_concept audit: %s", e)


def _record_bc_surrogate_audit(
    soa_id: int,
    action: str,
    surrogate_id: Optional[int],
    before: Optional[Dict[str, Any]] = None,
    after: Optional[Dict[str, Any]] = None,
):
    try:
        conn = _connect()
        cur = conn.cursor()
        cur.execute(
            "INSERT INTO biomedical_concept_surrogate_audit"
            " (soa_id, surrogate_id, action, before_json, after_json, performed_at)"
            " VALUES (?,?,?,?,?,?)",
            (
                soa_id,
                surrogate_id,
                action,
                json.dumps(before) if before else None,
                json.dumps(after) if after else None,
                datetime.now(timezone.utc).isoformat(),
            ),
        )
        conn.commit()
        conn.close()
    except Exception as e:
        logger.warning("Failed recording bc_surrogate audit: %s", e)


def _record_footnote_audit(
    soa_id: int,
    action: str,
    footnote_id: Optional[int],
    before: Optional[Dict[str, Any]] = None,
    after: Optional[Dict[str, Any]] = None,
):
    try:
        conn = _connect()
        cur = conn.cursor()
        cur.execute(
            "INSERT INTO footnote_audit (soa_id, footnote_id, action, before_json, after_json, performed_at) VALUES (?,?,?,?,?,?)",
            (
                soa_id,
                footnote_id,
                action,
                json.dumps(before) if before else None,
                json.dumps(after) if after else None,
                datetime.now(timezone.utc).isoformat(),
            ),
        )
        conn.commit()
        conn.close()
    except Exception as e:
        logger.warning("Failed recording footnote audit: %s", e)


def _record_objective_audit(
    soa_id: int,
    action: str,
    objective_id: Optional[int],
    before: Optional[Dict[str, Any]] = None,
    after: Optional[Dict[str, Any]] = None,
):
    try:
        conn = _connect()
        cur = conn.cursor()
        cur.execute(
            "INSERT INTO objective_audit (soa_id, objective_id, action, before_json, after_json, performed_at) VALUES (?,?,?,?,?,?)",
            (
                soa_id,
                objective_id,
                action,
                json.dumps(before) if before else None,
                json.dumps(after) if after else None,
                datetime.now(timezone.utc).isoformat(),
            ),
        )
        conn.commit()
        conn.close()
    except Exception as e:
        logger.warning("Failed recording objective audit: %s", e)


def _record_endpoint_audit(
    soa_id: int,
    action: str,
    endpoint_id: Optional[int],
    before: Optional[Dict[str, Any]] = None,
    after: Optional[Dict[str, Any]] = None,
):
    try:
        conn = _connect()
        cur = conn.cursor()
        cur.execute(
            "INSERT INTO endpoint_audit (soa_id, endpoint_id, action, before_json, after_json, performed_at) VALUES (?,?,?,?,?,?)",
            (
                soa_id,
                endpoint_id,
                action,
                json.dumps(before) if before else None,
                json.dumps(after) if after else None,
                datetime.now(timezone.utc).isoformat(),
            ),
        )
        conn.commit()
        conn.close()
    except Exception as e:
        logger.warning("Failed recording endpoint audit: %s", e)


def _record_amendment_audit(
    soa_id: int,
    action: str,
    amendment_id: Optional[int],
    before: Optional[Dict[str, Any]] = None,
    after: Optional[Dict[str, Any]] = None,
):
    try:
        conn = _connect()
        cur = conn.cursor()
        cur.execute(
            "INSERT INTO study_amendment_audit"
            " (soa_id, amendment_id, action, before_json, after_json,"
            " performed_at) VALUES (?,?,?,?,?,?)",
            (
                soa_id,
                amendment_id,
                action,
                json.dumps(before) if before else None,
                json.dumps(after) if after else None,
                datetime.now(timezone.utc).isoformat(),
            ),
        )
        conn.commit()
        conn.close()
    except Exception as e:
        logger.warning("Failed recording amendment audit: %s", e)


def _record_reason_audit(
    soa_id: int,
    action: str,
    reason_id: Optional[int],
    before: Optional[Dict[str, Any]] = None,
    after: Optional[Dict[str, Any]] = None,
):
    try:
        conn = _connect()
        cur = conn.cursor()
        cur.execute(
            "INSERT INTO study_amendment_reason_audit"
            " (soa_id, reason_id, action, before_json, after_json,"
            " performed_at) VALUES (?,?,?,?,?,?)",
            (
                soa_id,
                reason_id,
                action,
                json.dumps(before) if before else None,
                json.dumps(after) if after else None,
                datetime.now(timezone.utc).isoformat(),
            ),
        )
        conn.commit()
        conn.close()
    except Exception as e:
        logger.warning("Failed recording reason audit: %s", e)


def _record_impact_audit(
    soa_id: int,
    action: str,
    impact_id: Optional[int],
    before: Optional[Dict[str, Any]] = None,
    after: Optional[Dict[str, Any]] = None,
):
    try:
        conn = _connect()
        cur = conn.cursor()
        cur.execute(
            "INSERT INTO study_amendment_impact_audit"
            " (soa_id, impact_id, action, before_json, after_json,"
            " performed_at) VALUES (?,?,?,?,?,?)",
            (
                soa_id,
                impact_id,
                action,
                json.dumps(before) if before else None,
                json.dumps(after) if after else None,
                datetime.now(timezone.utc).isoformat(),
            ),
        )
        conn.commit()
        conn.close()
    except Exception as e:
        logger.warning("Failed recording impact audit: %s", e)


def _record_change_audit(
    soa_id: int,
    action: str,
    change_id: Optional[int],
    before: Optional[Dict[str, Any]] = None,
    after: Optional[Dict[str, Any]] = None,
):
    try:
        conn = _connect()
        cur = conn.cursor()
        cur.execute(
            "INSERT INTO study_change_audit"
            " (soa_id, change_id, action, before_json, after_json,"
            " performed_at) VALUES (?,?,?,?,?,?)",
            (
                soa_id,
                change_id,
                action,
                json.dumps(before) if before else None,
                json.dumps(after) if after else None,
                datetime.now(timezone.utc).isoformat(),
            ),
        )
        conn.commit()
        conn.close()
    except Exception as e:
        logger.warning("Failed recording change audit: %s", e)


def _record_ref_audit(
    soa_id: int,
    action: str,
    ref_id: Optional[int],
    before: Optional[Dict[str, Any]] = None,
    after: Optional[Dict[str, Any]] = None,
):
    try:
        conn = _connect()
        cur = conn.cursor()
        cur.execute(
            "INSERT INTO document_content_reference_audit"
            " (soa_id, ref_id, action, before_json, after_json,"
            " performed_at) VALUES (?,?,?,?,?,?)",
            (
                soa_id,
                ref_id,
                action,
                json.dumps(before) if before else None,
                json.dumps(after) if after else None,
                datetime.now(timezone.utc).isoformat(),
            ),
        )
        conn.commit()
        conn.close()
    except Exception as e:
        logger.warning("Failed recording ref audit: %s", e)


def _record_geo_scope_audit(
    soa_id: int,
    action: str,
    scope_id: Optional[int],
    before: Optional[Dict[str, Any]] = None,
    after: Optional[Dict[str, Any]] = None,
):
    try:
        conn = _connect()
        cur = conn.cursor()
        cur.execute(
            "INSERT INTO amendment_geographic_scope_audit"
            " (soa_id, scope_id, action, before_json, after_json,"
            " performed_at) VALUES (?,?,?,?,?,?)",
            (
                soa_id,
                scope_id,
                action,
                json.dumps(before) if before else None,
                json.dumps(after) if after else None,
                datetime.now(timezone.utc).isoformat(),
            ),
        )
        conn.commit()
        conn.close()
    except Exception as e:
        logger.warning("Failed recording geo_scope audit: %s", e)


def _record_enrollment_audit(
    soa_id: int,
    action: str,
    enrollment_id: Optional[int],
    before: Optional[Dict[str, Any]] = None,
    after: Optional[Dict[str, Any]] = None,
):
    try:
        conn = _connect()
        cur = conn.cursor()
        cur.execute(
            "INSERT INTO amendment_subject_enrollment_audit"
            " (soa_id, enrollment_id, action, before_json, after_json,"
            " performed_at) VALUES (?,?,?,?,?,?)",
            (
                soa_id,
                enrollment_id,
                action,
                json.dumps(before) if before else None,
                json.dumps(after) if after else None,
                datetime.now(timezone.utc).isoformat(),
            ),
        )
        conn.commit()
        conn.close()
    except Exception as e:
        logger.warning("Failed recording enrollment audit: %s", e)


def _record_organization_audit(
    soa_id: int,
    action: str,
    org_id: Optional[int],
    before: Optional[Dict[str, Any]] = None,
    after: Optional[Dict[str, Any]] = None,
):
    try:
        conn = _connect()
        cur = conn.cursor()
        cur.execute(
            "INSERT INTO organization_audit"
            " (soa_id, org_id, action, before_json, after_json,"
            " performed_at) VALUES (?,?,?,?,?,?)",
            (
                soa_id,
                org_id,
                action,
                json.dumps(before) if before else None,
                json.dumps(after) if after else None,
                datetime.now(timezone.utc).isoformat(),
            ),
        )
        conn.commit()
        conn.close()
    except Exception as e:
        logger.warning("Failed recording organization audit: %s", e)


def _record_gov_date_audit(
    soa_id: int,
    action: str,
    date_id: Optional[int],
    before: Optional[Dict[str, Any]] = None,
    after: Optional[Dict[str, Any]] = None,
):
    try:
        conn = _connect()
        cur = conn.cursor()
        cur.execute(
            "INSERT INTO amendment_governance_date_audit"
            " (soa_id, date_id, action, before_json, after_json,"
            " performed_at) VALUES (?,?,?,?,?,?)",
            (
                soa_id,
                date_id,
                action,
                json.dumps(before) if before else None,
                json.dumps(after) if after else None,
                datetime.now(timezone.utc).isoformat(),
            ),
        )
        conn.commit()
        conn.close()
    except Exception as e:
        logger.warning("Failed recording gov_date audit: %s", e)


def _record_study_title_audit(
    soa_id: int,
    action: str,
    title_id: Optional[int],
    before: Optional[Dict[str, Any]] = None,
    after: Optional[Dict[str, Any]] = None,
):
    try:
        conn = _connect()
        cur = conn.cursor()
        cur.execute(
            "INSERT INTO study_title_audit "
            "(soa_id, title_id, action, before_json, after_json, performed_at) "
            "VALUES (?,?,?,?,?,?)",
            (
                soa_id,
                title_id,
                action,
                json.dumps(before) if before else None,
                json.dumps(after) if after else None,
                datetime.now(timezone.utc).isoformat(),
            ),
        )
        conn.commit()
        conn.close()
    except Exception as e:
        logger.warning("Failed recording study_title audit: %s", e)
