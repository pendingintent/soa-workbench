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
