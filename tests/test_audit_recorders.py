"""Characterization tests for every audit recorder in web/audit.py."""

import json
import logging

import pytest

from soa_builder.web import audit
from soa_builder.web.app import _connect  # noqa: F401  (runs migrations)
from soa_builder.web.db import _connect as db_connect

# (recorder, audit table, entity id column)
STANDARD = [
    ("_record_arm_audit", "arm_audit", "arm_id"),
    ("_record_element_audit", "element_audit", "element_id"),
    ("_record_visit_audit", "visit_audit", "visit_id"),
    ("_record_activity_audit", "activity_audit", "activity_id"),
    ("_record_study_cell_audit", "study_cell_audit", "study_cell_id"),
    ("_record_timing_audit", "timing_audit", "timing_id"),
    (
        "_record_schedule_timeline_audit",
        "schedule_timelines_audit",
        "schedule_timeline_id",
    ),
    ("_record_instance_audit", "instance_audit", "instance_id"),
    (
        "_record_eligibility_criterion_audit",
        "eligibility_criterion_audit",
        "eligibility_criterion_id",
    ),
    (
        "_record_decision_instance_audit",
        "decision_instance_audit",
        "decision_instance_id",
    ),
    (
        "_record_condition_assignment_audit",
        "condition_assignment_audit",
        "condition_assignment_id",
    ),
    (
        "_record_transition_rule_audit",
        "transition_rule_audit",
        "transition_rule_id",
    ),
    (
        "_record_biomedical_concept_audit",
        "biomedical_concept_audit",
        "biomedical_concept_id",
    ),
    (
        "_record_bc_surrogate_audit",
        "biomedical_concept_surrogate_audit",
        "surrogate_id",
    ),
    ("_record_footnote_audit", "footnote_audit", "footnote_id"),
    ("_record_objective_audit", "objective_audit", "objective_id"),
    ("_record_endpoint_audit", "endpoint_audit", "endpoint_id"),
    ("_record_amendment_audit", "study_amendment_audit", "amendment_id"),
    ("_record_reason_audit", "study_amendment_reason_audit", "reason_id"),
    ("_record_impact_audit", "study_amendment_impact_audit", "impact_id"),
    ("_record_change_audit", "study_change_audit", "change_id"),
    ("_record_ref_audit", "document_content_reference_audit", "ref_id"),
    ("_record_geo_scope_audit", "amendment_geographic_scope_audit", "scope_id"),
    (
        "_record_enrollment_audit",
        "amendment_subject_enrollment_audit",
        "enrollment_id",
    ),
    ("_record_organization_audit", "organization_audit", "org_id"),
    ("_record_role_audit", "role_audit", "role_id"),
    ("_record_gov_date_audit", "amendment_governance_date_audit", "date_id"),
    ("_record_study_title_audit", "study_title_audit", "title_id"),
    (
        "_record_study_intervention_audit",
        "study_intervention_audit",
        "intervention_id",
    ),
    ("_record_estimand_audit", "estimand_audit", "estimand_id"),
    ("_record_indication_audit", "indication_audit", "indication_id"),
    ("_record_person_audit", "person_audit", "person_id"),
    ("_record_study_identifier_audit", "study_identifier_audit", "si_id"),
]
SOA_ID = 990001


def _last_row(table: str, id_col: str):
    conn = db_connect()
    try:
        return conn.execute(
            f"SELECT soa_id, {id_col}, action, before_json, after_json,"
            f" performed_at FROM {table} WHERE soa_id=? ORDER BY id DESC LIMIT 1",
            (SOA_ID,),
        ).fetchone()
    finally:
        conn.close()


def test_every_recorder_is_covered():
    recorders = {
        n for n in dir(audit) if n.startswith("_record_") and n != "_record_audit"
    }
    covered = {name for name, _, _ in STANDARD} | {"_record_reorder_audit"}
    assert recorders == covered


@pytest.mark.parametrize("name,table,id_col", STANDARD, ids=[s[0] for s in STANDARD])
def test_recorder_writes_row(name, table, id_col):
    getattr(audit, name)(SOA_ID, "update", 42, before={"a": 1}, after={"a": 2})
    soa_id, entity_id, action, before, after, performed_at = _last_row(table, id_col)
    assert (soa_id, entity_id, action) == (SOA_ID, 42, "update")
    assert json.loads(before) == {"a": 1} and json.loads(after) == {"a": 2}
    assert performed_at


@pytest.mark.parametrize(
    "name,table,id_col", STANDARD[:3], ids=[s[0] for s in STANDARD[:3]]
)
def test_empty_before_after_stored_as_null(name, table, id_col):
    getattr(audit, name)(SOA_ID, "create", 7, before=None, after={})
    row = _last_row(table, id_col)
    assert row[3] is None and row[4] is None


def test_reorder_recorder():
    audit._record_reorder_audit(SOA_ID, "visit", [3, 1], [1, 3])
    conn = db_connect()
    try:
        row = conn.execute(
            "SELECT entity_type, old_order_json, new_order_json FROM reorder_audit"
            " WHERE soa_id=? ORDER BY id DESC LIMIT 1",
            (SOA_ID,),
        ).fetchone()
    finally:
        conn.close()
    assert row == ("visit", "[3, 1]", "[1, 3]")


def test_biomedical_concept_audit_joins_caller_transaction():
    conn = db_connect()
    try:
        cur = conn.cursor()
        audit._record_biomedical_concept_audit(
            SOA_ID, "create", 99, after={"x": 1}, cur=cur
        )
        conn.rollback()  # caller aborts: the audit row must go too
    finally:
        conn.close()
    row = _last_row("biomedical_concept_audit", "biomedical_concept_id")
    assert row is None or row[1] != 99


def test_failure_is_logged_as_error_and_never_raises(caplog, monkeypatch):
    def broken_connect():
        raise RuntimeError("db down")

    monkeypatch.setattr(audit, "_connect", broken_connect)
    before = audit.audit_failure_count()
    with caplog.at_level(logging.ERROR, logger="soa_builder.audit"):
        audit._record_arm_audit(SOA_ID, "create", 1, after={"a": 1})
        audit._record_reorder_audit(SOA_ID, "visit", [], [])
    assert audit.audit_failure_count() == before + 2
    assert any(r.levelno == logging.ERROR for r in caplog.records)
    audit.reset_audit_failures()  # expected failures; keep the guard green
