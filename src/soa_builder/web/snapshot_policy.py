"""How each soa-scoped table behaves under freeze and rollback.

Every table with a ``soa_id`` column must appear in exactly one of the
groups below; ``tests/test_snapshot_coverage.py`` enforces this, so a
new table cannot be added without deciding what rollback does with
it (the failure mode behind #326, #369-#372, #378 and #371).

The rollback logic itself lives in ``routers/_freeze_helpers.py``;
this module is the reviewed contract it is checked against.
"""

# Deleted by rollback and re-created from the freeze snapshot.
RESTORED = frozenset(
    {
        "activity",
        "activity_concept",
        "arm",
        "code_association",
        "decision_instances",
        "element",
        "eligibility_criterion",
        "endpoint",
        "epoch",
        "instances",
        "matrix_cells",
        "objective",
        "schedule_timelines",
        "study_cell",
        "timing",
        "visit",
    }
)

# Derived data that rollback resets and rebuilds (or that the next
# populate rebuilds) rather than restoring rows verbatim.
REBUILT = {
    "biomedical_concept": "rebuilt from the restored activity concepts",
    "biomedical_concept_property": "wiped; rebuilt on the next BC populate",
    "bcp_response_code": "wiped; rebuilt on the next BC populate",
    "alias_code": (
        "BC-chain rows reset and rebuilt; rows owned by other entities kept"
    ),
    "code": "BC-chain rows reset and rebuilt; rows owned by other entities kept",
}

# Not captured, but keyed on activity.id, which rollback reassigns:
# rows are re-pointed to the restored activity or dropped with it.
ACTIVITY_ID_REMAPPED = frozenset({"activity_concept_dss", "activity_concept_crf"})

# Not captured, but keyed on the integer element.id, which rollback
# reassigns: rows are re-pointed through the element's immutable
# StudyElement_N uid, or dropped when the element is not restored.
ELEMENT_ID_REMAPPED = frozenset({"element_intervention"})

# Not part of the snapshot: rollback leaves these rows as they are.
# Study-level metadata that a SoA version does not own today. Moving a
# table into RESTORED needs capture + restore code and tests.
NOT_VERSIONED = {
    "activity_grouping_extension": "keyed on immutable activity_uid",
    "activity_surrogate": "keyed on immutable activity_uid",
    "amendment_geographic_scope": "amendment metadata",
    "amendment_governance_date": "amendment metadata",
    "amendment_subject_enrollment": "amendment metadata",
    "biomedical_concept_surrogate": "captured for diffs only, not restored",
    "condition_assignment": "study design metadata",
    "document_content_reference": "protocol document metadata",
    "estimand": "study design metadata",
    "estimand_intervention": "study design metadata",
    "estimand_variable": "study design metadata",
    "footnote": "SoA presentation metadata",
    "governance_date_geographic_scope": "amendment metadata",
    "indication": "study design metadata",
    "indication_code": "study design metadata",
    "intercurrent_event": "study design metadata",
    "organization": "study-level reference data",
    "person": "study-level reference data",
    "role": "study-level reference data",
    "role_person": "study-level reference data",
    "soa_tool_extension": "tool metadata",
    "study_amendment": "captured for diffs only; amendments outlive rollback",
    "study_amendment_impact": "amendment metadata",
    "study_amendment_reason": "amendment metadata",
    "study_change": "amendment change log",
    "study_identifier": "study-level reference data",
    "study_intervention": "study design metadata",
    "study_intervention_code": "study design metadata",
    "study_title": "study-level reference data",
    "transition_rule": "study design metadata",
    "wizard_state": "UI onboarding state; rollback must not rewind it",
}

# Bookkeeping that must never be rolled back.
EXCLUDED = {
    "uid_counter": "UIDs are monotonic; a rollback must never reissue one",
    "embedded_copy_uid": (
        "export ids of embedded copies, keyed on immutable uids;"
        " a rollback must not change them"
    ),
    "soa_freeze": "the snapshots themselves",
}
# Every ``*_audit`` table is excluded too: audit history is append-only.
EXCLUDED_SUFFIX = "_audit"


def classified_tables() -> dict:
    """Map every classified table name to its group name."""
    groups = {
        "RESTORED": RESTORED,
        "REBUILT": REBUILT,
        "ACTIVITY_ID_REMAPPED": ACTIVITY_ID_REMAPPED,
        "ELEMENT_ID_REMAPPED": ELEMENT_ID_REMAPPED,
        "NOT_VERSIONED": NOT_VERSIONED,
        "EXCLUDED": EXCLUDED,
    }
    result: dict = {}
    for group, tables in groups.items():
        for table in tables:
            result.setdefault(table, []).append(group)
    return result
