"""Add the eligibility_criterion table and its audit table.

USDM v4.0 InterventionalStudyDesign.eligibilityCriteria is a required,
non-empty list (CORE DDF00126). Each row holds one EligibilityCriterion
(criterion_uid) and the EligibilityCriterionItem with its text
(item_uid); the workbench keeps them one-to-one. Both UIDs come from
the uid_counter and are never reused.

The table is study content, so freeze and rollback restore it
(snapshot_policy.RESTORED).
"""

from ..db import _connect


def m0118_eligibility_criteria() -> None:
    conn = _connect()
    try:
        conn.execute(
            """CREATE TABLE IF NOT EXISTS eligibility_criterion (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                soa_id INTEGER NOT NULL REFERENCES soa(id) ON DELETE CASCADE,
                criterion_uid TEXT NOT NULL,
                item_uid TEXT NOT NULL,
                name TEXT NOT NULL,
                label TEXT,
                description TEXT,
                identifier TEXT NOT NULL,
                category_code TEXT NOT NULL,
                category_decode TEXT,
                category_version TEXT,
                text TEXT NOT NULL,
                order_index INTEGER NOT NULL DEFAULT 0,
                created_at TEXT,
                UNIQUE (soa_id, criterion_uid),
                UNIQUE (soa_id, item_uid)
            )"""
        )
        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_eligibility_criterion_soa"
            " ON eligibility_criterion(soa_id)"
        )
        conn.execute(
            """CREATE TABLE IF NOT EXISTS eligibility_criterion_audit (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                soa_id INTEGER NOT NULL,
                eligibility_criterion_id INTEGER,
                action TEXT NOT NULL,
                before_json TEXT,
                after_json TEXT,
                performed_at TEXT NOT NULL
            )"""
        )
        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_eligibility_criterion_audit_soa"
            " ON eligibility_criterion_audit(soa_id)"
        )
        conn.commit()
    finally:
        conn.close()
