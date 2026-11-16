"""Add the wizard_state table for the guided study setup (issue #382).

One row per study created or resumed through the setup wizard. It
records the step the user is on, the steps they completed, and whether
the wizard is still a draft or complete. It is UI onboarding state,
not study content, so rollback leaves it alone (see
snapshot_policy.NOT_VERSIONED).
"""

from ..db import _connect


def m0115_wizard_state() -> None:
    conn = _connect()
    try:
        conn.execute(
            """CREATE TABLE IF NOT EXISTS wizard_state (
                soa_id INTEGER PRIMARY KEY
                    REFERENCES soa(id) ON DELETE CASCADE,
                current_step TEXT NOT NULL,
                completed_steps TEXT NOT NULL DEFAULT '[]',
                status TEXT NOT NULL DEFAULT 'draft',
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL
            )"""
        )
        # Same idx_<table>_soa index migrate_index_all_soa_id_tables
        # gives every soa_id table, so the schema does not depend on
        # whether that migration is re-run after this one.
        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_wizard_state_soa ON wizard_state(soa_id)"
        )
        conn.commit()
    finally:
        conn.close()
