"""Every soa-scoped table must have a declared freeze/rollback policy.

If this fails after adding a table, decide what rollback should do
with it and add it to the right group in
``src/soa_builder/web/snapshot_policy.py`` (see CLAUDE.md, "Freeze
and Rollback").
"""

import re
from pathlib import Path

import pytest

from soa_builder.web import snapshot_policy as policy
from soa_builder.web.app import _connect  # noqa: F401  (runs migrations)
from soa_builder.web.db import _connect as db_connect
from soa_builder.web.routers import _freeze_helpers

FREEZE_HELPERS_SRC = Path(_freeze_helpers.__file__).read_text()


def _soa_tables() -> dict:
    """Map each table that has a soa_id column to its column set."""
    conn = db_connect()
    try:
        cur = conn.cursor()
        cur.execute(
            "SELECT name FROM sqlite_master WHERE type='table'"
            " AND name NOT LIKE 'sqlite_%'"
        )
        tables = {}
        for (name,) in cur.fetchall():
            cur.execute(f"PRAGMA table_info({name})")
            cols = {r[1] for r in cur.fetchall()}
            if "soa_id" in cols:
                tables[name] = cols
        return tables
    finally:
        conn.close()


SOA_TABLES = _soa_tables()
CLASSIFIED = policy.classified_tables()


def test_every_soa_table_has_a_policy():
    missing = sorted(
        t
        for t in SOA_TABLES
        if t not in CLASSIFIED and not t.endswith(policy.EXCLUDED_SUFFIX)
    )
    assert not missing, (
        "Tables with soa_id but no freeze/rollback policy: "
        f"{missing}. Classify them in snapshot_policy.py."
    )


def test_no_table_is_in_two_groups():
    dupes = {t: g for t, g in CLASSIFIED.items() if len(g) > 1}
    assert not dupes


def test_policy_names_only_real_tables():
    stale = sorted(t for t in CLASSIFIED if t not in SOA_TABLES)
    assert not stale, f"snapshot_policy.py lists unknown tables: {stale}"


@pytest.mark.parametrize("table", sorted(policy.RESTORED))
def test_restored_tables_are_cleared_by_rollback(table):
    """RESTORED must match what _rollback_freeze actually deletes."""
    assert re.search(rf"DELETE FROM {table} WHERE", FREEZE_HELPERS_SRC), table


@pytest.mark.parametrize("table", sorted(policy.ACTIVITY_ID_REMAPPED))
def test_remapped_tables_are_remapped_by_rollback(table):
    assert table in _freeze_helpers.ACTIVITY_ID_CHILD_TABLES


def test_activity_id_children_are_handled():
    """A table keyed on activity.id goes stale when rollback reassigns
    activity ids, unless rollback restores or re-points it."""
    unhandled = sorted(
        t
        for t, cols in SOA_TABLES.items()
        if "activity_id" in cols
        and t not in policy.RESTORED
        and t not in policy.ACTIVITY_ID_REMAPPED
        and not t.endswith(policy.EXCLUDED_SUFFIX)
    )
    assert not unhandled, (
        f"Tables keyed on activity.id that rollback leaves stale: {unhandled}"
    )
