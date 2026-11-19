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

FREEZE_HELPERS_SRC = "\n".join(
    path.read_text() for path in Path(_freeze_helpers.__file__).parent.glob("*.py")
)


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


def _column_types() -> dict:
    """Map each soa-scoped table to {column: declared type}."""
    conn = db_connect()
    try:
        cur = conn.cursor()
        types = {}
        for table in SOA_TABLES:
            cur.execute(f"PRAGMA table_info({table})")
            types[table] = {r[1]: (r[2] or "").upper() for r in cur.fetchall()}
        return types
    finally:
        conn.close()


COLUMN_TYPES = _column_types()
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


# Integer columns holding the row id of a RESTORED table whose ids
# rollback reassigns, and the policy group that must re-point them.
ROW_ID_CHILD_GROUPS = {
    "activity_id": policy.ACTIVITY_ID_REMAPPED,
    "element_id": policy.ELEMENT_ID_REMAPPED,
}


@pytest.mark.parametrize("column", sorted(ROW_ID_CHILD_GROUPS))
def test_row_id_children_are_handled(column):
    """A table keyed on a restored table's row id goes stale when
    rollback reassigns those ids, unless rollback restores or
    re-points it. element.element_id is the TEXT StudyElement_N uid,
    so only INTEGER columns count."""
    unhandled = sorted(
        t
        for t, cols in SOA_TABLES.items()
        if column in cols
        and COLUMN_TYPES[t][column] == "INTEGER"
        and t not in policy.RESTORED
        and t not in ROW_ID_CHILD_GROUPS[column]
        and not t.endswith(policy.EXCLUDED_SUFFIX)
    )
    assert not unhandled, (
        f"Tables keyed on the integer {column} that rollback leaves stale: {unhandled}"
    )


@pytest.mark.parametrize("table", sorted(policy.ELEMENT_ID_REMAPPED))
def test_element_remapped_tables_are_remapped_by_rollback(table):
    assert table in _freeze_helpers.ELEMENT_ID_CHILD_TABLES
