"""Every soa-scoped table must have a declared bundle-export decision.

If this fails after adding a table, decide whether a study bundle
should carry it: add it to ``_EXPORT_TABLES`` (after its parents, with
any integer foreign keys in ``_ID_REMAPS``) or to ``_NEVER_EXPORTED``
in ``src/soa_builder/web/routers/soa_bundle.py``. Audit and freeze
data is never exported.
"""

import pytest

from soa_builder.web.app import _connect  # noqa: F401  (runs migrations)
from soa_builder.web.db import _connect as db_connect
from soa_builder.web.routers import soa_bundle as sb


def _soa_tables() -> dict:
    """Map each table that has a soa_id column to {column: type}."""
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
            cols = {r[1]: (r[2] or "").upper() for r in cur.fetchall()}
            if "soa_id" in cols:
                tables[name] = cols
        return tables
    finally:
        conn.close()


SOA_TABLES = _soa_tables()
EXPORTED = list(sb._EXPORT_TABLES)


def _is_audit_or_freeze(table: str) -> bool:
    return table.endswith(sb._AUDIT_SUFFIX) or "freeze" in table


def test_every_soa_table_has_an_export_decision():
    undecided = sorted(
        t
        for t in SOA_TABLES
        if t not in EXPORTED
        and t not in sb._NEVER_EXPORTED
        and not t.endswith(sb._AUDIT_SUFFIX)
    )
    assert not undecided, (
        "Tables with soa_id but no bundle export decision: "
        f"{undecided}. Add them to _EXPORT_TABLES or _NEVER_EXPORTED "
        "in routers/soa_bundle.py."
    )


def test_no_table_is_both_exported_and_excluded():
    assert not set(EXPORTED) & set(sb._NEVER_EXPORTED)


def test_export_tables_are_unique():
    assert len(EXPORTED) == len(set(EXPORTED))


def test_policy_names_only_real_tables():
    stale = sorted(
        t for t in set(EXPORTED) | set(sb._NEVER_EXPORTED) if t not in SOA_TABLES
    )
    assert not stale, f"soa_bundle.py lists unknown tables: {stale}"


@pytest.mark.parametrize("table", EXPORTED)
def test_audit_and_freeze_tables_are_never_exported(table):
    assert not _is_audit_or_freeze(table), table


def test_freeze_table_is_explicitly_excluded():
    assert "soa_freeze" in sb._NEVER_EXPORTED


def test_freeze_id_is_stripped_from_export():
    assert "freeze_id" in sb._STRIPPED_COLUMNS


@pytest.mark.parametrize("table", EXPORTED)
def test_integer_id_columns_are_remapped_or_stripped(table):
    """A row id from the source SoA is meaningless after import."""
    cols = SOA_TABLES[table]
    remapped = sb._ID_REMAPS.get(table, {})
    unhandled = sorted(
        c
        for c, ctype in cols.items()
        if c.endswith("_id")
        and c != "soa_id"
        and ctype == "INTEGER"
        and c not in remapped
        and c not in sb._STRIPPED_COLUMNS
    )
    assert not unhandled, (
        f"{table} has integer id columns with no remap: {unhandled}. "
        "Add them to _ID_REMAPS in routers/soa_bundle.py."
    )


@pytest.mark.parametrize(
    "table,col,parent",
    [(t, c, spec[0]) for t, cols in sb._ID_REMAPS.items() for c, spec in cols.items()],
)
def test_remap_parents_are_imported_before_children(table, col, parent):
    assert table in EXPORTED, f"_ID_REMAPS names unexported table {table}"
    assert parent in EXPORTED, f"{table}.{col} remaps to unexported {parent}"
    assert EXPORTED.index(parent) < EXPORTED.index(table), (
        f"{parent} must come before {table} in _EXPORT_TABLES"
    )
    assert col in SOA_TABLES[table], f"{table} has no column {col}"
