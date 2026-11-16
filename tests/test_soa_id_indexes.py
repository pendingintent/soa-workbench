"""Every soa-scoped table needs an index led by soa_id.

Nearly every query filters on ``WHERE soa_id=?``; without a leading
index that is a full table scan, and audit tables grow without bound.
"""

from soa_builder.web.app import _connect  # noqa: F401  (runs migrations)
from soa_builder.web.db import _connect as db_connect


def _tables_without_leading_soa_id_index() -> list:
    conn = db_connect()
    try:
        cur = conn.cursor()
        cur.execute(
            "SELECT name FROM sqlite_master WHERE type='table'"
            " AND name NOT LIKE 'sqlite_%'"
        )
        missing = []
        for (table,) in cur.fetchall():
            cur.execute(f"PRAGMA table_info({table})")
            if "soa_id" not in {r[1] for r in cur.fetchall()}:
                continue
            cur.execute(f"PRAGMA index_list({table})")
            indexed = False
            for idx in cur.fetchall():
                cur.execute(f"PRAGMA index_info({idx[1]})")
                cols = [r[2] for r in cur.fetchall()]
                if cols and cols[0] == "soa_id":
                    indexed = True
                    break
            if not indexed:
                missing.append(table)
        return sorted(missing)
    finally:
        conn.close()


def test_every_soa_table_has_leading_soa_id_index():
    assert _tables_without_leading_soa_id_index() == []


def test_soa_id_lookup_uses_index():
    conn = db_connect()
    try:
        plan = conn.execute(
            "EXPLAIN QUERY PLAN SELECT * FROM code WHERE soa_id=?", (1,)
        ).fetchall()
    finally:
        conn.close()
    detail = " ".join(str(r[-1]) for r in plan)
    assert "USING INDEX" in detail or "USING COVERING INDEX" in detail, detail
