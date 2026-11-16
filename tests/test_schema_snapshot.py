"""Golden snapshot of the schema a fresh database ends up with.

Guards the migration registry and migrate_database split (plan
Phases 1 and 4): moving migrations around must not change the result.
"""

from soa_builder.web.app import _connect  # noqa: F401  (runs migrations)
from soa_builder.web.db import _connect as db_connect

from _snapshot import assert_matches_snapshot


def _schema() -> dict:
    conn = db_connect()
    try:
        cur = conn.cursor()
        cur.execute(
            "SELECT name FROM sqlite_master WHERE type='table'"
            " AND name NOT LIKE 'sqlite_%' ORDER BY name"
        )
        schema = {}
        for (table,) in cur.fetchall():
            cur.execute(f"PRAGMA table_info({table})")
            columns = [
                {
                    "name": r[1],
                    "type": r[2],
                    "notnull": r[3],
                    "default": r[4],
                    "pk": r[5],
                }
                for r in cur.fetchall()
            ]
            cur.execute(f"PRAGMA index_list({table})")
            indexes = {}
            for idx in cur.fetchall():
                cur.execute(f"PRAGMA index_info({idx[1]})")
                indexes[idx[1]] = {
                    "unique": idx[2],
                    "columns": [r[2] for r in cur.fetchall()],
                }
            schema[table] = {"columns": columns, "indexes": indexes}
        return schema
    finally:
        conn.close()


def test_schema_matches_snapshot():
    assert_matches_snapshot("schema.json", _schema())
