"""Legacy migrations: Organizations, roles and persons.

Bodies moved unchanged from migrate_database.py (frozen); new
migrations go in their own module (see README.md).
"""

from ..db import _connect
from .legacy_common import logger


def _migrate_add_organization_table():
    try:
        conn = _connect()
        cur = conn.cursor()
        cur.execute(
            "CREATE TABLE IF NOT EXISTS organization ("
            "id INTEGER PRIMARY KEY AUTOINCREMENT,"
            "soa_id INTEGER NOT NULL,"
            "organization_uid TEXT NOT NULL,"
            "name TEXT NOT NULL,"
            "label TEXT,"
            "identifier TEXT,"
            "identifier_scheme TEXT,"
            "type_code_uid TEXT,"
            "addr_text TEXT,"
            "addr_lines TEXT,"
            "addr_city TEXT,"
            "addr_district TEXT,"
            "addr_state TEXT,"
            "addr_postal_code TEXT,"
            "addr_country_code_uid TEXT,"
            "order_index INTEGER,"
            "UNIQUE(soa_id, organization_uid)"
            ")"
        )
        conn.commit()
        conn.close()
        logger.info("_migrate_add_organization_table complete")
    except Exception as e:
        logger.warning("_migrate_add_organization_table failed: %s", e)


def _migrate_add_organization_audit_table():
    try:
        conn = _connect()
        cur = conn.cursor()
        cur.execute(
            "CREATE TABLE IF NOT EXISTS organization_audit ("
            "id INTEGER PRIMARY KEY AUTOINCREMENT,"
            "soa_id INTEGER NOT NULL,"
            "org_id INTEGER,"
            "action TEXT NOT NULL,"
            "before_json TEXT,"
            "after_json TEXT,"
            "performed_at TEXT NOT NULL"
            ")"
        )
        conn.commit()
        conn.close()
        logger.info("_migrate_add_organization_audit_table complete")
    except Exception as e:
        logger.warning("_migrate_add_organization_audit_table failed: %s", e)


def _migrate_add_role_table():
    try:
        conn = _connect()
        cur = conn.cursor()
        cur.execute(
            "CREATE TABLE IF NOT EXISTS role ("
            "id INTEGER PRIMARY KEY AUTOINCREMENT,"
            "soa_id INTEGER NOT NULL,"
            "role_uid TEXT NOT NULL,"
            "name TEXT NOT NULL,"
            "label TEXT,"
            "description TEXT,"
            "code_uid TEXT,"
            "organization_ids TEXT,"
            "masking INTEGER NOT NULL DEFAULT 0,"
            "order_index INTEGER,"
            "UNIQUE(soa_id, role_uid)"
            ")"
        )
        conn.commit()
        conn.close()
        logger.info("_migrate_add_role_table complete")
    except Exception as e:
        logger.warning("_migrate_add_role_table failed: %s", e)


def _migrate_add_role_audit_table():
    try:
        conn = _connect()
        cur = conn.cursor()
        cur.execute(
            "CREATE TABLE IF NOT EXISTS role_audit ("
            "id INTEGER PRIMARY KEY AUTOINCREMENT,"
            "soa_id INTEGER NOT NULL,"
            "role_id INTEGER,"
            "action TEXT NOT NULL,"
            "before_json TEXT,"
            "after_json TEXT,"
            "performed_at TEXT NOT NULL"
            ")"
        )
        conn.commit()
        conn.close()
        logger.info("_migrate_add_role_audit_table complete")
    except Exception as e:
        logger.warning("_migrate_add_role_audit_table failed: %s", e)


def _migrate_add_person_table():
    try:
        conn = _connect()
        cur = conn.cursor()
        cur.execute(
            "CREATE TABLE IF NOT EXISTS person ("
            "id INTEGER PRIMARY KEY AUTOINCREMENT,"
            "soa_id INTEGER NOT NULL,"
            "person_uid TEXT NOT NULL,"
            "person_name_uid TEXT NOT NULL,"
            "name TEXT NOT NULL,"
            "label TEXT,"
            "description TEXT,"
            "job_title TEXT NOT NULL,"
            "organization_uid TEXT,"
            "order_index INTEGER,"
            "UNIQUE(soa_id, person_uid)"
            ")"
        )
        conn.commit()
        conn.close()
        logger.info("_migrate_add_person_table complete")
    except Exception as e:
        logger.warning("_migrate_add_person_table failed: %s", e)


def _migrate_add_person_audit_table():
    try:
        conn = _connect()
        cur = conn.cursor()
        cur.execute(
            "CREATE TABLE IF NOT EXISTS person_audit ("
            "id INTEGER PRIMARY KEY AUTOINCREMENT,"
            "soa_id INTEGER NOT NULL,"
            "person_id INTEGER,"
            "action TEXT NOT NULL,"
            "before_json TEXT,"
            "after_json TEXT,"
            "performed_at TEXT NOT NULL"
            ")"
        )
        conn.commit()
        conn.close()
        logger.info("_migrate_add_person_audit_table complete")
    except Exception as e:
        logger.warning("_migrate_add_person_audit_table failed: %s", e)


def _migrate_add_role_person_table():
    try:
        conn = _connect()
        cur = conn.cursor()
        cur.execute(
            "CREATE TABLE IF NOT EXISTS role_person ("
            "id INTEGER PRIMARY KEY AUTOINCREMENT,"
            "soa_id INTEGER NOT NULL,"
            "role_id INTEGER NOT NULL,"
            "person_id INTEGER NOT NULL,"
            "UNIQUE(soa_id, role_id, person_id)"
            ")"
        )
        conn.commit()
        conn.close()
        logger.info("_migrate_add_role_person_table complete")
    except Exception as e:
        logger.warning("_migrate_add_role_person_table failed: %s", e)


def _migrate_person_drop_job_title_notnull():
    """Recreate person table without NOT NULL on job_title."""
    try:
        conn = _connect()
        cur = conn.cursor()
        cur.execute(
            "SELECT sql FROM sqlite_master WHERE type='table' AND name='person'"
        )
        row = cur.fetchone()
        if not row or "job_title TEXT NOT NULL" not in (row[0] or ""):
            conn.close()
            return
        cur.executescript(
            "PRAGMA foreign_keys=OFF;"
            "BEGIN;"
            "CREATE TABLE IF NOT EXISTS person_new ("
            "id INTEGER PRIMARY KEY AUTOINCREMENT,"
            "soa_id INTEGER NOT NULL,"
            "person_uid TEXT NOT NULL,"
            "person_name_uid TEXT NOT NULL,"
            "name TEXT NOT NULL,"
            "label TEXT,"
            "description TEXT,"
            "job_title TEXT,"
            "organization_uid TEXT,"
            "order_index INTEGER,"
            "UNIQUE(soa_id, person_uid)"
            ");"
            "INSERT INTO person_new SELECT"
            " id,soa_id,person_uid,person_name_uid,name,label,"
            " description,job_title,organization_uid,order_index"
            " FROM person;"
            "DROP TABLE person;"
            "ALTER TABLE person_new RENAME TO person;"
            "COMMIT;"
            "PRAGMA foreign_keys=ON;"
        )
        conn.close()
        logger.info("_migrate_person_drop_job_title_notnull complete")
    except Exception as e:
        logger.warning("_migrate_person_drop_job_title_notnull failed: %s", e)


def _migrate_add_person_name_fields():
    """Add PersonName structured fields to the person table."""
    conn = _connect()
    cur = conn.cursor()
    cols = {
        "text": "TEXT",
        "family_name": "TEXT",
        "given_names": "TEXT",
        "prefixes": "TEXT",
        "suffixes": "TEXT",
    }
    for col, col_type in cols.items():
        try:
            cur.execute(f"ALTER TABLE person ADD COLUMN {col} {col_type}")
            conn.commit()
            logger.info("_migrate_add_person_name_fields: added column %s", col)
        except Exception:
            pass
    conn.close()
