"""Legacy migrations: Objectives, endpoints, footnotes, titles, interventions, estimands, indications and study identifiers.

Bodies moved unchanged from migrate_database.py (frozen); new
migrations go in their own module (see README.md).
"""

from ..db import _connect
from .legacy_common import logger


def _migrate_add_footnote_table():
    """Add the database table footnote"""
    try:
        conn = _connect()
        cur = conn.cursor()
        cur.execute(
            """CREATE TABLE IF NOT EXISTS footnote (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                soa_id INT,
                footnote_uid TEXT NOT NULL,
                name TEXT NOT NULL,
                label TEXT,
                description TEXT,
                text TEXT,
                dictionary_uid TEXT,
                UNIQUE(soa_id, footnote_uid)
            )"""
        )
        conn.commit()
        conn.close()
        logger.info("_migrate_add_footnote_table created footnote table")
    except Exception as e:
        logger.warning("_migrate_add_footnote_table failed: %s", e)


def _migrate_add_footnote_audit_table():
    """Create footnote_audit table for tracking create/update/delete operations."""
    try:
        conn = _connect()
        cur = conn.cursor()
        cur.execute(
            """CREATE TABLE IF NOT EXISTS footnote_audit (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                soa_id INTEGER NOT NULL,
                footnote_id INTEGER,
                action TEXT NOT NULL,
                before_json TEXT,
                after_json TEXT,
                performed_at TEXT NOT NULL
            )"""
        )
        conn.commit()
        conn.close()
        logger.info("_migrate_add_footnote_audit_table created footnote_audit table")
    except Exception as e:
        logger.warning("_migrate_add_footnote_audit_table failed: %s", e)


def _migrate_add_objective_table():
    """Create the objective table for USDM study objectives."""
    try:
        conn = _connect()
        cur = conn.cursor()
        cur.execute(
            """CREATE TABLE IF NOT EXISTS objective (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                soa_id INTEGER NOT NULL,
                objective_uid TEXT NOT NULL,
                name TEXT NOT NULL,
                label TEXT,
                description TEXT,
                text TEXT,
                level_code_uid TEXT,
                order_index INTEGER,
                UNIQUE(soa_id, objective_uid)
            )"""
        )
        conn.commit()
        conn.close()
        logger.info("_migrate_add_objective_table created objective table")
    except Exception as e:
        logger.warning("_migrate_add_objective_table failed: %s", e)


def _migrate_add_objective_audit_table():
    """Create objective_audit table for tracking objective mutations."""
    try:
        conn = _connect()
        cur = conn.cursor()
        cur.execute(
            """CREATE TABLE IF NOT EXISTS objective_audit (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                soa_id INTEGER NOT NULL,
                objective_id INTEGER,
                action TEXT NOT NULL,
                before_json TEXT,
                after_json TEXT,
                performed_at TEXT NOT NULL
            )"""
        )
        conn.commit()
        conn.close()
        logger.info("_migrate_add_objective_audit_table created objective_audit table")
    except Exception as e:
        logger.warning("_migrate_add_objective_audit_table failed: %s", e)


def _migrate_add_endpoint_table():
    """Create the endpoint table for USDM study endpoints."""
    try:
        conn = _connect()
        cur = conn.cursor()
        cur.execute(
            """CREATE TABLE IF NOT EXISTS endpoint (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                soa_id INTEGER NOT NULL,
                endpoint_uid TEXT NOT NULL,
                objective_uid TEXT,
                name TEXT NOT NULL,
                label TEXT,
                description TEXT,
                text TEXT,
                purpose TEXT,
                level_code_uid TEXT,
                order_index INTEGER,
                UNIQUE(soa_id, endpoint_uid)
            )"""
        )
        conn.commit()
        conn.close()
        logger.info("_migrate_add_endpoint_table created endpoint table")
    except Exception as e:
        logger.warning("_migrate_add_endpoint_table failed: %s", e)


def _migrate_add_endpoint_audit_table():
    """Create endpoint_audit table for tracking endpoint mutations."""
    try:
        conn = _connect()
        cur = conn.cursor()
        cur.execute(
            """CREATE TABLE IF NOT EXISTS endpoint_audit (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                soa_id INTEGER NOT NULL,
                endpoint_id INTEGER,
                action TEXT NOT NULL,
                before_json TEXT,
                after_json TEXT,
                performed_at TEXT NOT NULL
            )"""
        )
        conn.commit()
        conn.close()
        logger.info("_migrate_add_endpoint_audit_table created endpoint_audit table")
    except Exception as e:
        logger.warning("_migrate_add_endpoint_audit_table failed: %s", e)


def _migrate_add_study_title_table():
    try:
        conn = _connect()
        cur = conn.cursor()
        cur.execute(
            """CREATE TABLE IF NOT EXISTS study_title (
                id              INTEGER PRIMARY KEY AUTOINCREMENT,
                soa_id          INTEGER NOT NULL,
                study_title_uid TEXT NOT NULL,
                text            TEXT NOT NULL,
                type_code_uid   TEXT,
                order_index     INTEGER,
                UNIQUE(soa_id, study_title_uid)
            )"""
        )
        conn.commit()
        conn.close()
        logger.info("_migrate_add_study_title_table complete")
    except Exception as e:
        logger.warning("_migrate_add_study_title_table failed: %s", e)


def _migrate_add_study_title_audit_table():
    try:
        conn = _connect()
        cur = conn.cursor()
        cur.execute(
            """CREATE TABLE IF NOT EXISTS study_title_audit (
                id           INTEGER PRIMARY KEY AUTOINCREMENT,
                soa_id       INTEGER NOT NULL,
                title_id     INTEGER,
                action       TEXT NOT NULL,
                before_json  TEXT,
                after_json   TEXT,
                performed_at TEXT NOT NULL
            )"""
        )
        conn.commit()
        conn.close()
        logger.info("_migrate_add_study_title_audit_table complete")
    except Exception as e:
        logger.warning("_migrate_add_study_title_audit_table failed: %s", e)


def _migrate_add_study_intervention_table():
    try:
        conn = _connect()
        cur = conn.cursor()
        cur.execute(
            "CREATE TABLE IF NOT EXISTS study_intervention ("
            "id INTEGER PRIMARY KEY AUTOINCREMENT,"
            "soa_id INTEGER NOT NULL,"
            "intervention_uid TEXT NOT NULL,"
            "name TEXT NOT NULL,"
            "label TEXT,"
            "description TEXT,"
            "role_code_uid TEXT,"
            "type_code_uid TEXT,"
            "mrd_quantity_uid TEXT,"
            "mrd_value REAL,"
            "mrd_unit_alias_uid TEXT,"
            "order_index INTEGER,"
            "UNIQUE(soa_id, intervention_uid)"
            ")"
        )
        conn.commit()
        conn.close()
        logger.info("_migrate_add_study_intervention_table complete")
    except Exception as e:
        logger.warning("_migrate_add_study_intervention_table failed: %s", e)


def _migrate_add_study_intervention_code_table():
    try:
        conn = _connect()
        cur = conn.cursor()
        cur.execute(
            "CREATE TABLE IF NOT EXISTS study_intervention_code ("
            "id INTEGER PRIMARY KEY AUTOINCREMENT,"
            "soa_id INTEGER NOT NULL,"
            "intervention_id INTEGER NOT NULL,"
            "code_uid TEXT NOT NULL,"
            "order_index INTEGER"
            ")"
        )
        conn.commit()
        conn.close()
        logger.info("_migrate_add_study_intervention_code_table complete")
    except Exception as e:
        logger.warning("_migrate_add_study_intervention_code_table failed: %s", e)


def _migrate_add_study_intervention_audit_table():
    try:
        conn = _connect()
        cur = conn.cursor()
        cur.execute(
            "CREATE TABLE IF NOT EXISTS study_intervention_audit ("
            "id INTEGER PRIMARY KEY AUTOINCREMENT,"
            "soa_id INTEGER NOT NULL,"
            "intervention_id INTEGER,"
            "action TEXT NOT NULL,"
            "before_json TEXT,"
            "after_json TEXT,"
            "performed_at TEXT NOT NULL"
            ")"
        )
        conn.commit()
        conn.close()
        logger.info("_migrate_add_study_intervention_audit_table complete")
    except Exception as e:
        logger.warning("_migrate_add_study_intervention_audit_table failed: %s", e)


def _migrate_add_estimand_table():
    try:
        conn = _connect()
        cur = conn.cursor()
        cur.execute(
            "CREATE TABLE IF NOT EXISTS estimand ("
            "id INTEGER PRIMARY KEY AUTOINCREMENT,"
            "soa_id INTEGER NOT NULL,"
            "estimand_uid TEXT NOT NULL,"
            "name TEXT NOT NULL,"
            "label TEXT,"
            "description TEXT,"
            "population_summary TEXT,"
            "variable_of_interest_uid TEXT,"
            "order_index INTEGER,"
            "UNIQUE(soa_id, estimand_uid)"
            ")"
        )
        conn.commit()
        conn.close()
        logger.info("_migrate_add_estimand_table complete")
    except Exception as e:
        logger.warning("_migrate_add_estimand_table failed: %s", e)


def _migrate_add_estimand_intervention_table():
    try:
        conn = _connect()
        cur = conn.cursor()
        cur.execute(
            "CREATE TABLE IF NOT EXISTS estimand_intervention ("
            "id INTEGER PRIMARY KEY AUTOINCREMENT,"
            "soa_id INTEGER NOT NULL,"
            "estimand_id INTEGER NOT NULL,"
            "intervention_uid TEXT NOT NULL,"
            "order_index INTEGER"
            ")"
        )
        conn.commit()
        conn.close()
        logger.info("_migrate_add_estimand_intervention_table complete")
    except Exception as e:
        logger.warning("_migrate_add_estimand_intervention_table failed: %s", e)


def _migrate_add_intercurrent_event_table():
    try:
        conn = _connect()
        cur = conn.cursor()
        cur.execute(
            "CREATE TABLE IF NOT EXISTS intercurrent_event ("
            "id INTEGER PRIMARY KEY AUTOINCREMENT,"
            "soa_id INTEGER NOT NULL,"
            "estimand_id INTEGER NOT NULL,"
            "event_uid TEXT NOT NULL,"
            "name TEXT NOT NULL,"
            "label TEXT,"
            "description TEXT,"
            "text TEXT,"
            "strategy TEXT,"
            "order_index INTEGER,"
            "UNIQUE(soa_id, event_uid)"
            ")"
        )
        conn.commit()
        conn.close()
        logger.info("_migrate_add_intercurrent_event_table complete")
    except Exception as e:
        logger.warning("_migrate_add_intercurrent_event_table failed: %s", e)


def _migrate_add_estimand_audit_table():
    try:
        conn = _connect()
        cur = conn.cursor()
        cur.execute(
            "CREATE TABLE IF NOT EXISTS estimand_audit ("
            "id INTEGER PRIMARY KEY AUTOINCREMENT,"
            "soa_id INTEGER NOT NULL,"
            "estimand_id INTEGER,"
            "action TEXT NOT NULL,"
            "before_json TEXT,"
            "after_json TEXT,"
            "performed_at TEXT NOT NULL"
            ")"
        )
        conn.commit()
        conn.close()
        logger.info("_migrate_add_estimand_audit_table complete")
    except Exception as e:
        logger.warning("_migrate_add_estimand_audit_table failed: %s", e)


def _migrate_add_estimand_variable_table():
    try:
        conn = _connect()
        cur = conn.cursor()
        cur.execute(
            "CREATE TABLE IF NOT EXISTS estimand_variable ("
            "id INTEGER PRIMARY KEY AUTOINCREMENT,"
            "soa_id INTEGER NOT NULL,"
            "estimand_id INTEGER NOT NULL,"
            "endpoint_uid TEXT NOT NULL,"
            "order_index INTEGER"
            ")"
        )
        conn.commit()
        conn.close()
        logger.info("_migrate_add_estimand_variable_table complete")
    except Exception as e:
        logger.warning("_migrate_add_estimand_variable_table failed: %s", e)


def _migrate_add_indication_table():
    try:
        conn = _connect()
        cur = conn.cursor()
        cur.execute(
            "CREATE TABLE IF NOT EXISTS indication ("
            "id INTEGER PRIMARY KEY AUTOINCREMENT,"
            "soa_id INTEGER NOT NULL,"
            "indication_uid TEXT NOT NULL,"
            "name TEXT NOT NULL,"
            "label TEXT,"
            "description TEXT,"
            "is_rare_disease INTEGER NOT NULL DEFAULT 0,"
            "order_index INTEGER,"
            "UNIQUE(soa_id, indication_uid)"
            ")"
        )
        conn.commit()
        conn.close()
        logger.info("_migrate_add_indication_table complete")
    except Exception as e:
        logger.warning("_migrate_add_indication_table failed: %s", e)


def _migrate_add_indication_code_table():
    try:
        conn = _connect()
        cur = conn.cursor()
        cur.execute(
            "CREATE TABLE IF NOT EXISTS indication_code ("
            "id INTEGER PRIMARY KEY AUTOINCREMENT,"
            "soa_id INTEGER NOT NULL,"
            "indication_id INTEGER NOT NULL,"
            "code_uid TEXT NOT NULL,"
            "order_index INTEGER"
            ")"
        )
        conn.commit()
        conn.close()
        logger.info("_migrate_add_indication_code_table complete")
    except Exception as e:
        logger.warning("_migrate_add_indication_code_table failed: %s", e)


def _migrate_add_indication_audit_table():
    try:
        conn = _connect()
        cur = conn.cursor()
        cur.execute(
            "CREATE TABLE IF NOT EXISTS indication_audit ("
            "id INTEGER PRIMARY KEY AUTOINCREMENT,"
            "soa_id INTEGER NOT NULL,"
            "indication_id INTEGER,"
            "action TEXT NOT NULL,"
            "before_json TEXT,"
            "after_json TEXT,"
            "performed_at TEXT NOT NULL"
            ")"
        )
        conn.commit()
        conn.close()
        logger.info("_migrate_add_indication_audit_table complete")
    except Exception as e:
        logger.warning("_migrate_add_indication_audit_table failed: %s", e)


def _migrate_add_study_identifier_table():
    try:
        conn = _connect()
        cur = conn.cursor()
        cur.execute(
            "CREATE TABLE IF NOT EXISTS study_identifier ("
            "id INTEGER PRIMARY KEY AUTOINCREMENT,"
            "soa_id INTEGER NOT NULL,"
            "study_identifier_uid TEXT NOT NULL,"
            "text TEXT NOT NULL,"
            "scope_org_uid TEXT NOT NULL DEFAULT '',"
            "order_index INTEGER,"
            "UNIQUE(soa_id, study_identifier_uid)"
            ")"
        )
        conn.commit()
        conn.close()
        logger.info("_migrate_add_study_identifier_table complete")
    except Exception as e:
        logger.warning("_migrate_add_study_identifier_table failed: %s", e)


def _migrate_add_study_identifier_audit_table():
    try:
        conn = _connect()
        cur = conn.cursor()
        cur.execute(
            "CREATE TABLE IF NOT EXISTS study_identifier_audit ("
            "id INTEGER PRIMARY KEY AUTOINCREMENT,"
            "soa_id INTEGER NOT NULL,"
            "si_id INTEGER,"
            "action TEXT NOT NULL,"
            "before_json TEXT,"
            "after_json TEXT,"
            "performed_at TEXT NOT NULL"
            ")"
        )
        conn.commit()
        conn.close()
        logger.info("_migrate_add_study_identifier_audit_table complete")
    except Exception as e:
        logger.warning("_migrate_add_study_identifier_audit_table failed: %s", e)


def _migrate_add_element_intervention_table():
    """Create element_intervention junction table for StudyElement ↔ StudyIntervention."""
    try:
        conn = _connect()
        cur = conn.cursor()
        cur.execute(
            "CREATE TABLE IF NOT EXISTS element_intervention ("
            "id INTEGER PRIMARY KEY AUTOINCREMENT,"
            "soa_id INTEGER NOT NULL,"
            "element_id INTEGER NOT NULL,"
            "intervention_uid TEXT NOT NULL,"
            "order_index INTEGER,"
            "UNIQUE(soa_id, element_id, intervention_uid)"
            ")"
        )
        conn.commit()
        conn.close()
        logger.info("_migrate_add_element_intervention_table complete")
    except Exception as e:
        logger.warning("_migrate_add_element_intervention_table failed: %s", e)
