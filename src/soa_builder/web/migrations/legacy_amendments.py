"""Legacy migrations: Study amendments, changes, document references, geographic scope, enrollment and governance dates.

Bodies moved unchanged from migrate_database.py (frozen); new
migrations go in their own module (see README.md).
"""

from ..db import _connect
from .legacy_common import logger


def _migrate_add_study_amendment_table():
    """Create study_amendment table for protocol amendment metadata."""
    try:
        conn = _connect()
        cur = conn.cursor()
        cur.execute(
            """CREATE TABLE IF NOT EXISTS study_amendment (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                soa_id INTEGER NOT NULL,
                freeze_id INTEGER NOT NULL,
                amendment_uid TEXT NOT NULL,
                name TEXT NOT NULL,
                number TEXT NOT NULL,
                summary TEXT NOT NULL,
                label TEXT,
                description TEXT,
                UNIQUE(soa_id, amendment_uid)
            )"""
        )
        conn.commit()
        conn.close()
        logger.info("_migrate_add_study_amendment_table created study_amendment table")
    except Exception as e:
        logger.warning("_migrate_add_study_amendment_table failed: %s", e)


def _migrate_add_study_amendment_audit_table():
    """Create study_amendment_audit table for tracking amendment mutations."""
    try:
        conn = _connect()
        cur = conn.cursor()
        cur.execute(
            """CREATE TABLE IF NOT EXISTS study_amendment_audit (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                soa_id INTEGER NOT NULL,
                amendment_id INTEGER,
                action TEXT NOT NULL,
                before_json TEXT,
                after_json TEXT,
                performed_at TEXT NOT NULL
            )"""
        )
        conn.commit()
        conn.close()
        logger.info(
            "_migrate_add_study_amendment_audit_table created "
            "study_amendment_audit table"
        )
    except Exception as e:
        logger.warning("_migrate_add_study_amendment_audit_table failed: %s", e)


def _migrate_add_study_amendment_reason_table():
    """Create study_amendment_reason table for primary/secondary reasons."""
    try:
        conn = _connect()
        cur = conn.cursor()
        cur.execute(
            """CREATE TABLE IF NOT EXISTS study_amendment_reason (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                soa_id INTEGER NOT NULL,
                amendment_uid TEXT NOT NULL,
                reason_uid TEXT NOT NULL,
                role TEXT NOT NULL,
                code_uid TEXT NOT NULL,
                other_reason TEXT,
                UNIQUE(soa_id, reason_uid)
            )"""
        )
        conn.commit()
        conn.close()
        logger.info(
            "_migrate_add_study_amendment_reason_table created "
            "study_amendment_reason table"
        )
    except Exception as e:
        logger.warning("_migrate_add_study_amendment_reason_table failed: %s", e)


def _migrate_add_study_amendment_reason_audit_table():
    """Create study_amendment_reason_audit table."""
    try:
        conn = _connect()
        cur = conn.cursor()
        cur.execute(
            """CREATE TABLE IF NOT EXISTS study_amendment_reason_audit (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                soa_id INTEGER NOT NULL,
                reason_id INTEGER,
                action TEXT NOT NULL,
                before_json TEXT,
                after_json TEXT,
                performed_at TEXT NOT NULL
            )"""
        )
        conn.commit()
        conn.close()
        logger.info(
            "_migrate_add_study_amendment_reason_audit_table created "
            "study_amendment_reason_audit table"
        )
    except Exception as e:
        logger.warning("_migrate_add_study_amendment_reason_audit_table failed: %s", e)


def _migrate_add_study_amendment_impact_table():
    """Create study_amendment_impact table for amendment impacts."""
    try:
        conn = _connect()
        cur = conn.cursor()
        cur.execute(
            """CREATE TABLE IF NOT EXISTS study_amendment_impact (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                soa_id INTEGER NOT NULL,
                amendment_uid TEXT NOT NULL,
                impact_uid TEXT NOT NULL,
                type_code_uid TEXT NOT NULL,
                text TEXT NOT NULL,
                is_substantial INTEGER NOT NULL DEFAULT 0,
                UNIQUE(soa_id, impact_uid)
            )"""
        )
        conn.commit()
        conn.close()
        logger.info(
            "_migrate_add_study_amendment_impact_table created "
            "study_amendment_impact table"
        )
    except Exception as e:
        logger.warning("_migrate_add_study_amendment_impact_table failed: %s", e)


def _migrate_add_study_amendment_impact_audit_table():
    """Create study_amendment_impact_audit table."""
    try:
        conn = _connect()
        cur = conn.cursor()
        cur.execute(
            """CREATE TABLE IF NOT EXISTS study_amendment_impact_audit (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                soa_id INTEGER NOT NULL,
                impact_id INTEGER,
                action TEXT NOT NULL,
                before_json TEXT,
                after_json TEXT,
                performed_at TEXT NOT NULL
            )"""
        )
        conn.commit()
        conn.close()
        logger.info(
            "_migrate_add_study_amendment_impact_audit_table created "
            "study_amendment_impact_audit table"
        )
    except Exception as e:
        logger.warning("_migrate_add_study_amendment_impact_audit_table failed: %s", e)


def _migrate_add_study_change_table():
    """Create study_change table for amendment changes."""
    try:
        conn = _connect()
        cur = conn.cursor()
        cur.execute(
            """CREATE TABLE IF NOT EXISTS study_change (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                soa_id INTEGER NOT NULL,
                amendment_uid TEXT NOT NULL,
                change_uid TEXT NOT NULL,
                name TEXT NOT NULL,
                label TEXT,
                description TEXT,
                summary TEXT NOT NULL,
                rationale TEXT NOT NULL,
                UNIQUE(soa_id, change_uid)
            )"""
        )
        conn.commit()
        conn.close()
        logger.info("_migrate_add_study_change_table created study_change table")
    except Exception as e:
        logger.warning("_migrate_add_study_change_table failed: %s", e)


def _migrate_add_study_change_audit_table():
    """Create study_change_audit table."""
    try:
        conn = _connect()
        cur = conn.cursor()
        cur.execute(
            """CREATE TABLE IF NOT EXISTS study_change_audit (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                soa_id INTEGER NOT NULL,
                change_id INTEGER,
                action TEXT NOT NULL,
                before_json TEXT,
                after_json TEXT,
                performed_at TEXT NOT NULL
            )"""
        )
        conn.commit()
        conn.close()
        logger.info(
            "_migrate_add_study_change_audit_table created study_change_audit table"
        )
    except Exception as e:
        logger.warning("_migrate_add_study_change_audit_table failed: %s", e)


def _migrate_add_document_content_reference_table():
    """Create document_content_reference table for change sections."""
    try:
        conn = _connect()
        cur = conn.cursor()
        cur.execute(
            """CREATE TABLE IF NOT EXISTS document_content_reference (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                soa_id INTEGER NOT NULL,
                change_uid TEXT NOT NULL,
                ref_uid TEXT NOT NULL,
                section_number TEXT NOT NULL,
                section_title TEXT NOT NULL,
                applies_to_id TEXT NOT NULL,
                UNIQUE(soa_id, ref_uid)
            )"""
        )
        conn.commit()
        conn.close()
        logger.info(
            "_migrate_add_document_content_reference_table created "
            "document_content_reference table"
        )
    except Exception as e:
        logger.warning("_migrate_add_document_content_reference_table failed: %s", e)


def _migrate_add_document_content_reference_audit_table():
    """Create document_content_reference_audit table."""
    try:
        conn = _connect()
        cur = conn.cursor()
        cur.execute(
            """CREATE TABLE IF NOT EXISTS document_content_reference_audit (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                soa_id INTEGER NOT NULL,
                ref_id INTEGER,
                action TEXT NOT NULL,
                before_json TEXT,
                after_json TEXT,
                performed_at TEXT NOT NULL
            )"""
        )
        conn.commit()
        conn.close()
        logger.info(
            "_migrate_add_document_content_reference_audit_table created "
            "document_content_reference_audit table"
        )
    except Exception as e:
        logger.warning(
            "_migrate_add_document_content_reference_audit_table failed: %s",
            e,
        )


def _migrate_add_amendment_geographic_scope_table():
    """Create amendment_geographic_scope table for GeographicScope entities."""
    try:
        conn = _connect()
        cur = conn.cursor()
        cur.execute(
            """CREATE TABLE IF NOT EXISTS amendment_geographic_scope (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                soa_id INTEGER NOT NULL,
                amendment_uid TEXT NOT NULL,
                scope_uid TEXT NOT NULL,
                type_code_uid TEXT NOT NULL,
                UNIQUE(soa_id, scope_uid)
            )"""
        )
        conn.commit()
        conn.close()
        logger.info(
            "_migrate_add_amendment_geographic_scope_table created "
            "amendment_geographic_scope table"
        )
    except Exception as e:
        logger.warning("_migrate_add_amendment_geographic_scope_table failed: %s", e)


def _migrate_add_amendment_geographic_scope_audit_table():
    """Create amendment_geographic_scope_audit table."""
    try:
        conn = _connect()
        cur = conn.cursor()
        cur.execute(
            """CREATE TABLE IF NOT EXISTS amendment_geographic_scope_audit (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                soa_id INTEGER NOT NULL,
                scope_id INTEGER,
                action TEXT NOT NULL,
                before_json TEXT,
                after_json TEXT,
                performed_at TEXT NOT NULL
            )"""
        )
        conn.commit()
        conn.close()
        logger.info(
            "_migrate_add_amendment_geographic_scope_audit_table created "
            "amendment_geographic_scope_audit table"
        )
    except Exception as e:
        logger.warning(
            "_migrate_add_amendment_geographic_scope_audit_table failed: %s",
            e,
        )


def _migrate_add_amendment_subject_enrollment_table():
    """Create amendment_subject_enrollment table for SubjectEnrollment entities."""
    try:
        conn = _connect()
        cur = conn.cursor()
        cur.execute(
            """CREATE TABLE IF NOT EXISTS amendment_subject_enrollment (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                soa_id INTEGER NOT NULL,
                amendment_uid TEXT NOT NULL,
                enrollment_uid TEXT NOT NULL,
                name TEXT NOT NULL,
                label TEXT,
                description TEXT,
                quantity_value REAL NOT NULL,
                quantity_unit_code_uid TEXT,
                for_scope_uid TEXT,
                for_study_cohort_id TEXT,
                for_study_site_id TEXT,
                UNIQUE(soa_id, enrollment_uid)
            )"""
        )
        conn.commit()
        conn.close()
        logger.info(
            "_migrate_add_amendment_subject_enrollment_table created "
            "amendment_subject_enrollment table"
        )
    except Exception as e:
        logger.warning("_migrate_add_amendment_subject_enrollment_table failed: %s", e)


def _migrate_add_amendment_subject_enrollment_audit_table():
    """Create amendment_subject_enrollment_audit table."""
    try:
        conn = _connect()
        cur = conn.cursor()
        cur.execute(
            """CREATE TABLE IF NOT EXISTS amendment_subject_enrollment_audit (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                soa_id INTEGER NOT NULL,
                enrollment_id INTEGER,
                action TEXT NOT NULL,
                before_json TEXT,
                after_json TEXT,
                performed_at TEXT NOT NULL
            )"""
        )
        conn.commit()
        conn.close()
        logger.info(
            "_migrate_add_amendment_subject_enrollment_audit_table created "
            "amendment_subject_enrollment_audit table"
        )
    except Exception as e:
        logger.warning(
            "_migrate_add_amendment_subject_enrollment_audit_table failed: %s",
            e,
        )


def _migrate_add_amendment_governance_date_table():
    """Create amendment_governance_date table for GovernanceDate entities."""
    try:
        conn = _connect()
        cur = conn.cursor()
        cur.execute(
            """CREATE TABLE IF NOT EXISTS amendment_governance_date (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                soa_id INTEGER NOT NULL,
                amendment_uid TEXT NOT NULL,
                date_uid TEXT NOT NULL,
                name TEXT NOT NULL,
                label TEXT,
                description TEXT,
                type_code_uid TEXT NOT NULL,
                date_value TEXT NOT NULL,
                UNIQUE(soa_id, date_uid)
            )"""
        )
        conn.commit()
        conn.close()
        logger.info(
            "_migrate_add_amendment_governance_date_table created "
            "amendment_governance_date table"
        )
    except Exception as e:
        logger.warning("_migrate_add_amendment_governance_date_table failed: %s", e)


def _migrate_add_amendment_governance_date_audit_table():
    """Create amendment_governance_date_audit table."""
    try:
        conn = _connect()
        cur = conn.cursor()
        cur.execute(
            """CREATE TABLE IF NOT EXISTS amendment_governance_date_audit (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                soa_id INTEGER NOT NULL,
                date_id INTEGER,
                action TEXT NOT NULL,
                before_json TEXT,
                after_json TEXT,
                performed_at TEXT NOT NULL
            )"""
        )
        conn.commit()
        conn.close()
        logger.info(
            "_migrate_add_amendment_governance_date_audit_table created "
            "amendment_governance_date_audit table"
        )
    except Exception as e:
        logger.warning(
            "_migrate_add_amendment_governance_date_audit_table failed: %s",
            e,
        )


def _migrate_add_governance_date_geographic_scope_table():
    """Create governance_date_geographic_scope junction table."""
    try:
        conn = _connect()
        cur = conn.cursor()
        cur.execute(
            """CREATE TABLE IF NOT EXISTS governance_date_geographic_scope (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                soa_id INTEGER NOT NULL,
                date_uid TEXT NOT NULL,
                scope_uid TEXT NOT NULL
            )"""
        )
        conn.commit()
        conn.close()
        logger.info(
            "_migrate_add_governance_date_geographic_scope_table created "
            "governance_date_geographic_scope table"
        )
    except Exception as e:
        logger.warning(
            "_migrate_add_governance_date_geographic_scope_table failed: %s",
            e,
        )


def _migrate_add_location_code_uid_to_geo_scope():
    try:
        conn = _connect()
        cur = conn.cursor()
        cur.execute(
            "ALTER TABLE amendment_geographic_scope ADD COLUMN location_code_uid TEXT"
        )
        conn.commit()
        conn.close()
        logger.info("_migrate_add_location_code_uid_to_geo_scope added column")
    except Exception as e:
        logger.warning("_migrate_add_location_code_uid_to_geo_scope failed: %s", e)
