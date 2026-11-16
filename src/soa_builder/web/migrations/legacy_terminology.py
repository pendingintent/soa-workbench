"""Legacy migrations: Reference terminology tables (countries, regions) and dropped CT tables.

Bodies moved unchanged from migrate_database.py (frozen); new
migrations go in their own module (see README.md).
"""

from ..db import _connect
from .legacy_common import logger
import csv
import pathlib


def _migrate_drop_protocol_terminology_tables():
    """Drop the legacy protocol_terminology and protocol_terminology_audit tables.

    Protocol CT is now sourced live from the CDISC Library API, so these
    local tables are obsolete. Idempotent: DROP TABLE IF EXISTS is safe to
    re-run on an already-clean database.
    """
    try:
        conn = _connect()
        cur = conn.cursor()
        cur.execute("DROP TABLE IF EXISTS protocol_terminology")
        cur.execute("DROP TABLE IF EXISTS protocol_terminology_audit")
        conn.commit()
        conn.close()
    except Exception as e:
        logger.warning("_migrate_drop_protocol_terminology_tables failed: %s", e)


def _migrate_drop_ddf_terminology_tables():
    """Drop the legacy ddf_terminology and ddf_terminology_audit tables.

    DDF CT is now sourced live from the CDISC Library API, so these local
    tables are obsolete. Idempotent.
    """
    try:
        conn = _connect()
        cur = conn.cursor()
        cur.execute("DROP TABLE IF EXISTS ddf_terminology")
        cur.execute("DROP TABLE IF EXISTS ddf_terminology_audit")
        conn.commit()
        conn.close()
    except Exception as e:
        logger.warning("_migrate_drop_ddf_terminology_tables failed: %s", e)


_FILES_DIR = pathlib.Path(__file__).parent.parent.parent.parent.parent / "files"


def _migrate_create_country_codes_table():
    try:
        conn = _connect()
        cur = conn.cursor()
        cur.execute(
            "CREATE TABLE IF NOT EXISTS country_codes ("
            "id INTEGER PRIMARY KEY AUTOINCREMENT,"
            "country_name TEXT NOT NULL,"
            "country_numeric_code TEXT NOT NULL"
            ")"
        )
        cur.execute("SELECT COUNT(*) FROM country_codes")
        if cur.fetchone()[0] == 0:
            csv_path = _FILES_DIR / "country_codes.csv"
            with open(csv_path, newline="", encoding="utf-8") as f:
                reader = csv.DictReader(f)
                rows = [(r["country_name"], r["country_numeric_code"]) for r in reader]
            cur.executemany(
                "INSERT INTO country_codes "
                "(country_name,country_numeric_code) VALUES (?,?)",
                rows,
            )
        conn.commit()
        conn.close()
        logger.info("_migrate_create_country_codes_table complete")
    except Exception as e:
        logger.warning("_migrate_create_country_codes_table failed: %s", e)


def _migrate_create_geographic_regions_table():
    try:
        conn = _connect()
        cur = conn.cursor()
        cur.execute(
            "CREATE TABLE IF NOT EXISTS geographic_regions ("
            "id INTEGER PRIMARY KEY AUTOINCREMENT,"
            "region TEXT NOT NULL,"
            "subregion TEXT NOT NULL,"
            "region_numeric_code TEXT NOT NULL"
            ")"
        )
        cur.execute("SELECT COUNT(*) FROM geographic_regions")
        if cur.fetchone()[0] == 0:
            csv_path = _FILES_DIR / "geographic_regions.csv"
            with open(csv_path, newline="", encoding="utf-8") as f:
                reader = csv.DictReader(f)
                rows = [
                    (r["region"], r["subregion"], r["region_numeric_code"])
                    for r in reader
                ]
            cur.executemany(
                "INSERT INTO geographic_regions "
                "(region,subregion,region_numeric_code) VALUES (?,?,?)",
                rows,
            )
        conn.commit()
        conn.close()
        logger.info("_migrate_create_geographic_regions_table complete")
    except Exception as e:
        logger.warning("_migrate_create_geographic_regions_table failed: %s", e)
