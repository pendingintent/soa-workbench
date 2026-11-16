"""Legacy migrations: Biomedical concepts, surrogates, concept groups, DSS/CRF links and code associations.

Bodies moved unchanged from migrate_database.py (frozen); new
migrations go in their own module (see README.md).
"""

from ..db import _connect
from .legacy_common import logger
from typing import Dict
from typing import Optional
import os
import re


def _migrate_activity_concept_add_href():
    """Add href column to store the API URI from which codeSystem and codeSystemVersion USDM properties can be derived"""
    try:
        conn = _connect()
        cur = conn.cursor()
        cur.execute("PRAGMA table_info(activity_concept)")
        cols = {r[1] for r in cur.fetchall()}
        if "href" not in cols:
            cur.execute("ALTER TABLE activity_concept ADD COLUMN href TEXT")
            conn.commit()
            logger.info("Added href column to the activity_concept table")
        conn.close()
    except Exception as e:
        logger.warning("activity_concept href migration failed: %s", e)


def _migrate_activity_concept_add_dss():
    """Add dss_title and dss_href columns to activity_concept for DSS assignment."""
    try:
        conn = _connect()
        cur = conn.cursor()
        cur.execute("PRAGMA table_info(activity_concept)")
        cols = {r[1] for r in cur.fetchall()}
        if "dss_title" not in cols:
            cur.execute("ALTER TABLE activity_concept ADD COLUMN dss_title TEXT")
            conn.commit()
            logger.info("Added dss_title column to activity_concept table")
        if "dss_href" not in cols:
            cur.execute("ALTER TABLE activity_concept ADD COLUMN dss_href TEXT")
            conn.commit()
            logger.info("Added dss_href column to activity_concept table")
        if "dss_domain" not in cols:
            cur.execute("ALTER TABLE activity_concept ADD COLUMN dss_domain TEXT")
            conn.commit()
            logger.info("Added dss_domain column to activity_concept table")
        conn.close()
    except Exception as e:
        logger.warning("activity_concept dss migration failed: %s", e)


def _migrate_biomedical_concept_audit():
    """Create biomedical_concept_audit table for tracking create/delete operations."""
    try:
        conn = _connect()
        cur = conn.cursor()
        cur.execute(
            """CREATE TABLE IF NOT EXISTS biomedical_concept_audit (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                soa_id INTEGER NOT NULL,
                biomedical_concept_id INTEGER,
                action TEXT NOT NULL,
                before_json TEXT,
                after_json TEXT,
                performed_at TEXT NOT NULL
            )"""
        )
        conn.commit()
        conn.close()
    except Exception as e:
        logger.warning("_migrate_biomedical_concept_audit: %s", e)


def _migrate_backfill_biomedical_concept_codes():
    """One-time backfill: for biomedical_concept rows that have no matching alias_code entry,
    create code + alias_code rows and update biomedical_concept.code to the alias_code_uid.
    """
    try:
        conn = _connect()
        cur = conn.cursor()

        cur.execute(
            """
            SELECT bc.id, bc.soa_id, bc.biomedical_concept_uid, ac2.concept_code
            FROM biomedical_concept bc
            LEFT JOIN alias_code ac
                   ON ac.alias_code_uid = bc.code AND ac.soa_id = bc.soa_id
            LEFT JOIN activity_concept ac2
                   ON ac2.concept_uid = bc.biomedical_concept_uid
                  AND ac2.soa_id = bc.soa_id
            WHERE ac.id IS NULL
            """
        )
        rows = cur.fetchall()

        for bc_id, soa_id, bc_uid, concept_code in rows:
            if not concept_code:
                continue  # no raw code available — nothing to create

            # get-or-create code row
            cur.execute(
                "SELECT code_uid FROM code WHERE soa_id=? AND code=?",
                (soa_id, concept_code),
            )
            row = cur.fetchone()
            if row:
                code_uid = row[0]
            else:
                cur.execute(
                    "SELECT code_uid FROM code"
                    " WHERE soa_id=? AND code_uid LIKE 'Code_%'",
                    (soa_id,),
                )
                existing = [x[0] for x in cur.fetchall() if x[0]]
                n = 1
                if existing:
                    try:
                        n = max(int(x.split("_")[1]) for x in existing) + 1
                    except Exception:
                        n = len(existing) + 1
                code_uid = f"Code_{n}"
                cur.execute(
                    "INSERT INTO code (soa_id, code_uid, code) VALUES (?,?,?)",
                    (soa_id, code_uid, concept_code),
                )

            # get-or-create alias_code row
            cur.execute(
                "SELECT alias_code_uid FROM alias_code"
                " WHERE soa_id=? AND standard_code=?",
                (soa_id, code_uid),
            )
            row = cur.fetchone()
            if row:
                alias_uid = row[0]
            else:
                cur.execute(
                    "SELECT alias_code_uid FROM alias_code"
                    " WHERE soa_id=? AND alias_code_uid LIKE 'AliasCode_%'",
                    (soa_id,),
                )
                existing = [x[0] for x in cur.fetchall() if x[0]]
                n = 1
                if existing:
                    try:
                        n = max(int(x.split("_")[1]) for x in existing) + 1
                    except Exception:
                        n = len(existing) + 1
                alias_uid = f"AliasCode_{n}"
                cur.execute(
                    "INSERT INTO alias_code"
                    " (soa_id, alias_code_uid, standard_code) VALUES (?,?,?)",
                    (soa_id, alias_uid, code_uid),
                )

            # patch biomedical_concept.code
            cur.execute(
                "UPDATE biomedical_concept SET code=? WHERE id=?",
                (alias_uid, bc_id),
            )

        conn.commit()
        conn.close()
    except Exception as e:
        logger.warning("_migrate_backfill_biomedical_concept_codes: %s", e)


def _migrate_biomedical_concept_property_add_uid():
    """Add biomedical_concept_uid column to biomedical_concept_property table."""
    try:
        conn = _connect()
        cur = conn.cursor()
        cur.execute("PRAGMA table_info(biomedical_concept_property)")
        cols = {r[1] for r in cur.fetchall()}
        if "biomedical_concept_uid" not in cols:
            cur.execute(
                "ALTER TABLE biomedical_concept_property"
                " ADD COLUMN biomedical_concept_uid TEXT"
            )
            conn.commit()
            logger.info(
                "Added biomedical_concept_uid column to biomedical_concept_property"
            )
        conn.close()
    except Exception as e:
        logger.warning("_migrate_biomedical_concept_property_add_uid: %s", e)


def _migrate_repoint_stale_bc_code_chains():
    """Realign biomedical_concept.code chain with activity_concept.concept_code.

    Legacy data-loading paths left some biomedical_concept rows whose
    code/alias_code/code chain points at a different concept than the one
    recorded in activity_concept. For each mismatch, this migration:

      1. Ensures a (code, alias_code) chain exists for the correct
         activity_concept.concept_code in the same soa.
      2. Repoints biomedical_concept.code to the correct alias_code_uid
         and rewrites biomedical_concept.name to activity_concept.concept_title.
      3. Drops the now-orphaned old alias_code and code rows.
      4. Fires a synchronous CDISC BC API call for each repointed
         concept_code to populate code.decode/code_system/code_system_version
         and biomedical_concept.label/description/name. Network or API
         failures are logged and the migration continues — the normal
         enrichment path will backfill on next user interaction.

    Idempotent: re-running finds no mismatches and is a no-op.
    """
    try:
        import requests

        conn = _connect()
        cur = conn.cursor()
        cur.execute(
            """
            SELECT bc.id, bc.soa_id, bc.biomedical_concept_uid,
                   bc.code, ac.concept_code, ac.concept_title
            FROM biomedical_concept bc
            INNER JOIN activity_concept ac
              ON bc.biomedical_concept_uid = ac.concept_uid
             AND bc.soa_id = ac.soa_id
            INNER JOIN alias_code a
              ON bc.code = a.alias_code_uid AND bc.soa_id = a.soa_id
            INNER JOIN code c
              ON a.standard_code = c.code_uid AND a.soa_id = c.soa_id
            WHERE ac.concept_code IS NOT NULL
              AND ac.concept_code != c.code
            """
        )
        rows = cur.fetchall()
        if not rows:
            conn.close()
            return
        touched = set()  # (soa_id, concept_code)
        for bc_id, soa_id, bc_uid, old_alias, new_code, new_title in rows:
            # get-or-create the correct code row
            cur.execute(
                "SELECT code_uid FROM code WHERE soa_id=? AND code=?",
                (soa_id, new_code),
            )
            r = cur.fetchone()
            if r:
                new_code_uid = r[0]
            else:
                cur.execute(
                    "SELECT code_uid FROM code"
                    " WHERE soa_id=? AND code_uid LIKE 'Code_%'"
                    " UNION"
                    " SELECT code_uid FROM code_association"
                    " WHERE soa_id=? AND code_uid LIKE 'Code_%'",
                    (soa_id, soa_id),
                )
                existing = [x[0] for x in cur.fetchall() if x[0]]
                n = (
                    max(
                        (int(x.split("_")[1]) for x in existing),
                        default=0,
                    )
                    + 1
                )
                new_code_uid = f"Code_{n}"
                cur.execute(
                    "INSERT INTO code (soa_id, code_uid, code) VALUES (?,?,?)",
                    (soa_id, new_code_uid, new_code),
                )
            # get-or-create the alias_code row
            cur.execute(
                "SELECT alias_code_uid FROM alias_code"
                " WHERE soa_id=? AND standard_code=?",
                (soa_id, new_code_uid),
            )
            r = cur.fetchone()
            if r:
                new_alias_uid = r[0]
            else:
                cur.execute(
                    "SELECT alias_code_uid FROM alias_code"
                    " WHERE soa_id=? AND alias_code_uid LIKE 'AliasCode_%'",
                    (soa_id,),
                )
                existing = [x[0] for x in cur.fetchall() if x[0]]
                n = (
                    max(
                        (int(x.split("_")[1]) for x in existing),
                        default=0,
                    )
                    + 1
                )
                new_alias_uid = f"AliasCode_{n}"
                cur.execute(
                    "INSERT INTO alias_code"
                    " (soa_id, alias_code_uid, standard_code)"
                    " VALUES (?,?,?)",
                    (soa_id, new_alias_uid, new_code_uid),
                )
            # repoint the BC and refresh name to best-available title
            cur.execute(
                "UPDATE biomedical_concept SET code=?, name=? WHERE id=?",
                (new_alias_uid, new_title, bc_id),
            )
            touched.add((soa_id, new_code))
            # orphan-cleanup: old alias_code -> old code
            cur.execute(
                "SELECT 1 FROM biomedical_concept WHERE soa_id=? AND code=? LIMIT 1",
                (soa_id, old_alias),
            )
            if cur.fetchone():
                continue
            cur.execute(
                "SELECT standard_code FROM alias_code"
                " WHERE soa_id=? AND alias_code_uid=?",
                (soa_id, old_alias),
            )
            code_row = cur.fetchone()
            cur.execute(
                "DELETE FROM alias_code WHERE soa_id=? AND alias_code_uid=?",
                (soa_id, old_alias),
            )
            if code_row:
                old_code_uid = code_row[0]
                cur.execute(
                    "SELECT 1 FROM alias_code"
                    " WHERE soa_id=? AND standard_code=? LIMIT 1",
                    (soa_id, old_code_uid),
                )
                if not cur.fetchone():
                    cur.execute(
                        "DELETE FROM code WHERE soa_id=? AND code_uid=?",
                        (soa_id, old_code_uid),
                    )
        conn.commit()
        # synchronous enrichment for each touched (soa_id, concept_code)
        api_key = os.environ.get("CDISC_API_KEY") or os.environ.get(
            "CDISC_SUBSCRIPTION_KEY"
        )
        subscription_key = os.environ.get("CDISC_SUBSCRIPTION_KEY") or api_key
        headers = {"Accept": "application/json"}
        if subscription_key:
            headers["Ocp-Apim-Subscription-Key"] = subscription_key
        if api_key:
            headers["Authorization"] = f"Bearer {api_key}"
            headers["api-key"] = api_key
        for soa_id, concept_code in touched:
            try:
                url = (
                    "https://api.library.cdisc.org/api/cosmos/v2/"
                    "mdr/bc/biomedicalconcepts/" + concept_code
                )
                resp = requests.get(url, headers=headers, timeout=15)
                if resp.status_code != 200:
                    continue
                data = resp.json()
                href = (data.get("_links") or {}).get("parentPackage") or {}
                href = href.get("href", "") if isinstance(href, dict) else ""
                try:
                    code_system_version = href.split("/")[4]
                except Exception:
                    code_system_version = ""
                short_name = data.get("shortName")
                definition = data.get("definition")
                cur.execute(
                    "UPDATE code SET code_system=?, code_system_version=?,"
                    " decode=? WHERE code=? AND soa_id=?",
                    (
                        href,
                        code_system_version,
                        short_name,
                        concept_code,
                        soa_id,
                    ),
                )
                cur.execute(
                    """
                    UPDATE biomedical_concept
                       SET name=?, label=?, description=?
                     WHERE soa_id=?
                       AND biomedical_concept_uid IN (
                           SELECT concept_uid FROM activity_concept
                            WHERE soa_id=? AND concept_code=?
                              AND concept_uid IS NOT NULL
                       )
                    """,
                    (
                        short_name,
                        short_name,
                        definition,
                        soa_id,
                        soa_id,
                        concept_code,
                    ),
                )
                conn.commit()
            except Exception as e:
                logger.warning(
                    "_migrate_repoint_stale_bc_code_chains enrich soa=%s code=%s: %s",
                    soa_id,
                    concept_code,
                    e,
                )
        conn.close()
    except Exception as e:
        logger.warning("_migrate_repoint_stale_bc_code_chains: %s", e)


def _migrate_add_bc_surrogate_table():
    """Create biomedical_concept_surrogate table if missing."""
    try:
        conn = _connect()
        cur = conn.cursor()
        cur.execute(
            """CREATE TABLE IF NOT EXISTS biomedical_concept_surrogate (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                soa_id INT NOT NULL,
                surrogate_uid TEXT NOT NULL,
                name TEXT NOT NULL,
                label TEXT,
                description TEXT,
                reference TEXT,
                UNIQUE(surrogate_uid, soa_id)
            )"""
        )
        conn.commit()
        conn.close()
        logger.info(
            "_migrate_add_bc_surrogate_table: biomedical_concept_surrogate ready"
        )
    except Exception as e:
        logger.warning("_migrate_add_bc_surrogate_table failed: %s", e)


def _migrate_add_activity_surrogate_table():
    """Create activity_surrogate junction table if missing."""
    try:
        conn = _connect()
        cur = conn.cursor()
        cur.execute(
            """CREATE TABLE IF NOT EXISTS activity_surrogate (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                soa_id INT NOT NULL,
                activity_uid TEXT NOT NULL,
                surrogate_uid TEXT NOT NULL,
                UNIQUE(soa_id, activity_uid, surrogate_uid)
            )"""
        )
        conn.commit()
        conn.close()
        logger.info("_migrate_add_activity_surrogate_table: activity_surrogate ready")
    except Exception as e:
        logger.warning("_migrate_add_activity_surrogate_table failed: %s", e)


def _migrate_add_bc_surrogate_audit_table():
    """Create biomedical_concept_surrogate_audit table if missing."""
    try:
        conn = _connect()
        cur = conn.cursor()
        cur.execute(
            """CREATE TABLE IF NOT EXISTS biomedical_concept_surrogate_audit (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                soa_id INTEGER NOT NULL,
                surrogate_id INTEGER,
                action TEXT NOT NULL,
                before_json TEXT,
                after_json TEXT,
                performed_at TEXT NOT NULL
            )"""
        )
        conn.commit()
        conn.close()
        logger.info("_migrate_add_bc_surrogate_audit_table: audit table ready")
    except Exception as e:
        logger.warning("_migrate_add_bc_surrogate_audit_table failed: %s", e)


def _migrate_add_concept_group_table():
    """Create global concept_group and concept_group_concept tables if missing."""
    try:
        conn = _connect()
        cur = conn.cursor()
        cur.execute(
            """CREATE TABLE IF NOT EXISTS concept_group (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                concept_group_uid TEXT NOT NULL UNIQUE,
                name TEXT NOT NULL,
                label TEXT,
                description TEXT
            )"""
        )
        cur.execute(
            """CREATE TABLE IF NOT EXISTS concept_group_concept (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                concept_group_uid TEXT NOT NULL,
                concept_code TEXT NOT NULL,
                concept_title TEXT,
                UNIQUE(concept_group_uid, concept_code)
            )"""
        )
        conn.commit()
        conn.close()
        logger.info("_migrate_add_concept_group_table: concept_group tables ready")
    except Exception as e:
        logger.warning("_migrate_add_concept_group_table failed: %s", e)


def _migrate_concept_group_add_cdisc_source_columns():
    """Add columns to concept_group distinguishing CDISC-sourced rows
    (materialized from the cdisc-biomedical-concept-groupings service)
    from user-created Custom Concept Groups.
    """
    try:
        conn = _connect()
        cur = conn.cursor()
        cur.execute("PRAGMA table_info(concept_group)")
        cols = {r[1] for r in cur.fetchall()}
        if "source" not in cols:
            cur.execute(
                "ALTER TABLE concept_group ADD COLUMN source TEXT DEFAULT 'custom'"
            )
            logger.info("Added source column to concept_group")
        if "cdisc_scheme_id" not in cols:
            cur.execute("ALTER TABLE concept_group ADD COLUMN cdisc_scheme_id TEXT")
            logger.info("Added cdisc_scheme_id column to concept_group")
        if "cdisc_scheme_name" not in cols:
            cur.execute("ALTER TABLE concept_group ADD COLUMN cdisc_scheme_name TEXT")
            logger.info("Added cdisc_scheme_name column to concept_group")
        if "cdisc_value_id" not in cols:
            cur.execute("ALTER TABLE concept_group ADD COLUMN cdisc_value_id TEXT")
            logger.info("Added cdisc_value_id column to concept_group")
        conn.commit()
        conn.close()
    except Exception as e:
        logger.warning("_migrate_concept_group_add_cdisc_source_columns failed: %s", e)


def _migrate_activity_concept_add_concept_group_uid():
    """Add concept_group_uid column to activity_concept if missing."""
    try:
        conn = _connect()
        cur = conn.cursor()
        cur.execute("PRAGMA table_info(activity_concept)")
        cols = {r[1] for r in cur.fetchall()}
        if "concept_group_uid" not in cols:
            cur.execute(
                "ALTER TABLE activity_concept ADD COLUMN concept_group_uid TEXT"
            )
            conn.commit()
            logger.info("Added concept_group_uid column to activity_concept")
        conn.close()
    except Exception as e:
        logger.warning("_migrate_activity_concept_add_concept_group_uid failed: %s", e)


def _migrate_activity_concept_add_bc_category_name():
    """Add bc_category_name column to activity_concept if missing."""
    try:
        conn = _connect()
        cur = conn.cursor()
        cur.execute("PRAGMA table_info(activity_concept)")
        cols = {r[1] for r in cur.fetchall()}
        if "bc_category_name" not in cols:
            cur.execute("ALTER TABLE activity_concept ADD COLUMN bc_category_name TEXT")
            conn.commit()
            logger.info("Added bc_category_name column to activity_concept")
        conn.close()
    except Exception as e:
        logger.warning("_migrate_activity_concept_add_bc_category_name failed: %s", e)


def _migrate_surrogate_add_concept_group_uid():
    """Add concept_group_uid column to biomedical_concept_surrogate if missing."""
    try:
        conn = _connect()
        cur = conn.cursor()
        cur.execute("PRAGMA table_info(biomedical_concept_surrogate)")
        cols = {r[1] for r in cur.fetchall()}
        if "concept_group_uid" not in cols:
            cur.execute(
                "ALTER TABLE biomedical_concept_surrogate "
                "ADD COLUMN concept_group_uid TEXT"
            )
            conn.commit()
            logger.info(
                "Added concept_group_uid column to biomedical_concept_surrogate"
            )
        conn.close()
    except Exception as e:
        logger.warning("_migrate_surrogate_add_concept_group_uid failed: %s", e)


def _migrate_activity_surrogate_add_concept_group_uid():
    """Add concept_group_uid column to activity_surrogate if missing."""
    try:
        conn = _connect()
        cur = conn.cursor()
        cur.execute("PRAGMA table_info(activity_surrogate)")
        cols = {r[1] for r in cur.fetchall()}
        if "concept_group_uid" not in cols:
            cur.execute(
                "ALTER TABLE activity_surrogate ADD COLUMN concept_group_uid TEXT"
            )
            conn.commit()
            logger.info("Added concept_group_uid column to activity_surrogate")
        conn.close()
    except Exception as e:
        logger.warning(
            "_migrate_activity_surrogate_add_concept_group_uid failed: %s",
            e,
        )


def _migrate_add_activity_concept_dss_table():
    """Create activity_concept_dss table for one-to-many DSS assignments.

    Migrates any existing single-row assignments from the
    activity_concept.dss_title / dss_href columns into the new table.
    """
    try:
        conn = _connect()
        cur = conn.cursor()
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS activity_concept_dss (
                id          INTEGER PRIMARY KEY AUTOINCREMENT,
                soa_id      INTEGER NOT NULL,
                activity_id INTEGER NOT NULL,
                concept_code TEXT NOT NULL,
                dss_title   TEXT NOT NULL,
                dss_href    TEXT NOT NULL,
                dss_domain  TEXT
            )
            """
        )
        conn.commit()

        # Migrate existing single-row assignments from activity_concept
        cur.execute("PRAGMA table_info(activity_concept)")
        cols = {r[1] for r in cur.fetchall()}
        if "dss_title" in cols and "dss_href" in cols:
            # Only insert rows that are not already present
            cur.execute("SELECT COUNT(*) FROM activity_concept_dss")
            if cur.fetchone()[0] == 0:
                dss_domain_col = "dss_domain" if "dss_domain" in cols else "NULL"
                cur.execute(
                    f"""
                    INSERT INTO activity_concept_dss
                        (soa_id, activity_id, concept_code,
                         dss_title, dss_href, dss_domain)
                    SELECT soa_id, activity_id, concept_code,
                           dss_title, dss_href, {dss_domain_col}
                    FROM activity_concept
                    WHERE dss_title IS NOT NULL AND dss_title != ''
                    """
                )
                conn.commit()
                logger.info(
                    "Migrated existing DSS assignments into activity_concept_dss"
                )
        conn.close()
    except Exception as e:
        logger.warning("_migrate_add_activity_concept_dss_table failed: %s", e)


def _migrate_activity_concept_dss_add_display():
    """Add dss_display column to activity_concept_dss for human-readable title."""
    try:
        conn = _connect()
        cur = conn.cursor()
        cur.execute("PRAGMA table_info(activity_concept_dss)")
        cols = {r[1] for r in cur.fetchall()}
        if "dss_display" not in cols:
            cur.execute("ALTER TABLE activity_concept_dss ADD COLUMN dss_display TEXT")
            conn.commit()
            logger.info("Added dss_display column to activity_concept_dss")
        conn.close()
    except Exception as e:
        logger.warning("_migrate_activity_concept_dss_add_display failed: %s", e)


def _migrate_activity_concept_dss_add_extension_attribute_uid():
    """Add extension_attribute_uid column to activity_concept_dss.

    Stores the immutable ExtensionAttribute_N identifier for each DSS
    assignment so USDM exports produce stable IDs across runs.
    """
    try:
        conn = _connect()
        cur = conn.cursor()
        cur.execute("PRAGMA table_info(activity_concept_dss)")
        cols = {r[1] for r in cur.fetchall()}
        if "extension_attribute_uid" not in cols:
            cur.execute(
                "ALTER TABLE activity_concept_dss"
                " ADD COLUMN extension_attribute_uid TEXT"
            )
            conn.commit()
            logger.info("Added extension_attribute_uid column to activity_concept_dss")
        conn.close()
    except Exception as e:
        logger.warning(
            "_migrate_activity_concept_dss_add_extension_attribute_uid failed: %s",
            e,
        )


def _migrate_add_activity_concept_crf_table():
    """Create activity_concept_crf for 0-to-many CRF assignments per BC."""
    try:
        conn = _connect()
        cur = conn.cursor()
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS activity_concept_crf (
                id                    INTEGER PRIMARY KEY AUTOINCREMENT,
                soa_id                INTEGER NOT NULL,
                activity_id           INTEGER NOT NULL,
                concept_code          TEXT NOT NULL,
                crf_title             TEXT NOT NULL,
                crf_href              TEXT NOT NULL,
                extension_attribute_uid TEXT
            )
            """
        )
        conn.commit()
        conn.close()
    except Exception as e:
        logger.warning("_migrate_add_activity_concept_crf_table failed: %s", e)


def _migrate_add_bcp_response_code_table():
    """Create bcp_response_code table for ResponseCode entities."""
    try:
        conn = _connect()
        cur = conn.cursor()
        cur.execute(
            """CREATE TABLE IF NOT EXISTS bcp_response_code (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                soa_id INTEGER NOT NULL,
                biomedical_concept_property_uid TEXT NOT NULL,
                response_code_uid TEXT NOT NULL,
                name TEXT,
                label TEXT,
                is_enabled INTEGER NOT NULL DEFAULT 1,
                code TEXT,
                UNIQUE(response_code_uid, soa_id)
            )"""
        )
        conn.commit()
        conn.close()
        logger.info(
            "_migrate_add_bcp_response_code_table created bcp_response_code table"
        )
    except Exception as e:
        logger.warning("_migrate_add_bcp_response_code_table failed: %s", e)


def _migrate_add_decode_to_code_association():
    """Add decode TEXT column to code_association for human-readable term labels."""
    try:
        conn = _connect()
        cur = conn.cursor()
        cur.execute("PRAGMA table_info(code_association)")
        cols = {r[1] for r in cur.fetchall()}
        if "decode" not in cols:
            cur.execute("ALTER TABLE code_association ADD COLUMN decode TEXT")
            conn.commit()
            logger.info("_migrate_add_decode_to_code_association added decode column")
        conn.close()
    except Exception as e:
        logger.warning("_migrate_add_decode_to_code_association failed: %s", e)


def _migrate_remap_code_association_codes():
    """Remap code_association rows where code was stored as submission_value text.

    For codelists where the DDF CT now provides a proper conceptId (C-code),
    look up each stored value in the {submission_value: code} map.  When the
    stored value is found as a submission_value key and the mapped code differs,
    update code to the C-code and set decode to the original submission_value.
    Rows that already hold a C-code are not matched and are left unchanged.
    """
    try:
        from soa_builder.web.utils import get_ddf_ct_codelist_map_by_submission

        conn = _connect()
        cur = conn.cursor()
        cur.execute(
            "SELECT code_uid, soa_id, codelist_code, code "
            "FROM code_association "
            "WHERE codelist_code IS NOT NULL AND codelist_code != ''"
        )
        rows = cur.fetchall()
        updated = 0
        _sv_cache: Dict[str, Dict[str, str]] = {}
        for code_uid, soa_id, codelist_code, stored_code in rows:
            if not codelist_code or not stored_code:
                continue
            if codelist_code not in _sv_cache:
                _sv_cache[codelist_code] = get_ddf_ct_codelist_map_by_submission(
                    codelist_code
                )
            by_sv = _sv_cache[codelist_code]
            actual_code = by_sv.get(stored_code)
            if actual_code and actual_code != stored_code:
                cur.execute(
                    "UPDATE code_association SET code=?, decode=? "
                    "WHERE code_uid=? AND soa_id=?",
                    (actual_code, stored_code, code_uid, soa_id),
                )
                updated += 1
        conn.commit()
        conn.close()
        logger.info("_migrate_remap_code_association_codes remapped %d rows", updated)
    except Exception as e:
        logger.warning("_migrate_remap_code_association_codes failed: %s", e)


def _migrate_backfill_code_association_decode():
    """Populate decode for existing code_association rows via DDF CT lookup."""
    try:
        from soa_builder.web.utils import get_ddf_ct_term

        conn = _connect()
        cur = conn.cursor()
        cur.execute(
            "SELECT code_uid, soa_id, codelist_code, code "
            "FROM code_association "
            "WHERE (decode IS NULL OR decode = '') "
            "AND codelist_code IS NOT NULL AND codelist_code != ''"
        )
        rows = cur.fetchall()
        updated = 0
        for code_uid, soa_id, codelist_code, code in rows:
            if not codelist_code or not code:
                continue
            term: Optional[dict] = get_ddf_ct_term(codelist_code, code)
            decode = (term.get("submission_value") or code) if term else code
            cur.execute(
                "UPDATE code_association SET decode=? WHERE code_uid=? AND soa_id=?",
                (decode, code_uid, soa_id),
            )
            updated += 1
        conn.commit()
        conn.close()
        logger.info(
            "_migrate_backfill_code_association_decode backfilled %d rows",
            updated,
        )
    except Exception as e:
        logger.warning("_migrate_backfill_code_association_decode failed: %s", e)


def _migrate_repair_broken_bc_code_chains():
    """Repair biomedical_concept rows whose alias_code exists but code is missing.

    When code rows are deleted without cascading to their alias_code references,
    the INNER JOIN in build_usdm_biomedical_concepts silently excludes those BCs
    from USDM output. This migration re-creates the missing code rows using the
    concept_code from activity_concept and repoints biomedical_concept.code to
    a fresh alias_code/code pair. The now-orphaned old alias_code row is left in
    place (harmless) since the BC no longer references it.
    """
    try:
        conn = _connect()
        cur = conn.cursor()
        cur.execute(
            """
            SELECT bc.id, bc.soa_id, bc.biomedical_concept_uid, ac.concept_code
            FROM biomedical_concept bc
            INNER JOIN alias_code a
                    ON a.alias_code_uid = bc.code AND a.soa_id = bc.soa_id
            LEFT  JOIN code c
                    ON c.code_uid = a.standard_code AND c.soa_id = a.soa_id
            LEFT  JOIN activity_concept ac
                    ON ac.concept_uid = bc.biomedical_concept_uid
                   AND ac.soa_id = bc.soa_id
            WHERE c.code_uid IS NULL
            """
        )
        rows = cur.fetchall()
        if not rows:
            conn.close()
            return

        repaired = 0
        for bc_id, soa_id, bc_uid, concept_code in rows:
            if not concept_code:
                continue

            # get-or-create code row for this concept_code
            cur.execute(
                "SELECT code_uid FROM code WHERE soa_id=? AND code=?",
                (soa_id, concept_code),
            )
            row = cur.fetchone()
            if row:
                code_uid = row[0]
            else:
                cur.execute(
                    "SELECT code_uid FROM code"
                    " WHERE soa_id=? AND code_uid LIKE 'Code_%'"
                    " UNION SELECT code_uid FROM code_association"
                    " WHERE soa_id=? AND code_uid LIKE 'Code_%'",
                    (soa_id, soa_id),
                )
                existing = [x[0] for x in cur.fetchall() if x[0]]
                n = max((int(x.split("_")[1]) for x in existing), default=0) + 1
                code_uid = f"Code_{n}"
                cur.execute(
                    "INSERT INTO code (soa_id, code_uid, code) VALUES (?,?,?)",
                    (soa_id, code_uid, concept_code),
                )

            # get-or-create alias_code row pointing to that code
            cur.execute(
                "SELECT alias_code_uid FROM alias_code"
                " WHERE soa_id=? AND standard_code=?",
                (soa_id, code_uid),
            )
            row = cur.fetchone()
            if row:
                alias_uid = row[0]
            else:
                cur.execute(
                    "SELECT alias_code_uid FROM alias_code"
                    " WHERE soa_id=? AND alias_code_uid LIKE 'AliasCode_%'",
                    (soa_id,),
                )
                existing = [x[0] for x in cur.fetchall() if x[0]]
                n = max((int(x.split("_")[1]) for x in existing), default=0) + 1
                alias_uid = f"AliasCode_{n}"
                cur.execute(
                    "INSERT INTO alias_code"
                    " (soa_id, alias_code_uid, standard_code) VALUES (?,?,?)",
                    (soa_id, alias_uid, code_uid),
                )

            cur.execute(
                "UPDATE biomedical_concept SET code=? WHERE id=?",
                (alias_uid, bc_id),
            )
            repaired += 1

        conn.commit()
        conn.close()
        if repaired:
            logger.info(
                "_migrate_repair_broken_bc_code_chains: repaired %d BCs",
                repaired,
            )
    except Exception as e:
        logger.warning("_migrate_repair_broken_bc_code_chains failed: %s", e)


def _migrate_add_activity_grouping_extension_table():
    """Create activity_grouping_extension table.

    Persists stable ExtensionAttribute_N / ExtensionClass_N UIDs for
    each (soa, activity, concept_group) assignment so USDM export can
    reuse identical UIDs across regenerations.
    """
    try:
        conn = _connect()
        cur = conn.cursor()
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS activity_grouping_extension (
                id                INTEGER PRIMARY KEY AUTOINCREMENT,
                soa_id            INTEGER NOT NULL,
                activity_uid      TEXT NOT NULL,
                concept_group_uid TEXT NOT NULL,
                ea_outer_uid      TEXT,
                ec_uid            TEXT,
                ea_scheme_uid     TEXT,
                ea_value_uid      TEXT,
                UNIQUE(soa_id, activity_uid, concept_group_uid)
            )
            """
        )
        conn.commit()
        conn.close()
        logger.info("_migrate_add_activity_grouping_extension_table complete")
    except Exception as e:
        logger.warning("_migrate_add_activity_grouping_extension_table failed: %s", e)


def _migrate_backfill_crf_href_latest_version():
    """Rewrite activity_concept_crf.crf_href from the dated-package
    format to the latest-version format, e.g.:
      /mdr/specializations/crf/packages/2026-06-30/specializations/TEMP
      -> /mdr/specializations/crf/specializations/TEMP
    Idempotent: rows already in the new format are left untouched."""
    pattern = re.compile(r"/mdr/specializations/crf/packages/[^/]+/specializations/")
    replacement = "/mdr/specializations/crf/specializations/"
    try:
        conn = _connect()
        cur = conn.cursor()
        cur.execute(
            "SELECT name FROM sqlite_master WHERE type='table'"
            " AND name='activity_concept_crf'"
        )
        if not cur.fetchone():
            conn.close()
            return
        cur.execute(
            "SELECT id, crf_href FROM activity_concept_crf"
            " WHERE crf_href LIKE '%/mdr/specializations/crf/packages/%'"
        )
        rows = cur.fetchall()
        updated = 0
        for row_id, href in rows:
            new_href = pattern.sub(replacement, href)
            if new_href != href:
                cur.execute(
                    "UPDATE activity_concept_crf SET crf_href=? WHERE id=?",
                    (new_href, row_id),
                )
                updated += 1
        if updated:
            conn.commit()
            logger.info(
                "_migrate_backfill_crf_href_latest_version updated %d rows", updated
            )
        conn.close()
    except Exception as e:
        logger.warning("_migrate_backfill_crf_href_latest_version failed: %s", e)
