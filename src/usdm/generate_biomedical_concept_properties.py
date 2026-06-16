#!/usr/bin/env python3
"""USDM generator for BiomedicalConceptProperty.

Sources property data from each Biomedical Concept's
``dataElementConcepts[]`` array (generic path) or ``variables[]`` array
(SDTM specialization path), returned by the CDISC Library API.

Persists rows in ``biomedical_concept_property`` and
``bcp_response_code`` so UIDs are stable across re-exports.
"""

import logging
import os
import re
import threading
from typing import Any, Dict, List, Optional

from soa_builder.web.db import _connect
from soa_builder.web.utils import (
    get_next_alias_code_uid,
    get_next_biomedical_concept_property_uid,
    get_next_code_uid,
    get_next_response_code_uid,
    get_protocol_ct_term,
)
from .usdm_utils import (
    _fetch_dss_spec,
    _get_biomedical_concept_data,
    _get_latest_bc_package_version,
)

logger = logging.getLogger("usdm.generate_biomedical_concept_properties")

BCP_CODE_SYSTEM = "http://www.cdisc.org"
_C_CODE_RE = re.compile(r"^C\d+$")

# Per-SOA lock: prevents two threads from racing on UID allocation for
# the same SOA (startup backfill vs. per-request background task).
_soa_locks: Dict[int, threading.Lock] = {}
_soa_locks_mutex = threading.Lock()


def _soa_lock(soa_id: int) -> threading.Lock:
    with _soa_locks_mutex:
        if soa_id not in _soa_locks:
            _soa_locks[soa_id] = threading.Lock()
        return _soa_locks[soa_id]


# SDTM variable suffixes (after 2-char domain prefix) that should not
# become BCPs — matches _process_property in cdisc_bc_library.py.
_EXCLUDED_PROPERTY_SUFFIXES = frozenset(
    [
        "TEST",
        "STRESN",
        "STRESU",
        "STRESC",
        "CLASCD",
        "LOINC",
        "LOT",
        "CAT",
        "SCAT",
        "LLT",
        "LLTCD",
        "HLT",
        "HLTCD",
        "PTCD",
        "BODSYS",
        "BDSYCD",
        "SOC",
        "SOCCD",
        "RLDEV",
    ]
)
_EXCLUDED_PROPERTY_NAMES = frozenset(["EPOCH"])


def _include_property(var_name: str) -> bool:
    """Return False for SDTM variables that should not become BCPs."""
    if var_name in _EXCLUDED_PROPERTY_NAMES:
        return False
    if len(var_name) > 2 and var_name[2:] in _EXCLUDED_PROPERTY_SUFFIXES:
        return False
    return True


def _version_from_package_href(href: str) -> str:
    """Extract the YYYY-MM-DD date from any package href.

    Works for both BC (/mdr/bc/packages/2025-09-23/biomedicalconcepts)
    and SDTM (/mdr/specializations/sdtm/packages/2025-12-16/datasetspecializations).
    """
    m = re.search(r"(\d{4}-\d{2}-\d{2})", href or "")
    return m.group(1) if m else ""


# ---------------------------------------------------------------------------
# Internal helpers kept for future ResponseCode (valueList) population
# ---------------------------------------------------------------------------


def _is_c_code(value: str) -> bool:
    return bool(value and _C_CODE_RE.match(value.strip()))


def _resolve_rc_code(value: str, codelist_code: Optional[str]) -> tuple:
    """Return (ncit_code, decode) for a ResponseCode value.

    Tries CT lookup when a codelist_code is known and value is not
    already a C-code; falls back to using value directly.
    """
    v = (value or "").strip()
    if not v:
        return None, None
    if _is_c_code(v):
        return v, v
    if codelist_code:
        term = get_protocol_ct_term(codelist_code, v)
        if term:
            ncit = term.get("conceptId") or term.get("code") or v
            decode = term.get("preferredTerm") or term.get("submission_value") or v
            return ncit, decode
    return v, v


def _upsert_response_codes(
    cur,
    soa_id: int,
    bcp_uid: str,
    values: List[str],
    codelist_code: Optional[str],
) -> None:
    """Insert bcp_response_code + alias_code + code rows for a BCP.

    Reserved for future valueList ResponseCode population.
    """
    for val in values:
        ncit_code, decode = _resolve_rc_code(val, codelist_code)
        if not ncit_code:
            continue
        cur.execute(
            "SELECT rc.response_code_uid"
            " FROM bcp_response_code rc"
            " LEFT JOIN alias_code ac ON rc.code = ac.alias_code_uid"
            " AND rc.soa_id = ac.soa_id"
            " LEFT JOIN code c ON ac.standard_code = c.code_uid"
            " AND ac.soa_id = c.soa_id"
            " WHERE rc.soa_id = ?"
            " AND rc.biomedical_concept_property_uid = ?"
            " AND c.code = ?"
            " LIMIT 1",
            (soa_id, bcp_uid, ncit_code),
        )
        if cur.fetchone():
            continue
        code_uid = get_next_code_uid(cur, soa_id)
        cur.execute(
            "INSERT INTO code"
            " (code_uid, soa_id, code, code_system,"
            " code_system_version, decode)"
            " VALUES (?, ?, ?, ?, ?, ?)",
            (
                code_uid,
                soa_id,
                ncit_code,
                BCP_CODE_SYSTEM,
                _get_latest_bc_package_version(),
                decode or ncit_code,
            ),
        )
        alias_uid = get_next_alias_code_uid(cur, soa_id)
        cur.execute(
            "INSERT INTO alias_code"
            " (alias_code_uid, soa_id, standard_code) VALUES (?, ?, ?)",
            (alias_uid, soa_id, code_uid),
        )
        rc_uid = get_next_response_code_uid(cur, soa_id)
        cur.execute(
            "INSERT INTO bcp_response_code"
            " (soa_id, biomedical_concept_property_uid,"
            " response_code_uid, name, label, is_enabled, code)"
            " VALUES (?, ?, ?, ?, ?, ?, ?)",
            (soa_id, bcp_uid, rc_uid, ncit_code, "", 1, alias_uid),
        )


# ---------------------------------------------------------------------------
# Internal DB write helpers
# ---------------------------------------------------------------------------


def _delete_bcp_rows(cur, soa_id: int, bc_uid: str) -> None:
    """Delete all BCP + RC rows for one BC and their orphaned code chains."""
    cur.execute(
        "SELECT rc.code FROM bcp_response_code rc"
        " WHERE rc.soa_id=? AND rc.biomedical_concept_property_uid IN"
        " (SELECT biomedical_concept_property_uid FROM"
        " biomedical_concept_property WHERE soa_id=? AND"
        " biomedical_concept_uid=?)",
        (soa_id, soa_id, bc_uid),
    )
    rc_alias_uids = [r[0] for r in cur.fetchall() if r[0]]

    cur.execute(
        "SELECT code FROM biomedical_concept_property"
        " WHERE soa_id=? AND biomedical_concept_uid=?",
        (soa_id, bc_uid),
    )
    bcp_alias_uids = [r[0] for r in cur.fetchall() if r[0]]

    all_alias_uids = rc_alias_uids + bcp_alias_uids
    if all_alias_uids:
        ph = ",".join("?" * len(all_alias_uids))
        cur.execute(
            f"DELETE FROM code WHERE code_uid IN"
            f" (SELECT standard_code FROM alias_code"
            f" WHERE alias_code_uid IN ({ph}) AND soa_id=?)",
            (*all_alias_uids, soa_id),
        )
        cur.execute(
            f"DELETE FROM alias_code WHERE alias_code_uid IN ({ph}) AND soa_id=?",
            (*all_alias_uids, soa_id),
        )

    cur.execute(
        "DELETE FROM bcp_response_code WHERE soa_id=?"
        " AND biomedical_concept_property_uid IN"
        " (SELECT biomedical_concept_property_uid FROM"
        " biomedical_concept_property WHERE soa_id=? AND"
        " biomedical_concept_uid=?)",
        (soa_id, soa_id, bc_uid),
    )
    cur.execute(
        "DELETE FROM biomedical_concept_property"
        " WHERE soa_id=? AND biomedical_concept_uid=?",
        (soa_id, bc_uid),
    )


def delete_bc_cascade(cur, soa_id: int, bc_uid: str) -> None:
    """Delete a BC's BCP + RC rows and their owned code chains.

    Does NOT delete the ``biomedical_concept`` row itself — callers own
    that. Call this from any path that removes a BC so its properties and
    response codes never orphan. Reuses ``_delete_bcp_rows``.
    """
    _delete_bcp_rows(cur, soa_id, bc_uid)


def _delete_owned_code_chains(cur, soa_id: int, alias_uids: List[str]) -> None:
    """Delete the code + alias_code rows owned by the given alias UIDs.

    Each BCP/RC owns a freshly created code + alias_code chain, so
    deleting by the row's own ``code`` (alias) UID is safe.
    """
    alias_uids = [a for a in alias_uids if a]
    if not alias_uids:
        return
    ph = ",".join("?" * len(alias_uids))
    cur.execute(
        f"DELETE FROM code WHERE code_uid IN"
        f" (SELECT standard_code FROM alias_code"
        f" WHERE alias_code_uid IN ({ph}) AND soa_id=?)",
        (*alias_uids, soa_id),
    )
    cur.execute(
        f"DELETE FROM alias_code WHERE alias_code_uid IN ({ph}) AND soa_id=?",
        (*alias_uids, soa_id),
    )


def _sweep_one_soa(cur, soa_id: int) -> Dict[str, int]:
    """Remove orphaned BCP/RC rows for one SOA. Returns count dict."""
    # An RC is orphaned if no BCP+BC pair backs it (covers RCs whose
    # parent BCP is gone AND RCs whose BCP exists but its BC is gone).
    cur.execute(
        "SELECT rc.code FROM bcp_response_code rc"
        " WHERE rc.soa_id=? AND NOT EXISTS ("
        "   SELECT 1 FROM biomedical_concept_property bcp"
        "   JOIN biomedical_concept bc"
        "     ON bc.soa_id=bcp.soa_id"
        "     AND bc.biomedical_concept_uid=bcp.biomedical_concept_uid"
        "   WHERE bcp.soa_id=rc.soa_id"
        "     AND bcp.biomedical_concept_property_uid"
        "       =rc.biomedical_concept_property_uid"
        " )",
        (soa_id,),
    )
    rc_alias_uids = [r[0] for r in cur.fetchall() if r[0]]

    cur.execute(
        "SELECT bcp.code FROM biomedical_concept_property bcp"
        " WHERE bcp.soa_id=? AND NOT EXISTS ("
        "   SELECT 1 FROM biomedical_concept bc"
        "   WHERE bc.soa_id=bcp.soa_id"
        "     AND bc.biomedical_concept_uid=bcp.biomedical_concept_uid"
        " )",
        (soa_id,),
    )
    bcp_alias_uids = [r[0] for r in cur.fetchall() if r[0]]

    _delete_owned_code_chains(cur, soa_id, rc_alias_uids + bcp_alias_uids)

    cur.execute(
        "DELETE FROM bcp_response_code"
        " WHERE soa_id=? AND biomedical_concept_property_uid NOT IN ("
        "   SELECT bcp.biomedical_concept_property_uid"
        "   FROM biomedical_concept_property bcp"
        "   JOIN biomedical_concept bc"
        "     ON bc.soa_id=bcp.soa_id"
        "     AND bc.biomedical_concept_uid=bcp.biomedical_concept_uid"
        "   WHERE bcp.soa_id=?"
        " )",
        (soa_id, soa_id),
    )
    rc_deleted = cur.rowcount

    cur.execute(
        "DELETE FROM biomedical_concept_property"
        " WHERE soa_id=? AND biomedical_concept_uid NOT IN ("
        "   SELECT biomedical_concept_uid FROM biomedical_concept"
        "   WHERE soa_id=?"
        " )",
        (soa_id, soa_id),
    )
    bcp_deleted = cur.rowcount
    return {"response_codes": rc_deleted, "properties": bcp_deleted}


def sweep_orphaned_bcp_rows(soa_id: Optional[int] = None) -> Dict[str, int]:
    """Remove orphaned BCP/RC rows and their owned code chains.

    Deletes, for the given SOA (or every SOA when ``soa_id`` is None):
      * ``bcp_response_code`` rows whose parent BCP (and its BC) is absent
      * ``biomedical_concept_property`` rows whose parent BC is absent
      * the ``code``/``alias_code`` chains those rows owned

    Always operates per-SOA so alias UIDs (unique only within a SOA) are
    never deleted across SOA boundaries. Idempotent. Returns aggregate
    counts ``{"response_codes": n, "properties": n}``.
    """
    conn = _connect()
    cur = conn.cursor()
    try:
        if soa_id is not None:
            soa_ids = [soa_id]
        else:
            cur.execute(
                "SELECT DISTINCT soa_id FROM ("
                "  SELECT soa_id FROM bcp_response_code"
                "  UNION SELECT soa_id FROM biomedical_concept_property"
                ")"
            )
            soa_ids = [r[0] for r in cur.fetchall()]

        totals = {"response_codes": 0, "properties": 0}
        for sid in soa_ids:
            counts = _sweep_one_soa(cur, sid)
            totals["response_codes"] += counts["response_codes"]
            totals["properties"] += counts["properties"]

        conn.commit()
        if totals["response_codes"] or totals["properties"]:
            logger.info(
                "sweep_orphaned_bcp_rows soa_id=%s removed %d RC, %d BCP",
                soa_id,
                totals["response_codes"],
                totals["properties"],
            )
        return totals
    except Exception:
        conn.rollback()
        logger.exception("sweep_orphaned_bcp_rows failed soa_id=%s", soa_id)
        raise
    finally:
        conn.close()


def _insert_bcp(
    cur,
    soa_id: int,
    bc_uid: str,
    ncit_code: str,
    name: str,
    datatype: str,
    decode: str,
    code_system_version: str,
    is_required: bool,
) -> str:
    """Insert one BCP row and return its UID."""
    code_uid = get_next_code_uid(cur, soa_id)
    cur.execute(
        "INSERT INTO code"
        " (code_uid, soa_id, code, code_system, code_system_version, decode)"
        " VALUES (?,?,?,?,?,?)",
        (
            code_uid,
            soa_id,
            ncit_code,
            BCP_CODE_SYSTEM,
            code_system_version,
            decode,
        ),
    )
    alias_uid = get_next_alias_code_uid(cur, soa_id)
    cur.execute(
        "INSERT INTO alias_code (alias_code_uid, soa_id, standard_code) VALUES (?,?,?)",
        (alias_uid, soa_id, code_uid),
    )
    bcp_uid = get_next_biomedical_concept_property_uid(cur, soa_id)
    cur.execute(
        "INSERT INTO biomedical_concept_property"
        " (soa_id, biomedical_concept_uid, biomedical_concept_property_uid,"
        " name, label, isRequired, isEnabled, datatype, code)"
        " VALUES (?,?,?,?,?,?,?,?,?)",
        (
            soa_id,
            bc_uid,
            bcp_uid,
            name,
            name,
            int(is_required),
            1,
            datatype,
            alias_uid,
        ),
    )
    return bcp_uid


def _insert_assigned_term_rc(
    cur,
    soa_id: int,
    bcp_uid: str,
    concept_id: str,
    value: str,
    code_system_version: str,
) -> None:
    """Insert one ResponseCode from an SDTM variable's assignedTerm.

    Idempotent: skips silently if a RC with the same concept_id already
    exists for this BCP (guards against concurrent process writes during
    hot reload).
    """
    cur.execute(
        "SELECT rc.id FROM bcp_response_code rc"
        " JOIN alias_code ac ON rc.code=ac.alias_code_uid AND rc.soa_id=ac.soa_id"
        " JOIN code c ON ac.standard_code=c.code_uid AND ac.soa_id=c.soa_id"
        " WHERE rc.soa_id=? AND rc.biomedical_concept_property_uid=?"
        " AND c.code=? LIMIT 1",
        (soa_id, bcp_uid, concept_id),
    )
    if cur.fetchone():
        return
    code_uid = get_next_code_uid(cur, soa_id)
    cur.execute(
        "INSERT INTO code"
        " (code_uid, soa_id, code, code_system, code_system_version, decode)"
        " VALUES (?,?,?,?,?,?)",
        (code_uid, soa_id, concept_id, BCP_CODE_SYSTEM, code_system_version, value),
    )
    alias_uid = get_next_alias_code_uid(cur, soa_id)
    cur.execute(
        "INSERT INTO alias_code (alias_code_uid, soa_id, standard_code) VALUES (?,?,?)",
        (alias_uid, soa_id, code_uid),
    )
    rc_uid = get_next_response_code_uid(cur, soa_id)
    cur.execute(
        "INSERT INTO bcp_response_code"
        " (soa_id, biomedical_concept_property_uid, response_code_uid,"
        " name, label, is_enabled, code)"
        " VALUES (?,?,?,?,?,?,?)",
        (soa_id, bcp_uid, rc_uid, value, value, 1, alias_uid),
    )


# ---------------------------------------------------------------------------
# Public: scoped population function (eager path)
# ---------------------------------------------------------------------------


def populate_biomedical_concept_properties_for_bc(
    soa_id: int, bc_uid: str, concept_code: str
) -> None:
    """Populate BCP + ResponseCode rows for one BC.

    Prefers the SDTM specialization ``variables[]`` when available;
    falls back to the generic BC ``dataElementConcepts[]``.

    Always deletes existing rows for this BC before inserting, so the
    function correctly handles DSS assignment changes.
    """
    data = _get_biomedical_concept_data(concept_code) or {}

    # Look up the DSS href stored by the user via the UI (activity_concept_dss)
    dss_href = _get_dss_href_for_bc(soa_id, bc_uid)
    sdtm = _fetch_dss_spec(dss_href) if dss_href else {}

    # Build lookup: dataElementConceptId → DEC dict for code resolution
    decs = data.get("dataElementConcepts") or []
    dec_by_id: Dict[str, Dict] = {
        dec["conceptId"]: dec for dec in decs if dec.get("conceptId")
    }

    with _soa_lock(soa_id):
        _populate_bcp_locked(soa_id, bc_uid, data, decs, dec_by_id, sdtm)


def _get_dss_href_for_bc(soa_id: int, bc_uid: str) -> str:
    """Return the stored DSS href for a BC, or '' if none is associated."""
    conn = _connect()
    cur = conn.cursor()
    try:
        cur.execute(
            "SELECT acd.dss_href FROM activity_concept_dss acd"
            " JOIN activity_concept ac"
            " ON ac.soa_id = acd.soa_id"
            " AND ac.activity_id = acd.activity_id"
            " AND ac.concept_code = acd.concept_code"
            " WHERE ac.concept_uid = ? AND acd.soa_id = ?"
            " LIMIT 1",
            (bc_uid, soa_id),
        )
        row = cur.fetchone()
        return row[0] if row and row[0] else ""
    finally:
        conn.close()


def _has_insertable_data(
    decs: List[Dict],
    dec_by_id: Dict[str, Dict],
    sdtm: Dict,
) -> bool:
    """Return True only if populate would actually insert at least one BCP.

    Prevents delete-then-no-insert data loss when API calls return empty.
    """
    if sdtm and sdtm.get("variables") and dec_by_id:
        # SDTM path requires both DSS variables AND DEC lookup to be non-empty
        for var in sdtm["variables"]:
            var_name = var.get("name") or ""
            if not _include_property(var_name):
                continue
            dec_id = var.get("dataElementConceptId")
            if dec_id and dec_by_id.get(dec_id):
                return True
        return False
    # Generic DEC path
    return any((dec.get("conceptId") or dec.get("ncitCode")) for dec in decs)


def _populate_bcp_locked(
    soa_id: int,
    bc_uid: str,
    data: Dict,
    decs: List[Dict],
    dec_by_id: Dict[str, Dict],
    sdtm: Dict,
) -> None:
    """Execute the DB writes for one BC under the SOA lock."""
    if not _has_insertable_data(decs, dec_by_id, sdtm):
        logger.warning(
            "No insertable BCP data for soa_id=%s bc_uid=%s — "
            "skipping populate to preserve existing rows",
            soa_id,
            bc_uid,
        )
        return

    conn = _connect()
    cur = conn.cursor()
    try:
        _delete_bcp_rows(cur, soa_id, bc_uid)

        if sdtm and sdtm.get("variables"):
            pkg_href = sdtm.get("_links", {}).get("parentPackage", {}).get("href", "")
            version = _version_from_package_href(pkg_href)

            for var in sdtm["variables"]:
                var_name = var.get("name") or ""
                if not _include_property(var_name):
                    continue
                is_req = bool(var.get("mandatoryValue", True))
                dec_id = var.get("dataElementConceptId")
                dec = dec_by_id.get(dec_id) if dec_id else None

                if not dec_id or not dec:
                    continue

                datatype = dec.get("dataType") or dec.get("datatype") or ""
                bcp_uid_new = _insert_bcp(
                    cur,
                    soa_id,
                    bc_uid,
                    dec["conceptId"],
                    var_name,
                    datatype,
                    dec["shortName"],
                    version,
                    is_req,
                )

                at = var.get("assignedTerm") or {}
                if at.get("conceptId") and at.get("value"):
                    _insert_assigned_term_rc(
                        cur,
                        soa_id,
                        bcp_uid_new,
                        at["conceptId"],
                        at["value"],
                        version,
                    )
        else:
            pkg_href = data.get("_links", {}).get("parentPackage", {}).get("href", "")
            version = _version_from_package_href(pkg_href)

            for dec in decs:
                ncit_code = dec.get("conceptId") or dec.get("ncitCode") or ""
                short_name = dec.get("shortName") or ""
                datatype = dec.get("dataType") or dec.get("datatype") or ""
                if not ncit_code:
                    continue
                _insert_bcp(
                    cur,
                    soa_id,
                    bc_uid,
                    ncit_code,
                    short_name,
                    datatype,
                    short_name,
                    version,
                    True,
                )

        conn.commit()
    except Exception:
        conn.rollback()
        logger.exception(
            "_populate_bcp_locked failed soa_id=%s bc_uid=%s",
            soa_id,
            bc_uid,
        )
        raise
    finally:
        conn.close()


# ---------------------------------------------------------------------------
# Public: SOA-wide population (lazy path + safety net)
# ---------------------------------------------------------------------------


def populate_biomedical_concept_properties(soa_id: int) -> None:
    """Upsert BCP + ResponseCode rows for every BC in this SOA.

    Delegates to ``populate_biomedical_concept_properties_for_bc`` per
    BC so both the eager and lazy paths share the same logic.
    """
    conn = _connect()
    cur = conn.cursor()
    try:
        cur.execute(
            "SELECT DISTINCT bc.biomedical_concept_uid, ac.concept_code"
            " FROM biomedical_concept bc"
            " INNER JOIN activity_concept ac"
            " ON bc.biomedical_concept_uid = ac.concept_uid"
            " AND bc.soa_id = ac.soa_id"
            " WHERE bc.soa_id = ?",
            (soa_id,),
        )
        bc_rows = cur.fetchall()
    finally:
        conn.close()

    for bc_uid, concept_code in bc_rows:
        if not concept_code:
            continue
        populate_biomedical_concept_properties_for_bc(soa_id, bc_uid, concept_code)


# ---------------------------------------------------------------------------
# Public: USDM JSON builders
# ---------------------------------------------------------------------------


def _build_response_codes(cur, soa_id: int, bcp_uid: str) -> List[Dict[str, Any]]:
    """Read bcp_response_code rows for a BCP and return USDM dicts."""
    cur.execute(
        "SELECT rc.response_code_uid, rc.name, rc.label,"
        " rc.is_enabled,"
        " c.code_uid, c.code, c.decode, c.code_system_version"
        " FROM bcp_response_code rc"
        " LEFT JOIN alias_code ac"
        " ON rc.code = ac.alias_code_uid AND rc.soa_id = ac.soa_id"
        " LEFT JOIN code c"
        " ON ac.standard_code = c.code_uid AND ac.soa_id = c.soa_id"
        " WHERE rc.soa_id = ?"
        " AND rc.biomedical_concept_property_uid = ?"
        " ORDER BY rc.id",
        (soa_id, bcp_uid),
    )
    out = []
    for (
        rc_uid,
        rc_name,
        rc_label,
        is_enabled,
        code_uid,
        ncit_code,
        decode,
        code_system_version,
    ) in cur.fetchall():
        code_dict = None
        if code_uid:
            code_dict = {
                "id": code_uid,
                "extensionAttributes": [],
                "code": ncit_code or "",
                "codeSystem": BCP_CODE_SYSTEM,
                "codeSystemVersion": code_system_version or "",
                "decode": decode or "",
                "instanceType": "Code",
            }
        out.append(
            {
                "id": rc_uid,
                "instanceType": "ResponseCode",
                "name": rc_name or "",
                "label": rc_label or "",
                "isEnabled": bool(is_enabled) if is_enabled is not None else True,
                "code": code_dict,
            }
        )
    return out


def build_usdm_biomedical_concept_properties(
    soa_id: int, biomedical_concept_uid: str
) -> List[Dict[str, Any]]:
    """Read persisted BCP rows for one BC and return USDM dicts."""
    conn = _connect()
    cur = conn.cursor()
    try:
        cur.execute(
            "SELECT bcp.biomedical_concept_property_uid, bcp.name,"
            " bcp.label, bcp.isRequired, bcp.isEnabled, bcp.datatype,"
            " bcp.code AS alias_code_uid, c.code_uid, c.code,"
            " c.decode, c.code_system, c.code_system_version"
            " FROM biomedical_concept_property bcp"
            " INNER JOIN alias_code ac"
            " ON bcp.code = ac.alias_code_uid AND bcp.soa_id = ac.soa_id"
            " INNER JOIN code c"
            " ON ac.standard_code = c.code_uid AND ac.soa_id = c.soa_id"
            " WHERE bcp.soa_id = ?"
            " AND bcp.biomedical_concept_uid = ?"
            " ORDER BY bcp.id",
            (soa_id, biomedical_concept_uid),
        )
        rows = cur.fetchall()

        out: List[Dict[str, Any]] = []
        for r in rows:
            (
                bcp_uid,
                name,
                label,
                is_required,
                is_enabled,
                datatype,
                alias_uid,
                code_uid,
                ncit_code,
                decode,
                _,
                code_system_version,
            ) = r
            response_codes = _build_response_codes(cur, soa_id, bcp_uid)
            out.append(
                {
                    "id": bcp_uid,
                    "extensionAttributes": [],
                    "name": name,
                    "label": label,
                    "isRequired": (
                        bool(is_required) if is_required is not None else True
                    ),
                    "isEnabled": (bool(is_enabled) if is_enabled is not None else True),
                    "datatype": datatype or "",
                    "responseCodes": response_codes,
                    "code": {
                        "id": alias_uid,
                        "standardCode": {
                            "id": code_uid,
                            "extensionAttributes": [],
                            "code": ncit_code,
                            "codeSystem": BCP_CODE_SYSTEM,
                            "codeSystemVersion": code_system_version or "",
                            "decode": decode,
                            "instanceType": "Code",
                        },
                        "instanceType": "AliasCode",
                    },
                    "notes": [],
                    "instanceType": "BiomedicalConceptProperty",
                }
            )
        return out
    finally:
        conn.close()


def populate_biomedical_concept_properties_for_all_soas() -> None:
    """Backfill BCP + ResponseCode rows for BCs that have no properties yet.

    Called once at startup. Only processes BCs without existing property rows
    so repeated restarts do not re-fetch data that is already up to date.
    """
    conn = _connect()
    cur = conn.cursor()
    try:
        cur.execute(
            "SELECT DISTINCT bc.soa_id, bc.biomedical_concept_uid, ac.concept_code"
            " FROM biomedical_concept bc"
            " INNER JOIN activity_concept ac"
            " ON bc.biomedical_concept_uid = ac.concept_uid"
            " AND bc.soa_id = ac.soa_id"
            " WHERE NOT EXISTS ("
            "   SELECT 1 FROM biomedical_concept_property bcp"
            "   WHERE bcp.soa_id = bc.soa_id"
            "   AND bcp.biomedical_concept_uid = bc.biomedical_concept_uid"
            " )"
            " ORDER BY bc.soa_id, bc.id"
        )
        unpopulated = cur.fetchall()
    finally:
        conn.close()

    if not unpopulated:
        logger.info(
            "populate_biomedical_concept_properties_for_all_soas: "
            "all BCs already populated, nothing to do"
        )
        return

    logger.info(
        "populate_biomedical_concept_properties_for_all_soas: "
        "backfilling %d unpopulated BC(s)",
        len(unpopulated),
    )
    for soa_id, bc_uid, concept_code in unpopulated:
        if not concept_code:
            continue
        try:
            populate_biomedical_concept_properties_for_bc(soa_id, bc_uid, concept_code)
        except Exception:
            logger.exception(
                "populate_biomedical_concept_properties_for_all_soas "
                "failed soa_id=%s bc_uid=%s",
                soa_id,
                bc_uid,
            )


def build_usdm_biomedical_concept_properties_for_soa(
    soa_id: int,
) -> List[Dict[str, Any]]:
    """Return USDM BCP dicts for all BCs in the SOA.

    Default is eager (rows pre-populated by background tasks).
    Set ``SOA_EAGER_BCP_POPULATION=0`` to revert to lazy populate at
    export time.
    """
    lazy = os.environ.get("SOA_EAGER_BCP_POPULATION", "1").strip().lower() in (
        "0",
        "false",
    )
    if lazy:
        populate_biomedical_concept_properties(soa_id)

    conn = _connect()
    cur = conn.cursor()
    try:
        cur.execute(
            "SELECT DISTINCT biomedical_concept_uid"
            " FROM biomedical_concept WHERE soa_id=? ORDER BY id",
            (soa_id,),
        )
        bc_uids = [r[0] for r in cur.fetchall()]
    finally:
        conn.close()

    out: List[Dict[str, Any]] = []
    for bc_uid in bc_uids:
        out.extend(build_usdm_biomedical_concept_properties(soa_id, bc_uid))
    return out
