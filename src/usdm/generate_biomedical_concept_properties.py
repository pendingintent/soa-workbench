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
    _get_biomedical_concept_data,
    _get_latest_bc_package_version,
    _get_sdtm_specialization_data,
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


# ---------------------------------------------------------------------------
# Internal helpers
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
    """Insert bcp_response_code + alias_code + code rows for a BCP."""
    for val in values:
        ncit_code, decode = _resolve_rc_code(val, codelist_code)
        if not ncit_code:
            continue

        # Check if a response code with this ncit_code already exists
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
            " (alias_code_uid, soa_id, standard_code)"
            " VALUES (?, ?, ?)",
            (alias_uid, soa_id, code_uid),
        )
        rc_uid = get_next_response_code_uid(cur, soa_id)
        name = ncit_code
        cur.execute(
            "INSERT INTO bcp_response_code"
            " (soa_id, biomedical_concept_property_uid,"
            " response_code_uid, name, label, is_enabled, code)"
            " VALUES (?, ?, ?, ?, ?, ?, ?)",
            (soa_id, bcp_uid, rc_uid, name, "", 1, alias_uid),
        )


def _upsert_bcp(
    cur,
    soa_id: int,
    bc_uid: str,
    ncit_code: str,
    name: str,
    datatype: str,
    rc_values: List[str],
    codelist_code: Optional[str],
) -> None:
    """Insert a BCP row + ResponseCode rows if not already present."""
    if not ncit_code:
        return

    cur.execute(
        "SELECT bcp.biomedical_concept_property_uid"
        " FROM biomedical_concept_property bcp"
        " LEFT JOIN alias_code ac"
        " ON bcp.code = ac.alias_code_uid AND bcp.soa_id = ac.soa_id"
        " LEFT JOIN code c"
        " ON ac.standard_code = c.code_uid AND ac.soa_id = c.soa_id"
        " WHERE bcp.soa_id = ?"
        " AND bcp.biomedical_concept_uid = ?"
        " AND c.code = ?"
        " LIMIT 1",
        (soa_id, bc_uid, ncit_code),
    )
    row = cur.fetchone()
    if row:
        bcp_uid = row[0]
    else:
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
                name,
            ),
        )
        alias_uid = get_next_alias_code_uid(cur, soa_id)
        cur.execute(
            "INSERT INTO alias_code"
            " (alias_code_uid, soa_id, standard_code)"
            " VALUES (?, ?, ?)",
            (alias_uid, soa_id, code_uid),
        )
        bcp_uid = get_next_biomedical_concept_property_uid(cur, soa_id)
        cur.execute(
            "INSERT INTO biomedical_concept_property"
            " (soa_id, biomedical_concept_uid,"
            " biomedical_concept_property_uid, name, label,"
            " isRequired, isEnabled, datatype, code)"
            " VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
            (
                soa_id,
                bc_uid,
                bcp_uid,
                name,
                name,
                1,
                1,
                datatype,
                alias_uid,
            ),
        )

    if rc_values:
        _upsert_response_codes(cur, soa_id, bcp_uid, rc_values, codelist_code)


# ---------------------------------------------------------------------------
# Public: scoped population function (eager path)
# ---------------------------------------------------------------------------


def populate_biomedical_concept_properties_for_bc(
    soa_id: int, bc_uid: str, concept_code: str
) -> None:
    """Populate BCP + ResponseCode rows for one BC. Idempotent.

    Prefers the SDTM specialization ``variables[]`` when available;
    falls back to the generic BC ``dataElementConcepts[]``.
    """
    data = _get_biomedical_concept_data(concept_code) or {}
    sdtm = _get_sdtm_specialization_data(concept_code) or {}

    # Build lookup: dataElementConceptId → DEC dict for code resolution
    decs = data.get("dataElementConcepts") or []
    dec_by_id: Dict[str, Dict] = {
        dec["conceptId"]: dec for dec in decs if dec.get("conceptId")
    }

    with _soa_lock(soa_id):
        _populate_bcp_locked(soa_id, bc_uid, decs, dec_by_id, sdtm)


def _populate_bcp_locked(
    soa_id: int,
    bc_uid: str,
    decs: List[Dict],
    dec_by_id: Dict[str, Dict],
    sdtm: Dict,
) -> None:
    """Execute the DB writes for one BC under the SOA lock."""
    conn = _connect()
    cur = conn.cursor()
    try:
        if sdtm and sdtm.get("variables"):
            for var in sdtm["variables"]:
                var_name = var.get("name") or var.get("shortName") or ""
                if not _include_property(var_name):
                    continue
                datatype = var.get("dataType") or var.get("datatype") or ""
                dec_id = var.get("dataElementConceptId")
                dec = dec_by_id.get(dec_id) if dec_id else None

                if dec:
                    ncit_code = dec["conceptId"]
                elif var.get("assignedTerm"):
                    at = var["assignedTerm"]
                    ncit_code = at.get("conceptId") or at.get("value") or ""
                else:
                    ncit_code = dec_id or ""

                if not ncit_code:
                    continue

                codelist_code = None
                cl = var.get("codelist")
                if isinstance(cl, dict):
                    codelist_code = cl.get("conceptId")

                rc_values = var.get("valueList") or []
                _upsert_bcp(
                    cur,
                    soa_id,
                    bc_uid,
                    ncit_code,
                    var_name,
                    datatype,
                    rc_values,
                    codelist_code,
                )
        else:
            for dec in decs:
                ncit_code = dec.get("conceptId") or dec.get("ncitCode") or ""
                short_name = dec.get("shortName") or ""
                datatype = dec.get("dataType") or dec.get("datatype") or ""
                if not ncit_code:
                    continue
                rc_values = dec.get("exampleSet") or []
                _upsert_bcp(
                    cur,
                    soa_id,
                    bc_uid,
                    ncit_code,
                    short_name,
                    datatype,
                    rc_values,
                    None,
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
    Idempotent.
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
        " rc.is_enabled, rc.code AS alias_uid,"
        " c.code_uid, c.code, c.decode,"
        " c.code_system, c.code_system_version"
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
        alias_uid,
        code_uid,
        ncit_code,
        decode,
        _,
        code_system_version,
    ) in cur.fetchall():
        code_dict = None
        if code_uid:
            code_dict = {
                "id": code_uid,
                "extensionAttributes": [],
                "code": ncit_code or "",
                "codeSystem": BCP_CODE_SYSTEM,
                "codeSystemVersion": (
                    code_system_version or _get_latest_bc_package_version()
                ),
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
            " ON bcp.code = ac.alias_code_uid"
            " AND bcp.soa_id = ac.soa_id"
            " INNER JOIN code c"
            " ON ac.standard_code = c.code_uid"
            " AND ac.soa_id = c.soa_id"
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
                            "codeSystemVersion": (
                                code_system_version or _get_latest_bc_package_version()
                            ),
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
    """Backfill BCP + ResponseCode rows for every BC in every SOA.

    Called once at startup so existing SOAs gain property rows without
    waiting for a USDM export. Idempotent.
    """
    conn = _connect()
    cur = conn.cursor()
    try:
        cur.execute("SELECT id FROM soa ORDER BY id")
        soa_ids = [r[0] for r in cur.fetchall()]
    finally:
        conn.close()

    for soa_id in soa_ids:
        try:
            populate_biomedical_concept_properties(soa_id)
        except Exception:
            logger.exception(
                "populate_biomedical_concept_properties_for_all_soas failed soa_id=%s",
                soa_id,
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
            " FROM biomedical_concept"
            " WHERE soa_id=? ORDER BY id",
            (soa_id,),
        )
        bc_uids = [r[0] for r in cur.fetchall()]
    finally:
        conn.close()

    out: List[Dict[str, Any]] = []
    for bc_uid in bc_uids:
        out.extend(build_usdm_biomedical_concept_properties(soa_id, bc_uid))
    return out
