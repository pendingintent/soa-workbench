#!/usr/bin/env python3
"""USDM generator for BiomedicalConceptProperty.

Sources property data from each Biomedical Concept's
``dataElementConcepts[]`` array, returned by the CDISC Library
endpoint
``GET https://api.library.cdisc.org/api/cosmos/v2/mdr/bc/biomedicalconcepts/{concept_code}``.

Persists rows in ``biomedical_concept_property`` so UIDs are stable
across re-exports, matching the project's monotonic, unique-within-SOA
UID convention.
"""

import logging
from typing import List, Dict, Any

from soa_builder.web.db import _connect
from soa_builder.web.utils import (
    get_next_alias_code_uid,
    get_next_code_uid,
    get_next_biomedical_concept_property_uid,
)
from .usdm_utils import _get_biomedical_concept_data

logger = logging.getLogger("usdm.generate_biomedical_concept_properties")

NCIT_CODE_SYSTEM = "https://evsexplore.semantics.cancer.gov"
NCIT_CODE_SYSTEM_VERSION = "1"


def populate_biomedical_concept_properties(soa_id: int) -> None:
    """Upsert BiomedicalConceptProperty rows for every BC in this SOA.

    For each BC in the ``biomedical_concept`` table for ``soa_id``,
    fetch the BC API response and create one
    ``biomedical_concept_property`` row per ``dataElementConcepts[]``
    entry, allocating ``BiomedicalConceptProperty_N``, ``AliasCode_N``,
    and ``Code_N`` UIDs from the existing project sequences.

    Idempotent: existing rows (matched by ``biomedical_concept_uid``
    and ``ncitCode``) are not duplicated; only new dataElementConcepts
    get freshly-allocated UIDs.
    """
    conn = _connect()
    cur = conn.cursor()
    try:
        cur.execute(
            "SELECT bc.biomedical_concept_uid, ac.concept_code"
            " FROM biomedical_concept bc"
            " INNER JOIN activity_concept ac"
            " ON bc.biomedical_concept_uid = ac.concept_uid"
            " AND bc.soa_id = ac.soa_id"
            " WHERE bc.soa_id = ?",
            (soa_id,),
        )
        bc_rows = cur.fetchall()

        for bc_uid, concept_code in bc_rows:
            if not concept_code:
                continue
            data = _get_biomedical_concept_data(concept_code) or {}
            decs = data.get("dataElementConcepts") or []
            for dec in decs:
                ncit_code = dec.get("conceptId") or dec.get("ncitCode")
                short_name = dec.get("shortName") or ""
                datatype = dec.get("dataType") or dec.get("datatype") or ""
                if not ncit_code:
                    continue

                cur.execute(
                    "SELECT bcp.biomedical_concept_property_uid, bcp.code,"
                    " ac.standard_code"
                    " FROM biomedical_concept_property bcp"
                    " LEFT JOIN alias_code ac"
                    " ON bcp.code = ac.alias_code_uid"
                    " AND bcp.soa_id = ac.soa_id"
                    " LEFT JOIN code c"
                    " ON ac.standard_code = c.code_uid"
                    " AND ac.soa_id = c.soa_id"
                    " WHERE bcp.soa_id = ?"
                    " AND bcp.biomedical_concept_uid = ?"
                    " AND c.code = ?"
                    " LIMIT 1",
                    (soa_id, bc_uid, ncit_code),
                )
                if cur.fetchone():
                    continue

                code_uid = get_next_code_uid(cur, soa_id)
                cur.execute(
                    "INSERT INTO code (code_uid, soa_id, code,"
                    " code_system, code_system_version, decode)"
                    " VALUES (?, ?, ?, ?, ?, ?)",
                    (
                        code_uid,
                        soa_id,
                        ncit_code,
                        NCIT_CODE_SYSTEM,
                        NCIT_CODE_SYSTEM_VERSION,
                        short_name,
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
                        short_name,
                        short_name,
                        1,
                        1,
                        datatype,
                        alias_uid,
                    ),
                )
        conn.commit()
    except Exception:
        conn.rollback()
        logger.exception(
            "populate_biomedical_concept_properties failed soa_id=%s",
            soa_id,
        )
        raise
    finally:
        conn.close()


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
    finally:
        conn.close()

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
            code_system,
            code_system_version,
        ) = r
        out.append(
            {
                "id": bcp_uid,
                "extensionAttributes": [],
                "name": name,
                "label": label,
                "isRequired": bool(is_required) if is_required is not None else True,
                "isEnabled": bool(is_enabled) if is_enabled is not None else True,
                "datatype": datatype or "",
                "responseCodes": [],
                "code": {
                    "id": alias_uid,
                    "standardCode": {
                        "id": code_uid,
                        "extensionAttributes": [],
                        "code": ncit_code,
                        "codeSystem": code_system or NCIT_CODE_SYSTEM,
                        "codeSystemVersion": (
                            code_system_version or NCIT_CODE_SYSTEM_VERSION
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
