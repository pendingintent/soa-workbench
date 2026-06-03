#!/usr/bin/env python3
"""Build USDM Indication-Output objects for a SOA.

Reference: USDM_API_v4.0.0.json
  Indication.name: str  (required)
  Indication.isRareDisease: bool  (required)
  Indication.codes: list[Code-Output]
  Indication.instanceType: "Indication"
"""

from typing import Any, Dict, List, Optional

from soa_builder.web.db import _connect


def build_usdm_indications(soa_id: int) -> List[Dict[str, Any]]:
    conn = _connect()
    cur = conn.cursor()
    cur.execute(
        "SELECT id, indication_uid, name, label, description, is_rare_disease"
        " FROM indication WHERE soa_id=? ORDER BY order_index, id",
        (soa_id,),
    )
    rows = cur.fetchall()

    result = []
    for row in rows:
        iid, indication_uid, name, label, description, is_rare_disease = row

        cur.execute(
            "SELECT c.code_uid, c.code, c.code_system,"
            " c.code_system_version, c.decode"
            " FROM indication_code ic"
            " JOIN code c ON c.code_uid = ic.code_uid AND c.soa_id = ic.soa_id"
            " WHERE ic.soa_id=? AND ic.indication_id=?"
            " ORDER BY ic.order_index, ic.id",
            (soa_id, iid),
        )
        code_rows = cur.fetchall()

        result.append(
            _build_indication(
                indication_uid,
                name,
                label,
                description,
                bool(is_rare_disease),
                code_rows,
            )
        )
    conn.close()
    return result


def _build_indication(
    indication_uid: str,
    name: str,
    label: Optional[str],
    description: Optional[str],
    is_rare_disease: bool,
    code_rows: list,
) -> Dict[str, Any]:
    return {
        "id": indication_uid,
        "extensionAttributes": [],
        "name": name or "",
        "label": label or None,
        "description": description or None,
        "isRareDisease": is_rare_disease,
        "codes": [_build_code(r) for r in code_rows],
        "notes": [],
        "instanceType": "Indication",
    }


def _build_code(row: tuple) -> Dict[str, Any]:
    code_uid, code, code_system, code_system_version, decode = row
    return {
        "id": code_uid,
        "extensionAttributes": [],
        "code": code or "",
        "codeSystem": code_system or "",
        "codeSystemVersion": code_system_version or "",
        "decode": decode or "",
        "instanceType": "Code",
    }
