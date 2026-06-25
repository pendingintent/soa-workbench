#!/usr/bin/env python3
"""Build USDM StudyIntervention-Output objects for a SOA.

Reference: https://github.com/cdisc-org/usdm_api/blob/main/model/study_intervention.py
  StudyIntervention.role: Code  (required)
  StudyIntervention.type: Code  (required)
  StudyIntervention.minimumResponseDuration: Union[Quantity, None]
    Quantity.value: float
    Quantity.unit: Union[AliasCode, None]
      AliasCode.standardCode: Code
"""

from typing import Any, Dict, List, Optional

from soa_builder.web.db import _connect


def _missing_code(uid: str) -> Dict[str, Any]:
    """Minimal valid Code-Output when no code has been assigned."""
    return {
        "id": uid,
        "extensionAttributes": [],
        "code": "",
        "codeSystem": "",
        "codeSystemVersion": "",
        "decode": "",
        "instanceType": "Code",
    }


def build_usdm_study_interventions(soa_id: int) -> List[Dict[str, Any]]:
    conn = _connect()
    cur = conn.cursor()
    cur.execute(
        "SELECT id, intervention_uid, name, label, description,"
        " role_code_uid, type_code_uid,"
        " mrd_quantity_uid, mrd_value, mrd_unit_alias_uid"
        " FROM study_intervention"
        " WHERE soa_id=? ORDER BY order_index, id",
        (soa_id,),
    )
    rows = cur.fetchall()

    result = []
    for row in rows:
        (
            iid,
            intervention_uid,
            name,
            label,
            description,
            role_code_uid,
            type_code_uid,
            mrd_quantity_uid,
            mrd_value,
            mrd_unit_alias_uid,
        ) = row
        cur.execute(
            "SELECT sic.code_uid"
            " FROM study_intervention_code sic"
            " WHERE sic.soa_id=? AND sic.intervention_id=?"
            " ORDER BY sic.order_index, sic.id",
            (soa_id, iid),
        )
        code_uids = [r[0] for r in cur.fetchall()]
        result.append(
            _build_intervention(
                soa_id,
                intervention_uid,
                name,
                label,
                description,
                role_code_uid,
                type_code_uid,
                mrd_quantity_uid,
                mrd_value,
                mrd_unit_alias_uid,
                code_uids,
            )
        )
    conn.close()
    return result


def _build_intervention(
    soa_id: int,
    intervention_uid: str,
    name: str,
    label: Optional[str],
    description: Optional[str],
    role_code_uid: Optional[str],
    type_code_uid: Optional[str],
    mrd_quantity_uid: Optional[str],
    mrd_value: Optional[float],
    mrd_unit_alias_uid: Optional[str],
    code_uids: List[str],
) -> Dict[str, Any]:
    role = _read_code(soa_id, role_code_uid) or _missing_code(
        f"Code_{intervention_uid}_role"
    )
    type_ = _read_code(soa_id, type_code_uid) or _missing_code(
        f"Code_{intervention_uid}_type"
    )

    mrd = None
    if mrd_quantity_uid and mrd_value is not None:
        mrd = {
            "id": mrd_quantity_uid,
            "extensionAttributes": [],
            "value": float(mrd_value),
            "unit": _read_alias_code(soa_id, mrd_unit_alias_uid),
            "instanceType": "Quantity",
        }

    codes = [c for c in (_read_code(soa_id, uid) for uid in code_uids) if c]

    return {
        "id": intervention_uid,
        "extensionAttributes": [],
        "name": name or "",
        "label": label or None,
        "description": description or None,
        "role": role,
        "type": type_,
        "minimumResponseDuration": mrd,
        "codes": codes,
        "administrations": [],
        "notes": [],
        "instanceType": "StudyIntervention",
    }


def _read_code(soa_id: int, code_uid: Optional[str]) -> Optional[Dict[str, Any]]:
    if not code_uid:
        return None
    conn = _connect()
    cur = conn.cursor()
    cur.execute(
        "SELECT code, code_system, code_system_version, decode"
        " FROM code WHERE soa_id=? AND code_uid=? LIMIT 1",
        (soa_id, code_uid),
    )
    row = cur.fetchone()
    conn.close()
    if not row:
        return None
    code, cs, csv, decode = row
    return {
        "id": code_uid,
        "extensionAttributes": [],
        "code": code or "",
        "codeSystem": cs or "",
        "codeSystemVersion": csv or "",
        "decode": decode or "",
        "instanceType": "Code",
    }


def _read_alias_code(soa_id: int, alias_uid: Optional[str]) -> Optional[Dict[str, Any]]:
    """Return AliasCode-Output wrapping its standard Code-Output."""
    if not alias_uid:
        return None
    conn = _connect()
    cur = conn.cursor()
    cur.execute(
        "SELECT standard_code FROM alias_code"
        " WHERE soa_id=? AND alias_code_uid=? LIMIT 1",
        (soa_id, alias_uid),
    )
    row = cur.fetchone()
    conn.close()
    if not row:
        return None
    standard_code = _read_code(soa_id, row[0])
    return {
        "id": alias_uid,
        "extensionAttributes": [],
        "standardCode": standard_code,
        "standardCodeAliases": [],
        "instanceType": "AliasCode",
    }
