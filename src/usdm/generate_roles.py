#!/usr/bin/env python3
"""Build USDM StudyRole-Output objects for a SOA."""

import json
from typing import Any, Dict, List, Optional

from soa_builder.web.db import _connect


def build_usdm_roles(soa_id: int) -> List[Dict[str, Any]]:
    conn = _connect()
    cur = conn.cursor()
    cur.execute(
        "SELECT role_uid, name, label, description, code_uid, "
        "organization_ids, masking "
        "FROM role WHERE soa_id=? ORDER BY order_index, id",
        (soa_id,),
    )
    rows = cur.fetchall()
    conn.close()
    return [_build_role(soa_id, r) for r in rows]


def _build_role(soa_id: int, row) -> Dict[str, Any]:
    (
        role_uid,
        name,
        label,
        description,
        code_uid,
        organization_ids_json,
        masking,
    ) = row
    org_ids = json.loads(organization_ids_json) if organization_ids_json else []
    suffix = role_uid.split("_")[-1]
    masking_obj: Optional[Dict[str, Any]] = (
        {
            "id": f"Masking_{suffix}",
            "extensionAttributes": [],
            "text": "Masked",
            "isMasked": True,
            "instanceType": "Masking",
        }
        if masking
        else None
    )
    return {
        "id": role_uid,
        "extensionAttributes": [],
        "name": name or "",
        "label": label or None,
        "description": description or None,
        "code": _read_code(soa_id, code_uid),
        "organizationIds": org_ids,
        "appliesToIds": [],
        "assignedPersons": [],
        "masking": masking_obj,
        "notes": [],
        "instanceType": "StudyRole",
    }


def _read_code(soa_id: int, code_uid: Optional[str]) -> Optional[Dict[str, Any]]:
    if not code_uid:
        return None
    conn = _connect()
    cur = conn.cursor()
    cur.execute(
        "SELECT code, code_system, code_system_version, decode "
        "FROM code WHERE soa_id=? AND code_uid=? LIMIT 1",
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
