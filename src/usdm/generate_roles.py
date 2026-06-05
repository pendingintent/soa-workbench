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
        "code": _read_code(soa_id, code_uid) or _empty_code(f"Code_{role_uid}_type"),
        "organizationIds": org_ids,
        "appliesToIds": [],
        "assignedPersons": _read_assigned_persons(
            soa_id, role_uid, role_has_orgs=bool(org_ids)
        ),
        "masking": masking_obj,
        "notes": [],
        "instanceType": "StudyRole",
    }


def _read_assigned_persons(
    soa_id: int, role_uid: str, role_has_orgs: bool = False
) -> List[Dict[str, Any]]:
    conn = _connect()
    cur = conn.cursor()
    cur.execute(
        "SELECT id FROM role WHERE soa_id=? AND role_uid=? LIMIT 1",
        (soa_id, role_uid),
    )
    row = cur.fetchone()
    if not row:
        conn.close()
        return []
    role_id = row[0]
    cur.execute(
        "SELECT p.person_uid, p.person_name_uid, p.name, p.job_title,"
        " p.text, p.family_name, p.given_names,"
        " p.prefixes, p.suffixes, p.organization_uid"
        " FROM role_person rp"
        " JOIN person p ON p.id = rp.person_id AND p.soa_id = rp.soa_id"
        " WHERE rp.soa_id=? AND rp.role_id=?"
        " ORDER BY p.order_index, p.id",
        (soa_id, role_id),
    )
    rows = cur.fetchall()
    conn.close()
    result = []
    for (
        person_uid,
        person_name_uid,
        name,
        job_title,
        text,
        family_name,
        given_names_raw,
        prefixes_raw,
        suffixes_raw,
        org_uid,
    ) in rows:
        given = json.loads(given_names_raw) if given_names_raw else []
        prefixes = json.loads(prefixes_raw) if prefixes_raw else []
        suffixes = json.loads(suffixes_raw) if suffixes_raw else []
        result.append(
            {
                "id": person_uid,
                "extensionAttributes": [],
                "name": name or "",
                "label": None,
                "description": None,
                "personName": {
                    "id": person_name_uid,
                    "extensionAttributes": [],
                    "text": text or None,
                    "familyName": family_name or None,
                    "givenNames": given,
                    "prefixes": prefixes,
                    "suffixes": suffixes,
                    "instanceType": "PersonName",
                },
                "jobTitle": job_title or "",
                **({} if role_has_orgs else {"organizationId": org_uid or None}),
                "instanceType": "AssignedPerson",
            }
        )
    return result


def _empty_code(uid: str) -> Dict[str, Any]:
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
