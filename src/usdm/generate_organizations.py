#!/usr/bin/env python3
"""Build USDM Organization-Output objects for a SOA."""

import json
from typing import Any, Dict, List, Optional

from soa_builder.web.db import _connect


def build_usdm_organizations(soa_id: int) -> List[Dict[str, Any]]:
    conn = _connect()
    cur = conn.cursor()
    cur.execute(
        "SELECT organization_uid, name, label, identifier, "
        "identifier_scheme, type_code_uid, addr_text, addr_lines, "
        "addr_city, addr_district, addr_state, addr_postal_code, "
        "addr_country_code_uid "
        "FROM organization WHERE soa_id=? ORDER BY order_index, id",
        (soa_id,),
    )
    rows = cur.fetchall()
    conn.close()
    return [_build_org(soa_id, r) for r in rows]


def _build_org(soa_id: int, row) -> Dict[str, Any]:
    (
        org_uid,
        name,
        label,
        identifier,
        id_scheme,
        type_code_uid,
        addr_text,
        addr_lines_json,
        addr_city,
        addr_district,
        addr_state,
        addr_postal_code,
        addr_country_code_uid,
    ) = row
    return {
        "id": org_uid,
        "extensionAttributes": [],
        "name": name or "",
        "label": label or None,
        "identifier": identifier or "",
        "identifierScheme": id_scheme or "",
        "type": _read_code(soa_id, type_code_uid),
        "legalAddress": _build_address(
            soa_id,
            org_uid,
            addr_text,
            addr_lines_json,
            addr_city,
            addr_district,
            addr_state,
            addr_postal_code,
            addr_country_code_uid,
        ),
        "managedSites": [],
        "instanceType": "Organization",
    }


def _build_address(
    soa_id: int,
    org_uid: str,
    text: Optional[str],
    lines_json: Optional[str],
    city: Optional[str],
    district: Optional[str],
    state: Optional[str],
    postal_code: Optional[str],
    country_code_uid: Optional[str],
) -> Optional[Dict[str, Any]]:
    lines = json.loads(lines_json) if lines_json else []
    has_data = any([text, lines, city, district, state, postal_code, country_code_uid])
    if not has_data:
        return None
    suffix = org_uid.split("_")[-1]
    return {
        "id": f"Address_{suffix}",
        "extensionAttributes": [],
        "text": text or None,
        "lines": lines,
        "city": city or None,
        "district": district or None,
        "state": state or None,
        "postalCode": postal_code or None,
        "country": _read_code(soa_id, country_code_uid),
        "instanceType": "Address",
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
