#!/usr/bin/env python3
"""Build StudyDesignPopulation.plannedSex and plannedAge (USDM v4.0).

plannedSex: up to two Code-Output objects from the Sex of Participants
codelist (C66732, CORE DDF00098).

plannedAge: a Range-Output (DDF00097) whose minValue and maxValue are
Quantity-Output objects with an AliasCode unit. A bound recorded as
n/a is exported as null. USDM v4.0 requires both bounds, so a v4.0
schema check flags a one-sided range; v4.1 is expected to allow it.

Ids are derived from the population id so they are unique and stable.
"""

import json
from typing import Any, Dict, List, Optional

from soa_builder.web.db import _connect

POPULATION_ID = "StudyDesignPopulation_1"
CDISC_CODE_SYSTEM = "http://www.cdisc.org"


def _code(code_id: str, code: str, decode: str, version: str) -> Dict[str, Any]:
    return {
        "id": code_id,
        "extensionAttributes": [],
        "code": code,
        "codeSystem": CDISC_CODE_SYSTEM,
        "codeSystemVersion": version or "",
        "decode": decode or "",
        "instanceType": "Code",
    }


def _read(soa_id: int):
    conn = _connect()
    try:
        return conn.execute(
            "SELECT planned_sex, planned_age_min, planned_age_max,"
            " planned_age_unit_code, planned_age_unit_decode,"
            " planned_age_unit_version, planned_age_is_approximate"
            " FROM soa WHERE id=?",
            (soa_id,),
        ).fetchone()
    finally:
        conn.close()


def build_usdm_planned_sex(soa_id: int) -> List[Dict[str, Any]]:
    row = _read(soa_id)
    terms = json.loads(row[0]) if row and row[0] else []
    return [
        _code(
            f"{POPULATION_ID}_PlannedSex_{n}",
            t["code"],
            t.get("decode", ""),
            t.get("code_system_version", ""),
        )
        for n, t in enumerate(terms, start=1)
    ]


def _quantity(prefix: str, value: Optional[float], unit: tuple):
    if value is None:
        return None
    code, decode, version = unit
    return {
        "id": prefix,
        "extensionAttributes": [],
        "value": float(value),
        "unit": {
            "id": f"{prefix}Unit",
            "extensionAttributes": [],
            "standardCode": _code(f"{prefix}UnitCode", code, decode, version),
            "standardCodeAliases": [],
            "instanceType": "AliasCode",
        },
        "instanceType": "Quantity",
    }


def build_usdm_planned_age(soa_id: int) -> Optional[Dict[str, Any]]:
    row = _read(soa_id)
    if not row:
        return None
    _, low, high, code, decode, version, approximate = row
    if low is None and high is None:
        return None
    unit = (code, decode, version)
    return {
        "id": f"{POPULATION_ID}_PlannedAge",
        "extensionAttributes": [],
        "minValue": _quantity(f"{POPULATION_ID}_PlannedAgeMin", low, unit),
        "maxValue": _quantity(f"{POPULATION_ID}_PlannedAgeMax", high, unit),
        "isApproximate": bool(approximate),
        "instanceType": "Range",
    }
