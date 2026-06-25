#!/usr/bin/env python3
"""Generate USDM BiomedicalConceptCategory objects from activity_concept rows."""

from typing import Any, Dict, List

from soa_builder.web.db import _connect


def build_usdm_bc_categories(soa_id: int) -> List[Dict[str, Any]]:
    """Build BiomedicalConceptCategory-Output objects for the given SOA.

    Each distinct bc_category_name found in activity_concept becomes one
    category entry. memberIds reflects only the concept_uid values that
    remain assigned — individual removals by the user are naturally absent.
    """
    conn = _connect()
    cur = conn.cursor()

    cur.execute("PRAGMA table_info(activity_concept)")
    col_names = {r[1] for r in cur.fetchall()}
    if "bc_category_name" not in col_names:
        conn.close()
        return []

    cur.execute(
        "SELECT DISTINCT bc_category_name FROM activity_concept "
        "WHERE soa_id=? AND bc_category_name IS NOT NULL",
        (soa_id,),
    )
    category_names = sorted(r[0] for r in cur.fetchall())

    out: List[Dict[str, Any]] = []
    for idx, cat_name in enumerate(category_names, start=1):
        cur.execute(
            "SELECT DISTINCT concept_uid FROM activity_concept "
            "WHERE soa_id=? AND bc_category_name=? AND concept_uid IS NOT NULL",
            (soa_id, cat_name),
        )
        member_ids = [r[0] for r in cur.fetchall()]
        out.append(
            {
                "id": f"BiomedicalConceptCategory_{idx}",
                "extensionAttributes": [],
                "name": cat_name,
                "label": cat_name,
                "description": None,
                "childIds": [],
                "memberIds": member_ids,
                "code": None,
                "notes": [],
                "instanceType": "BiomedicalConceptCategory",
            }
        )

    conn.close()
    return out
