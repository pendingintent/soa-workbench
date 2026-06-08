#!/usr/bin/env python3
"""Build USDM StudyIdentifier-Output objects for a SOA."""

from typing import Any, Dict, List

from soa_builder.web.db import _connect
from usdm.usdm_utils import _get_soa_metadata

# NCIt code for "Clinical Study Sponsor" (C70793)
_SPONSOR_CODE = "C70793"


def _find_sponsor_org_uid(cur: Any, soa_id: int) -> str:
    """Return organization_uid of the first Clinical Study Sponsor, or ''."""
    cur.execute(
        "SELECT o.organization_uid"
        " FROM organization o"
        " LEFT JOIN code c"
        "   ON c.code_uid = o.type_code_uid AND c.soa_id = o.soa_id"
        " WHERE o.soa_id = ? AND c.code = ?"
        " ORDER BY o.order_index, o.id LIMIT 1",
        (soa_id, _SPONSOR_CODE),
    )
    row = cur.fetchone()
    return row[0] if row else ""


def build_usdm_study_identifiers(soa_id: int) -> List[Dict[str, Any]]:
    """Return list of StudyIdentifier dicts for the USDM output.

    USDM requires exactly one StudyIdentifier whose scopeId references
    a Clinical Study Sponsor organization (C70793).

    - When no study_identifier rows exist: falls back to a single entry
      derived from soa.study_id, with scopeId set to the sponsor org UID.
    - When rows exist: uses them as-is. If none carry the sponsor org as
      scopeId, the first identifier's scopeId is overridden in the output
      (not persisted) so the USDM document is always valid.
    """
    conn = _connect()
    cur = conn.cursor()

    sponsor_uid = _find_sponsor_org_uid(cur, soa_id)

    cur.execute(
        "SELECT study_identifier_uid, text, scope_org_uid"
        " FROM study_identifier"
        " WHERE soa_id=? ORDER BY order_index, id",
        (soa_id,),
    )
    rows = cur.fetchall()
    conn.close()

    if not rows:
        meta = _get_soa_metadata(soa_id)
        return [
            {
                "id": "StudyIdentifier_1",
                "extensionAttributes": [],
                "text": meta["study_id"] or "",
                "scopeId": sponsor_uid,
                "instanceType": "StudyIdentifier",
            }
        ]

    identifiers = [
        {
            "id": row[0],
            "extensionAttributes": [],
            "text": row[1],
            "scopeId": row[2] or "",
            "instanceType": "StudyIdentifier",
        }
        for row in rows
    ]

    # Enforce: at least one identifier must reference the sponsor org.
    # If none do, override the first one's scopeId in the output only.
    if sponsor_uid:
        has_sponsor_link = any(si["scopeId"] == sponsor_uid for si in identifiers)
        if not has_sponsor_link:
            identifiers[0]["scopeId"] = sponsor_uid

    return identifiers
