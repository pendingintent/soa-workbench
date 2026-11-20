#!/usr/bin/env python3
"""Build USDM StudyIdentifier-Output objects for a SOA."""

from typing import Any, Dict, List

from soa_builder.web.db import _connect
from usdm.usdm_utils import _get_soa_metadata

# NCIt code for "Clinical Study Sponsor" (C70793)
_SPONSOR_CODE = "C70793"


_SPONSOR_CODE_SYSTEM = "http://www.cdisc.org"


def _find_sponsor_org_uid(cur: Any, soa_id: int) -> str:
    """Return organization_uid of the first Clinical Study Sponsor, or ''.

    Matches DDF00005: type.code = C70793 AND
    type.codeSystem = http://www.cdisc.org
    """
    cur.execute(
        "SELECT o.organization_uid"
        " FROM organization o"
        " LEFT JOIN code c"
        "   ON c.code_uid = o.type_code_uid AND c.soa_id = o.soa_id"
        " WHERE o.soa_id = ? AND c.code = ? AND c.code_system = ?"
        " ORDER BY o.order_index, o.id LIMIT 1",
        (soa_id, _SPONSOR_CODE, _SPONSOR_CODE_SYSTEM),
    )
    row = cur.fetchone()
    return row[0] if row else ""


def build_usdm_study_identifiers(soa_id: int) -> List[Dict[str, Any]]:
    """Return list of StudyIdentifier dicts for the USDM output.

    A study version may carry several identifiers (1..*), each scoped
    to its own organization. DDF00172 requires exactly one whose
    scopeId references the Clinical Study Sponsor organization
    (C70793).

    - No study_identifier rows: a single entry derived from
      soa.study_id, scoped to the sponsor org.
    - Rows exist: emitted as stored, in order_index order. A stored
      scopeId is never rewritten. If none is sponsor-scoped and
      soa.study_id is set, a sponsor identifier derived from it is
      appended (output only, not persisted).
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

    if sponsor_uid and not any(si["scopeId"] == sponsor_uid for si in identifiers):
        study_id = _get_soa_metadata(soa_id)["study_id"]
        if study_id:
            identifiers.append(
                {
                    "id": f"StudyIdentifier_{_max_uid_number(rows) + 1}",
                    "extensionAttributes": [],
                    "text": study_id,
                    "scopeId": sponsor_uid,
                    "instanceType": "StudyIdentifier",
                }
            )

    return identifiers


def _max_uid_number(rows: List[Any]) -> int:
    """Highest N among StudyIdentifier_N uids in ``rows`` (0 if none)."""
    numbers = [
        int(uid.rsplit("_", 1)[-1])
        for uid, *_ in rows
        if uid.rsplit("_", 1)[-1].isdigit()
    ]
    return max(numbers, default=0)
