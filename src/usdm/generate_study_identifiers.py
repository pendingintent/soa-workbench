#!/usr/bin/env python3
"""Build USDM StudyIdentifier-Output objects for a SOA."""

from typing import Any, Dict, List

from soa_builder.web.db import _connect
from soa_builder.web.utils import sponsor_org_uid
from usdm.usdm_utils import _get_soa_metadata


def build_usdm_study_identifiers(soa_id: int) -> List[Dict[str, Any]]:
    """Return list of StudyIdentifier dicts for the USDM output.

    A study version may carry several identifiers (1..*), each scoped
    to its own organization. DDF00172 requires exactly one whose
    scopeId is the organization of the sponsor StudyRole (see
    ``sponsor_org_uid``).

    - No study_identifier rows: a single entry derived from
      soa.study_id, scoped to the sponsor org.
    - Rows exist: emitted as stored, in order_index order. A stored
      scopeId is never rewritten. If none is sponsor-scoped and
      soa.study_id is set, a sponsor identifier derived from it is
      appended (output only, not persisted).
    """
    conn = _connect()
    cur = conn.cursor()

    sponsor_uid = sponsor_org_uid(cur, soa_id)

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
