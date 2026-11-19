#!/usr/bin/env python3
"""Build USDM v4.0 eligibility criteria (CORE DDF00126).

- InterventionalStudyDesign.eligibilityCriteria: EligibilityCriterion
  objects in the stored order, linked by nextId/previousId, each with a
  category Code (C66797) and a criterionItemId.
- StudyVersion.eligibilityCriterionItems: one EligibilityCriterionItem
  per criterion, holding the text.

Criterion text is stored as plain text and escaped on export, because
EligibilityCriterionItem.text is XHTML (DDF00247): a bare "<" or "&"
would make it invalid.
"""

import html
from typing import Any, Dict, List

from soa_builder.web.db import _connect
from soa_builder.web.utils import table_has_columns

CDISC_CODE_SYSTEM = "http://www.cdisc.org"


def _rows(soa_id: int) -> List[tuple]:
    conn = _connect()
    try:
        cur = conn.cursor()
        if not table_has_columns(cur, "eligibility_criterion", ("criterion_uid",)):
            return []
        cur.execute(
            "SELECT criterion_uid, item_uid, name, label, description,"
            " identifier, category_code, category_decode, category_version,"
            " text FROM eligibility_criterion WHERE soa_id=?"
            " ORDER BY order_index, id",
            (soa_id,),
        )
        return cur.fetchall()
    finally:
        conn.close()


def build_usdm_eligibility_criteria(soa_id: int) -> List[Dict[str, Any]]:
    rows = _rows(soa_id)
    uids = [r[0] for r in rows]
    out = []
    for n, row in enumerate(rows):
        (
            uid,
            item_uid,
            name,
            label,
            description,
            identifier,
            code,
            decode,
            version,
            _text,
        ) = row
        out.append(
            {
                "id": uid,
                "extensionAttributes": [],
                "name": name,
                "label": label or None,
                "description": description or None,
                "category": {
                    "id": f"{uid}_Category",
                    "extensionAttributes": [],
                    "code": code,
                    "codeSystem": CDISC_CODE_SYSTEM,
                    "codeSystemVersion": version or "",
                    "decode": decode or "",
                    "instanceType": "Code",
                },
                "identifier": identifier,
                "criterionItemId": item_uid,
                "nextId": uids[n + 1] if n + 1 < len(uids) else None,
                "previousId": uids[n - 1] if n > 0 else None,
                "notes": [],
                "instanceType": "EligibilityCriterion",
            }
        )
    return out


def build_usdm_eligibility_criterion_items(soa_id: int) -> List[Dict[str, Any]]:
    return [
        {
            "id": item_uid,
            "extensionAttributes": [],
            "name": name,
            "label": label or None,
            "description": None,
            "text": html.escape(text or "", quote=False),
            "dictionaryId": None,
            "notes": [],
            "instanceType": "EligibilityCriterionItem",
        }
        for (
            _uid,
            item_uid,
            name,
            label,
            _desc,
            _ident,
            _code,
            _decode,
            _version,
            text,
        ) in _rows(soa_id)
    ]
