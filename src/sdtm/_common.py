"""Shared lookups for the SDTM Trial Design domain generators.

The USDM-to-SDTM source for each variable follows the CDISC mapping
file ``sdtm_mapping.xlsx`` (USDM v4.0.0 to SDTMIG v3.4) and the USDM
Implementation Handbook 1, section 4.3.
"""

import sqlite3

from usdm.generate_study_identifiers import _find_sponsor_org_uid


def first_text(*values) -> str:
    """Return the first value that is a non-blank string, else ''."""
    for value in values:
        if value is not None and str(value).strip():
            return str(value).strip()
    return ""


def sponsor_study_id(cur: sqlite3.Cursor, soa_id: int) -> str:
    """Return STUDYID: the sponsor StudyIdentifier text.

    The mapping takes StudyIdentifier.text where the identifier's scope
    is the Clinical Study Sponsor organization (C70793). The fallbacks
    mirror ``build_usdm_study_identifiers``: the first identifier, then
    ``soa.study_id``, then ``soa.name``.
    """
    sponsor_uid = _find_sponsor_org_uid(cur, soa_id)
    cur.execute(
        "SELECT text, scope_org_uid FROM study_identifier"
        " WHERE soa_id=? ORDER BY order_index, id",
        (soa_id,),
    )
    identifiers = cur.fetchall()
    for text, scope_org_uid in identifiers:
        if sponsor_uid and scope_org_uid == sponsor_uid and first_text(text):
            return first_text(text)
    if identifiers and first_text(identifiers[0][0]):
        return first_text(identifiers[0][0])
    cur.execute("SELECT study_id, name FROM soa WHERE id=?", (soa_id,))
    row = cur.fetchone()
    return first_text(row[0], row[1]) if row else ""


def main_timeline_uid(cur: sqlite3.Cursor, soa_id: int) -> str:
    """Return the schedule_timeline_uid flagged as main, or ''."""
    cur.execute(
        "SELECT schedule_timeline_uid FROM schedule_timelines"
        " WHERE soa_id=? AND main_timeline=1"
        " ORDER BY order_index, id LIMIT 1",
        (soa_id,),
    )
    row = cur.fetchone()
    return row[0] if row else ""
