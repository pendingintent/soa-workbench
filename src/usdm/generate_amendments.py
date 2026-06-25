#!/usr/bin/env python3
"""Build USDM StudyAmendment-Output objects for a SOA."""

from typing import Any, Dict, List

from soa_builder.web.db import _connect
from soa_builder.web.utils import _nz


def _code_obj(code_uid: str, row: tuple) -> Dict[str, Any]:
    """Build a Code-Output dict from a code_association row tuple.

    Row tuple: (code_uid, codelist_table, codelist_code, code, decode).
    decode is stored in the DB; falls back to code if NULL.
    """
    if row is None:
        return {
            "id": code_uid or "Code_unknown",
            "extensionAttributes": [],
            "code": "",
            "codeSystem": "",
            "codeSystemVersion": "",
            "decode": "",
            "instanceType": "Code",
        }
    _, codelist_table, _codelist_code, code, decode = row
    version = ""
    slug = (codelist_table or "").rstrip("/").split("/")[-1]
    if slug:
        parts = slug.split("-")
        if len(parts) >= 4:
            version = f"{parts[-3]}-{parts[-2]}-{parts[-1]}"
    return {
        "id": code_uid,
        "extensionAttributes": [],
        "code": code or "",
        "codeSystem": "http://www.cdisc.org",
        "codeSystemVersion": version,
        "decode": decode or code or "",
        "instanceType": "Code",
    }


def _alias_code_obj(location_code_uid: str, location_code_map: Dict[str, tuple]) -> Any:
    """Build an AliasCode-Output dict wrapping a code table row, or None."""
    row = location_code_map.get(location_code_uid)
    if row is None:
        return None
    _uid, code, code_system, code_system_version, decode = row
    n = location_code_uid.split("_", 1)[-1]
    return {
        "id": f"AliasCode_{n}",
        "extensionAttributes": [],
        "standardCode": {
            "id": location_code_uid,
            "extensionAttributes": [],
            "code": code or "",
            "codeSystem": code_system or "",
            "codeSystemVersion": code_system_version or "",
            "decode": decode or code or "",
            "instanceType": "Code",
        },
        "standardCodeAliases": [],
        "instanceType": "AliasCode",
    }


def _geo_scope_obj(
    scope_uid: str,
    type_code_uid: str,
    code_map: Dict[str, tuple],
    location_code_uid: str = None,
    location_code_map: Dict[str, tuple] = None,
) -> Dict[str, Any]:
    alias = None
    if location_code_uid and location_code_map:
        alias = _alias_code_obj(location_code_uid, location_code_map)
    return {
        "id": scope_uid,
        "extensionAttributes": [],
        "type": _code_obj(type_code_uid, code_map.get(type_code_uid)),
        "code": alias,
        "instanceType": "GeographicScope",
    }


def build_usdm_amendments(soa_id: int) -> List[Dict[str, Any]]:
    """Build USDM StudyAmendment-Output objects for all amendments in a SOA."""
    conn = _connect()
    cur = conn.cursor()

    cur.execute(
        "SELECT id,amendment_uid,name,number,summary,label,description "
        "FROM study_amendment WHERE soa_id=? ORDER BY id",
        (soa_id,),
    )
    amendment_rows = cur.fetchall()
    if not amendment_rows:
        conn.close()
        return []

    amendment_uids = [r[1] for r in amendment_rows]
    placeholders = ",".join("?" * len(amendment_uids))

    cur.execute(
        f"SELECT id,amendment_uid,reason_uid,role,code_uid,other_reason "
        f"FROM study_amendment_reason "
        f"WHERE soa_id=? AND amendment_uid IN ({placeholders}) ORDER BY id",
        [soa_id, *amendment_uids],
    )
    reason_rows = cur.fetchall()

    cur.execute(
        f"SELECT id,amendment_uid,impact_uid,type_code_uid,text,is_substantial "
        f"FROM study_amendment_impact "
        f"WHERE soa_id=? AND amendment_uid IN ({placeholders}) ORDER BY id",
        [soa_id, *amendment_uids],
    )
    impact_rows = cur.fetchall()

    cur.execute(
        f"SELECT id,amendment_uid,change_uid,name,label,description,"
        f"summary,rationale "
        f"FROM study_change "
        f"WHERE soa_id=? AND amendment_uid IN ({placeholders}) ORDER BY id",
        [soa_id, *amendment_uids],
    )
    change_rows = cur.fetchall()

    change_uids = [r[2] for r in change_rows]
    section_rows: list = []
    if change_uids:
        sec_ph = ",".join("?" * len(change_uids))
        cur.execute(
            f"SELECT id,change_uid,ref_uid,section_number,section_title,"
            f"applies_to_id "
            f"FROM document_content_reference "
            f"WHERE soa_id=? AND change_uid IN ({sec_ph}) ORDER BY id",
            [soa_id, *change_uids],
        )
        section_rows = cur.fetchall()

    cur.execute(
        f"SELECT id,amendment_uid,scope_uid,type_code_uid,location_code_uid "
        f"FROM amendment_geographic_scope "
        f"WHERE soa_id=? AND amendment_uid IN ({placeholders}) ORDER BY id",
        [soa_id, *amendment_uids],
    )
    scope_rows = cur.fetchall()

    cur.execute(
        f"SELECT id,amendment_uid,enrollment_uid,name,label,description,"
        f"quantity_value,for_scope_uid,for_study_cohort_id,for_study_site_id "
        f"FROM amendment_subject_enrollment "
        f"WHERE soa_id=? AND amendment_uid IN ({placeholders}) ORDER BY id",
        [soa_id, *amendment_uids],
    )
    enrollment_rows = cur.fetchall()

    cur.execute(
        f"SELECT id,amendment_uid,date_uid,name,label,description,"
        f"type_code_uid,date_value "
        f"FROM amendment_governance_date "
        f"WHERE soa_id=? AND amendment_uid IN ({placeholders}) ORDER BY id",
        [soa_id, *amendment_uids],
    )
    date_rows = cur.fetchall()

    date_uids = [r[2] for r in date_rows]
    date_scope_rows: list = []
    if date_uids:
        ds_ph = ",".join("?" * len(date_uids))
        cur.execute(
            f"SELECT id,date_uid,scope_uid "
            f"FROM governance_date_geographic_scope "
            f"WHERE soa_id=? AND date_uid IN ({ds_ph}) ORDER BY id",
            [soa_id, *date_uids],
        )
        date_scope_rows = cur.fetchall()

    # Batch-fetch all code_association rows
    all_code_uids = (
        [r[4] for r in reason_rows]
        + [r[3] for r in impact_rows]
        + [r[3] for r in scope_rows]
        + [r[6] for r in date_rows]
    )
    code_map: Dict[str, tuple] = {}
    if all_code_uids:
        code_ph = ",".join("?" * len(all_code_uids))
        cur.execute(
            f"SELECT code_uid, codelist_table, codelist_code, code, decode "
            f"FROM code_association WHERE soa_id=? AND code_uid IN ({code_ph})",
            [soa_id, *all_code_uids],
        )
        for row in cur.fetchall():
            code_map[row[0]] = row

    # Batch-fetch location codes (country/region) from the code table
    location_code_uids = [r[4] for r in scope_rows if r[4]]
    location_code_map: Dict[str, tuple] = {}
    if location_code_uids:
        loc_ph = ",".join("?" * len(location_code_uids))
        cur.execute(
            f"SELECT code_uid, code, code_system, code_system_version, decode "
            f"FROM code WHERE soa_id=? AND code_uid IN ({loc_ph})",
            [soa_id, *location_code_uids],
        )
        for row in cur.fetchall():
            location_code_map[row[0]] = row

    conn.close()

    # Index by amendment_uid / change_uid
    reasons_by_amendment: Dict[str, list] = {}
    for r in reason_rows:
        reasons_by_amendment.setdefault(r[1], []).append(r)

    impacts_by_amendment: Dict[str, list] = {}
    for r in impact_rows:
        impacts_by_amendment.setdefault(r[1], []).append(r)

    changes_by_amendment: Dict[str, list] = {}
    for r in change_rows:
        changes_by_amendment.setdefault(r[1], []).append(r)

    sections_by_change: Dict[str, list] = {}
    for s in section_rows:
        sections_by_change.setdefault(s[1], []).append(s)

    scopes_by_amendment: Dict[str, list] = {}
    for s in scope_rows:
        scopes_by_amendment.setdefault(s[1], []).append(s)

    scope_by_uid: Dict[str, tuple] = {s[2]: s for s in scope_rows}

    enrollments_by_amendment: Dict[str, list] = {}
    for e in enrollment_rows:
        enrollments_by_amendment.setdefault(e[1], []).append(e)

    dates_by_amendment: Dict[str, list] = {}
    for d in date_rows:
        dates_by_amendment.setdefault(d[1], []).append(d)

    date_scopes_by_date: Dict[str, list] = {}
    for ds in date_scope_rows:
        date_scopes_by_date.setdefault(ds[1], []).append(ds[2])

    out: List[Dict[str, Any]] = []
    for ar in amendment_rows:
        (
            _am_id,
            amendment_uid,
            name,
            number,
            summary,
            label,
            description,
        ) = ar

        primary_reason = None
        secondary_reasons = []
        for r in reasons_by_amendment.get(amendment_uid, []):
            _rid, _auid, reason_uid, role, code_uid, other_reason = r
            reason_obj = {
                "id": reason_uid,
                "extensionAttributes": [],
                "code": _code_obj(code_uid, code_map.get(code_uid)),
                "otherReason": _nz(other_reason),
                "instanceType": "StudyAmendmentReason",
            }
            if role == "primary":
                primary_reason = reason_obj
            else:
                secondary_reasons.append(reason_obj)

        if primary_reason is None:
            primary_reason = {
                "id": f"{amendment_uid}_PrimaryReason",
                "extensionAttributes": [],
                "code": _code_obj("", None),
                "otherReason": None,
                "instanceType": "StudyAmendmentReason",
            }

        impacts = []
        for i in impacts_by_amendment.get(amendment_uid, []):
            _iid, _auid, impact_uid, type_code_uid, text, is_substantial = i
            impacts.append(
                {
                    "id": impact_uid,
                    "extensionAttributes": [],
                    "notes": [],
                    "type": _code_obj(type_code_uid, code_map.get(type_code_uid)),
                    "text": text or "",
                    "isSubstantial": bool(is_substantial),
                    "instanceType": "StudyAmendmentImpact",
                }
            )

        changes = []
        for c in changes_by_amendment.get(amendment_uid, []):
            (
                _cid,
                _auid,
                change_uid,
                c_name,
                c_label,
                c_description,
                c_summary,
                c_rationale,
            ) = c
            sections = [
                {
                    "id": s[2],
                    "extensionAttributes": [],
                    "sectionNumber": s[3] or "",
                    "sectionTitle": s[4] or "",
                    "appliesToId": s[5] or "",
                    "instanceType": "DocumentContentReference",
                }
                for s in sections_by_change.get(change_uid, [])
            ]
            changes.append(
                {
                    "id": change_uid,
                    "extensionAttributes": [],
                    "name": c_name or "",
                    "label": _nz(c_label),
                    "description": _nz(c_description),
                    "summary": c_summary or "",
                    "rationale": c_rationale or "",
                    "changedSections": sections,
                    "instanceType": "StudyChange",
                }
            )

        geo_scopes = [
            _geo_scope_obj(s[2], s[3], code_map, s[4], location_code_map)
            for s in scopes_by_amendment.get(amendment_uid, [])
        ]

        enrollments = []
        for e in enrollments_by_amendment.get(amendment_uid, []):
            (
                _eid,
                _auid,
                enrollment_uid,
                e_name,
                e_label,
                e_description,
                qty_value,
                for_scope_uid,
                cohort_id,
                site_id,
            ) = e
            for_scope = None
            if for_scope_uid and for_scope_uid in scope_by_uid:
                sr = scope_by_uid[for_scope_uid]
                for_scope = _geo_scope_obj(
                    sr[2], sr[3], code_map, sr[4], location_code_map
                )
            enrollments.append(
                {
                    "id": enrollment_uid,
                    "extensionAttributes": [],
                    "name": e_name or "",
                    "label": _nz(e_label),
                    "description": _nz(e_description),
                    "quantity": {
                        "id": f"{enrollment_uid}_Qty",
                        "extensionAttributes": [],
                        "value": qty_value,
                        "unit": None,
                        "instanceType": "Quantity",
                    },
                    "forGeographicScope": for_scope,
                    "forStudyCohortId": _nz(cohort_id),
                    "forStudySiteId": _nz(site_id),
                    "instanceType": "SubjectEnrollment",
                }
            )

        date_values = []
        for d in dates_by_amendment.get(amendment_uid, []):
            (
                _did,
                _auid,
                date_uid,
                d_name,
                d_label,
                d_description,
                d_type_code_uid,
                d_date_value,
            ) = d
            linked_scopes = [
                _geo_scope_obj(
                    s_uid,
                    scope_by_uid[s_uid][3],
                    code_map,
                    scope_by_uid[s_uid][4],
                    location_code_map,
                )
                for s_uid in date_scopes_by_date.get(date_uid, [])
                if s_uid in scope_by_uid
            ]
            date_values.append(
                {
                    "id": date_uid,
                    "extensionAttributes": [],
                    "name": d_name or "",
                    "label": _nz(d_label),
                    "description": _nz(d_description),
                    "type": _code_obj(d_type_code_uid, code_map.get(d_type_code_uid)),
                    "dateValue": d_date_value or "",
                    "geographicScopes": linked_scopes,
                    "instanceType": "GovernanceDate",
                }
            )

        out.append(
            {
                "id": amendment_uid,
                "extensionAttributes": [],
                "notes": [],
                "name": name or "",
                "label": _nz(label),
                "description": _nz(description),
                "number": number or "",
                "summary": summary or "",
                "primaryReason": primary_reason,
                "secondaryReasons": secondary_reasons,
                "changes": changes,
                "impacts": impacts,
                "geographicScopes": geo_scopes,
                "enrollments": enrollments,
                "dateValues": date_values,
                "instanceType": "StudyAmendment",
            }
        )
    return out


if __name__ == "__main__":
    import argparse
    import json
    import logging
    import sys

    logger = logging.getLogger("usdm.generate_amendments")

    parser = argparse.ArgumentParser(description="Export USDM amendments for a SOA.")
    parser.add_argument("soa_id", type=int, help="SOA id to export")
    parser.add_argument(
        "-o", "--output", default="-", help="Output file path or '-' for stdout"
    )
    parser.add_argument("--indent", type=int, default=2, help="JSON indent")
    args = parser.parse_args()

    try:
        amendments = build_usdm_amendments(args.soa_id)
    except Exception:
        logger.exception("Failed to build amendments for soa_id=%s", args.soa_id)
        sys.exit(1)

    payload = json.dumps(amendments, indent=args.indent)
    if args.output in ("-", "/dev/stdout"):
        sys.stdout.write(payload + "\n")
    else:
        with open(args.output, "w", encoding="utf-8") as f:
            f.write(payload + "\n")
