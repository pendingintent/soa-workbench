#!/usr/bin/env python3
# Prefer absolute import; fallback to adding src/ to sys.path when run directly
from typing import Optional, List, Dict, Any, Tuple

try:
    from soa_builder.web.app import _connect  # reuse existing DB connector
except ImportError:
    import sys
    from pathlib import Path

    here = Path(__file__).resolve()
    src_dir = here.parents[2] / "src"
    if src_dir.exists() and str(src_dir) not in sys.path:
        sys.path.insert(0, str(src_dir))
    from soa_builder.web.app import _connect  # type: ignore


def _nz(s: Optional[str]) -> Optional[str]:
    s = (s or "").strip()
    return s or None


def _get_timing_code_values(soa_id: int, code_uid: str) -> Tuple[str, str, str, str]:
    conn = _connect()
    cur = conn.cursor()
    cur.execute(
        "SELECT DISTINCT c.codelist_table,d.code,d.cdisc_submission_value,d.dataset_date "
        "FROM code_association c INNER JOIN ddf_terminology d ON c.codelist_code = d.codelist_code "
        "AND c.code = d.code WHERE c.soa_id=? AND c.code_uid=?",
        (
            soa_id,
            code_uid,
        ),
    )
    rows = cur.fetchall()
    conn.close()
    code_system = [r[0] for r in rows]
    code_code = [r[1] for r in rows]
    code_decode = [r[2] for r in rows]
    code_system_version = [r[3] for r in rows]

    return code_code, code_decode, code_system, code_system_version


def build_usdm_timings(
    soa_id: int, member_of_timeline: Optional[str] = None
) -> List[Dict[str, Any]]:
    """
    Build USDM Timings-Output objects for the given SOA.

    USDM Timings-Output (subset):
        - id: string
        - extensionAttributes?: string[]
        - name: string
        - label?: string
        - description?: string
        - type?: {
            - id: string
            - extensionAttributes: string[]
            - code: string
            - codeSystem: string
            - codeSystemVersion: string
            - decode: string
            - instanceType: "Code"
        }
        - value?: string
        - valueLabel?: string
        - relativeToFrom?: {
            - id: string
            - extensionAttributes: string[]
            - code: string
            - codeSystem: string
            - codeSystemVersion: string
            - decode: string
            - instanceType: "Code"
        }
        - relativeFromScheduledInstanceId?: string
        - relativeToScheduledInstanceId?: string
        - windowLower?: string
        - windowUpper?: string
        - windowLabel?: string
        - instanceType: "Timing"
    """
    conn = _connect()
    cur = conn.cursor()
    if member_of_timeline and member_of_timeline.strip():
        cur.execute(
            """
            SELECT id,timing_uid,name,label,description,type,value,value_label,relative_to_from,
            relative_from_schedule_instance,relative_to_schedule_instance,window_label,window_upper,
            window_lower,order_index FROM timing WHERE soa_id=? AND member_of_timeline=? order by length(timing_uid),
            timing_uid
            """,
            (soa_id, member_of_timeline.strip()),
        )
    else:
        cur.execute(
            """
            SELECT id,timing_uid,name,label,description,type,value,value_label,relative_to_from,
            relative_from_schedule_instance,relative_to_schedule_instance,window_label,window_upper,
            window_lower,order_index FROM timing WHERE soa_id=? order by length(timing_uid), timing_uid
            """,
            (soa_id,),
        )
    rows = cur.fetchall()
    conn.close()
    out: List[Dict[str, Any]] = []

    for i, r in enumerate(rows):
        (
            row_id,
            timing_uid,
            name,
            label,
            description,
            type,
            value,
            value_label,
            relative_to_from,
            relative_from_schedule_instance,
            relative_to_schedule_instance,
            window_label,
            window_upper,
            window_lower,
            order_index,
        ) = (
            r[0],
            r[1],
            r[2],
            r[3],
            r[4],
            r[5],
            r[6],
            r[7],
            r[8],
            r[9],
            r[10],
            r[11],
            r[12],
            r[13],
            r[14],
        )
        t_code, t_decode, t_codeSystem, t_codeSystemVersion = _get_timing_code_values(
            soa_id, type
        )
        rtf_code, rtf_decode, rtf_codeSystem, rtf_codeSystemVersion = (
            _get_timing_code_values(soa_id, relative_to_from)
        )

        timing = {
            "id": timing_uid,
            "extensionAttributes": [],
            "name": name,
            "label": _nz(label),
            "description": _nz(description),
            "type": {
                "id": type,
                "extensionAttributes": [],
                "code": t_code[0],
                "codeSystem": "db://" + t_codeSystem[0],
                "codeSystemVersion": t_codeSystemVersion[0],
                "decode": t_decode[0],
                "instanceType": "Code",
            },
            "value": value,
            "valueLabel": value_label,
            "relativeToFrom": {
                "id": relative_to_from,
                "extensionAttributes": [],
                "code": rtf_code[0],
                "codeSystem": "db://" + rtf_codeSystem[0],
                "codeSystemVersion": rtf_codeSystemVersion[0],
                "decode": rtf_decode[0],
                "instanceType": "Code",
            },
            "relativeFromScheduledInstanceId": relative_from_schedule_instance,
            "relativeToScheduledInstanceId": relative_to_schedule_instance,
            "windowLower": window_lower,
            "windowUpper": window_upper,
            "windowLabel": window_label,
            "instanceType": "Timing",
        }
        out.append(timing)

    return out


if __name__ == "__main__":
    import argparse
    import json
    import logging
    import sys

    logger = logging.getLogger("usdm.generate_timings")

    parser = argparse.ArgumentParser(description="Export USDM timings for a SOA.")
    parser.add_argument("soa_id", type=int, help="SOA id to export timings for")
    parser.add_argument(
        "-o", "--output", default="-", help="Output file path or '-' for stdout"
    )
    parser.add_argument("--indent", type=int, default=2, help="JSON indent")
    parser.add_argument(
        "--member-of-timeline",
        dest="member_of_timeline",
        default=None,
        help="Optional schedule_timeline_uid to filter timings",
    )
    args = parser.parse_args()

    try:
        activities = build_usdm_timings(
            args.soa_id, member_of_timeline=args.member_of_timeline
        )
    except Exception:
        logger.exception("Failed to build timings for soa_id=%s", args.soa_id)
        sys.exit(1)

    payload = json.dumps(activities, indent=args.indent)
    if args.output in ("-", "/dev/stdout"):
        sys.stdout.write(payload + "\n")
    else:
        with open(args.output, "w", encoding="utf-8") as f:
            f.write(payload + "\n")
