#!/usr/bin/env python3
# Prefer absolute import; fallback to adding src/ to sys.path when run directly
from typing import Optional, List, Dict, Any
from soa_builder.web.db import _connect
from soa_builder.web.utils import _nz
from .usdm_utils import _get_timing_code_values


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
            SELECT timing_uid,name,label,description,type,value,value_label,relative_to_from,
            relative_from_schedule_instance,relative_to_schedule_instance,window_label,window_upper,
            window_lower FROM timing WHERE soa_id=? AND member_of_timeline=? order by length(timing_uid),
            timing_uid
            """,
            (soa_id, member_of_timeline.strip()),
        )
    else:
        cur.execute(
            """
            SELECT timing_uid,name,label,description,type,value,value_label,relative_to_from,
            relative_from_schedule_instance,relative_to_schedule_instance,window_label,window_upper,
            window_lower FROM timing WHERE soa_id=? order by length(timing_uid), timing_uid
            """,
            (soa_id,),
        )
    rows = cur.fetchall()
    conn.close()
    out: List[Dict[str, Any]] = []

    for i, r in enumerate(rows):
        (
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
        )
        type_code_obj = None
        if type:
            t_code, t_decode, t_codeSystem, t_codeSystemVersion = (
                _get_timing_code_values(soa_id, type)
            )
            if t_code:
                type_code_obj = {
                    "id": type,
                    "extensionAttributes": [],
                    "code": t_code[0],
                    "codeSystem": "http://www.cdisc.org",
                    "codeSystemVersion": t_codeSystemVersion[0],
                    "decode": t_decode[0],
                    "instanceType": "Code",
                }

        rtf_code_obj = None
        if relative_to_from:
            rtf_code, rtf_decode, rtf_codeSystem, rtf_codeSystemVersion = (
                _get_timing_code_values(soa_id, relative_to_from)
            )
            if rtf_code:
                rtf_code_obj = {
                    "id": relative_to_from,
                    "extensionAttributes": [],
                    "code": rtf_code[0],
                    "codeSystem": "http://www.cdisc.org",
                    "codeSystemVersion": rtf_codeSystemVersion[0],
                    "decode": rtf_decode[0],
                    "instanceType": "Code",
                }

        timing = {
            "id": timing_uid,
            "extensionAttributes": [],
            "name": name,
            "label": _nz(label),
            "description": _nz(description),
            "type": type_code_obj,
            "value": value,
            "valueLabel": value_label,
            "relativeToFrom": rtf_code_obj,
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
