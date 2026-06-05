#!/usr/bin/env python3
from typing import List, Dict, Any
from soa_builder.web.utils import _nz
from soa_builder.web.db import _connect
from .usdm_utils import _get_type_code_tuple, _get_data_origin_type_tuple


def build_usdm_arms(soa_id: int) -> List[Dict[str, Any]]:
    """
    Build USDM Arms-Output objects for the given SOA.

    USDM Activity-Output (subset):
      - id: string
      - extensionAttributes?: string[]|[]
      - name: string
      - label?: string | null
      - description?: string | null
      - type: {
        - id: string
        - extensionAttributes?: string[]|[]
        - code: string
        - codeSystem: string
        - codeSystemVersion: string
        - decode: string
        - instanceType: "Code"
        }
      - dataOriginDescription?: string|null
      - dataOriginType?: {
        - id: string
        - extensionAttributes?: string[]|[]
        - code: string
        - codeSystem: string
        - codeSystemVersion: string
        - decode: string
        - instanceType: "Code"
        }
      - popultionIds?: int[]
      - notes?: string[]|[]
      - instanceType: "Activity"
    """
    conn = _connect()
    cur = conn.cursor()
    cur.execute(
        "SELECT id,arm_uid,name,label,description,type,data_origin_type FROM arm WHERE soa_id=? ORDER BY arm_uid",
        (soa_id,),
    )
    rows = cur.fetchall()
    conn.close()
    out: List[Dict[str, Any]] = []

    for i, r in enumerate(rows):
        _, arm_uid, name, label, description, type, data_origin_type = (
            r[0],
            r[1],
            r[2],
            r[3],
            r[4],
            r[5],
            r[6],
        )
        t_code, t_decode, t_codeSystem, t_codeSystemVersion = _get_type_code_tuple(
            soa_id, type
        )
        dto_code, dto_decode, dto_code_system, dto_code_system_version = (
            _get_data_origin_type_tuple(soa_id, data_origin_type)
        )

        arm = {
            "id": arm_uid,
            "extensionAttributes": [],
            "name": name,
            "label": _nz(label),
            "description": _nz(description),
            "type": {
                "id": type,
                "extensionAttributes": [],
                "code": t_code[0],
                "codeSystem": "http://www.cdisc.org",
                "codeSystemVersion": t_codeSystemVersion[0],
                "decode": t_decode[0],
                "instanceType": "Code",
            },
            "dataOriginDescription": "Data collected from subjects",
            "dataOriginType": {
                "id": data_origin_type,
                "extensionAttributes": [],
                "code": dto_code[0],
                "codeSystem": "http://www.cdisc.org",
                "codeSystemVersion": dto_code_system_version[0],
                "decode": dto_decode[0],
                "instanceType": "Code",
            },
            "populationIds": [],
            "notes": [],
            "instanceType": "StudyArm",
        }
        out.append(arm)

    return out


if __name__ == "__main__":
    import argparse
    import json
    import logging
    import sys

    logger = logging.getLogger("usdm.generate_arms")

    parser = argparse.ArgumentParser(description="Export USDM arms for a SOA.")
    parser.add_argument("soa_id", type=int, help="SOA id to export arms for")
    parser.add_argument(
        "-o", "--output", default="-", help="Output file path or '-' for stdout"
    )
    parser.add_argument("--indent", type=int, default=2, help="JSON indent")
    args = parser.parse_args()

    try:
        activities = build_usdm_arms(args.soa_id)
    except Exception:
        logger.exception("Failed to build arms for soa_id=%s", args.soa_id)
        sys.exit(1)

    payload = json.dumps(activities, indent=args.indent)
    if args.output in ("-", "/dev/stdout"):
        sys.stdout.write(payload + "\n")
    else:
        with open(args.output, "w", encoding="utf-8") as f:
            f.write(payload + "\n")
