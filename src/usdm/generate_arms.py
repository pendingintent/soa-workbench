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


def _get_type_code_tuple(soa_id: int, code_uid: str) -> Tuple[str, str, str, str]:
    conn = _connect()
    cur = conn.cursor()
    cur.execute(
        "SELECT DISTINCT c.codelist_table, p.code,p.cdisc_submission_value,p.dataset_date "
        "FROM code c INNER JOIN protocol_terminology p ON c.codelist_code = p.codelist_code "
        "AND c.code = p.code WHERE c.soa_id=? AND c.code_uid=?",
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


def _get_data_origin_type_tuple(
    soa_id: int, code_uid: str
) -> Tuple[str, str, str, str]:
    conn = _connect()
    cur = conn.cursor()
    cur.execute(
        "SELECT DISTINCT c.codelist_table,d.code,d.cdisc_submission_value,d.dataset_date "
        "FROM code c INNER JOIN ddf_terminology d ON c.codelist_code = d.codelist_code "
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
        - instanceTye: "Code"
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
        id, arm_uid, name, label, description, type, data_origin_type = (
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
                "codeSystem": "db://" + t_codeSystem[0],
                "codeSystemVersion": t_codeSystemVersion[0],
                "decode": t_decode[0],
                "instanceType": "Code",
            },
            "dataOriginDescription": "Data collected from subjects",
            "dataOriginType": {
                "id": data_origin_type,
                "extensionAttributes": [],
                "code": dto_code[0],
                "codeSystem": "db://" + dto_code_system[0],
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
