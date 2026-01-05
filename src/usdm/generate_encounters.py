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


def _get_timing_name(soa_id: int, timing_id: Optional[int]) -> str:
    conn = _connect()
    cur = conn.cursor()
    cur.execute(
        "SELECT timing_uid FROM timing WHERE id=? AND soa_id=?",
        (
            timing_id,
            soa_id,
        ),
    )
    row = cur.fetchone()
    conn.close()
    timing_uid = row[0] if (row and row[0] is not None) else None

    return timing_uid


def _get_transition_start_rule(
    soa_id: int, transition_rule_uid: Optional[str]
) -> Optional[Dict[str, Any]]:
    if not transition_rule_uid:
        return None
    conn = _connect()
    cur = conn.cursor()
    cur.execute(
        "SELECT tr.name, tr.label, tr.description, tr.text FROM transition_rule tr WHERE soa_id=? AND transition_rule_uid=?",
        (soa_id, transition_rule_uid),
    )
    row = cur.fetchone()
    conn.close()
    if not row:
        return None
    return {
        "id": transition_rule_uid,
        "extensionAttributes": [],
        "name": row[0] or None,
        "label": row[1] or None,
        "description": row[2] or None,
        "text": row[3] or None,
        "instanceType": "TransitionRule",
    }


def _get_transition_end_rule(
    soa_id: int, transition_rule_uid: Optional[str]
) -> Optional[Dict[str, Any]]:
    if not transition_rule_uid:
        return None
    conn = _connect()
    cur = conn.cursor()
    cur.execute(
        "SELECT tr.name, tr.label, tr.description, tr.text FROM transition_rule tr WHERE soa_id=? AND transition_rule_uid=?",
        (soa_id, transition_rule_uid),
    )
    row = cur.fetchone()
    conn.close()
    if not row:
        return None
    return {
        "id": transition_rule_uid,
        "extensionAttributes": [],
        "name": row[0] or None,
        "label": row[1] or None,
        "description": row[2] or None,
        "text": row[3] or None,
        "instanceType": "TransitionRule",
    }


def _get_type_code_tuple(soa_id: int, code_uid: str) -> Tuple[str, str, str, str]:
    conn = _connect()
    cur = conn.cursor()
    cur.execute(
        "SELECT DISTINCT c.codelist_table, p.code,p.cdisc_submission_value,p.dataset_date "
        "FROM code c INNER JOIN ddf_terminology p ON c.codelist_code = p.codelist_code "
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


def _get_environment_code_tuple(soa_id: int, code_uid: str) -> Tuple[str, str]:
    conn = _connect()
    cur = conn.cursor()
    cur.execute(
        "SELECT DISTINCT c.codelist_table,c.code "
        "FROM code c WHERE c.soa_id=? AND c.code_uid=?",
        (
            soa_id,
            code_uid,
        ),
    )
    rows = cur.fetchall()
    conn.close()
    code_system = [r[0] for r in rows]
    code = [r[1] for r in rows]
    return code, code_system


def build_usdm_encounters(soa_id: int) -> List[Dict[str, Any]]:
    """
    Build USDM Encounters-Output objects for the given SOA

    USDM Encounters-Output (subset):
        - id: string
        - extensionAttributes?: string[]
        - name: string
        - label?: string
        - description?: string
        - type?: {
            - id: string
            - extensionAttributes: []
            - code: string
            - codeSystem: string
            - codeSystemVersion: string
            - decode: string
            - instanceType: "Code"
        }
        - previousId?: string
        - nextId?: string
        - scheduledAtId?: string
        - environmentalSettings?: [
            {
                - id: string
                - extensionAttributes: []
                - code: string      -- I do not know from which codelist these codes originate
                - codeSystem: string
                - codeSystemVersion: string
                - decode: string
                - instanceType: "Code"
            },
        ]
        - contactModes: []
        - transitionStartRule?: {}
        - transitionEndRule": {}
        - notes: []
        - instanceType: "Encounter"
    """
    conn = _connect()
    cur = conn.cursor()
    cur.execute(
        "SELECT name,label,order_index,encounter_uid,description,type,environmentalSettings,scheduledAtId,transitionStartRule,transitionEndRule FROM visit WHERE soa_id=?",
        (soa_id,),
    )
    rows = cur.fetchall()
    conn.close()

    uids = [r[3] for r in rows]
    id_by_index = {i: uid for i, uid in enumerate(uids)}
    # print(id_by_index)

    out: List[Dict[str, Any]] = []

    for i, r in enumerate(rows):
        (
            name,
            label,
            order_index,
            encounter_uid,
            description,
            type,
            environmentalSettings,
            scheduledAtId,
            transition_start_rule_uid,
            transition_end_rule_uid,
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
        )
        eid = encounter_uid
        t_code, t_decode, t_codeSystem, t_codeSystemVersion = _get_type_code_tuple(
            soa_id, type
        )
        e_code, e_codesystem = _get_environment_code_tuple(
            soa_id, environmentalSettings
        )
        # print(e_code, e_codesystem)
        prev_id = id_by_index.get(i - 1)
        next_id = id_by_index.get(i + 1)

        timing_uid = _get_timing_name(
            soa_id,
            (
                int(scheduledAtId)
                if (scheduledAtId is not None and str(scheduledAtId).isdigit())
                else None
            ),
        )

        transition_start_rule_obj = _get_transition_start_rule(
            soa_id, transition_start_rule_uid
        )

        transition_end_rule_obj = _get_transition_end_rule(
            soa_id, transition_end_rule_uid
        )

        encounter = {
            "id": eid,
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
            "previousId": prev_id,
            "nextId": next_id,
            "scheduledAt": timing_uid,
            "environmentSettings": [
                {
                    "id": environmentalSettings,
                    "extensionAttributes": [],
                    "code": e_code[0],
                    "codeSystem": e_codesystem[0],
                    "codeSystemVersion": "2024-09-27",
                    "decode": "Clinic",
                    "instanceType": "Code",
                },
            ],
            "contactModes": [],
            "transitionStartRule": transition_start_rule_obj or {},
            "transitionEndRule": transition_end_rule_obj or {},
            "notes": [],
            "instanceType": "Encounter",
        }
        out.append(encounter)
    return out


if __name__ == "__main__":
    import argparse
    import json
    import logging
    import sys

    logger = logging.getLogger("usdm.generate_encounters")

    parser = argparse.ArgumentParser(description="Export USDM Encounters for a SOA.")
    parser.add_argument("soa_id", type=int, help="SOA id to export Encounters for")
    parser.add_argument(
        "-o", "--output", default="-", help="Output file path or '-' for stdout"
    )
    parser.add_argument("--indent", type=int, default=2, help="JSON indent")
    args = parser.parse_args()

    try:
        encounters = build_usdm_encounters(args.soa_id)
    except Exception:
        logger.exception("Failed to build Encounters for soa_id=%s", args.soa_id)
        sys.exit(1)

    payload = json.dumps(encounters, indent=args.indent)
    if args.output in ("-", "/dev/stdout"):
        sys.stdout.write(payload + "\n")
    else:
        with open(args.output, "w", encoding="utf-8") as f:
            f.write(payload + "\n")
