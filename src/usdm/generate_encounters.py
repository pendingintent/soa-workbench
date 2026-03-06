#!/usr/bin/env python3
from typing import List, Dict, Any, Tuple
from soa_builder.web.utils import get_submission_value_for_code, _nz
from soa_builder.web.db import _connect
from .usdm_utils import (
    _get_timing_name,
    _get_transition_start_rule,
    _get_transition_end_rule,
    _get_code_tuple,
)


# Override the definition in usdm_utils.py
# Encounters are currently storing type codes in the ddf_terminology table
def _get_type_code_tuple(soa_id: int, code_uid: str) -> Tuple[str, str, str, str]:
    """Fetch type codes for ENCOUNTERS only.  These values are stored in the ddf_terminology table."""
    conn = _connect()
    cur = conn.cursor()
    cur.execute(
        "SELECT DISTINCT c.codelist_table, p.code,p.cdisc_submission_value,p.dataset_date "
        "FROM code_association c INNER JOIN ddf_terminology p ON c.codelist_code = p.codelist_code "
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
        - transitionEndRule?: {}
        - notes: []
        - instanceType: "Encounter"
    """
    conn = _connect()
    cur = conn.cursor()
    cur.execute(
        "SELECT name,label,order_index,encounter_uid,description,type,environmentalSettings,scheduledAtId,transitionStartRule,transitionEndRule,contactModes FROM visit WHERE soa_id=?",
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
            _,
            encounter_uid,
            description,
            type,
            environmentalSettings,
            scheduledAtId,
            transition_start_rule_uid,
            transition_end_rule_uid,
            contactModes,
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
        )
        eid = encounter_uid
        t_code, t_decode, t_codeSystem, t_codeSystemVersion = _get_type_code_tuple(
            soa_id, type
        )

        e_code: List[str] = []
        e_codesystem: List[str] = []

        if environmentalSettings:
            e_code, e_codesystem = _get_code_tuple(soa_id, environmentalSettings)

        c_code: List[str] = []
        c_codesystem: List[str] = []

        if contactModes:
            c_code, c_codesystem = _get_code_tuple(soa_id, contactModes)

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

        # Build optional environmentalSettings array
        env_settings: List[Dict[str, Any]] = []
        if e_code and e_codesystem:
            _seg = e_codesystem[0].rsplit("/", 1)[-1]
            code_system_version = _seg[_seg.index("-") + 1 :] if "-" in _seg else _seg
            decode = get_submission_value_for_code(
                soa_id,
                "C127262",
                environmentalSettings,
            )
            env_settings.append(
                {
                    "id": environmentalSettings,
                    "extensionAttributes": [],
                    "code": e_code[0],
                    "codeSystem": e_codesystem[0],
                    "codeSystemVersion": code_system_version,
                    "decode": decode,
                    "instanceType": "Code",
                }
            )

        # Build optional contactMode array
        contact_mode: List[Dict[str, Any]] = []
        if c_code and c_codesystem:
            _cseg = c_codesystem[0].rsplit("/", 1)[-1]
            c_code_system_version = (
                _cseg[_cseg.index("-") + 1 :] if "-" in _cseg else _cseg
            )
            c_decode = get_submission_value_for_code(
                soa_id,
                "C171445",
                contactModes,
            )
            contact_mode.append(
                {
                    "id": contactModes,
                    "extensionAttributes": [],
                    "code": c_code[0],
                    "codeSystem": c_codesystem[0],
                    "codeSystemVersion": c_code_system_version,
                    "decode": c_decode,
                    "instanceType": "Code",
                }
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
                "codeSystem": t_codeSystem[0],
                "codeSystemVersion": t_codeSystemVersion[0],
                "decode": t_decode[0],
                "instanceType": "Code",
            },
            "previousId": prev_id,
            "nextId": next_id,
            "scheduledAt": timing_uid,
            "environmentSettings": env_settings,
            "contactModes": contact_mode,
            "transitionStartRule": transition_start_rule_obj or {},
            "transitionEndRule": transition_end_rule_obj or {},
            "notes": [],
            "instanceType": "Encounter",
        }
        if timing_uid:
            encounter["scheduledAt"] = timing_uid
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
