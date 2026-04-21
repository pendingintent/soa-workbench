#!/usr/bin/env python3
from typing import List, Dict, Any, Tuple
from soa_builder.web.utils import get_submission_value_for_code, _nz
from soa_builder.web.db import _connect


# Override the definition in usdm_utils.py
# Encounter type codes are resolved via CDISC Library DDF CT.
def _get_type_code_tuple(soa_id: int, code_uid: str) -> Tuple[str, str, str, str]:
    """Enrich ENCOUNTER type code_association rows via CDISC Library DDF CT."""
    from soa_builder.web.utils import get_ddf_ct_rows, get_ddf_ct_term

    conn = _connect()
    cur = conn.cursor()
    cur.execute(
        "SELECT DISTINCT codelist_table, code, codelist_code "
        "FROM code_association WHERE soa_id=? AND code_uid=?",
        (soa_id, code_uid),
    )
    rows = cur.fetchall()
    conn.close()

    payload = get_ddf_ct_rows()
    slug = payload.get("slug") or ""
    version = ""
    if slug:
        parts = slug.split("-")
        if len(parts) >= 4:
            version = f"{parts[-3]}-{parts[-2]}-{parts[-1]}"

    code_system: list = []
    code_code: list = []
    code_decode: list = []
    code_system_version: list = []
    for codelist_table, code, codelist_code in rows:
        term = get_ddf_ct_term(codelist_code, code)
        if not term:
            continue
        code_system.append(codelist_table)
        code_code.append(code)
        code_decode.append(term.get("submission_value") or "")
        code_system_version.append(version)
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

    # Pre-fetch code_association for type codes (keyed by code_uid); enrich via
    # CDISC Library DDF CT.
    from soa_builder.web.utils import get_ddf_ct_rows, get_ddf_ct_term

    cur.execute(
        "SELECT DISTINCT code_uid, codelist_table, code, codelist_code "
        "FROM code_association WHERE soa_id=?",
        (soa_id,),
    )
    ddf_payload = get_ddf_ct_rows()
    ddf_slug = ddf_payload.get("slug") or ""
    ddf_version = ""
    if ddf_slug:
        parts = ddf_slug.split("-")
        if len(parts) >= 4:
            ddf_version = f"{parts[-3]}-{parts[-2]}-{parts[-1]}"
    type_code_map: dict = {}
    for code_uid, codelist_table, code, codelist_code in cur.fetchall():
        term = get_ddf_ct_term(codelist_code, code)
        if not term:
            continue
        type_code_map.setdefault(code_uid, ([], [], [], []))
        type_code_map[code_uid][0].append(code)
        type_code_map[code_uid][1].append(term.get("submission_value") or "")
        type_code_map[code_uid][2].append(codelist_table)
        type_code_map[code_uid][3].append(ddf_version)

    # Pre-fetch code_association for env/contact codes (keyed by code_uid)
    cur.execute(
        "SELECT DISTINCT code_uid, codelist_table, code FROM code_association WHERE soa_id=?",
        (soa_id,),
    )
    code_tuple_map: dict = {}
    for code_uid, codelist_table, code in cur.fetchall():
        code_tuple_map.setdefault(code_uid, ([], []))
        code_tuple_map[code_uid][0].append(code)
        code_tuple_map[code_uid][1].append(codelist_table)

    # Pre-fetch all transition rules for this SOA (keyed by transition_rule_uid)
    cur.execute(
        "SELECT transition_rule_uid, name, label, description, text FROM transition_rule WHERE soa_id=?",
        (soa_id,),
    )
    transition_rule_map: dict = {}
    for tr_uid, tr_name, tr_label, tr_desc, tr_text in cur.fetchall():
        transition_rule_map[tr_uid] = {
            "id": tr_uid,
            "extensionAttributes": [],
            "name": tr_name or None,
            "label": tr_label or None,
            "description": tr_desc or None,
            "text": tr_text or None,
            "instanceType": "TransitionRule",
        }

    # Pre-fetch all timing UIDs for this SOA (keyed by timing id)
    cur.execute("SELECT id, timing_uid FROM timing WHERE soa_id=?", (soa_id,))
    timing_id_map: dict = {row[0]: row[1] for row in cur.fetchall()}

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
        _type_entry = type_code_map.get(type, ([], [], [], []))
        t_code, t_decode, t_codeSystem, t_codeSystemVersion = _type_entry

        e_code: List[str] = []
        e_codesystem: List[str] = []

        if environmentalSettings:
            e_code, e_codesystem = code_tuple_map.get(environmentalSettings, ([], []))

        c_code: List[str] = []
        c_codesystem: List[str] = []

        if contactModes:
            c_code, c_codesystem = code_tuple_map.get(contactModes, ([], []))

        prev_id = id_by_index.get(i - 1)
        next_id = id_by_index.get(i + 1)

        _sched_id = (
            int(scheduledAtId)
            if (scheduledAtId is not None and str(scheduledAtId).isdigit())
            else None
        )
        timing_uid = timing_id_map.get(_sched_id) if _sched_id is not None else None

        transition_start_rule_obj = transition_rule_map.get(transition_start_rule_uid)
        transition_end_rule_obj = transition_rule_map.get(transition_end_rule_uid)

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
            "transitionStartRule": transition_start_rule_obj or None,
            "transitionEndRule": transition_end_rule_obj or None,
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
