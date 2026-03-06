#!/usr/bin/env python3
from typing import List, Dict, Any
from soa_builder.web.db import _connect
from soa_builder.web.utils import _nz
from .usdm_utils import _get_transition_end_rule, _get_transition_start_rule


def build_usdm_elements(soa_id: int) -> List[Dict[str, Any]]:
    """
    Build USDM Elements-Output objects for the given SOA

    USDM Elements-Output (subset):
        - id: string
        - extensionAttributes?: string[]
        - name: string
        - label?: string
        - description?: string
        - transitionStartRule?: {}
        - transitionEndRule?: {}
        - studyInterventionIds?: string[]
        - notes?: string[]
        - instanceType: "StudyElement"

    """
    conn = _connect()
    cur = conn.cursor()
    cur.execute(
        "SELECT name,label,description,element_id,testrl,teenrl FROM element WHERE soa_id=? ORDER BY order_index",
        (soa_id,),
    )
    rows = cur.fetchall()
    conn.close()
    out: List[Dict[str, Any]] = []

    for i, r in enumerate(rows):
        (
            name,
            label,
            description,
            element_id,
            testrl,
            teenrl,
        ) = (
            r[0],
            r[1],
            r[2],
            r[3],
            r[4],
            r[5],
        )

        transition_start_rule_obj = _get_transition_start_rule(soa_id, testrl)

        transition_end_rule_obj = _get_transition_end_rule(soa_id, teenrl)

        element = {
            "id": element_id,
            "extensionAttributes": [],
            "name": name,
            "label": _nz(label),
            "description": _nz(description),
            "transitionStartRule": transition_start_rule_obj or {},
            "transitionEndRule": transition_end_rule_obj or {},
            "studyInterventionIds": [],
            "notes": [],
            "instanceType": "StudyElement",
        }
        out.append(element)
    return out


if __name__ == "__main__":
    import argparse
    import json
    import logging
    import sys

    logger = logging.getLogger("usdm.generate_elements")

    parser = argparse.ArgumentParser(description="Export USDM Elements for a SOA.")
    parser.add_argument("soa_id", type=int, help="SOA id to export Elements for")
    parser.add_argument(
        "-o", "--output", default="-", help="Output file path or '-' for stdout"
    )
    parser.add_argument("--indent", type=int, default=2, help="JSON indent")
    args = parser.parse_args()

    try:
        elements = build_usdm_elements(args.soa_id)
    except Exception:
        logger.exception("Failed to build Elements for soa_id=%s", args.soa_id)
        sys.exit(1)

    payload = json.dumps(elements, indent=args.indent)
    if args.output in ("-", "/dev/stdout"):
        sys.stdout.write(payload + "\n")
    else:
        with open(args.output, "w", encoding="utf-8") as f:
            f.write(payload + "\n")
