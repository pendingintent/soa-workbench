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
