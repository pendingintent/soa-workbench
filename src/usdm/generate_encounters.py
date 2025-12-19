#!/usr/bin/env python3
# Prefer absolute import; fallback to adding src/ to sys.path when run directly
from typing import Optional, List, Dict, Any

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
        "SELECT name,label,order_index,encounter_uid,description FROM visit WHERE soa_id=?",
        (soa_id,),
    )
    rows = cur.fetchall()
    conn.close()

    uids = [r[3] for r in rows]
    id_by_index = {i: uid for i, uid in enumerate(uids)}
    print(id_by_index)

    out: List[Dict[str, Any]] = []

    for i, r in enumerate(rows):
        name, label, order_index, encounter_uid, description = (
            r[0],
            r[1],
            r[2],
            r[3],
            r[4],
        )
        eid = encounter_uid
        prev_id = id_by_index.get(i - 1)
        next_id = id_by_index.get(i + 1)

        encounter = {
            "id": eid,
            "extensionAttributes": [],
            "name": name,
            "label": _nz(label),
            "description": _nz(description),
            "type": {
                "id": "<placeholder>",
                "extensionAttributes": [],
                "code": "C25716",
                "codeSystem": "db://ddf_terminology",
                "codeSystemVersion": "2025-09-26",
                "decode": "Visit",
                "instanceType": "Code",
            },
            "previousId": prev_id,
            "nextId": next_id,
            "scheduledAt": "<placeholder>",
            "environmentSettings": [],
            "contactModes": [],
            "transitionStartRule": {},
            "transitionEndRule": {},
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
        activities = build_usdm_encounters(args.soa_id)
    except Exception:
        logger.exception("Failed to build Encounters for soa_id=%s", args.soa_id)
        sys.exit(1)

    payload = json.dumps(activities, indent=args.indent)
    if args.output in ("-", "/dev/stdout"):
        sys.stdout.write(payload + "\n")
    else:
        with open(args.output, "w", encoding="utf-8") as f:
            f.write(payload + "\n")
