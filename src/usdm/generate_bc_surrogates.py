#!/usr/bin/env python3
import json
import logging
import sys
from typing import List, Dict, Any

from soa_builder.web.db import _connect
from soa_builder.web.utils import _nz

logger = logging.getLogger("usdm.generate_bc_surrogates")


def build_usdm_bc_surrogates(soa_id: int) -> List[Dict[str, Any]]:
    """
    Build USDM BiomedicalConceptSurrogate-Output objects for the given SOA.

    USDM BiomedicalConceptSurrogate-Output:
      - id: string
      - name: string
      - label?: string | null
      - description?: string | null
      - reference?: string | null
      - notes: CommentAnnotation-Output[]      (left empty here)
      - extensionAttributes: ExtensionAttribute-Output[] (empty)
      - instanceType: "BiomedicalConceptSurrogate"
    """
    conn = _connect()
    cur = conn.cursor()
    cur.execute(
        "SELECT surrogate_uid, name, label, description, reference "
        "FROM biomedical_concept_surrogate WHERE soa_id=? ORDER BY id",
        (soa_id,),
    )
    rows = cur.fetchall()
    conn.close()

    return [
        {
            "id": r[0],
            "extensionAttributes": [],
            "name": r[1],
            "label": _nz(r[2]),
            "description": _nz(r[3]),
            "reference": _nz(r[4]),
            "notes": [],
            "instanceType": "BiomedicalConceptSurrogate",
        }
        for r in rows
    ]


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Export USDM BC surrogates for a SOA.")
    parser.add_argument("soa_id", type=int, help="SOA id to export surrogates for")
    parser.add_argument(
        "-o", "--output", default="-", help="Output file path or '-' for stdout"
    )
    parser.add_argument("--indent", type=int, default=2, help="JSON indent")
    args = parser.parse_args()

    try:
        surrogates = build_usdm_bc_surrogates(args.soa_id)
    except Exception:
        logger.exception("Failed to build bc_surrogates for soa_id=%s", args.soa_id)
        sys.exit(1)

    payload = json.dumps(surrogates, indent=args.indent)
    if args.output in ("-", "/dev/stdout"):
        sys.stdout.write(
            "Output suppressed: this document may contain sensitive data. "
            "Use an explicit -o <file> path to export.\n"
        )
    else:
        with open(args.output, "w", encoding="utf-8") as f:
            f.write(payload + "\n")
