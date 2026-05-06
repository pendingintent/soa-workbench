#!/usr/bin/env python3
"""Build USDM Objective-Output objects for a SOA."""

from typing import List, Dict, Any

from soa_builder.web.db import _connect
from soa_builder.web.utils import _nz
from .usdm_utils import _build_level_code
from .generate_endpoints import build_usdm_endpoints


def build_usdm_objectives(soa_id: int) -> List[Dict[str, Any]]:
    """
    Build USDM Objective-Output objects for the given SOA.

    Each objective embeds its child Endpoint-Output objects under the
    `endpoints` field, matching the USDM v4.0.0 schema where Endpoint
    is nested under Objective rather than directly under StudyDesign.
    """
    conn = _connect()
    cur = conn.cursor()
    cur.execute(
        "SELECT objective_uid,name,label,description,text,level_code_uid "
        "FROM objective WHERE soa_id=? ORDER BY order_index, id",
        (soa_id,),
    )
    objective_rows = cur.fetchall()

    cur.execute(
        "SELECT endpoint_uid,objective_uid FROM endpoint WHERE soa_id=?",
        (soa_id,),
    )
    endpoint_parent_map = {
        endpoint_uid: objective_uid for endpoint_uid, objective_uid in cur.fetchall()
    }
    conn.close()

    all_endpoints = build_usdm_endpoints(soa_id)

    out: List[Dict[str, Any]] = []
    for r in objective_rows:
        (
            objective_uid,
            name,
            label,
            description,
            text,
            level_code_uid,
        ) = r
        nested = [
            e
            for e in all_endpoints
            if endpoint_parent_map.get(e["id"]) == objective_uid
        ]
        out.append(
            {
                "id": objective_uid,
                "extensionAttributes": [],
                "name": name,
                "label": _nz(label),
                "description": _nz(description),
                "text": text or "",
                "dictionaryId": None,
                "notes": [],
                "level": _build_level_code(soa_id, level_code_uid, "Objective"),
                "endpoints": nested,
                "instanceType": "Objective",
            }
        )
    return out


if __name__ == "__main__":
    import argparse
    import json
    import logging
    import sys

    logger = logging.getLogger("usdm.generate_objectives")

    parser = argparse.ArgumentParser(description="Export USDM objectives for a SOA.")
    parser.add_argument("soa_id", type=int, help="SOA id to export objectives for")
    parser.add_argument(
        "-o", "--output", default="-", help="Output file path or '-' for stdout"
    )
    parser.add_argument("--indent", type=int, default=2, help="JSON indent")
    args = parser.parse_args()

    try:
        objectives = build_usdm_objectives(args.soa_id)
    except Exception:
        logger.exception("Failed to build objectives for soa_id=%s", args.soa_id)
        sys.exit(1)

    payload = json.dumps(objectives, indent=args.indent)
    if args.output in ("-", "/dev/stdout"):
        sys.stdout.write(payload + "\n")
    else:
        with open(args.output, "w", encoding="utf-8") as f:
            f.write(payload + "\n")
