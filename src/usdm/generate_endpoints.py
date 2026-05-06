#!/usr/bin/env python3
"""Build USDM Endpoint-Output objects for a SOA."""

from typing import List, Dict, Any

from soa_builder.web.db import _connect
from soa_builder.web.utils import _nz
from .usdm_utils import _build_level_code


def build_usdm_endpoints(soa_id: int) -> List[Dict[str, Any]]:
    """
    Build USDM Endpoint-Output objects for the given SOA.

    USDM Endpoint-Output required fields:
      - id, name, text, purpose, level (Code), instanceType="Endpoint"
    """
    conn = _connect()
    cur = conn.cursor()
    cur.execute(
        "SELECT endpoint_uid,objective_uid,name,label,description,"
        "text,purpose,level_code_uid "
        "FROM endpoint WHERE soa_id=? ORDER BY order_index, id",
        (soa_id,),
    )
    rows = cur.fetchall()
    conn.close()

    out: List[Dict[str, Any]] = []
    for r in rows:
        (
            endpoint_uid,
            _objective_uid,
            name,
            label,
            description,
            text,
            purpose,
            level_code_uid,
        ) = r
        out.append(
            {
                "id": endpoint_uid,
                "extensionAttributes": [],
                "name": name,
                "label": _nz(label),
                "description": _nz(description),
                "text": text or "",
                "dictionaryId": None,
                "notes": [],
                "purpose": purpose or "",
                "level": _build_level_code(soa_id, level_code_uid, "Endpoint"),
                "instanceType": "Endpoint",
            }
        )
    return out


if __name__ == "__main__":
    import argparse
    import json
    import logging
    import sys

    logger = logging.getLogger("usdm.generate_endpoints")

    parser = argparse.ArgumentParser(description="Export USDM endpoints for a SOA.")
    parser.add_argument("soa_id", type=int, help="SOA id to export endpoints for")
    parser.add_argument(
        "-o", "--output", default="-", help="Output file path or '-' for stdout"
    )
    parser.add_argument("--indent", type=int, default=2, help="JSON indent")
    args = parser.parse_args()

    try:
        endpoints = build_usdm_endpoints(args.soa_id)
    except Exception:
        logger.exception("Failed to build endpoints for soa_id=%s", args.soa_id)
        sys.exit(1)

    payload = json.dumps(endpoints, indent=args.indent)
    if args.output in ("-", "/dev/stdout"):
        sys.stdout.write(payload + "\n")
    else:
        with open(args.output, "w", encoding="utf-8") as f:
            f.write(payload + "\n")
