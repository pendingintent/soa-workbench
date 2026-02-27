#!/usr/bin/env python3
# Prefer absolute import; fallback to adding src/ to sys.path when run directly
from typing import Optional, List, Dict, Any, Tuple
from urllib.parse import urlparse


import os
import requests

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


def _get_epoch_code_values(soa_id: int, epoch_type: str, code: str) -> Tuple[
    str,
    str,
    str,
]:
    logger = logging.getLogger("usdm.generate_epochs")
    url = "https://library.cdisc.org/api/mdr/ct/packages/sdtmct-2025-09-26/codelists/C99079"
    headers: dict[str, str] = {"Accept": "application/json"}
    subscription_key = os.environ.get("CDISC_SUBSCRIPTION_KEY")
    api_key = os.environ.get("CDISC_API_KEY") or os.environ.get(
        "CDISC_SUBSCRIPTION_KEY"
    )
    unified_key = subscription_key or api_key
    if unified_key:
        headers["Ocp-Apim-Subscription-Key"] = unified_key
    if api_key:
        headers["Authorization"] = f"Bearer {api_key}"
        headers["api-key"] = api_key

    resp = requests.get(url, headers=headers, timeout=10)
    if resp.status_code != 200:
        logger.exception("No response from {} for code {}".format(url, epoch_type))
    else:
        content = resp.json()
        parsed_url = urlparse(url)
        code_system = parsed_url.scheme + "://" + parsed_url.netloc
        code_system_version = parsed_url.path.split("/", 7)[5]

        top_terms = content.get("terms")
        for term in top_terms:
            if term.get("conceptId") == code:
                decode = term.get("submissionValue")

    return code_system, code_system_version, decode


def build_usdm_epochs(soa_id: int) -> List[Dict[str, Any]]:
    """
    Build USDM Epoch-Output objects for the given SOA.

    USDM Epoch-Output (subset):
      - id: string
      - extensionAttributes?: string[]
      - name: string
      - label?: string | null
      - description?: string | null
      - type: {
            - id: string
            - extensionAttributes?: string[]
            - code: string
            - codeSystem: string
            - codeSystemVersion: string
            - decode: string
            - instanceType: "Code"
        }
      - previousId?: string | null
      - nextId?: string | null
      - notes?: string[]
      - instanceType: "StudyEpoch"
    """
    conn = _connect()
    cur = conn.cursor()
    # Order by order_index if present, else by id for deterministic output
    cur.execute("PRAGMA table_info(epoch)")
    cols = {r[1] for r in cur.fetchall()}
    if "order_index" in cols:
        cur.execute(
            "SELECT e.id, e.epoch_uid, e.name, e.epoch_label, e.epoch_description, e.type, c.code "
            "FROM epoch e INNER JOIN code_association c ON e.soa_id = c.soa_id AND e.type = c.code_uid "
            "WHERE e.soa_id=? ORDER BY e.order_index, e.id",
            (soa_id,),
        )
    else:
        cur.execute(
            "SELECT e.id, e.epoch_uid, e.name, e.epoch_label, e.epoch_description, e.type, c.code "
            "FROM epoch e INNER JOIN code_association c ON e.soa_id = c.soa_id AND e.type = c.code_uid "
            "WHERE e.soa_id=? ORDER BY e.id",
            (soa_id,),
        )
    rows = cur.fetchall()
    conn.close()

    uids = [r[1] for r in rows]
    id_by_index = {i: uid for i, uid in enumerate(uids)}

    out: List[Dict[str, Any]] = []

    for i, r in enumerate(rows):
        id, epoch_uid, name, label, description, epoch_type, code = (
            r[0],
            r[1],
            r[2],
            r[3],
            r[4],
            r[5],
            r[6],
        )
        eid = epoch_uid
        prev_id = id_by_index.get(i - 1)
        next_id = id_by_index.get(i + 1)

        try:
            code_system, code_system_version, decode = _get_epoch_code_values(
                soa_id, epoch_type, code
            )
        except Exception:
            code_system = None
            code_system_version = None
            decode = None

        epoch = {
            "id": eid,
            "extensionAttributes": [],
            "name": name,
            "label": _nz(label),
            "description": _nz(description),
            "type": {
                "id": epoch_type,
                "extensionAttributes": [],
                "code": code,
                "codeSystem": code_system,
                "codeSystemVersion": code_system_version,
                "decode": decode,
                "instanceType": "Code",
            },
            "previousId": prev_id,
            "nextId": next_id,
            "notes": [],
            "instanceType": "StudyEpoch",
        }
        out.append(epoch)
    return out


if __name__ == "__main__":
    import argparse
    import json
    import logging
    import sys

    logger = logging.getLogger("usdm.generate_epochs")

    parser = argparse.ArgumentParser(description="Export USDM epochs for a SOA.")
    parser.add_argument("soa_id", type=int, help="SOA id to export epochs for")
    parser.add_argument(
        "-o", "--output", default="-", help="Output file path or '-' for stdout"
    )
    parser.add_argument("--indent", type=int, default=2, help="JSON indent")
    args = parser.parse_args()

    try:
        activities = build_usdm_epochs(args.soa_id)
    except Exception:
        logger.exception("Failed to build epochs for soa_id=%s", args.soa_id)
        sys.exit(1)

    payload = json.dumps(activities, indent=args.indent)
    if args.output in ("-", "/dev/stdout"):
        sys.stdout.write(payload + "\n")
    else:
        with open(args.output, "w", encoding="utf-8") as f:
            f.write(payload + "\n")
