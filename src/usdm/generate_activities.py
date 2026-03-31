#!/usr/bin/env python3
from typing import List, Dict, Any
from soa_builder.web.db import _connect
from soa_builder.web.utils import _nz


def build_usdm_activities(soa_id: int) -> List[Dict[str, Any]]:
    """
    Build USDM Activity-Output objects for the given SOA.

    USDM Activity-Output (subset):
      - id: string
      - name: string
      - label?: string | null
      - description?: string | null
      - previousId?: string | null
      - nextId?: string | null
      - childIds: string[]
      - definedProcedures: Procedure-Output[]   (left empty here)
      - biomedicalConceptIds: string[]
      - bcCategoryIds: string[]                (left empty here)
      - bcSurrogateIds: string[]               (left empty here)
      - timelineId?: string | null             (left null here)
      - notes: CommentAnnotation-Output[]      (left empty here)
      - extensionAttributes: ExtensionAttribute-Output[] (empty)
      - instanceType: "Activity"
    """
    conn = _connect()
    cur = conn.cursor()
    # Order by order_index if present, else by id for deterministic output
    cur.execute("PRAGMA table_info(activity)")
    cols = {r[1] for r in cur.fetchall()}
    if "order_index" in cols:
        cur.execute(
            "SELECT id, activity_uid, name, label, description FROM activity WHERE soa_id=? ORDER BY order_index, id",
            (soa_id,),
        )
    else:
        cur.execute(
            "SELECT id, activity_uid, name, label, description FROM activity WHERE soa_id=? ORDER BY id",
            (soa_id,),
        )
    rows = cur.fetchall()

    # Pre-fetch all concept mappings for this SOA in one query to avoid N+1
    cur.execute(
        "SELECT activity_uid, concept_uid FROM activity_concept WHERE soa_id=?",
        (soa_id,),
    )
    bc_map: dict[str, list[str]] = {}
    for act_uid, concept_uid in cur.fetchall():
        bc_map.setdefault(act_uid, []).append(concept_uid)

    # Pre-fetch surrogate mappings
    cur.execute(
        "SELECT activity_uid, surrogate_uid FROM activity_surrogate WHERE soa_id=?",
        (soa_id,),
    )
    surrogate_map: dict[str, list[str]] = {}
    for act_uid, sur_uid in cur.fetchall():
        surrogate_map.setdefault(act_uid, []).append(sur_uid)
    conn.close()

    # Build simple linear previous/next links by list order
    uids = [r[1] for r in rows]
    id_by_index = {i: uid for i, uid in enumerate(uids)}

    out: List[Dict[str, Any]] = []
    for i, r in enumerate(rows):
        _, activity_uid, name, label, description = r[0], r[1], r[2], r[3], r[4]
        aid = activity_uid
        prev_id = id_by_index.get(i - 1)
        next_id = id_by_index.get(i + 1)
        bcs = bc_map.get(aid, [])

        activity = {
            "id": aid,
            "extensionAttributes": [],
            "name": name,
            "label": _nz(label),
            "description": _nz(description),
            "previousId": prev_id,
            "nextId": next_id,
            "childIds": [],
            "definedProcedures": [],
            "biomedicalConceptIds": bcs,
            "bcCategoryIds": [],
            "bcSurrogateIds": surrogate_map.get(aid, []),
            "timelineId": None,
            "notes": [],
            "instanceType": "Activity",
        }
        out.append(activity)

    return out


if __name__ == "__main__":
    import argparse
    import json
    import logging
    import sys

    logger = logging.getLogger("usdm.generate_activities")

    parser = argparse.ArgumentParser(description="Export USDM activities for a SOA.")
    parser.add_argument("soa_id", type=int, help="SOA id to export activities for")
    parser.add_argument(
        "-o", "--output", default="-", help="Output file path or '-' for stdout"
    )
    parser.add_argument("--indent", type=int, default=2, help="JSON indent")
    args = parser.parse_args()

    try:
        activities = build_usdm_activities(args.soa_id)
    except Exception:
        logger.exception("Failed to build activities for soa_id=%s", args.soa_id)
        sys.exit(1)

    payload = json.dumps(activities, indent=args.indent)
    if args.output in ("-", "/dev/stdout"):
        sys.stdout.write(
            "Output suppressed: this document may contain sensitive data. "
            "Use an explicit -o <file> path to export.\n"
        )
    else:
        with open(args.output, "w", encoding="utf-8") as f:
            f.write(payload + "\n")
