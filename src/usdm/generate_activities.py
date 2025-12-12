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
      - biomedicalConceptIds: string[]         (left empty here)
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
    conn.close()

    # Build simple linear previous/next links by list order
    # ids = [f"Activity_{r[0]}" for r in rows]
    uids = [r[1] for r in rows]
    id_by_index = {i: uid for i, uid in enumerate(uids)}

    out: List[Dict[str, Any]] = []
    for i, r in enumerate(rows):
        id, activity_uid, name, label, description = r[0], r[1], r[2], r[3], r[4]
        aid = activity_uid
        prev_id = id_by_index.get(i - 1)
        next_id = id_by_index.get(i + 1)

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
            "biomedicalConceptIds": [],
            "bcCategoryIds": [],
            "bcSurrogateIds": [],
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
        sys.stdout.write(payload + "\n")
    else:
        with open(args.output, "w", encoding="utf-8") as f:
            f.write(payload + "\n")
