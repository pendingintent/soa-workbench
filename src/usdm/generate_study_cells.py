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


def _get_element_ids(soa_id: int, study_cell_uid: str) -> List[str]:
    conn = _connect()
    cur = conn.cursor()
    cur.execute(
        "SELECT element_uid from study_cell WHERE soa_id=? AND study_cell_uid=? ORDER BY element_uid",
        (
            soa_id,
            study_cell_uid,
        ),
    )
    rows = cur.fetchall()
    conn.close()
    # Deduplicate and preserve stable order
    element_uids = [r[0] for r in rows] or []
    seen = set()
    ordered_unique: List[str] = []
    for uid in element_uids:
        if uid and uid not in seen:
            seen.add(uid)
            ordered_unique.append(uid)
    return ordered_unique


def build_usdm_study_cells(soa_id: int) -> List[Dict[str, Any]]:
    """
    Build USDM StudyCells-Output objects for the given SOA

    USDM StudyCells-Output:
        - id: string
        - extensionAttributes: string[]
        - armdId: string
        - epochId: string
        - elementIds: string[]
        - instanceType: "StudyCell"
    """

    conn = _connect()
    cur = conn.cursor()
    # Select distinct StudyCell groups by (study_cell_uid, arm_uid, epoch_uid)
    cur.execute(
        "SELECT DISTINCT study_cell_uid, arm_uid, epoch_uid FROM study_cell WHERE soa_id=? ORDER BY id,study_cell_uid",
        (soa_id,),
    )
    rows = cur.fetchall()
    conn.close()

    out: List[Dict[str, Any]] = []
    for i, r in enumerate(rows):
        study_cell_uid, arm_uid, epoch_uid = r[0], r[1], r[2]
        scid = study_cell_uid
        armId = arm_uid
        epochId = epoch_uid
        elementIds = _get_element_ids(soa_id, scid)

        study_cells = {
            "id": scid,
            "extensionAttributes": [],
            "armId": armId,
            "epochId": epochId,
            "elementIds": elementIds,
            "instanceType": "StudyCell",
        }
        out.append(study_cells)

    return out


if __name__ == "__main__":
    import argparse
    import json
    import logging
    import sys

    logger = logging.getLogger("usdm.generate_study_cells")

    parser = argparse.ArgumentParser(description="Export USDM StudyCells for an SOA.")
    parser.add_argument("soa_id", type=int, help="SOA id to export activities for")
    parser.add_argument(
        "-o", "--output", default="-", help="Output file path or '-' for stdout"
    )
    parser.add_argument("--indent", type=int, default=2, help="JSON indent")
    args = parser.parse_args()

    try:
        study_cells = build_usdm_study_cells(args.soa_id)
    except Exception:
        logger.exception("Failed to build StudyCells for soa_id=%s", args.soa_id)
        sys.exit(1)

    payload = json.dumps(study_cells, indent=args.indent)
    if args.output in ("-", "/dev/stdout"):
        sys.stdout.write(payload + "\n")
    else:
        with open(args.output, "w", encoding="utf-8") as f:
            f.write(payload + "\n")
