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


def _get_activity_ids(soa_id: int, encounter_uid: str) -> List[str]:
    conn = _connect()
    cur = conn.cursor()
    cur.execute(
        "SELECT a.activity_uid from activity a "
        "INNER JOIN matrix_cells m ON a.id = m.activity_id AND a.soa_id = m.soa_id "
        "INNER JOIN visit v ON m.visit_id = v.id AND m.soa_id = v.soa_id "
        "INNER JOIN instances i ON v.encounter_uid = i.encounter_uid AND v.soa_id = i.soa_id "
        "WHERE i.soa_id=? and i.encounter_uid=?",
        (
            soa_id,
            encounter_uid,
        ),
    )
    rows = cur.fetchall()
    conn.close()
    activity_uids = [r[0] for r in rows] or []
    return activity_uids


def build_usdm_instances(soa_id: int) -> List[Dict[str, Any]]:
    """
    Build USDM instances objects for the given SOA

    USDM instances:
        - id: string
        - extensionAttributes: string[]|[]
        - name: string
        - label?: string
        - description?: string
        - defaultConditionId?: string
        - epochId?: string
        - instanceType: "ScheduledActivityInstance"
        - timelineId?: string
        - timelineExitId?: string
        - activityIds?: string[]
        - encounterId?: string
    """
    conn = _connect()
    cur = conn.cursor()
    cur.execute(
        "SELECT id,instance_uid,name,label,description,default_condition_uid,epoch_uid,"
        "timeline_id,timeline_exit_id,encounter_uid FROM instances where soa_id=? ORDER BY instance_uid",
        (soa_id,),
    )
    rows = cur.fetchall()
    conn.close()
    out: List[Dict[str, Any]] = []

    for i, r in enumerate(rows):
        (
            id,
            instance_uid,
            name,
            label,
            description,
            default_condition_uid,
            epoch_uid,
            timeline_id,
            timeline_exit_id,
            encounter_uid,
        ) = (
            r[0],
            r[1],
            r[2],
            r[3],
            r[4],
            r[5],
            r[6],
            r[7],
            r[8],
            r[9],
        )

        instances = {
            "id": instance_uid,
            "extensionAttributes": [],
            "name": name,
            "label": _nz(label),
            "description": _nz(description),
            "defaultConditionId": _nz(default_condition_uid),
            "epochId": _nz(epoch_uid),
            "instanceType": "ScheduledActivityInstance",
            "timelineId": _nz(timeline_id),
            "timelineExitId": _nz(timeline_exit_id),
            "activityIds": _get_activity_ids(soa_id, encounter_uid),
            "encounterId": _nz(encounter_uid),
        }
        out.append(instances)

    return out


if __name__ == "__main__":
    import argparse
    import json
    import logging
    import sys

    logger = logging.getLogger("usdm.generate_instances")

    parser = argparse.ArgumentParser(
        description="Export USDM Scheduled Activity Instances for a SOA."
    )
    parser.add_argument(
        "soa_id", type=int, help="SOA id to export Scheduled Activity Instances for"
    )
    parser.add_argument(
        "-o", "--output", default="-", help="Output file path or '-' for stdout"
    )
    parser.add_argument("--indent", type=int, default=2, help="JSON indent")
    args = parser.parse_args()

    try:
        activities = build_usdm_instances(args.soa_id)
    except Exception:
        logger.exception(
            "Failed to build Scheduled Activity Instances for soa_id=%s", args.soa_id
        )
        sys.exit(1)

    payload = json.dumps(activities, indent=args.indent)
    if args.output in ("-", "/dev/stdout"):
        sys.stdout.write(payload + "\n")
    else:
        with open(args.output, "w", encoding="utf-8") as f:
            f.write(payload + "\n")
