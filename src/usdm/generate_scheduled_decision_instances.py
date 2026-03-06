#!/usr/bin/env python3
# Prefer absolute import; fallback to adding src/ to sys.path when run directly
from typing import Optional, List, Dict, Any
from soa_builder.web.db import _connect
from soa_builder.web.utils import _nz
from .usdm_utils import _get_condition_assignments


def build_usdm_decision_instances(
    soa_id: int, member_of_timeline: Optional[str] = None
) -> List[Dict[str, Any]]:
    """
    Build USDM ScheduledDecisionInstance objects for the given SOA.

    USDM output per instance:
        - id: string  (ScheduledDecisionInstance_N)
        - extensionAttributes: []
        - name: string
        - label?: string
        - description?: string
        - defaultConditionId?: string
        - epochId?: string
        - instanceType: "ScheduledDecisionInstance"
        - conditionAssignments: ConditionAssignment[]
    """
    conn = _connect()
    cur = conn.cursor()
    if member_of_timeline and member_of_timeline.strip():
        cur.execute(
            "SELECT id, instance_uid, name, label, description, "
            "default_condition_uid, epoch_uid "
            "FROM decision_instances "
            "WHERE soa_id=? AND member_of_timeline=? "
            "ORDER BY length(instance_uid), instance_uid",
            (soa_id, member_of_timeline.strip()),
        )
    else:
        cur.execute(
            "SELECT id, instance_uid, name, label, description, "
            "default_condition_uid, epoch_uid "
            "FROM decision_instances "
            "WHERE soa_id=? "
            "ORDER BY length(instance_uid), instance_uid",
            (soa_id,),
        )
    rows = cur.fetchall()
    conn.close()

    out: List[Dict[str, Any]] = []
    for r in rows:
        (
            _id,
            instance_uid,
            name,
            label,
            description,
            default_condition_uid,
            epoch_uid,
        ) = r

        out.append(
            {
                "id": instance_uid,
                "extensionAttributes": [],
                "name": name,
                "label": _nz(label),
                "description": _nz(description),
                "defaultConditionId": _nz(default_condition_uid),
                "epochId": _nz(epoch_uid),
                "instanceType": "ScheduledDecisionInstance",
                "conditionAssignments": _get_condition_assignments(
                    soa_id, instance_uid
                ),
            }
        )

    return out


if __name__ == "__main__":
    import argparse
    import json
    import logging
    import sys

    logger = logging.getLogger("usdm.generate_scheduled_decision_instances")

    parser = argparse.ArgumentParser(
        description="Export USDM Scheduled Decision Instances for a SOA."
    )
    parser.add_argument("soa_id", type=int, help="SOA id to export")
    parser.add_argument(
        "-o", "--output", default="-", help="Output file path or '-' for stdout"
    )
    parser.add_argument("--indent", type=int, default=2, help="JSON indent")
    parser.add_argument(
        "--member-of-timeline",
        dest="member_of_timeline",
        default=None,
        help="Optional schedule_timeline_uid to filter instances",
    )
    args = parser.parse_args()

    try:
        result = build_usdm_decision_instances(args.soa_id, args.member_of_timeline)
    except Exception:
        logger.exception(
            "Failed to build Scheduled Decision Instances for soa_id=%s", args.soa_id
        )
        sys.exit(1)

    payload = json.dumps(result, indent=args.indent)
    if args.output in ("-", "/dev/stdout"):
        sys.stdout.write(payload + "\n")
    else:
        with open(args.output, "w", encoding="utf-8") as f:
            f.write(payload + "\n")
