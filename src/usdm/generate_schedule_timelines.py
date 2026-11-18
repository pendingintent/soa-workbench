#!/usr/bin/env python3
from typing import List, Dict, Any
import logging
from soa_builder.web.db import _connect
from soa_builder.web.utils import _nz
from .usdm_utils import (
    _load_generate_study_timings,
    _load_generate_study_instances,
    _load_generate_decision_instances,
)


generate_study_timings = _load_generate_study_timings()
generate_study_instances = _load_generate_study_instances()
generate_decision_instances = _load_generate_decision_instances()


def _first_instance_uid(soa_id: int, timeline_uid: str):
    """Return the UID of the first instance on this timeline, or None."""
    conn = _connect()
    cur = conn.cursor()
    cur.execute(
        "SELECT instance_uid FROM instances"
        " WHERE soa_id=? AND member_of_timeline=?"
        " ORDER BY length(instance_uid), instance_uid LIMIT 1",
        (soa_id, timeline_uid),
    )
    row = cur.fetchone()
    conn.close()
    return row[0] if row else None


def _timeline_exits(
    exit_id: str | None, instances: List[Dict[str, Any]]
) -> List[Dict[str, Any]]:
    """Return the ScheduleTimelineExit objects for one timeline.

    An exit is declared by the timeline's own exit_id or by the
    timelineExitId of any of its instances. Each instance reference
    must resolve to an exit owned by the timeline (DDF00037,
    DDF00108), so both sources are collected, in that order, once
    each.
    """
    exit_ids: List[str] = []
    candidates = [exit_id] + [i.get("timelineExitId") for i in instances]
    for candidate in candidates:
        value = (candidate or "").strip()
        if value and value not in exit_ids:
            exit_ids.append(value)
    return [
        {
            "id": value,
            "extensionAttributes": [],
            "instanceType": "ScheduleTimelineExit",
        }
        for value in exit_ids
    ]


_DURATION_COLUMNS = (
    "planned_duration_text",
    "planned_duration_value",
    "planned_duration_unit_code",
    "planned_duration_unit_decode",
    "planned_duration_unit_version",
    "planned_duration_will_vary",
    "planned_duration_reason",
)


def _planned_durations(soa_id: int) -> Dict[str, Dict[str, Any]]:
    """Return {timeline uid: planned duration columns} for the SoA."""
    conn = _connect()
    try:
        rows = conn.execute(
            "SELECT schedule_timeline_uid, "
            + ", ".join(_DURATION_COLUMNS)
            + " FROM schedule_timelines WHERE soa_id=?",
            (soa_id,),
        ).fetchall()
    finally:
        conn.close()
    return {row[0]: dict(zip(_DURATION_COLUMNS, row[1:])) for row in rows}


def _planned_duration(timeline_uid: str, cols: Dict[str, Any]):
    """Build ScheduleTimeline.plannedDuration (USDM v4.0 Duration-Output).

    Duration requires id and durationWillVary; text, quantity and
    reasonDurationWillVary are optional. The quantity unit is an
    AliasCode wrapping the CDISC unit Code. Ids are derived from the
    timeline UID, so they are unique and stable across exports.
    """
    value = cols.get("planned_duration_value")
    text = cols.get("planned_duration_text")
    if value is None and not text:
        return None
    quantity = None
    if value is not None:
        unit = None
        if cols.get("planned_duration_unit_code"):
            unit = {
                "id": f"{timeline_uid}_PlannedDurationUnit",
                "extensionAttributes": [],
                "standardCode": {
                    "id": f"{timeline_uid}_PlannedDurationUnitCode",
                    "extensionAttributes": [],
                    "code": cols["planned_duration_unit_code"],
                    "codeSystem": "http://www.cdisc.org",
                    "codeSystemVersion": cols.get("planned_duration_unit_version")
                    or "",
                    "decode": cols.get("planned_duration_unit_decode") or "",
                    "instanceType": "Code",
                },
                "standardCodeAliases": [],
                "instanceType": "AliasCode",
            }
        quantity = {
            "id": f"{timeline_uid}_PlannedDurationQuantity",
            "extensionAttributes": [],
            "value": float(value),
            "unit": unit,
            "instanceType": "Quantity",
        }
    return {
        "id": f"{timeline_uid}_PlannedDuration",
        "extensionAttributes": [],
        "text": text or None,
        "quantity": quantity,
        "durationWillVary": bool(cols.get("planned_duration_will_vary")),
        "reasonDurationWillVary": cols.get("planned_duration_reason") or None,
        "instanceType": "Duration",
    }


def build_usdm_schedule_timelines(soa_id: int) -> List[Dict[str, Any]]:
    """
    Build USDM scheduleTimelines

    :param soa_id: soa identifier
    :type soa_id: int
    :return: USDM JSON for scheduleTimelines entity
    :rtype: List[Dict[str, Any]]


    Output:
        - id: string
        - extensionAttributes?: string[]
        - name: string
        - label?: string
        - description?: string
        - mainTimeline?: bool
        - entryCondition?: string
        - entryId?: string
        - exits?: dict[
            {
                - id: string
                - extensionAttributes?: string[]
                - instanceType: "ScheduleTimelineExit"
            },
        ]
        - timings?: [
            {
                - id: string
                - extensionAttributes?: string[]
                - name: string
                - label?: string
                - description?: string
                - type?: {
                    - id: string
                    - extensionAttributes: string[]
                    - code: string
                    - codeSystem: string
                    - codeSystemVersion: string
                    - decode: string
                    - instanceType: "Code"
                }
                - value?: string
                - valueLabel?: string
                - relativeToFrom?: {
                    - id: string
                    - extensionAttributes: string[]
                    - code: string
                    - codeSystem: string
                    - codeSystemVersion: string
                    - decode: string
                    - instanceType: "Code"
                }
                - relativeFromScheduledInstanceId?: string
                - relativeToScheduledInstanceId?: string
                - windowLower?: string
                - windowUpper?: string
                - windowLabel?: string
                - instanceType: "Timing"
            },
        ]
        - instances?: [
            {
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
            }
        ]
        - plannedDuration?: string
        - instanceType: "ScheduleTimeline"
    """
    conn = _connect()
    cur = conn.cursor()
    cur.execute(
        """
        SELECT schedule_timeline_uid,name,label,description,main_timeline,entry_condition,
        entry_id,exit_id FROM schedule_timelines WHERE soa_id=? ORDER BY length(schedule_timeline_uid),
        schedule_timeline_uid
        """,
        (soa_id,),
    )
    rows = cur.fetchall()
    conn.close()
    durations = _planned_durations(soa_id)
    out: List[Dict[str, Any]] = []

    for i, r in enumerate(rows):
        (
            schedule_timeline_uid,
            name,
            label,
            description,
            mainTimeline,
            entryCondition,
            entryId,
            exitId,
        ) = (
            r[0],
            r[1],
            r[2],
            r[3],
            r[4],
            r[5],
            r[6],
            r[7],
        )

        instances = generate_study_instances(
            soa_id, schedule_timeline_uid
        ) + generate_decision_instances(soa_id, schedule_timeline_uid)
        scheduleTimeline = {
            "id": schedule_timeline_uid,
            "extensionAttributes": [],
            "name": name,
            "label": _nz(label),
            "description": _nz(description),
            "mainTimeline": bool(mainTimeline),
            "entryCondition": _nz(entryCondition),
            "entryId": entryId
            or _first_instance_uid(soa_id, schedule_timeline_uid)
            or "",
            "exits": _timeline_exits(exitId, instances),
            "timings": generate_study_timings(soa_id, schedule_timeline_uid),
            "instances": instances,
            "plannedDuration": _planned_duration(
                schedule_timeline_uid, durations.get(schedule_timeline_uid, {})
            ),
            "instanceType": "ScheduleTimeline",
        }
        out.append(scheduleTimeline)
    return out


if __name__ == "__main__":
    import argparse
    import json
    import sys

    logger = logging.getLogger("usdm.generate_schedule_timelines")

    parser = argparse.ArgumentParser(
        description="Export USDM Schedule Timelines for an SOA."
    )
    parser.add_argument(
        "soa_id", type=int, help="SOA id to export Schedule Timelines for"
    )
    parser.add_argument(
        "-o", "--output", default="-", help="Output file path or '-' for stdout"
    )
    parser.add_argument("--indent", type=int, default=2, help="JSON indent")
    args = parser.parse_args()

    try:
        activities = build_usdm_schedule_timelines(args.soa_id)
    except Exception:
        logger.exception(
            "Failed to build Schedule Timelines for soa_id=%s", args.soa_id
        )
        sys.exit(1)

    payload = json.dumps(activities, indent=args.indent)
    if args.output in ("-", "/dev/stdout"):
        sys.stdout.write(payload + "\n")
    else:
        with open(args.output, "w", encoding="utf-8") as f:
            f.write(payload + "\n")
