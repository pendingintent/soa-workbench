#!/usr/bin/env python3
# Prefer absolute import; fallback to adding src/ to sys.path when run directly
from typing import Optional, List, Dict, Any, Callable, Set
import logging

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


def _timing_uids_for_timeline(soa_id: int, schedule_timeline_uid: str) -> Set[str]:
    conn = _connect()
    cur = conn.cursor()
    cur.execute(
        """
        SELECT t.timing_uid FROM schedule_timelines s INNER JOIN timing t
        ON s.schedule_timeline_uid = t.member_of_timeline AND s.soa_id = t.soa_id 
        WHERE s.soa_id=? AND s.schedule_timeline_uid=? ORDER BY t.timing_uid
        """,
        (
            soa_id,
            schedule_timeline_uid,
        ),
    )
    rows = cur.fetchall()
    conn.close()
    return {r[0] for r in rows if r and r[0]}


def _load_generate_study_timings():
    """Return the timing builder from usdm.generate_study_timings (tries several names)."""
    try:
        import usdm.generate_study_timings as gst
    except Exception:
        import sys
        from pathlib import Path

        here = Path(__file__).resolve()
        src_dir = here.parents[2] / "src"
        if src_dir.exists() and str(src_dir) not in sys.path:
            sys.path.insert(0, str(src_dir))
        import usdm.generate_study_timings as gst
    for name in (
        "build_usdm_study_timings",
        "build_usdm_timings",
        "generate_study_timings",
    ):
        fn = getattr(gst, name, None)
        if callable(fn):
            return fn
    raise ImportError("usdm.generate_study_timings missing expected builder function")


def _nz(s: Optional[str]) -> Optional[str]:
    s = (s or "").strip()
    return s or None


generate_study_timings = _load_generate_study_timings()


def build_usdm_schedule_timelines(soa_id: int) -> List[Dict[str, Any]]:
    """
    Build USDM scheduleTimelines

    :param soa_id: soa identifier
    :type soa_id: int
    :return: USDM JSON for scheduleTimelines entity
    :rtype: List[Dict[str, Any]]


    Output:
        - id: string
        - entensionAttributes?: string[]
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

        scheduleTimeline = {
            "id": schedule_timeline_uid,
            "extensionAttributes": [],
            "name": name,
            "label": _nz(label),
            "description": _nz(description),
            "mainTimeline": bool(mainTimeline),
            "entryCondition": _nz(entryCondition),
            "entryId": _nz(entryId),
            "exits": [],
            "timings": generate_study_timings(soa_id, schedule_timeline_uid),
            "instances": [],
            "plannedDuration": None,
            "instanceType": "ScheduleTimeline",
        }
        out.append(scheduleTimeline)
    return out


if __name__ == "__main__":
    import argparse
    import json
    import logging
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
