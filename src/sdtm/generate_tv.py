"""Generate the SDTM Trial Visits (TV) domain from the SOA workbench DB."""

import re

from soa_builder.web.db import _connect


def _iso_duration_to_days(value: str) -> str:
    """Convert an ISO 8601 duration string to an integer number of days.

    Handles the patterns used in clinical trial timing values:
      P{n}D        → n days
      P{n}W        → n * 7 days
      P{n}Y{n}M{n}D → years * 365 + months * 30 + days (approximate)
      -P...        → negative day count
      PT{n}H / time-only → "" (cannot express as integer days)

    Returns the day count as a string, or "" if the value is absent or
    cannot be converted to an integer number of days.
    """
    if not value:
        return ""
    s = value.strip()
    negative = s.startswith("-")
    if negative:
        s = s[1:]
    if not s.startswith("P"):
        return ""
    s = s[1:]  # strip leading 'P'

    # Weeks-only shorthand: {n}W
    m = re.fullmatch(r"(\d+(?:\.\d+)?)W", s)
    if m:
        days = round(float(m.group(1)) * 7)
        return str(-days if negative else days)

    # General form: split date/time at 'T'
    date_part = s.partition("T")[0]

    y = re.search(r"(\d+(?:\.\d+)?)Y", date_part)
    mo = re.search(r"(\d+(?:\.\d+)?)M", date_part)
    d = re.search(r"(\d+(?:\.\d+)?)D", date_part)

    if not y and not mo and not d:
        return ""  # time-only duration (e.g. PT8H) — not a day count

    days = 0
    if y:
        days += round(float(y.group(1)) * 365)
    if mo:
        days += round(float(mo.group(1)) * 30)
    if d:
        days += round(float(d.group(1)))

    return str(-days if negative else days)


def build_sdtm_tv(soa_id: int) -> list[dict]:
    """One record per planned (visit, arm) combination (TV domain).

    Mapping follows SDTM IG v3.4 Section 7 / docs/Create TDD.docx:
      VISITNUM = Encounter ordering (visit.order_index)
      VISIT    = Encounter/@name  (visit.name)
      VISITDY  = Encounter/@timing/Timing/@timingValue
                 (timing.value via visit.scheduledAtId → timing.id)
      ARMCD    = StudyArm/@name via ScheduledActivityInstance → StudyCell → arm
                 (one row per arm when encounter is linked; blank otherwise)
      ARM      = StudyArm/@description via same path
      TVSTRL   = Encounter/@transitionStartRule/TransitionRule/@text
      TVENRL   = Encounter/@transitionEndRule/TransitionRule/@text

    Row cardinality:
      - If a visit's encounter_uid appears in instances that link to arm(s)
        via epoch→study_cell, one TV row is emitted per (visit, arm).
      - If there is no instance linkage, one TV row is emitted with ARMCD/ARM blank.
    """
    conn = _connect()
    cur = conn.cursor()

    cur.execute("SELECT study_id, name FROM soa WHERE id=?", (soa_id,))
    row = cur.fetchone()
    study_id = (row[0] or row[1]) if row else ""

    # Query 1: all visits with timing and transition rule text
    cur.execute(
        """
        SELECT v.encounter_uid,
               v.name,
               v.order_index,
               t.value        AS timing_value,
               tr_s.text      AS tvstrl_text,
               tr_e.text      AS tvenrl_text
        FROM visit v
        LEFT JOIN timing t
               ON t.soa_id = v.soa_id
              AND v.scheduledAtId IS NOT NULL
              AND v.scheduledAtId != ''
              AND t.id = CAST(v.scheduledAtId AS INTEGER)
        LEFT JOIN transition_rule tr_s
               ON tr_s.transition_rule_uid = v.transitionStartRule
        LEFT JOIN transition_rule tr_e
               ON tr_e.transition_rule_uid = v.transitionEndRule
        WHERE v.soa_id = ?
        ORDER BY v.order_index
        """,
        (soa_id,),
    )
    visits = cur.fetchall()

    # Query 2: arm linkage per encounter via instances → epoch → study_cell → arm
    cur.execute(
        """
        SELECT DISTINCT inst.encounter_uid,
               a.name        AS arm_name,
               a.description AS arm_desc,
               a.label       AS arm_label,
               a.order_index AS arm_ord
        FROM instances inst
        JOIN study_cell sc ON sc.soa_id = inst.soa_id
                          AND sc.epoch_uid = inst.epoch_uid
        JOIN arm a ON a.soa_id = sc.soa_id
                  AND a.arm_uid = sc.arm_uid
        WHERE inst.soa_id = ?
          AND inst.encounter_uid IS NOT NULL
          AND inst.encounter_uid != ''
        ORDER BY inst.encounter_uid, a.order_index
        """,
        (soa_id,),
    )
    arm_map: dict[str, list[tuple[str, str, str]]] = {}
    for enc_uid, arm_name, arm_desc, arm_label, _ in cur.fetchall():
        arm_map.setdefault(enc_uid, []).append((arm_name, arm_desc, arm_label))

    conn.close()

    records = []
    for enc_uid, visit_name, order_index, timing_val, tvstrl, tvenrl in visits:
        arms = arm_map.get(enc_uid or "", [])
        if arms:
            for arm_name, arm_desc, arm_label in arms:
                records.append(
                    {
                        "STUDYID": study_id,
                        "DOMAIN": "TV",
                        "VISITNUM": order_index,
                        "VISIT": visit_name or "",
                        "VISITDY": _iso_duration_to_days(timing_val or ""),
                        "ARMCD": (arm_name or "")[:20],
                        "ARM": arm_desc or arm_label or arm_name or "",
                        "TVSTRL": tvstrl or "",
                        "TVENRL": tvenrl or "",
                    }
                )
        else:
            records.append(
                {
                    "STUDYID": study_id,
                    "DOMAIN": "TV",
                    "VISITNUM": order_index,
                    "VISIT": visit_name or "",
                    "VISITDY": timing_val or "",
                    "ARMCD": "",
                    "ARM": "",
                    "TVSTRL": tvstrl or "",
                    "TVENRL": tvenrl or "",
                }
            )
    records.sort(key=lambda r: (r["ARMCD"], r["VISITNUM"]))
    return records
