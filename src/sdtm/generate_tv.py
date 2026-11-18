"""Generate the SDTM Trial Visits (TV) domain from the SOA workbench DB."""

import re

from sdtm._common import first_text, sponsor_study_id
from soa_builder.web.db import _connect

# "Day -14", "day 1", "15", "-14". Unicode minus is accepted.
_STUDY_DAY_RE = re.compile(r"^(?:day\s*)?([+\-−]?\d+)$", re.IGNORECASE)


def _study_day_from_label(label: str | None) -> str:
    """Return the planned study day held in a Timing label, or ''.

    The mapping file sources VISITDY from Encounter/@scheduledAt/
    Timing/@label, so the label must hold the study day. A label that
    is not a study day (e.g. "Week 4") gives '' rather than a guess.
    """
    match = _STUDY_DAY_RE.match(first_text(label))
    if not match:
        return ""
    return str(int(match.group(1).replace("−", "-")))


def build_sdtm_tv(soa_id: int) -> list[dict]:
    """One record per planned visit, or per visit and arm (TV domain).

    Mapping follows the CDISC USDM v4.0 to SDTMIG v3.4 mapping file:
      STUDYID  = sponsor StudyIdentifier/@text (see sponsor_study_id)
      VISITNUM = Encounter order (visit.order_index, the next chain)
      VISIT    = Encounter/@label (falls back to @name)
      VISITDY  = Encounter/@scheduledAt/Timing/@label (study day)
      ARMCD    = StudyArm/@label (falls back to @name)
      ARM      = StudyArm/@description (falls back to @label, @name)
      TVSTRL   = Encounter/@transitionStartRule/TransitionRule/@text
      TVENRL   = Encounter/@transitionEndRule/TransitionRule/@text

    Row cardinality (SDTMIG v3.4; USDM IHB 1 section 4.3): ARMCD and
    ARM are only populated when visits differ between arms. Arms per
    visit come from instance -> epoch -> study_cell -> arm. When every
    linked visit has the same arms, one row per visit is emitted with
    ARMCD and ARM null; otherwise one row per (visit, arm).
    """
    conn = _connect()
    cur = conn.cursor()
    study_id = sponsor_study_id(cur, soa_id)

    # Rule UIDs repeat across studies, so the joins match on soa_id.
    cur.execute(
        """
        SELECT v.encounter_uid,
               v.name,
               v.label,
               v.order_index,
               t.label   AS timing_label,
               tr_s.text AS tvstrl_text,
               tr_e.text AS tvenrl_text
        FROM visit v
        LEFT JOIN timing t
               ON t.soa_id = v.soa_id
              AND v.scheduledAtId IS NOT NULL
              AND v.scheduledAtId != ''
              AND t.id = CAST(v.scheduledAtId AS INTEGER)
        LEFT JOIN transition_rule tr_s
               ON tr_s.soa_id = v.soa_id
              AND tr_s.transition_rule_uid = v.transitionStartRule
        LEFT JOIN transition_rule tr_e
               ON tr_e.soa_id = v.soa_id
              AND tr_e.transition_rule_uid = v.transitionEndRule
        WHERE v.soa_id = ?
        ORDER BY v.order_index, v.id
        """,
        (soa_id,),
    )
    visits = cur.fetchall()

    cur.execute(
        """
        SELECT DISTINCT inst.encounter_uid,
               a.arm_uid,
               a.name,
               a.label,
               a.description,
               a.order_index
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
    arm_map: dict[str, dict[str, tuple[str, str]]] = {}
    for enc_uid, arm_uid, name, label, desc, _order in cur.fetchall():
        arm_map.setdefault(enc_uid, {})[arm_uid] = (
            first_text(label, name),
            first_text(desc, label, name),
        )
    conn.close()

    arm_sets = {frozenset(arms) for arms in arm_map.values()}
    by_arm = len(arm_sets) > 1

    records = []
    for enc_uid, name, label, order_index, timing_label, strl, enrl in visits:
        base = {
            "STUDYID": study_id,
            "DOMAIN": "TV",
            "VISITNUM": order_index,
            "VISIT": first_text(label, name),
            "VISITDY": _study_day_from_label(timing_label),
            "ARMCD": "",
            "ARM": "",
            "TVSTRL": strl or "",
            "TVENRL": enrl or "",
        }
        arms = arm_map.get(enc_uid or "", {})
        if by_arm and arms:
            for armcd, arm in arms.values():
                records.append({**base, "ARMCD": armcd, "ARM": arm})
        else:
            records.append(base)
    records.sort(key=lambda r: (r["ARMCD"], r["VISITNUM"]))
    return records
