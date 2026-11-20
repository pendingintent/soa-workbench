"""Generate the SDTM Trial Arms (TA) domain from the SOA workbench DB."""

import sqlite3

from sdtm._common import first_text, main_timeline_uid, sponsor_study_id
from soa_builder.web.db import _connect


def _epoch_transitions(
    cur: sqlite3.Cursor, soa_id: int
) -> dict[str, list[tuple[str, str]]]:
    """Map each epoch to its (condition, target epoch uid) pairs.

    Only decision instances on the main timeline count (USDM IHB 1,
    section 4.3, TATRANS). The target is the epoch of the instance the
    condition points to, whether a scheduled activity or decision
    instance.
    """
    timeline_uid = main_timeline_uid(cur, soa_id)
    if not timeline_uid:
        return {}
    cur.execute(
        """
        SELECT di.epoch_uid,
               ca.condition,
               COALESCE(sai.epoch_uid, tdi.epoch_uid) AS target_epoch_uid
        FROM decision_instances di
        JOIN condition_assignment ca
          ON ca.soa_id = di.soa_id
         AND ca.decision_instance_uid = di.instance_uid
        LEFT JOIN instances sai
          ON sai.soa_id = ca.soa_id
         AND sai.instance_uid = ca.condition_target_uid
        LEFT JOIN decision_instances tdi
          ON tdi.soa_id = ca.soa_id
         AND tdi.instance_uid = ca.condition_target_uid
        WHERE di.soa_id = ? AND di.member_of_timeline = ?
        ORDER BY di.order_index, di.id, ca.order_index, ca.id
        """,
        (soa_id, timeline_uid),
    )
    transitions: dict[str, list[tuple[str, str]]] = {}
    for epoch_uid, condition, target_epoch_uid in cur.fetchall():
        if epoch_uid and target_epoch_uid and first_text(condition):
            transitions.setdefault(epoch_uid, []).append(
                (first_text(condition), target_epoch_uid)
            )
    return transitions


def _sub_timeline_epochs(cur: sqlite3.Cursor, soa_id: int) -> set[str]:
    """Return epochs reached only from timelines other than the main one.

    TA describes the planned path through each arm. An epoch whose
    instances all sit on a sub-timeline (e.g. a premature
    discontinuation schedule) is off that path, even though USDM
    still needs a StudyCell for it in every arm (DDF00243). Epochs
    with no instances at all are kept, since nothing says they are
    off the path.
    """
    timeline_uid = main_timeline_uid(cur, soa_id)
    if not timeline_uid:
        return set()
    cur.execute(
        """
        SELECT epoch_uid, member_of_timeline FROM instances
        WHERE soa_id = ? AND epoch_uid IS NOT NULL AND epoch_uid != ''
        UNION ALL
        SELECT epoch_uid, member_of_timeline FROM decision_instances
        WHERE soa_id = ? AND epoch_uid IS NOT NULL AND epoch_uid != ''
        """,
        (soa_id, soa_id),
    )
    with_instances: set[str] = set()
    on_main: set[str] = set()
    for epoch_uid, timeline in cur.fetchall():
        with_instances.add(epoch_uid)
        if timeline == timeline_uid:
            on_main.add(epoch_uid)
    return with_instances - on_main


def _tatrans(
    epoch_uid: str,
    arm_epochs: list[str],
    transitions: dict[str, list[tuple[str, str]]],
    epoch_labels: dict[str, str],
) -> str:
    """Return the TATRANS text for one arm's epoch, or ''.

    SDTMIG v3.4 uses TATRANS only for a transition to an element other
    than the next one in the arm, so a condition is kept only when its
    target is another epoch of this arm and not the next epoch. The
    text follows IHB 1: "<condition>: transition to <epoch>".
    """
    position = arm_epochs.index(epoch_uid)
    next_epoch = arm_epochs[position + 1] if position + 1 < len(arm_epochs) else None
    parts = [
        f"{condition}: transition to {epoch_labels[target]}"
        for condition, target in transitions.get(epoch_uid, [])
        if target != epoch_uid and target != next_epoch and target in arm_epochs
    ]
    return "; ".join(parts)


def build_sdtm_ta(soa_id: int) -> list[dict]:
    """One record per planned element per arm (TA domain).

    Mapping follows the CDISC USDM v4.0 to SDTMIG v3.4 mapping file:
      STUDYID  = sponsor StudyIdentifier/@text (see sponsor_study_id)
      ARMCD    = StudyArm/@label (falls back to @name)
      ARM      = StudyArm/@description (falls back to @label, @name)
      TAETORD  = order within arm, following the StudyEpoch next chain
                 (epoch.order_index) then the cell order
      ETCD     = StudyElement/@label (falls back to @name)
      ELEMENT  = StudyElement/@description (falls back to @label, @name)
      TABRANCH = blank (no USDM v4.0 mapping)
      TATRANS  = ConditionAssignment/@condition of a main-timeline
                 ScheduledDecisionInstance in the epoch (see _tatrans)
      EPOCH    = StudyEpoch/@label (falls back to @name)

    Cells in an epoch reached only from a sub-timeline are left out
    (see _sub_timeline_epochs); their elements stay in TE.
    """
    conn = _connect()
    cur = conn.cursor()
    study_id = sponsor_study_id(cur, soa_id)
    transitions = _epoch_transitions(cur, soa_id)
    off_path = _sub_timeline_epochs(cur, soa_id)

    cur.execute(
        "SELECT epoch_uid, epoch_label, name FROM epoch WHERE soa_id=?",
        (soa_id,),
    )
    epoch_labels = {uid: first_text(label, name) for uid, label, name in cur.fetchall()}

    cur.execute(
        """
        SELECT sc.arm_uid,
               a.name, a.label, a.description,
               sc.epoch_uid,
               el.name, el.label, el.description
        FROM study_cell sc
        JOIN arm     a  ON a.arm_uid    = sc.arm_uid    AND a.soa_id  = sc.soa_id
        JOIN epoch   e  ON e.epoch_uid  = sc.epoch_uid  AND e.soa_id  = sc.soa_id
        JOIN element el ON el.element_id = sc.element_uid AND el.soa_id = sc.soa_id
        WHERE sc.soa_id = ?
        ORDER BY a.order_index, e.order_index, sc.order_index
        """,
        (soa_id,),
    )
    rows = [row for row in cur.fetchall() if row[4] not in off_path]
    conn.close()

    arm_epochs: dict[str, list[str]] = {}
    for row in rows:
        arm_uid, epoch_uid = row[0], row[4]
        epochs = arm_epochs.setdefault(arm_uid, [])
        if epoch_uid not in epochs:
            epochs.append(epoch_uid)

    records = []
    arm_seq: dict[str, int] = {}
    for index, row in enumerate(rows):
        (
            arm_uid,
            arm_name,
            arm_label,
            arm_desc,
            epoch_uid,
            el_name,
            el_label,
            el_desc,
        ) = row
        arm_seq[arm_uid] = arm_seq.get(arm_uid, 0) + 1
        # A transition leaves the epoch after its last element.
        following = rows[index + 1] if index + 1 < len(rows) else None
        last_in_epoch = following is None or (
            following[0] != arm_uid or following[4] != epoch_uid
        )
        tatrans = (
            _tatrans(epoch_uid, arm_epochs[arm_uid], transitions, epoch_labels)
            if last_in_epoch
            else ""
        )
        records.append(
            {
                "STUDYID": study_id,
                "DOMAIN": "TA",
                "ARMCD": first_text(arm_label, arm_name),
                "ARM": first_text(arm_desc, arm_label, arm_name),
                "TAETORD": arm_seq[arm_uid],
                "ETCD": first_text(el_label, el_name),
                "ELEMENT": first_text(el_desc, el_label, el_name),
                "TABRANCH": "",
                "TATRANS": tatrans,
                "EPOCH": epoch_labels.get(epoch_uid, ""),
            }
        )
    return records
