"""Generate the SDTM Trial Arms (TA) domain from the SOA workbench DB."""

from soa_builder.web.db import _connect


def build_sdtm_ta(soa_id: int) -> list[dict]:
    """One record per planned element per arm (TA domain).

    Mapping follows SDTM IG v3.4 Section 7 / docs/Create TDD.docx:
      ARMCD   = StudyArm/@name (≤20 chars)
      ARM     = StudyArm/@description
      TAETORD = sequential order within arm (by epoch.order_index then sc.order_index)
      ETCD    = StudyElement/@name
      ELEMENT = StudyElement/@description
      EPOCH   = StudyEpoch/@name
      TABRANCH / TATRANS = blank (require ScheduledDecisionInstance, not in DB)
    """
    conn = _connect()
    cur = conn.cursor()
    cur.execute("SELECT study_id, name FROM soa WHERE id=?", (soa_id,))
    row = cur.fetchone()
    study_id = (row[0] or row[1]) if row else ""

    cur.execute(
        """
        SELECT sc.arm_uid,
               a.name        AS arm_name,
               a.description AS arm_desc,
               a.label       AS arm_label,
               e.name        AS epoch_name,
               e.order_index AS epoch_ord,
               el.name       AS el_name,
               el.description AS el_desc,
               el.label      AS el_label,
               sc.order_index
        FROM study_cell sc
        JOIN arm     a  ON a.arm_uid    = sc.arm_uid    AND a.soa_id  = sc.soa_id
        JOIN epoch   e  ON e.epoch_uid  = sc.epoch_uid  AND e.soa_id  = sc.soa_id
        JOIN element el ON el.element_id = sc.element_uid AND el.soa_id = sc.soa_id
        WHERE sc.soa_id = ?
        ORDER BY a.order_index, e.order_index, sc.order_index
        """,
        (soa_id,),
    )
    rows = cur.fetchall()
    conn.close()

    records = []
    arm_seq: dict[str, int] = {}
    for (
        arm_uid,
        arm_name,
        arm_desc,
        arm_label,
        epoch_name,
        _epoch_ord,
        el_name,
        el_desc,
        el_label,
        _sc_ord,
    ) in rows:
        arm_seq.setdefault(arm_uid, 0)
        arm_seq[arm_uid] += 1
        records.append(
            {
                "STUDYID": study_id,
                "DOMAIN": "TA",
                "ARMCD": (arm_name or "")[:20],
                "ARM": arm_desc or arm_label or arm_name or "",
                "TAETORD": arm_seq[arm_uid],
                "ETCD": el_name or "",
                "ELEMENT": el_desc or el_label or el_name or "",
                "TABRANCH": "",
                "TATRANS": "",
                "EPOCH": epoch_name or "",
            }
        )
    return records
