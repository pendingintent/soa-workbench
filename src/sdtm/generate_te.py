"""Generate the SDTM Trial Elements (TE) domain from the SOA workbench DB."""

from soa_builder.web.db import _connect


def build_sdtm_te(soa_id: int) -> list[dict]:
    """One record per unique study element (TE domain).

    Mapping follows SDTM IG v3.4 Section 7 / docs/Create TDD.docx:
      STUDYID = StudyIdentifier (sponsor org) → soa.study_id or soa.name
      ETCD    = StudyElement/@name
      ELEMENT = StudyElement/@description
      TESTRL  = StudyElement/@transitionStartRule/TransitionRule/@text
      TEENRL  = StudyElement/@transitionEndRule/TransitionRule/@text
      TEDUR   = blank (requires Timing value derivation, not directly in DB)
    """
    conn = _connect()
    cur = conn.cursor()
    cur.execute("SELECT study_id, name FROM soa WHERE id=?", (soa_id,))
    row = cur.fetchone()
    study_id = (row[0] or row[1]) if row else ""

    cur.execute(
        """
        SELECT el.name,
               el.description,
               el.label,
               tr_start.text AS testrl_text,
               tr_end.text   AS teenrl_text
        FROM element el
        LEFT JOIN transition_rule tr_start ON tr_start.transition_rule_uid = el.testrl
        LEFT JOIN transition_rule tr_end   ON tr_end.transition_rule_uid   = el.teenrl
        WHERE el.soa_id = ?
        ORDER BY el.order_index
        """,
        (soa_id,),
    )
    rows = cur.fetchall()
    conn.close()

    records = []
    for el_name, el_desc, el_label, testrl, teenrl in rows:
        records.append(
            {
                "STUDYID": study_id,
                "DOMAIN": "TE",
                "ETCD": el_name or "",
                "ELEMENT": el_desc or el_label or el_name or "",
                "TESTRL": testrl or "",
                "TEENRL": teenrl or "",
                "TEDUR": "",
            }
        )
    return records
