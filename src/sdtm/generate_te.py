"""Generate the SDTM Trial Elements (TE) domain from the SOA workbench DB."""

from sdtm._common import first_text, sponsor_study_id
from soa_builder.web.db import _connect


def build_sdtm_te(soa_id: int) -> list[dict]:
    """One record per unique study element (TE domain).

    Mapping follows the CDISC USDM v4.0 to SDTMIG v3.4 mapping file:
      STUDYID = sponsor StudyIdentifier/@text (see sponsor_study_id)
      ETCD    = StudyElement/@label (falls back to @name)
      ELEMENT = StudyElement/@description (falls back to @label, @name)
      TESTRL  = StudyElement/@transitionStartRule/TransitionRule/@text
      TEENRL  = StudyElement/@transitionEndRule/TransitionRule/@text
      TEDUR   = blank (derived from Timing values; not yet implemented)
    """
    conn = _connect()
    cur = conn.cursor()
    study_id = sponsor_study_id(cur, soa_id)

    # Rule UIDs repeat across studies, so the joins match on soa_id.
    cur.execute(
        """
        SELECT el.name,
               el.label,
               el.description,
               tr_start.text AS testrl_text,
               tr_end.text   AS teenrl_text
        FROM element el
        LEFT JOIN transition_rule tr_start
               ON tr_start.soa_id = el.soa_id
              AND tr_start.transition_rule_uid = el.testrl
        LEFT JOIN transition_rule tr_end
               ON tr_end.soa_id = el.soa_id
              AND tr_end.transition_rule_uid = el.teenrl
        WHERE el.soa_id = ?
        ORDER BY el.order_index, el.id
        """,
        (soa_id,),
    )
    rows = cur.fetchall()
    conn.close()

    return [
        {
            "STUDYID": study_id,
            "DOMAIN": "TE",
            "ETCD": first_text(el_label, el_name),
            "ELEMENT": first_text(el_desc, el_label, el_name),
            "TESTRL": testrl or "",
            "TEENRL": teenrl or "",
            "TEDUR": "",
        }
        for el_name, el_label, el_desc, testrl, teenrl in rows
    ]
