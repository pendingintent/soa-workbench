"""Tests for the SDTM TE, TA and TV generators in ``src/sdtm``.

Sources follow the CDISC USDM v4.0 to SDTMIG v3.4 mapping file
(``sdtm_mapping.xlsx``) and USDM Implementation Handbook 1, section 4.3.
"""

from fastapi.testclient import TestClient

from sdtm.generate_ta import build_sdtm_ta
from sdtm.generate_te import build_sdtm_te
from sdtm.generate_tv import build_sdtm_tv
from soa_builder.web.app import app
from soa_builder.web.db import _connect

client = TestClient(app)

MAIN = "ScheduleTimeline_1"
OTHER = "ScheduleTimeline_2"


def _new_soa(name: str) -> int:
    resp = client.post("/soa", json={"name": name})
    assert resp.status_code in (200, 201), resp.text
    return resp.json()["id"]


def _seed_foreign_rules(soa_id: int) -> None:
    """Give another study transition rules with the same UIDs."""
    conn = _connect()
    cur = conn.cursor()
    for n in range(1, 8):
        cur.execute(
            "INSERT INTO transition_rule"
            " (soa_id, transition_rule_uid, name, text)"
            " VALUES (?, ?, ?, ?)",
            (soa_id, f"TransitionRule_{n}", f"OTHER_{n}", "OTHER STUDY"),
        )
    conn.commit()
    conn.close()


def _seed_design(soa_id: int, *, arm_specific_visit: bool = False) -> None:
    """Seed a two-arm design with rules, visits and decisions.

    Epoch order for both arms: SCREENING, TREATMENT, END OF TREATMENT,
    FOLLOW-UP. When ``arm_specific_visit`` is set, Arm B has no
    FOLLOW-UP cell, so the visit in FOLLOW-UP belongs to Arm A only.
    """
    conn = _connect()
    try:
        _insert_design(conn.cursor(), soa_id, arm_specific_visit)
        conn.commit()
    finally:
        conn.close()


def _insert_design(cur, soa_id: int, arm_specific_visit: bool) -> None:
    # soa.study_id is unique across studies.
    cur.execute("UPDATE soa SET study_id=? WHERE id=?", (f"NCT{soa_id:08d}", soa_id))

    # Sponsor organization (C70793) and a registry organization.
    cur.execute(
        "INSERT INTO code (soa_id, code_uid, code, code_system)"
        " VALUES (?, 'Code_901', 'C70793', 'http://www.cdisc.org')",
        (soa_id,),
    )
    cur.execute(
        "INSERT INTO code (soa_id, code_uid, code, code_system)"
        " VALUES (?, 'Code_902', 'C188863', 'http://www.cdisc.org')",
        (soa_id,),
    )
    cur.executemany(
        "INSERT INTO organization"
        " (soa_id, organization_uid, name, type_code_uid, order_index)"
        " VALUES (?, ?, ?, ?, ?)",
        [
            (soa_id, "Organization_1", "Registry", "Code_902", 1),
            (soa_id, "Organization_2", "Sponsor Inc", "Code_901", 2),
        ],
    )
    cur.executemany(
        "INSERT INTO study_identifier"
        " (soa_id, study_identifier_uid, text, scope_org_uid, order_index)"
        " VALUES (?, ?, ?, ?, ?)",
        [
            (soa_id, "StudyIdentifier_1", "NCT00000001", "Organization_1", 1),
            (soa_id, "StudyIdentifier_2", "SPONSOR-001", "Organization_2", 2),
        ],
    )

    cur.executemany(
        "INSERT INTO arm (soa_id, arm_uid, name, label, description,"
        " order_index) VALUES (?, ?, ?, ?, ?, ?)",
        [
            (soa_id, "StudyArm_1", "Arm A name", "ARMA", "Drug A 10 mg", 1),
            (soa_id, "StudyArm_2", "Arm B name", "ARMB", "Drug B 20 mg", 2),
        ],
    )
    epochs = [
        ("StudyEpoch_1", "SCREEN", "SCREENING", 1),
        ("StudyEpoch_2", "TRT", "TREATMENT", 2),
        ("StudyEpoch_3", "EOT", "END OF TREATMENT", 3),
        ("StudyEpoch_4", "FU", "FOLLOW-UP", 4),
    ]
    cur.executemany(
        "INSERT INTO epoch (soa_id, epoch_uid, name, epoch_label,"
        " order_index, epoch_seq) VALUES (?, ?, ?, ?, ?, ?)",
        [(soa_id, uid, name, label, o, o) for uid, name, label, o in epochs],
    )

    rules = {
        1: "Informed consent signed",
        2: "First dose",
        3: "Last dose",
        4: "Subject attends fasting",
        5: "First dose given",
    }
    cur.executemany(
        "INSERT INTO transition_rule"
        " (soa_id, transition_rule_uid, name, text) VALUES (?, ?, ?, ?)",
        [
            (soa_id, f"TransitionRule_{n}", f"RULE_{n}", text)
            for n, text in rules.items()
        ],
    )
    elements = [
        ("StudyElement_1", "SCREENING_ELEMENT", "SCRN", "Screening", 1, 2),
        ("StudyElement_2", "DRUG_A_ELEMENT", "DRUGA", "Drug A", 2, 3),
        ("StudyElement_3", "DRUG_B_ELEMENT", "DRUGB", "Drug B", 2, 3),
        ("StudyElement_4", "EOT_ELEMENT", "EOT", "End of treatment", 3, 0),
        ("StudyElement_5", "FU_ELEMENT", "", "Follow-up", 3, 0),
    ]
    cur.executemany(
        "INSERT INTO element (soa_id, element_id, name, label, description,"
        " testrl, teenrl, order_index) VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
        [
            (
                soa_id,
                uid,
                name,
                label,
                desc,
                f"TransitionRule_{start}",
                f"TransitionRule_{end}" if end else None,
                idx,
            )
            for idx, (uid, name, label, desc, start, end) in enumerate(
                elements, start=1
            )
        ],
    )
    cells = []
    for arm_uid, trt in (
        ("StudyArm_1", "StudyElement_2"),
        ("StudyArm_2", "StudyElement_3"),
    ):
        cells += [
            (arm_uid, "StudyEpoch_1", "StudyElement_1"),
            (arm_uid, "StudyEpoch_2", trt),
            (arm_uid, "StudyEpoch_3", "StudyElement_4"),
        ]
        if arm_uid == "StudyArm_1" or not arm_specific_visit:
            cells.append((arm_uid, "StudyEpoch_4", "StudyElement_5"))
    cur.executemany(
        "INSERT INTO study_cell (soa_id, study_cell_uid, arm_uid,"
        " epoch_uid, element_uid, order_index) VALUES (?, ?, ?, ?, ?, ?)",
        [
            (soa_id, f"StudyCell_{i}", arm, ep, el, i)
            for i, (arm, ep, el) in enumerate(cells, start=1)
        ],
    )

    cur.executemany(
        "INSERT INTO schedule_timelines (soa_id, schedule_timeline_uid,"
        " name, main_timeline, order_index) VALUES (?, ?, ?, ?, ?)",
        [(soa_id, MAIN, "Main", 1, 1), (soa_id, OTHER, "Other", 0, 2)],
    )
    timings = [
        ("Timing_1", "Day -14", "P2W"),
        ("Timing_2", "Day 1", "P0D"),
        ("Timing_3", "Week 4", "P4W"),
    ]
    timing_ids = []
    for uid, label, value in timings:
        cur.execute(
            "INSERT INTO timing (soa_id, timing_uid, name, label, value)"
            " VALUES (?, ?, ?, ?, ?)",
            (soa_id, uid, uid, label, value),
        )
        timing_ids.append(cur.lastrowid)
    visits = [
        ("V1", "Visit 1", "Encounter_1", "TransitionRule_1"),
        ("V2", "Visit 2", "Encounter_2", "TransitionRule_4"),
        ("V3", "Visit 3", "Encounter_3", "TransitionRule_4"),
    ]
    for idx, (name, label, enc, rule) in enumerate(visits, start=1):
        cur.execute(
            "INSERT INTO visit (soa_id, name, label, order_index,"
            " encounter_uid, transitionStartRule, scheduledAtId)"
            " VALUES (?, ?, ?, ?, ?, ?, ?)",
            (soa_id, name, label, idx, enc, rule, str(timing_ids[idx - 1])),
        )
    cur.executemany(
        "INSERT INTO instances (soa_id, instance_uid, name, epoch_uid,"
        " encounter_uid, member_of_timeline, order_index)"
        " VALUES (?, ?, ?, ?, ?, ?, ?)",
        [
            (soa_id, "SAI_1", "V1", "StudyEpoch_1", "Encounter_1", MAIN, 1),
            (soa_id, "SAI_2", "V2", "StudyEpoch_2", "Encounter_2", MAIN, 2),
            (soa_id, "SAI_3", "V3", "StudyEpoch_4", "Encounter_3", MAIN, 3),
        ],
    )
    cur.executemany(
        "INSERT INTO decision_instances (soa_id, instance_uid, name,"
        " epoch_uid, member_of_timeline, order_index)"
        " VALUES (?, ?, ?, ?, ?, ?)",
        [
            (soa_id, "SDI_1", "ELIG", "StudyEpoch_1", MAIN, 1),
            (soa_id, "SDI_2", "PROG", "StudyEpoch_2", MAIN, 2),
            (soa_id, "SDI_3", "ALT", "StudyEpoch_2", OTHER, 3),
        ],
    )
    cur.executemany(
        "INSERT INTO condition_assignment (soa_id,"
        " condition_assignment_uid, name, condition,"
        " decision_instance_uid, condition_target_uid, order_index)"
        " VALUES (?, ?, ?, ?, ?, ?, ?)",
        [
            # Next epoch in sequence: not a TATRANS.
            (soa_id, "Condition_1", "C1", "Eligible", "SDI_1", "SAI_2", 1),
            # Skips END OF TREATMENT: a TATRANS.
            (
                soa_id,
                "Condition_2",
                "C2",
                "Disease progression",
                "SDI_2",
                "SAI_3",
                2,
            ),
            # Not on the main timeline: ignored.
            (soa_id, "Condition_3", "C3", "Ignored", "SDI_3", "SAI_3", 3),
        ],
    )


def _seeded_soa(name: str, **kwargs) -> int:
    other = _new_soa(f"{name} (other study)")
    _seed_foreign_rules(other)
    soa_id = _new_soa(name)
    _seed_design(soa_id, **kwargs)
    return soa_id


# --- TE ----------------------------------------------------------------


def test_te_one_row_per_element_with_own_rules():
    soa_id = _seeded_soa("SDTM TE rules")

    rows = build_sdtm_te(soa_id)

    assert len(rows) == 5
    texts = {r["TESTRL"] for r in rows} | {r["TEENRL"] for r in rows}
    assert "OTHER STUDY" not in texts
    scrn = rows[0]
    assert scrn["TESTRL"] == "Informed consent signed"
    assert scrn["TEENRL"] == "First dose"


def test_te_etcd_from_label_element_from_description():
    soa_id = _seeded_soa("SDTM TE codes")

    rows = {r["ELEMENT"]: r for r in build_sdtm_te(soa_id)}

    assert rows["Screening"]["ETCD"] == "SCRN"
    assert rows["Drug A"]["ETCD"] == "DRUGA"
    # Blank label falls back to the element name.
    assert rows["Follow-up"]["ETCD"] == "FU_ELEMENT"


def test_te_studyid_is_sponsor_identifier():
    soa_id = _seeded_soa("SDTM TE studyid")

    rows = build_sdtm_te(soa_id)

    assert {r["STUDYID"] for r in rows} == {"SPONSOR-001"}


# --- TA ----------------------------------------------------------------


def test_ta_arm_and_epoch_from_labels():
    soa_id = _seeded_soa("SDTM TA labels")

    rows = build_sdtm_ta(soa_id)

    assert len(rows) == 8
    first = rows[0]
    assert first["STUDYID"] == "SPONSOR-001"
    assert first["ARMCD"] == "ARMA"
    assert first["ARM"] == "Drug A 10 mg"
    assert first["ETCD"] == "SCRN"
    assert first["ELEMENT"] == "Screening"
    assert first["EPOCH"] == "SCREENING"
    assert [r["TAETORD"] for r in rows if r["ARMCD"] == "ARMB"] == [1, 2, 3, 4]


def test_ta_tatrans_from_main_timeline_decisions():
    soa_id = _seeded_soa("SDTM TA tatrans")

    rows = build_sdtm_ta(soa_id)

    by_key = {(r["ARMCD"], r["EPOCH"]): r["TATRANS"] for r in rows}
    for armcd in ("ARMA", "ARMB"):
        # Skips END OF TREATMENT, so it is a transition rule.
        assert by_key[(armcd, "TREATMENT")] == (
            "Disease progression: transition to FOLLOW-UP"
        )
        # Moving on to the next epoch is not a transition rule.
        assert by_key[(armcd, "SCREENING")] == ""
        assert by_key[(armcd, "END OF TREATMENT")] == ""
        assert by_key[(armcd, "FOLLOW-UP")] == ""


def test_ta_tatrans_skipped_when_target_epoch_not_in_arm():
    soa_id = _seeded_soa("SDTM TA tatrans arm", arm_specific_visit=True)

    rows = build_sdtm_ta(soa_id)

    by_key = {(r["ARMCD"], r["EPOCH"]): r["TATRANS"] for r in rows}
    assert by_key[("ARMA", "TREATMENT")].startswith("Disease progression")
    # Arm B has no FOLLOW-UP cell, so the condition does not apply.
    assert by_key[("ARMB", "TREATMENT")] == ""


# --- TV ----------------------------------------------------------------


def test_tv_one_row_per_visit_when_arms_share_visits():
    soa_id = _seeded_soa("SDTM TV shared")

    rows = build_sdtm_tv(soa_id)

    assert [r["VISITNUM"] for r in rows] == [1, 2, 3]
    assert {r["ARMCD"] for r in rows} == {""}
    assert {r["ARM"] for r in rows} == {""}
    assert {r["STUDYID"] for r in rows} == {"SPONSOR-001"}


def test_tv_visit_label_rules_and_visitdy_from_timing_label():
    soa_id = _seeded_soa("SDTM TV fields")

    rows = build_sdtm_tv(soa_id)

    assert [r["VISIT"] for r in rows] == ["Visit 1", "Visit 2", "Visit 3"]
    assert [r["TVSTRL"] for r in rows] == [
        "Informed consent signed",
        "Subject attends fasting",
        "Subject attends fasting",
    ]
    # "Day -14" and "Day 1" are study days; "Week 4" is not.
    assert [r["VISITDY"] for r in rows] == ["-14", "1", ""]


def test_tv_rows_per_arm_when_visits_differ_by_arm():
    soa_id = _seeded_soa("SDTM TV per arm", arm_specific_visit=True)

    rows = build_sdtm_tv(soa_id)

    pairs = [(r["ARMCD"], r["VISITNUM"]) for r in rows]
    assert pairs == [
        ("ARMA", 1),
        ("ARMA", 2),
        ("ARMA", 3),
        ("ARMB", 1),
        ("ARMB", 2),
    ]
    assert rows[0]["ARM"] == "Drug A 10 mg"
