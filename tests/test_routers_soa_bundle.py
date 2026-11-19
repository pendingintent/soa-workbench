"""Tests for SOA bundle export and import endpoints."""

import io
import json
import uuid

import pytest
from fastapi.testclient import TestClient

from soa_builder.web.app import app
from soa_builder.web.db import _connect
from soa_builder.web.routers import soa_bundle

client = TestClient(app)


def _new_soa(name: str) -> int:
    r = client.post(
        "/soa",
        json={"name": name, "study_id": f"TEST-{uuid.uuid4().hex[:12]}"},
    )
    assert r.status_code in (200, 201), r.text
    return r.json()["id"]


def _populate_soa(soa_id: int):
    """Add epochs, visits, activities, cells, arm, and an objective."""
    # Epochs
    ep1 = client.post(
        f"/soa/{soa_id}/epochs",
        json={"name": "Screening", "type": "SCREENING"},
    )
    assert ep1.status_code in (200, 201)
    ep1_uid = ep1.json()["epoch_uid"]

    ep2 = client.post(
        f"/soa/{soa_id}/epochs",
        json={"name": "Treatment", "type": "TREATMENT"},
    )
    assert ep2.status_code in (200, 201)
    ep2_uid = ep2.json()["epoch_uid"]

    # Visits
    v1 = client.post(
        f"/soa/{soa_id}/visits",
        json={"name": "Screening Visit", "epoch_uid": ep1_uid},
    )
    assert v1.status_code in (200, 201)
    v1_id = v1.json()["id"]

    v2 = client.post(
        f"/soa/{soa_id}/visits",
        json={"name": "Week 1", "epoch_uid": ep2_uid},
    )
    assert v2.status_code in (200, 201)
    v2_id = v2.json()["id"]

    # Activities
    a1 = client.post(f"/soa/{soa_id}/activities", json={"name": "Informed Consent"})
    assert a1.status_code in (200, 201)
    a1_id = a1.json()["activity_id"]

    a2 = client.post(f"/soa/{soa_id}/activities", json={"name": "Vital Signs"})
    assert a2.status_code in (200, 201)
    a2_id = a2.json()["activity_id"]

    # Cells
    for v_id in [v1_id, v2_id]:
        for a_id in [a1_id, a2_id]:
            r = client.post(
                f"/soa/{soa_id}/cells",
                json={"visit_id": v_id, "activity_id": a_id, "status": "X"},
            )
            assert r.status_code in (200, 201), r.text

    # Arm
    arm_r = client.post(
        f"/soa/{soa_id}/arms",
        json={"name": "Treatment Arm", "type": "INVESTIGATIONAL"},
    )
    assert arm_r.status_code in (200, 201)

    return {
        "epochs": [ep1_uid, ep2_uid],
        "visit_ids": [v1_id, v2_id],
        "activity_ids": [a1_id, a2_id],
    }


def _export_bundle(soa_id: int) -> dict:
    r = client.get(f"/soa/{soa_id}/export/bundle")
    assert r.status_code == 200, r.text
    assert "attachment" in r.headers.get("content-disposition", "")
    return json.loads(r.content)


def _import_bundle(bundle: dict, name: str = None) -> dict:
    buf = io.BytesIO(json.dumps(bundle).encode())
    data = {}
    if name:
        data["name"] = name
    r = client.post(
        "/soa/import/bundle",
        files={"file": ("bundle.json", buf, "application/json")},
        data=data,
    )
    assert r.status_code == 201, r.text
    return r.json()


def _count(soa_id: int, table: str) -> int:
    conn = _connect()
    cur = conn.cursor()
    cur.execute(f"SELECT COUNT(*) FROM {table} WHERE soa_id=?", (soa_id,))
    n = cur.fetchone()[0]
    conn.close()
    return n


def test_export_bundle_has_required_keys():
    soa_id = _new_soa("ExportKeysTest")
    bundle = _export_bundle(soa_id)
    assert bundle["format_version"] == "1.0"
    assert "exported_at" in bundle
    assert bundle["soa"]["name"] == "ExportKeysTest"
    for key in ["epoch", "visit", "activity", "matrix_cells", "arm"]:
        assert key in bundle, f"Missing key: {key}"


def test_export_bundle_roundtrip():
    soa_id = _new_soa("RoundtripTest")
    _populate_soa(soa_id)

    bundle = _export_bundle(soa_id)

    # Verify bundle content counts
    assert len(bundle["epoch"]) == 2
    assert len(bundle["visit"]) == 2
    assert len(bundle["activity"]) == 2
    assert len(bundle["matrix_cells"]) == 4
    assert len(bundle["arm"]) == 1

    # Import into a new SOA
    result = _import_bundle(bundle)
    new_soa_id = result["soa_id"]
    assert new_soa_id != soa_id

    # Verify counts match
    assert _count(new_soa_id, "epoch") == 2
    assert _count(new_soa_id, "visit") == 2
    assert _count(new_soa_id, "activity") == 2
    assert _count(new_soa_id, "matrix_cells") == 4
    assert _count(new_soa_id, "arm") == 1

    # Verify cells reference the new SOA's visit/activity IDs
    conn = _connect()
    cur = conn.cursor()
    cur.execute(
        "SELECT visit_id, activity_id FROM matrix_cells WHERE soa_id=?",
        (new_soa_id,),
    )
    cells = cur.fetchall()
    conn.close()
    new_visit_ids = {r[0] for r in cells}
    new_act_ids = {r[1] for r in cells}

    # IDs must belong to the new SOA, not the old one
    conn = _connect()
    cur = conn.cursor()
    cur.execute("SELECT id FROM visit WHERE soa_id=?", (new_soa_id,))
    expected_visit_ids = {r[0] for r in cur.fetchall()}
    cur.execute("SELECT id FROM activity WHERE soa_id=?", (new_soa_id,))
    expected_act_ids = {r[0] for r in cur.fetchall()}
    conn.close()

    assert new_visit_ids == expected_visit_ids
    assert new_act_ids == expected_act_ids


def test_export_empty_soa_bundle():
    soa_id = _new_soa("EmptyBundleTest")
    bundle = _export_bundle(soa_id)
    assert bundle["epoch"] == []
    assert bundle["visit"] == []
    assert bundle["activity"] == []

    result = _import_bundle(bundle)
    new_soa_id = result["soa_id"]
    assert _count(new_soa_id, "epoch") == 0
    assert _count(new_soa_id, "visit") == 0
    assert _count(new_soa_id, "activity") == 0


def test_import_name_override():
    soa_id = _new_soa("OriginalName")
    bundle = _export_bundle(soa_id)

    result = _import_bundle(bundle, name="Custom Name")
    assert result["name"] == "Custom Name"

    new_soa_id = result["soa_id"]
    conn = _connect()
    cur = conn.cursor()
    cur.execute("SELECT name FROM soa WHERE id=?", (new_soa_id,))
    db_name = cur.fetchone()[0]
    conn.close()
    assert db_name == "Custom Name"


def test_import_invalid_format_version():
    bundle = {
        "format_version": "99.0",
        "soa": {"name": "Bad"},
        "epoch": [],
        "visit": [],
        "activity": [],
        "matrix_cells": [],
    }
    buf = io.BytesIO(json.dumps(bundle).encode())
    r = client.post(
        "/soa/import/bundle",
        files={"file": ("bad.json", buf, "application/json")},
    )
    assert r.status_code == 422


def test_export_not_found():
    r = client.get("/soa/999999/export/bundle")
    assert r.status_code == 404


def test_import_invalid_json():
    buf = io.BytesIO(b"not json at all {{{")
    r = client.post(
        "/soa/import/bundle",
        files={"file": ("bad.json", buf, "application/json")},
    )
    assert r.status_code == 422


# ---------------------------------------------------------------------------
# Completeness, remapping, and audit/freeze exclusion
# ---------------------------------------------------------------------------


def _insert(cur, table: str, **values) -> int:
    cols = ",".join(values)
    marks = ",".join("?" * len(values))
    cur.execute(f"INSERT INTO {table} ({cols}) VALUES ({marks})", list(values.values()))
    return cur.lastrowid


def _one(sql: str, args=()):
    conn = _connect()
    try:
        cur = conn.cursor()
        cur.execute(sql, args)
        return cur.fetchone()
    finally:
        conn.close()


def _seed_linked_rows(soa_id: int) -> None:
    """Rows in tables that link to their parents by integer row id."""
    conn = _connect()
    cur = conn.cursor()
    s = {"soa_id": soa_id}
    act = _insert(
        cur, "activity", **s, name="Labs", activity_uid="Activity_1", order_index=1
    )
    tim = _insert(cur, "timing", **s, timing_uid="Timing_1", name="T1")
    _insert(
        cur,
        "visit",
        **s,
        name="V1",
        encounter_uid="Encounter_1",
        order_index=1,
        scheduledAtId=str(tim),
    )
    el = _insert(cur, "element", **s, element_id="StudyElement_1", name="EL1")
    si = _insert(
        cur,
        "study_intervention",
        **s,
        intervention_uid="StudyIntervention_1",
        name="Drug",
    )
    _insert(cur, "study_intervention_code", **s, intervention_id=si, code_uid="Code_1")
    _insert(
        cur,
        "element_intervention",
        **s,
        element_id=el,
        intervention_uid="StudyIntervention_1",
    )
    est = _insert(cur, "estimand", **s, estimand_uid="Estimand_1", name="E1")
    _insert(
        cur,
        "estimand_intervention",
        **s,
        estimand_id=est,
        intervention_uid="StudyIntervention_1",
    )
    _insert(cur, "estimand_variable", **s, estimand_id=est, endpoint_uid="Endpoint_1")
    _insert(
        cur,
        "intercurrent_event",
        **s,
        estimand_id=est,
        event_uid="IntercurrentEvent_1",
        name="ICE",
    )
    ind = _insert(cur, "indication", **s, indication_uid="Indication_1", name="Ind")
    _insert(cur, "indication_code", **s, indication_id=ind, code_uid="Code_2")
    role = _insert(cur, "role", **s, role_uid="StudyRole_1", name="Chair")
    person = _insert(
        cur,
        "person",
        **s,
        person_uid="Person_1",
        person_name_uid="PersonName_1",
        name="Dr X",
    )
    _insert(cur, "role_person", **s, role_id=role, person_id=person)
    _insert(
        cur,
        "activity_concept_crf",
        **s,
        activity_id=act,
        concept_code="C1",
        crf_title="CRF",
        crf_href="https://example.invalid/crf",
    )
    _insert(
        cur,
        "study_identifier",
        **s,
        study_identifier_uid="StudyIdentifier_1",
        text="NCT00000000",
        scope_org_uid="Organization_1",
    )
    _insert(
        cur,
        "eligibility_criterion",
        **s,
        criterion_uid="EligibilityCriterion_1",
        item_uid="EligibilityCriterionItem_1",
        name="INC1",
        identifier="1",
        category_code="C25532",
        text="Age 18 or older",
    )
    _insert(cur, "soa_tool_extension", **s, ea_outer_uid="ExtensionAttribute_1")
    _insert(
        cur,
        "activity_grouping_extension",
        **s,
        activity_uid="Activity_1",
        concept_group_uid="cdisc:concept_group:cg_test",
    )
    _insert(cur, "uid_counter", **s, prefix="Encounter_", next_n=7)
    conn.commit()
    conn.close()


def test_roundtrip_carries_all_study_content_tables():
    soa_id = _new_soa("FullContentTest")
    _seed_linked_rows(soa_id)
    bundle = _export_bundle(soa_id)
    new_id = _import_bundle(bundle)["soa_id"]

    for table in [
        "study_intervention",
        "study_intervention_code",
        "element_intervention",
        "estimand",
        "estimand_intervention",
        "estimand_variable",
        "intercurrent_event",
        "indication",
        "indication_code",
        "person",
        "role_person",
        "activity_concept_crf",
        "study_identifier",
        "eligibility_criterion",
        "soa_tool_extension",
        "activity_grouping_extension",
        "uid_counter",
    ]:
        assert _count(new_id, table) == _count(soa_id, table) == 1, table

    assert _one(
        "SELECT next_n FROM uid_counter WHERE soa_id=? AND prefix='Encounter_'",
        (new_id,),
    ) == (7,)


@pytest.mark.parametrize(
    "sql",
    [
        # visit.scheduledAtId holds timing.id as text
        "SELECT t.timing_uid FROM visit v JOIN timing t"
        " ON t.id = CAST(v.scheduledAtId AS INTEGER) AND t.soa_id = v.soa_id"
        " WHERE v.soa_id=?",
        "SELECT e.element_id FROM element_intervention x JOIN element e"
        " ON e.id = x.element_id AND e.soa_id = x.soa_id WHERE x.soa_id=?",
        "SELECT p.intervention_uid FROM study_intervention_code x"
        " JOIN study_intervention p ON p.id = x.intervention_id"
        " AND p.soa_id = x.soa_id WHERE x.soa_id=?",
        "SELECT p.estimand_uid FROM estimand_intervention x JOIN estimand p"
        " ON p.id = x.estimand_id AND p.soa_id = x.soa_id WHERE x.soa_id=?",
        "SELECT p.estimand_uid FROM estimand_variable x JOIN estimand p"
        " ON p.id = x.estimand_id AND p.soa_id = x.soa_id WHERE x.soa_id=?",
        "SELECT p.estimand_uid FROM intercurrent_event x JOIN estimand p"
        " ON p.id = x.estimand_id AND p.soa_id = x.soa_id WHERE x.soa_id=?",
        "SELECT p.indication_uid FROM indication_code x JOIN indication p"
        " ON p.id = x.indication_id AND p.soa_id = x.soa_id WHERE x.soa_id=?",
        "SELECT r.role_uid, p.person_uid FROM role_person x"
        " JOIN role r ON r.id = x.role_id AND r.soa_id = x.soa_id"
        " JOIN person p ON p.id = x.person_id AND p.soa_id = x.soa_id"
        " WHERE x.soa_id=?",
        "SELECT a.activity_uid FROM activity_concept_crf x JOIN activity a"
        " ON a.id = x.activity_id AND a.soa_id = x.soa_id WHERE x.soa_id=?",
    ],
)
def test_import_remaps_integer_references_to_new_parents(sql):
    soa_id = _new_soa("RemapTest")
    _seed_linked_rows(soa_id)
    new_id = _import_bundle(_export_bundle(soa_id))["soa_id"]

    before = _one(sql, (soa_id,))
    after = _one(sql, (new_id,))
    assert before is not None
    assert after == before


def test_export_never_contains_audit_or_freeze_data():
    soa_id = _new_soa("NoAuditFreezeTest")
    _populate_soa(soa_id)
    r = client.post(f"/soa/{soa_id}/freezes", json={})
    assert r.status_code == 201, r.text
    freeze_id = r.json()["id"]
    conn = _connect()
    cur = conn.cursor()
    _insert(
        cur,
        "study_amendment",
        soa_id=soa_id,
        freeze_id=freeze_id,
        amendment_uid="StudyAmendment_1",
        name="A1",
        number="1",
        summary="S",
    )
    conn.commit()
    conn.close()
    # There is audit and freeze data that could leak.
    assert _count(soa_id, "soa_freeze") == 1
    assert _count(soa_id, "epoch_audit") > 0

    bundle = _export_bundle(soa_id)

    for key, rows in bundle.items():
        assert not key.endswith("_audit"), key
        assert "freeze" not in key, key
        if isinstance(rows, list):
            for row in rows:
                assert "freeze_id" not in row, key
    assert len(bundle["study_amendment"]) == 1

    new_id = _import_bundle(bundle)["soa_id"]
    assert _count(new_id, "soa_freeze") == 0
    assert _one("SELECT freeze_id FROM study_amendment WHERE soa_id=?", (new_id,)) == (
        0,
    )


def test_import_ignores_freeze_id_in_older_bundles():
    soa_id = _new_soa("LegacyFreezeIdTest")
    bundle = _export_bundle(soa_id)
    bundle["study_amendment"] = [
        {
            "id": 1,
            "soa_id": soa_id,
            "freeze_id": 424242,
            "amendment_uid": "StudyAmendment_1",
            "name": "A1",
            "number": "1",
            "summary": "S",
            "label": None,
            "description": None,
        }
    ]
    new_id = _import_bundle(bundle)["soa_id"]
    assert _one("SELECT freeze_id FROM study_amendment WHERE soa_id=?", (new_id,)) == (
        0,
    )


def test_export_carries_every_study_column_of_the_soa_row():
    soa_id = _new_soa("SoaColumnsTest")
    conn = _connect()
    cur = conn.cursor()
    cur.execute(
        "UPDATE soa SET planned_sex=?, planned_age_min=?, planned_age_max=?,"
        " intervention_model_code=? WHERE id=?",
        ("C16576", 18, 75, "C82639", soa_id),
    )
    conn.commit()
    cur.execute("PRAGMA table_info(soa)")
    soa_cols = {r[1] for r in cur.fetchall()}
    conn.close()

    bundle = _export_bundle(soa_id)
    assert set(bundle["soa"]) == soa_cols - set(soa_bundle._EXCLUDED_SOA_COLUMNS)

    new_id = _import_bundle(bundle)["soa_id"]
    cols = ["planned_sex", "planned_age_min", "planned_age_max"]
    cols.append("intervention_model_code")
    sql = f"SELECT {','.join(cols)} FROM soa WHERE id=?"
    assert _one(sql, (new_id,)) == _one(sql, (soa_id,)) == ("C16576", 18, 75, "C82639")


def test_import_accepts_bundle_with_only_basic_soa_fields():
    bundle = {
        "format_version": "1.0",
        "soa": {
            "name": "BasicSoaMeta",
            "study_id": None,
            "study_label": "L",
            "study_description": "D",
        },
    }
    new_id = _import_bundle(bundle)["soa_id"]
    assert _one("SELECT study_label FROM soa WHERE id=?", (new_id,)) == ("L",)


def test_import_drops_rows_whose_parent_is_not_in_the_bundle():
    soa_id = _new_soa("DanglingRefTest")
    ids = _populate_soa(soa_id)
    bundle = _export_bundle(soa_id)
    orphan = dict(bundle["matrix_cells"][0])
    orphan.update(id=-1, visit_id=None, instance_id=987654321)
    orphan["activity_id"] = ids["activity_ids"][0]
    bundle["matrix_cells"].append(orphan)

    new_id = _import_bundle(bundle)["soa_id"]
    assert _count(new_id, "matrix_cells") == 4


def test_rows_keep_their_order_through_export_and_import():
    """Generators list rows by id, so a bundle must carry rows in id
    order and import must give them ids in that order. A table with a
    UNIQUE uid index otherwise comes back in uid text order
    (StudyCell_10 before StudyCell_2)."""
    soa_id = _new_soa("RowOrderTest")
    uids = ["StudyCell_2", "StudyCell_10", "StudyCell_3"]
    conn = _connect()
    cur = conn.cursor()
    for i, uid in enumerate(uids, start=1):
        _insert(
            cur,
            "study_cell",
            soa_id=soa_id,
            study_cell_uid=uid,
            arm_uid="StudyArm_1",
            epoch_uid=f"StudyEpoch_{i}",
            element_uid="StudyElement_1",
            order_index=i,
        )
    conn.commit()
    conn.close()

    bundle = _export_bundle(soa_id)
    assert [r["study_cell_uid"] for r in bundle["study_cell"]] == uids

    # Bundles exported before the fix list rows in uid text order;
    # import must still restore the original order.
    bundle["study_cell"].sort(key=lambda r: r["study_cell_uid"])
    new_id = _import_bundle(bundle)["soa_id"]
    conn = _connect()
    rows = conn.execute(
        "SELECT study_cell_uid FROM study_cell WHERE soa_id=? ORDER BY id",
        (new_id,),
    ).fetchall()
    conn.close()
    assert [r[0] for r in rows] == uids


def test_import_repairs_duplicate_extension_attribute_uids():
    """Bundles exported before the fix carry ExtensionAttribute uids
    held twice (a DSS row and the tool extension). Import must leave
    every uid with one holder, keeping the DSS row's."""
    soa_id = _new_soa("DuplicateEaTest")
    ids = _populate_soa(soa_id)
    conn = _connect()
    cur = conn.cursor()
    _insert(
        cur,
        "activity_concept_dss",
        soa_id=soa_id,
        activity_id=ids["activity_ids"][0],
        concept_code="C1",
        dss_title="DSS",
        dss_href="h",
        extension_attribute_uid="ExtensionAttribute_1",
    )
    _insert(
        cur,
        "soa_tool_extension",
        soa_id=soa_id,
        ea_outer_uid="ExtensionAttribute_1",
        ec_uid="ExtensionClass_1",
        ea_name_uid="ExtensionAttribute_2",
        ea_version_uid="ExtensionAttribute_3",
        ea_date_uid="ExtensionAttribute_4",
    )
    conn.commit()
    conn.close()

    new_id = _import_bundle(_export_bundle(soa_id))["soa_id"]

    dss = _one(
        "SELECT extension_attribute_uid FROM activity_concept_dss WHERE soa_id=?",
        (new_id,),
    )
    tool = _one(
        "SELECT ea_outer_uid, ea_name_uid FROM soa_tool_extension WHERE soa_id=?",
        (new_id,),
    )
    assert dss == ("ExtensionAttribute_1",)
    assert tool[0] not in ("ExtensionAttribute_1", "ExtensionAttribute_2")
    assert tool[1] == "ExtensionAttribute_2"
