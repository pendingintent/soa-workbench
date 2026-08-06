"""Tests for the Activity BC-grouping ExtensionAttribute generator."""

from soa_builder.web.app import app  # noqa: F401  (triggers DB init)
from soa_builder.web.db import _connect
from usdm.generate_activity_grouping_extensions import (
    populate_activity_grouping_extensions,
    build_usdm_activity_grouping_extensions_bulk,
)

SOA_ID = 9201
ACTIVITY_UID = "Activity_1"
ACTIVITY_UID_2 = "Activity_2"
CG_CONCEPT_GROUP = "cdisc:concept_group:cg_6mwt"
CG_THERAPEUTIC_AREA = "cdisc:therapeutic_area:ta_onc"
CG_CUSTOM = "custom:1"


def _clean():
    conn = _connect()
    cur = conn.cursor()
    cur.execute("DELETE FROM activity_grouping_extension WHERE soa_id=?", (SOA_ID,))
    cur.execute("DELETE FROM activity_concept WHERE soa_id=?", (SOA_ID,))
    cur.execute(
        "DELETE FROM concept_group WHERE concept_group_uid IN (?, ?, ?)",
        (CG_CONCEPT_GROUP, CG_THERAPEUTIC_AREA, CG_CUSTOM),
    )
    conn.commit()
    conn.close()


def _seed_concept_group(
    concept_group_uid,
    scheme_id="concept_group",
    scheme_name="Concept Group",
    value_id="cg_6mwt",
    source="cdisc",
):
    conn = _connect()
    cur = conn.cursor()
    cur.execute(
        "INSERT INTO concept_group"
        " (concept_group_uid, name, source,"
        "  cdisc_scheme_id, cdisc_scheme_name, cdisc_value_id)"
        " VALUES (?, ?, ?, ?, ?, ?)",
        (concept_group_uid, value_id, source, scheme_id, scheme_name, value_id),
    )
    conn.commit()
    conn.close()


def _seed_ac(activity_uid, concept_group_uid, activity_id=1):
    conn = _connect()
    cur = conn.cursor()
    cur.execute(
        "INSERT INTO activity_concept"
        " (soa_id, activity_id, activity_uid, concept_uid,"
        "  concept_code, concept_title, concept_group_uid)"
        " VALUES (?, ?, ?, ?, ?, ?, ?)",
        (
            SOA_ID,
            activity_id,
            activity_uid,
            "BiomedicalConcept_1",
            "C25564",
            "Vital Signs",
            concept_group_uid,
        ),
    )
    conn.commit()
    conn.close()


def test_populate_creates_row_for_assigned_cdisc_group():
    _clean()
    _seed_concept_group(CG_CONCEPT_GROUP)
    _seed_ac(ACTIVITY_UID, CG_CONCEPT_GROUP)

    populate_activity_grouping_extensions(SOA_ID)

    conn = _connect()
    cur = conn.cursor()
    cur.execute(
        "SELECT activity_uid, concept_group_uid, ea_outer_uid, ec_uid,"
        " ea_scheme_uid, ea_value_uid"
        " FROM activity_grouping_extension WHERE soa_id=?",
        (SOA_ID,),
    )
    rows = cur.fetchall()
    conn.close()
    assert len(rows) == 1
    activity_uid, cg_uid, ea_outer, ec, ea_scheme, ea_value = rows[0]
    assert activity_uid == ACTIVITY_UID
    assert cg_uid == CG_CONCEPT_GROUP
    assert ea_outer.startswith("ExtensionAttribute_")
    assert ec.startswith("ExtensionClass_")
    assert ea_scheme.startswith("ExtensionAttribute_")
    assert ea_value.startswith("ExtensionAttribute_")
    assert len({ea_outer, ea_scheme, ea_value}) == 3


def test_populate_is_idempotent():
    _clean()
    _seed_concept_group(CG_CONCEPT_GROUP)
    _seed_ac(ACTIVITY_UID, CG_CONCEPT_GROUP)

    populate_activity_grouping_extensions(SOA_ID)
    conn = _connect()
    cur = conn.cursor()
    cur.execute(
        "SELECT ea_outer_uid FROM activity_grouping_extension WHERE soa_id=?",
        (SOA_ID,),
    )
    first_uid = cur.fetchall()
    conn.close()

    populate_activity_grouping_extensions(SOA_ID)
    conn = _connect()
    cur = conn.cursor()
    cur.execute(
        "SELECT ea_outer_uid FROM activity_grouping_extension WHERE soa_id=?",
        (SOA_ID,),
    )
    second_uid = cur.fetchall()
    conn.close()

    assert first_uid == second_uid
    assert len(second_uid) == 1


def test_populate_skips_custom_source_group():
    _clean()
    _seed_concept_group(CG_CUSTOM, source="custom", value_id="my_custom_group")
    _seed_ac(ACTIVITY_UID, CG_CUSTOM)

    populate_activity_grouping_extensions(SOA_ID)

    conn = _connect()
    cur = conn.cursor()
    cur.execute(
        "SELECT COUNT(*) FROM activity_grouping_extension WHERE soa_id=?",
        (SOA_ID,),
    )
    count = cur.fetchone()[0]
    conn.close()
    assert count == 0


def test_build_returns_nested_shape_with_correct_values():
    _clean()
    _seed_concept_group(CG_CONCEPT_GROUP)
    _seed_ac(ACTIVITY_UID, CG_CONCEPT_GROUP)
    populate_activity_grouping_extensions(SOA_ID)

    out = build_usdm_activity_grouping_extensions_bulk(SOA_ID)
    assert list(out.keys()) == [ACTIVITY_UID]
    units = out[ACTIVITY_UID]
    assert len(units) == 1
    ea = units[0]
    assert ea["instanceType"] == "ExtensionAttribute"
    assert ea["url"] == (
        "http://www.cdisc.org/usdm/extensions/biomedicalConceptGrouping"
    )
    ec = ea["valueExtensionClass"]
    assert ec["instanceType"] == "ExtensionClass"
    assert ec["url"] == (
        "http://www.cdisc.org/usdm/extensions/BiomedicalConceptGrouping"
    )
    children = ec["extensionAttributes"]
    assert len(children) == 2
    scheme_child, value_child = children
    assert scheme_child["url"] == "classification-scheme-id"
    assert scheme_child["valueString"] == "concept_group"
    assert scheme_child["instanceType"] == "ExtensionAttribute"
    assert value_child["url"] == "classification-value-id"
    assert value_child["valueString"] == "cg_6mwt"
    assert value_child["instanceType"] == "ExtensionAttribute"


def test_build_two_groups_on_one_activity_emit_distinct_units():
    _clean()
    _seed_concept_group(CG_CONCEPT_GROUP, scheme_id="concept_group", value_id="cg_6mwt")
    _seed_concept_group(
        CG_THERAPEUTIC_AREA, scheme_id="therapeutic_area", value_id="ta_onc"
    )
    _seed_ac(ACTIVITY_UID, CG_CONCEPT_GROUP, activity_id=1)
    _seed_ac(ACTIVITY_UID, CG_THERAPEUTIC_AREA, activity_id=1)
    populate_activity_grouping_extensions(SOA_ID)

    out = build_usdm_activity_grouping_extensions_bulk(SOA_ID)
    units = out[ACTIVITY_UID]
    assert len(units) == 2

    all_ids = set()
    for unit in units:
        all_ids.add(unit["id"])
        all_ids.add(unit["valueExtensionClass"]["id"])
        for child in unit["valueExtensionClass"]["extensionAttributes"]:
            all_ids.add(child["id"])
    assert len(all_ids) == 8

    value_ids = {
        unit["valueExtensionClass"]["extensionAttributes"][1]["valueString"]
        for unit in units
    }
    assert value_ids == {"cg_6mwt", "ta_onc"}


def test_build_returns_empty_for_activity_without_groups():
    _clean()

    out = build_usdm_activity_grouping_extensions_bulk(SOA_ID)
    assert out.get(ACTIVITY_UID_2, []) == []
