"""Tests for the ExtensionAttribute USDM generator."""

from soa_builder.web.app import app  # noqa: F401  (triggers DB init)
from soa_builder.web.db import _connect
from usdm.generate_extension_attributes import (
    populate_extension_attributes,
    build_usdm_extension_attributes,
)


SOA_ID = 9101
BC_UID = "BiomedicalConcept_1"
ACTIVITY_ID = 7777
CONCEPT_CODE = "C25564"
DSS_HREF = (
    "https://api.library.cdisc.org/api/cosmos/v2/mdr/specializations/"
    "sdtm/datasetspecializations/VSORRES"
)
DSS_HREF_2 = (
    "https://api.library.cdisc.org/api/cosmos/v2/mdr/specializations/"
    "sdtm/datasetspecializations/VSSTRESN"
)


def _clean():
    conn = _connect()
    cur = conn.cursor()
    cur.execute("DELETE FROM activity_concept_dss WHERE soa_id=?", (SOA_ID,))
    cur.execute("DELETE FROM activity_concept WHERE soa_id=?", (SOA_ID,))
    conn.commit()
    conn.close()


def _seed_ac(activity_id=ACTIVITY_ID, concept_uid=BC_UID):
    conn = _connect()
    cur = conn.cursor()
    cur.execute(
        "INSERT INTO activity_concept (soa_id, activity_id, activity_uid,"
        " concept_uid, concept_code, concept_title)"
        " VALUES (?, ?, ?, ?, ?, ?)",
        (SOA_ID, activity_id, 1, concept_uid, CONCEPT_CODE, "Vital Signs"),
    )
    conn.commit()
    conn.close()


def _seed_dss(href=DSS_HREF, title="VSORRES", activity_id=ACTIVITY_ID):
    conn = _connect()
    cur = conn.cursor()
    cur.execute(
        "INSERT INTO activity_concept_dss (soa_id, activity_id,"
        " concept_code, dss_title, dss_href, dss_display)"
        " VALUES (?, ?, ?, ?, ?, ?)",
        (SOA_ID, activity_id, CONCEPT_CODE, title, href, title),
    )
    conn.commit()
    conn.close()


def test_populate_assigns_uid_to_existing_dss_row():
    _clean()
    _seed_ac()
    _seed_dss()

    populate_extension_attributes(SOA_ID)

    conn = _connect()
    cur = conn.cursor()
    cur.execute(
        "SELECT extension_attribute_uid FROM activity_concept_dss WHERE soa_id=?",
        (SOA_ID,),
    )
    rows = cur.fetchall()
    conn.close()
    assert len(rows) == 1
    assert rows[0][0] == "ExtensionAttribute_1"


def test_populate_is_idempotent():
    _clean()
    _seed_ac()
    _seed_dss()

    populate_extension_attributes(SOA_ID)
    populate_extension_attributes(SOA_ID)

    conn = _connect()
    cur = conn.cursor()
    cur.execute(
        "SELECT extension_attribute_uid FROM activity_concept_dss WHERE soa_id=?",
        (SOA_ID,),
    )
    rows = cur.fetchall()
    conn.close()
    assert [r[0] for r in rows] == ["ExtensionAttribute_1"]


def test_populate_assigns_monotonic_uids_for_multiple_rows():
    _clean()
    _seed_ac()
    _seed_dss(href=DSS_HREF, title="VSORRES")
    _seed_dss(href=DSS_HREF_2, title="VSSTRESN")

    populate_extension_attributes(SOA_ID)

    conn = _connect()
    cur = conn.cursor()
    cur.execute(
        "SELECT extension_attribute_uid FROM activity_concept_dss"
        " WHERE soa_id=? ORDER BY id",
        (SOA_ID,),
    )
    uids = [r[0] for r in cur.fetchall()]
    conn.close()
    assert uids == ["ExtensionAttribute_1", "ExtensionAttribute_2"]


def test_build_returns_single_ea_for_bc():
    _clean()
    _seed_ac()
    _seed_dss()
    populate_extension_attributes(SOA_ID)

    out = build_usdm_extension_attributes(SOA_ID, BC_UID)
    assert len(out) == 1
    ea = out[0]
    assert ea["id"] == "ExtensionAttribute_1"
    assert ea["url"] == ("http://www.cdisc.org/usdm/extensions/specializations/sdtm")
    assert ea["valueString"] == (
        "/mdr/specializations/sdtm/datasetspecializations/VSORRES"
    )
    assert ea["instanceType"] == "ExtensionAttribute"


def test_build_returns_one_ea_per_dss_row():
    _clean()
    _seed_ac()
    _seed_dss(href=DSS_HREF, title="VSORRES")
    _seed_dss(href=DSS_HREF_2, title="VSSTRESN")
    populate_extension_attributes(SOA_ID)

    out = build_usdm_extension_attributes(SOA_ID, BC_UID)
    assert len(out) == 2
    assert [e["id"] for e in out] == [
        "ExtensionAttribute_1",
        "ExtensionAttribute_2",
    ]
    assert out[0]["valueString"].endswith("/VSORRES")
    assert out[1]["valueString"].endswith("/VSSTRESN")


def test_build_returns_empty_list_when_no_dss():
    _clean()
    _seed_ac()

    out = build_usdm_extension_attributes(SOA_ID, BC_UID)
    assert out == []
