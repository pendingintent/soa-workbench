"""Tests for the BiomedicalConceptProperty USDM generator."""

from unittest.mock import patch

from soa_builder.web.app import app  # noqa: F401  (triggers DB init)
from soa_builder.web.db import _connect
from usdm.generate_biomedical_concept_properties import (
    populate_biomedical_concept_properties,
    build_usdm_biomedical_concept_properties,
)


SOA_ID = 9001
BC_UID = "BiomedicalConcept_1"
CONCEPT_CODE = "C25564"

MOCK_BC_API = {
    "href": "https://example/bc/C25564",
    "synonyms": ["Demo"],
    "dataElementConcepts": [
        {
            "conceptId": "C25347",
            "shortName": "Height",
            "dataType": "decimal",
        },
        {
            "conceptId": "C25208",
            "shortName": "Weight",
            "dataType": "decimal",
        },
        {
            "conceptId": "C49669",
            "shortName": "Unit",
            "dataType": "string",
        },
    ],
}


def _seed_bc():
    """Insert a single BC + activity_concept + alias_code + code chain."""
    conn = _connect()
    cur = conn.cursor()
    cur.execute("DELETE FROM biomedical_concept_property WHERE soa_id=?", (SOA_ID,))
    cur.execute("DELETE FROM biomedical_concept WHERE soa_id=?", (SOA_ID,))
    cur.execute("DELETE FROM activity_concept WHERE soa_id=?", (SOA_ID,))
    cur.execute("DELETE FROM alias_code WHERE soa_id=?", (SOA_ID,))
    cur.execute("DELETE FROM code WHERE soa_id=?", (SOA_ID,))

    cur.execute(
        "INSERT INTO code (code_uid, soa_id, code, code_system,"
        " code_system_version, decode) VALUES (?, ?, ?, ?, ?, ?)",
        ("Code_1", SOA_ID, CONCEPT_CODE, "http://www.cdisc.org", "v1", "Vital Signs"),
    )
    cur.execute(
        "INSERT INTO alias_code (alias_code_uid, soa_id, standard_code)"
        " VALUES (?, ?, ?)",
        ("AliasCode_1", SOA_ID, "Code_1"),
    )
    cur.execute(
        "INSERT INTO biomedical_concept (biomedical_concept_uid, soa_id,"
        " name, label, code) VALUES (?, ?, ?, ?, ?)",
        (BC_UID, SOA_ID, "Vital Signs", "VS", "AliasCode_1"),
    )
    cur.execute(
        "INSERT INTO activity_concept (soa_id, activity_uid, concept_uid,"
        " concept_code, concept_title) VALUES (?, ?, ?, ?, ?)",
        (SOA_ID, 1, BC_UID, CONCEPT_CODE, "Vital Signs"),
    )
    conn.commit()
    conn.close()


def test_populate_creates_property_rows_with_uids():
    _seed_bc()
    with patch(
        "usdm.generate_biomedical_concept_properties._get_biomedical_concept_data",
        return_value=MOCK_BC_API,
    ):
        populate_biomedical_concept_properties(SOA_ID)

    conn = _connect()
    cur = conn.cursor()
    cur.execute(
        "SELECT biomedical_concept_property_uid, name, datatype, code"
        " FROM biomedical_concept_property"
        " WHERE soa_id=? AND biomedical_concept_uid=?"
        " ORDER BY id",
        (SOA_ID, BC_UID),
    )
    rows = cur.fetchall()
    conn.close()

    assert len(rows) == 3
    bcp_uids = [r[0] for r in rows]
    assert bcp_uids == [
        "BiomedicalConceptProperty_1",
        "BiomedicalConceptProperty_2",
        "BiomedicalConceptProperty_3",
    ]
    assert [r[1] for r in rows] == ["Height", "Weight", "Unit"]
    assert [r[2] for r in rows] == ["decimal", "decimal", "string"]
    # nested AliasCode UIDs were allocated above the seeded AliasCode_1
    for _, _, _, alias in rows:
        assert alias.startswith("AliasCode_")
        assert int(alias.split("_")[1]) >= 2


def test_populate_is_idempotent():
    _seed_bc()
    with patch(
        "usdm.generate_biomedical_concept_properties._get_biomedical_concept_data",
        return_value=MOCK_BC_API,
    ):
        populate_biomedical_concept_properties(SOA_ID)
        populate_biomedical_concept_properties(SOA_ID)

    conn = _connect()
    cur = conn.cursor()
    cur.execute(
        "SELECT COUNT(*) FROM biomedical_concept_property WHERE soa_id=?",
        (SOA_ID,),
    )
    assert cur.fetchone()[0] == 3
    conn.close()


def test_build_returns_usdm_shape():
    _seed_bc()
    with patch(
        "usdm.generate_biomedical_concept_properties._get_biomedical_concept_data",
        return_value=MOCK_BC_API,
    ):
        populate_biomedical_concept_properties(SOA_ID)

    out = build_usdm_biomedical_concept_properties(SOA_ID, BC_UID)
    assert len(out) == 3
    p = out[0]
    assert p["instanceType"] == "BiomedicalConceptProperty"
    assert p["id"] == "BiomedicalConceptProperty_1"
    assert p["name"] == "Height"
    assert p["label"] == "Height"
    assert p["datatype"] == "decimal"
    assert p["isRequired"] is True
    assert p["isEnabled"] is True
    assert p["responseCodes"] == []
    assert p["extensionAttributes"] == []
    assert p["notes"] == []

    code = p["code"]
    assert code["instanceType"] == "AliasCode"
    assert code["id"].startswith("AliasCode_")

    sc = code["standardCode"]
    assert sc["instanceType"] == "Code"
    assert sc["id"].startswith("Code_")
    assert sc["code"] == "C25347"
    assert sc["codeSystem"] == "https://evsexplore.semantics.cancer.gov"
    assert sc["codeSystemVersion"] == "1"
    assert sc["decode"] == "Height"
