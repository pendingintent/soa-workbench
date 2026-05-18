"""Tests for the BiomedicalConceptProperty USDM generator."""

import json
from unittest.mock import patch

from soa_builder.web.app import app  # noqa: F401  (triggers DB init)
from soa_builder.web.db import _connect
from usdm.generate_biomedical_concept_properties import (
    build_usdm_biomedical_concept_properties,
    build_usdm_biomedical_concept_properties_for_soa,
    populate_biomedical_concept_properties,
    populate_biomedical_concept_properties_for_bc,
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

MOCK_BC_WITH_EXAMPLE_SET = {
    "href": "https://example/bc/C66742",
    "synonyms": [],
    "dataElementConcepts": [
        {
            "conceptId": "C25347",
            "shortName": "Result",
            "dataType": "string",
            "exampleSet": ["C49488", "C49487"],
        },
    ],
}

MOCK_SDTM_API = {
    "shortName": "VS",
    "variables": [
        {
            "name": "VSORRES",
            "dataType": "string",
            "dataElementConceptId": "C25347",
            "valueList": [],
        },
        {
            "name": "VSORRESU",
            "dataType": "string",
            "dataElementConceptId": "C49669",
            "valueList": ["C28253", "C48155"],
            "codelist": {"conceptId": "C71620"},
        },
    ],
}


def _seed_bc(soa_id=SOA_ID, bc_uid=BC_UID, concept_code=CONCEPT_CODE):
    """Insert a single BC + activity_concept + alias_code + code chain."""
    conn = _connect()
    cur = conn.cursor()
    cur.execute("DELETE FROM bcp_response_code WHERE soa_id=?", (soa_id,))
    cur.execute(
        "DELETE FROM biomedical_concept_property WHERE soa_id=?",
        (soa_id,),
    )
    cur.execute("DELETE FROM biomedical_concept WHERE soa_id=?", (soa_id,))
    cur.execute("DELETE FROM activity_concept WHERE soa_id=?", (soa_id,))
    cur.execute("DELETE FROM alias_code WHERE soa_id=?", (soa_id,))
    cur.execute("DELETE FROM code WHERE soa_id=?", (soa_id,))

    cur.execute(
        "INSERT INTO code (code_uid, soa_id, code, code_system,"
        " code_system_version, decode) VALUES (?, ?, ?, ?, ?, ?)",
        (
            "Code_1",
            soa_id,
            concept_code,
            "http://www.cdisc.org",
            "v1",
            "Vital Signs",
        ),
    )
    cur.execute(
        "INSERT INTO alias_code (alias_code_uid, soa_id, standard_code)"
        " VALUES (?, ?, ?)",
        ("AliasCode_1", soa_id, "Code_1"),
    )
    cur.execute(
        "INSERT INTO biomedical_concept"
        " (biomedical_concept_uid, soa_id, name, label, code)"
        " VALUES (?, ?, ?, ?, ?)",
        (bc_uid, soa_id, "Vital Signs", "VS", "AliasCode_1"),
    )
    cur.execute(
        "INSERT INTO activity_concept"
        " (soa_id, activity_uid, concept_uid, concept_code,"
        " concept_title) VALUES (?, ?, ?, ?, ?)",
        (soa_id, 1, bc_uid, concept_code, "Vital Signs"),
    )
    conn.commit()
    conn.close()


# ---------------------------------------------------------------------------
# Tests: populate_biomedical_concept_properties (SOA-wide, lazy path)
# ---------------------------------------------------------------------------


def test_populate_creates_property_rows_with_uids():
    _seed_bc()
    with (
        patch(
            "usdm.generate_biomedical_concept_properties._get_biomedical_concept_data",
            return_value=MOCK_BC_API,
        ),
        patch(
            "usdm.usdm_utils._get_sdtm_package_specialization_index",
            return_value={},
        ),
        patch(
            "usdm.generate_biomedical_concept_properties._get_sdtm_specialization_data",
            return_value={},
        ),
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
    for _, _, _, alias in rows:
        assert alias.startswith("AliasCode_")
        assert int(alias.split("_")[1]) >= 2


def test_populate_is_idempotent():
    _seed_bc()
    with (
        patch(
            "usdm.generate_biomedical_concept_properties._get_biomedical_concept_data",
            return_value=MOCK_BC_API,
        ),
        patch(
            "usdm.usdm_utils._get_sdtm_package_specialization_index",
            return_value={},
        ),
        patch(
            "usdm.generate_biomedical_concept_properties._get_sdtm_specialization_data",
            return_value={},
        ),
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


# ---------------------------------------------------------------------------
# Tests: populate_biomedical_concept_properties_for_bc (scoped, eager path)
# ---------------------------------------------------------------------------


def test_scoped_populate_creates_bcp_rows():
    _seed_bc()
    with (
        patch(
            "usdm.generate_biomedical_concept_properties._get_biomedical_concept_data",
            return_value=MOCK_BC_API,
        ),
        patch(
            "usdm.usdm_utils._get_sdtm_package_specialization_index",
            return_value={},
        ),
        patch(
            "usdm.generate_biomedical_concept_properties._get_sdtm_specialization_data",
            return_value={},
        ),
    ):
        populate_biomedical_concept_properties_for_bc(SOA_ID, BC_UID, CONCEPT_CODE)

    conn = _connect()
    cur = conn.cursor()
    cur.execute(
        "SELECT COUNT(*) FROM biomedical_concept_property WHERE soa_id=?",
        (SOA_ID,),
    )
    assert cur.fetchone()[0] == 3
    conn.close()


def test_scoped_populate_is_idempotent():
    _seed_bc()
    with (
        patch(
            "usdm.generate_biomedical_concept_properties._get_biomedical_concept_data",
            return_value=MOCK_BC_API,
        ),
        patch(
            "usdm.usdm_utils._get_sdtm_package_specialization_index",
            return_value={},
        ),
        patch(
            "usdm.generate_biomedical_concept_properties._get_sdtm_specialization_data",
            return_value={},
        ),
    ):
        populate_biomedical_concept_properties_for_bc(SOA_ID, BC_UID, CONCEPT_CODE)
        populate_biomedical_concept_properties_for_bc(SOA_ID, BC_UID, CONCEPT_CODE)

    conn = _connect()
    cur = conn.cursor()
    cur.execute(
        "SELECT COUNT(*) FROM biomedical_concept_property WHERE soa_id=?",
        (SOA_ID,),
    )
    assert cur.fetchone()[0] == 3
    conn.close()


def test_scoped_populate_creates_response_codes_from_example_set():
    _seed_bc()
    with (
        patch(
            "usdm.generate_biomedical_concept_properties._get_biomedical_concept_data",
            return_value=MOCK_BC_WITH_EXAMPLE_SET,
        ),
        patch(
            "usdm.usdm_utils._get_sdtm_package_specialization_index",
            return_value={},
        ),
        patch(
            "usdm.generate_biomedical_concept_properties._get_sdtm_specialization_data",
            return_value={},
        ),
    ):
        populate_biomedical_concept_properties_for_bc(SOA_ID, BC_UID, CONCEPT_CODE)

    conn = _connect()
    cur = conn.cursor()
    cur.execute(
        "SELECT COUNT(*) FROM bcp_response_code WHERE soa_id=?",
        (SOA_ID,),
    )
    count = cur.fetchone()[0]
    conn.close()
    assert count == 2


def test_scoped_populate_no_response_codes_when_example_set_absent():
    _seed_bc()
    with (
        patch(
            "usdm.generate_biomedical_concept_properties._get_biomedical_concept_data",
            return_value=MOCK_BC_API,
        ),
        patch(
            "usdm.usdm_utils._get_sdtm_package_specialization_index",
            return_value={},
        ),
        patch(
            "usdm.generate_biomedical_concept_properties._get_sdtm_specialization_data",
            return_value={},
        ),
    ):
        populate_biomedical_concept_properties_for_bc(SOA_ID, BC_UID, CONCEPT_CODE)

    conn = _connect()
    cur = conn.cursor()
    cur.execute(
        "SELECT COUNT(*) FROM bcp_response_code WHERE soa_id=?",
        (SOA_ID,),
    )
    count = cur.fetchone()[0]
    conn.close()
    assert count == 0


def test_sdtm_path_uses_variables_array():
    _seed_bc()
    with (
        patch(
            "usdm.generate_biomedical_concept_properties._get_biomedical_concept_data",
            return_value=MOCK_BC_API,
        ),
        patch(
            "usdm.generate_biomedical_concept_properties._get_sdtm_specialization_data",
            return_value=MOCK_SDTM_API,
        ),
    ):
        populate_biomedical_concept_properties_for_bc(SOA_ID, BC_UID, CONCEPT_CODE)

    conn = _connect()
    cur = conn.cursor()
    cur.execute(
        "SELECT name FROM biomedical_concept_property WHERE soa_id=? ORDER BY id",
        (SOA_ID,),
    )
    names = [r[0] for r in cur.fetchall()]

    cur.execute(
        "SELECT COUNT(*) FROM bcp_response_code WHERE soa_id=?",
        (SOA_ID,),
    )
    rc_count = cur.fetchone()[0]
    conn.close()

    assert names == ["VSORRES", "VSORRESU"]
    assert rc_count == 2  # from VSORRESU.valueList


# ---------------------------------------------------------------------------
# Tests: build_usdm_biomedical_concept_properties (USDM output)
# ---------------------------------------------------------------------------


def test_build_returns_usdm_shape():
    _seed_bc()
    with (
        patch(
            "usdm.generate_biomedical_concept_properties._get_biomedical_concept_data",
            return_value=MOCK_BC_API,
        ),
        patch(
            "usdm.usdm_utils._get_sdtm_package_specialization_index",
            return_value={},
        ),
        patch(
            "usdm.generate_biomedical_concept_properties._get_sdtm_specialization_data",
            return_value={},
        ),
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
    assert sc["codeSystem"] == "http://www.cdisc.org"
    assert sc["decode"] == "Height"


def test_build_includes_response_codes_when_present():
    _seed_bc()
    with (
        patch(
            "usdm.generate_biomedical_concept_properties._get_biomedical_concept_data",
            return_value=MOCK_BC_WITH_EXAMPLE_SET,
        ),
        patch(
            "usdm.usdm_utils._get_sdtm_package_specialization_index",
            return_value={},
        ),
        patch(
            "usdm.generate_biomedical_concept_properties._get_sdtm_specialization_data",
            return_value={},
        ),
    ):
        populate_biomedical_concept_properties_for_bc(SOA_ID, BC_UID, CONCEPT_CODE)

    out = build_usdm_biomedical_concept_properties(SOA_ID, BC_UID)
    assert len(out) == 1
    rcs = out[0]["responseCodes"]
    assert len(rcs) == 2
    assert all(rc["instanceType"] == "ResponseCode" for rc in rcs)
    assert all(rc["id"].startswith("ResponseCode_") for rc in rcs)


def test_build_usdm_for_soa_is_json_serialisable():
    _seed_bc()
    with (
        patch(
            "usdm.generate_biomedical_concept_properties._get_biomedical_concept_data",
            return_value=MOCK_BC_API,
        ),
        patch(
            "usdm.usdm_utils._get_sdtm_package_specialization_index",
            return_value={},
        ),
        patch(
            "usdm.generate_biomedical_concept_properties._get_sdtm_specialization_data",
            return_value={},
        ),
    ):
        # Pre-populate so the eager default finds rows
        populate_biomedical_concept_properties_for_bc(SOA_ID, BC_UID, CONCEPT_CODE)
        out = build_usdm_biomedical_concept_properties_for_soa(SOA_ID)

    serialised = json.dumps(out)
    assert isinstance(serialised, str)
    assert "BiomedicalConceptProperty" in serialised
