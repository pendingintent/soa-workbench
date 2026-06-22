"""Tests for the BiomedicalConceptProperty USDM generator."""

import json
from unittest.mock import patch

from soa_builder.web.app import app  # noqa: F401  (triggers DB init)
from soa_builder.web.db import _connect
from usdm.generate_biomedical_concept_properties import (
    build_usdm_biomedical_concept_properties,
    build_usdm_biomedical_concept_properties_for_soa,
    populate_biomedical_concept_properties,
    populate_biomedical_concept_properties_for_all_soas,
    populate_biomedical_concept_properties_for_bc,
)

SOA_ID = 9001
BC_UID = "BiomedicalConcept_1"
CONCEPT_CODE = "C25564"
FAKE_DSS_HREF = "https://api.library.cdisc.org/api/cosmos/v2/mdr/specializations/sdtm/datasetspecializations/VS"

MOCK_BC_API = {
    "_links": {
        "parentPackage": {"href": "/mdr/bc/packages/2024-09-27/biomedicalconcepts"}
    },
    "href": "https://example/bc/C25564",
    "synonyms": ["Demo"],
    "dataElementConcepts": [
        {"conceptId": "C25347", "shortName": "Height", "dataType": "decimal"},
        {"conceptId": "C25208", "shortName": "Weight", "dataType": "decimal"},
        {"conceptId": "C49669", "shortName": "Unit", "dataType": "string"},
    ],
}

MOCK_BC_WITH_EXAMPLE_SET = {
    "_links": {
        "parentPackage": {"href": "/mdr/bc/packages/2024-09-27/biomedicalconcepts"}
    },
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
    "_links": {
        "parentPackage": {
            "href": "/mdr/specializations/sdtm/packages/2024-09-27/datasetspecializations"
        }
    },
    "shortName": "VS",
    "variables": [
        {
            "name": "VSORRES",
            "dataType": "string",
            "mandatoryValue": True,
            "dataElementConceptId": "C25347",
        },
        {
            "name": "VSORRESU",
            "dataType": "string",
            "mandatoryValue": False,
            "dataElementConceptId": "C49669",
        },
    ],
}

MOCK_SDTM_WITH_ASSIGNED_TERM = {
    "_links": {
        "parentPackage": {
            "href": "/mdr/specializations/sdtm/packages/2024-09-27/datasetspecializations"
        }
    },
    "shortName": "VS",
    "variables": [
        {
            "name": "VSORRES",
            "dataType": "string",
            "mandatoryValue": True,
            "dataElementConceptId": "C25347",
        },
        {
            "name": "VSSTAT",
            "dataType": "string",
            "mandatoryValue": False,
            "dataElementConceptId": "C25208",
            "assignedTerm": {"conceptId": "C61585", "value": "NOT DONE"},
        },
    ],
}


def _seed_bc(soa_id=SOA_ID, bc_uid=BC_UID, concept_code=CONCEPT_CODE):
    conn = _connect()
    cur = conn.cursor()
    cur.execute("DELETE FROM bcp_response_code WHERE soa_id=?", (soa_id,))
    cur.execute("DELETE FROM biomedical_concept_property WHERE soa_id=?", (soa_id,))
    cur.execute("DELETE FROM biomedical_concept WHERE soa_id=?", (soa_id,))
    cur.execute("DELETE FROM activity_concept WHERE soa_id=?", (soa_id,))
    cur.execute("DELETE FROM alias_code WHERE soa_id=?", (soa_id,))
    cur.execute("DELETE FROM code WHERE soa_id=?", (soa_id,))

    cur.execute(
        "INSERT INTO code (code_uid, soa_id, code, code_system,"
        " code_system_version, decode) VALUES (?, ?, ?, ?, ?, ?)",
        ("Code_1", soa_id, concept_code, "http://www.cdisc.org", "v1", "Vital Signs"),
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
        " (soa_id, activity_uid, concept_uid, concept_code, concept_title)"
        " VALUES (?, ?, ?, ?, ?)",
        (soa_id, 1, bc_uid, concept_code, "Vital Signs"),
    )
    conn.commit()
    conn.close()


# ---------------------------------------------------------------------------
# Generic path (no DSS associated)
# ---------------------------------------------------------------------------


def test_populate_creates_property_rows_with_uids():
    _seed_bc()
    with (
        patch(
            "usdm.generate_biomedical_concept_properties._get_biomedical_concept_data",
            return_value=MOCK_BC_API,
        ),
        patch(
            "usdm.generate_biomedical_concept_properties._get_dss_href_for_bc",
            return_value="",
        ),
    ):
        populate_biomedical_concept_properties(SOA_ID)

    conn = _connect()
    cur = conn.cursor()
    cur.execute(
        "SELECT biomedical_concept_property_uid, name, datatype"
        " FROM biomedical_concept_property"
        " WHERE soa_id=? AND biomedical_concept_uid=? ORDER BY id",
        (SOA_ID, BC_UID),
    )
    rows = cur.fetchall()
    conn.close()

    assert len(rows) == 3
    assert [r[0] for r in rows] == [
        "BiomedicalConceptProperty_1",
        "BiomedicalConceptProperty_2",
        "BiomedicalConceptProperty_3",
    ]
    assert [r[1] for r in rows] == ["Height", "Weight", "Unit"]
    assert [r[2] for r in rows] == ["decimal", "decimal", "string"]


def test_populate_delete_then_recreate_no_duplicates():
    _seed_bc()
    with (
        patch(
            "usdm.generate_biomedical_concept_properties._get_biomedical_concept_data",
            return_value=MOCK_BC_API,
        ),
        patch(
            "usdm.generate_biomedical_concept_properties._get_dss_href_for_bc",
            return_value="",
        ),
    ):
        populate_biomedical_concept_properties(SOA_ID)
        populate_biomedical_concept_properties(SOA_ID)

    conn = _connect()
    cur = conn.cursor()
    cur.execute(
        "SELECT COUNT(*) FROM biomedical_concept_property WHERE soa_id=?", (SOA_ID,)
    )
    assert cur.fetchone()[0] == 3
    conn.close()


def test_generic_path_decode_equals_shortname():
    _seed_bc()
    with (
        patch(
            "usdm.generate_biomedical_concept_properties._get_biomedical_concept_data",
            return_value=MOCK_BC_API,
        ),
        patch(
            "usdm.generate_biomedical_concept_properties._get_dss_href_for_bc",
            return_value="",
        ),
    ):
        populate_biomedical_concept_properties_for_bc(SOA_ID, BC_UID, CONCEPT_CODE)

    conn = _connect()
    cur = conn.cursor()
    cur.execute(
        "SELECT c.decode FROM biomedical_concept_property bcp"
        " JOIN alias_code ac ON bcp.code=ac.alias_code_uid AND bcp.soa_id=ac.soa_id"
        " JOIN code c ON ac.standard_code=c.code_uid AND ac.soa_id=c.soa_id"
        " WHERE bcp.soa_id=? ORDER BY bcp.id",
        (SOA_ID,),
    )
    decodes = [r[0] for r in cur.fetchall()]
    conn.close()
    assert decodes == ["Height", "Weight", "Unit"]


def test_generic_path_code_system_version_from_parent_package():
    _seed_bc()
    with (
        patch(
            "usdm.generate_biomedical_concept_properties._get_biomedical_concept_data",
            return_value=MOCK_BC_API,
        ),
        patch(
            "usdm.generate_biomedical_concept_properties._get_dss_href_for_bc",
            return_value="",
        ),
    ):
        populate_biomedical_concept_properties_for_bc(SOA_ID, BC_UID, CONCEPT_CODE)

    conn = _connect()
    cur = conn.cursor()
    cur.execute(
        "SELECT DISTINCT c.code_system_version FROM biomedical_concept_property bcp"
        " JOIN alias_code ac ON bcp.code=ac.alias_code_uid AND bcp.soa_id=ac.soa_id"
        " JOIN code c ON ac.standard_code=c.code_uid AND ac.soa_id=c.soa_id"
        " WHERE bcp.soa_id=?",
        (SOA_ID,),
    )
    versions = [r[0] for r in cur.fetchall()]
    conn.close()
    assert versions == ["2024-09-27"]


def test_generic_path_no_response_codes():
    _seed_bc()
    with (
        patch(
            "usdm.generate_biomedical_concept_properties._get_biomedical_concept_data",
            return_value=MOCK_BC_WITH_EXAMPLE_SET,
        ),
        patch(
            "usdm.generate_biomedical_concept_properties._get_dss_href_for_bc",
            return_value="",
        ),
    ):
        populate_biomedical_concept_properties_for_bc(SOA_ID, BC_UID, CONCEPT_CODE)

    conn = _connect()
    cur = conn.cursor()
    cur.execute("SELECT COUNT(*) FROM bcp_response_code WHERE soa_id=?", (SOA_ID,))
    assert cur.fetchone()[0] == 0
    conn.close()


# ---------------------------------------------------------------------------
# SDTM path (DSS associated via activity_concept_dss)
# ---------------------------------------------------------------------------


def test_sdtm_path_uses_variable_names():
    _seed_bc()
    with (
        patch(
            "usdm.generate_biomedical_concept_properties._get_biomedical_concept_data",
            return_value=MOCK_BC_API,
        ),
        patch(
            "usdm.generate_biomedical_concept_properties._get_dss_href_for_bc",
            return_value=FAKE_DSS_HREF,
        ),
        patch(
            "usdm.generate_biomedical_concept_properties._fetch_dss_spec",
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
    conn.close()
    assert names == ["VSORRES", "VSORRESU"]


def test_sdtm_path_decode_is_dec_shortname():
    _seed_bc()
    with (
        patch(
            "usdm.generate_biomedical_concept_properties._get_biomedical_concept_data",
            return_value=MOCK_BC_API,
        ),
        patch(
            "usdm.generate_biomedical_concept_properties._get_dss_href_for_bc",
            return_value=FAKE_DSS_HREF,
        ),
        patch(
            "usdm.generate_biomedical_concept_properties._fetch_dss_spec",
            return_value=MOCK_SDTM_API,
        ),
    ):
        populate_biomedical_concept_properties_for_bc(SOA_ID, BC_UID, CONCEPT_CODE)

    conn = _connect()
    cur = conn.cursor()
    cur.execute(
        "SELECT c.decode FROM biomedical_concept_property bcp"
        " JOIN alias_code ac ON bcp.code=ac.alias_code_uid AND bcp.soa_id=ac.soa_id"
        " JOIN code c ON ac.standard_code=c.code_uid AND ac.soa_id=c.soa_id"
        " WHERE bcp.soa_id=? ORDER BY bcp.id",
        (SOA_ID,),
    )
    decodes = [r[0] for r in cur.fetchall()]
    conn.close()
    assert decodes == ["Height", "Unit"]


def test_sdtm_mandatory_value_maps_to_is_required():
    _seed_bc()
    with (
        patch(
            "usdm.generate_biomedical_concept_properties._get_biomedical_concept_data",
            return_value=MOCK_BC_API,
        ),
        patch(
            "usdm.generate_biomedical_concept_properties._get_dss_href_for_bc",
            return_value=FAKE_DSS_HREF,
        ),
        patch(
            "usdm.generate_biomedical_concept_properties._fetch_dss_spec",
            return_value=MOCK_SDTM_API,
        ),
    ):
        populate_biomedical_concept_properties_for_bc(SOA_ID, BC_UID, CONCEPT_CODE)

    conn = _connect()
    cur = conn.cursor()
    cur.execute(
        "SELECT name, isRequired FROM biomedical_concept_property"
        " WHERE soa_id=? ORDER BY id",
        (SOA_ID,),
    )
    rows = {r[0]: r[1] for r in cur.fetchall()}
    conn.close()
    assert rows["VSORRES"] == 1
    assert rows["VSORRESU"] == 0


def test_sdtm_code_system_version_from_parent_package():
    _seed_bc()
    with (
        patch(
            "usdm.generate_biomedical_concept_properties._get_biomedical_concept_data",
            return_value=MOCK_BC_API,
        ),
        patch(
            "usdm.generate_biomedical_concept_properties._get_dss_href_for_bc",
            return_value=FAKE_DSS_HREF,
        ),
        patch(
            "usdm.generate_biomedical_concept_properties._fetch_dss_spec",
            return_value=MOCK_SDTM_API,
        ),
    ):
        populate_biomedical_concept_properties_for_bc(SOA_ID, BC_UID, CONCEPT_CODE)

    conn = _connect()
    cur = conn.cursor()
    cur.execute(
        "SELECT DISTINCT c.code_system_version FROM biomedical_concept_property bcp"
        " JOIN alias_code ac ON bcp.code=ac.alias_code_uid AND bcp.soa_id=ac.soa_id"
        " JOIN code c ON ac.standard_code=c.code_uid AND ac.soa_id=c.soa_id"
        " WHERE bcp.soa_id=?",
        (SOA_ID,),
    )
    versions = [r[0] for r in cur.fetchall()]
    conn.close()
    assert versions == ["2024-09-27"]


def test_sdtm_assigned_term_creates_response_code():
    _seed_bc()
    with (
        patch(
            "usdm.generate_biomedical_concept_properties._get_biomedical_concept_data",
            return_value=MOCK_BC_API,
        ),
        patch(
            "usdm.generate_biomedical_concept_properties._get_dss_href_for_bc",
            return_value=FAKE_DSS_HREF,
        ),
        patch(
            "usdm.generate_biomedical_concept_properties._fetch_dss_spec",
            return_value=MOCK_SDTM_WITH_ASSIGNED_TERM,
        ),
    ):
        populate_biomedical_concept_properties_for_bc(SOA_ID, BC_UID, CONCEPT_CODE)

    conn = _connect()
    cur = conn.cursor()
    cur.execute(
        "SELECT rc.name, c.code, c.decode"
        " FROM bcp_response_code rc"
        " JOIN alias_code ac ON rc.code=ac.alias_code_uid AND rc.soa_id=ac.soa_id"
        " JOIN code c ON ac.standard_code=c.code_uid AND ac.soa_id=c.soa_id"
        " WHERE rc.soa_id=?",
        (SOA_ID,),
    )
    rc_rows = cur.fetchall()
    conn.close()

    assert len(rc_rows) == 1
    name, code, decode = rc_rows[0]
    assert name == "NOT DONE"
    assert code == "C61585"
    assert decode == "NOT DONE"


def test_sdtm_skip_variable_without_dec_match():
    _seed_bc()
    sdtm_with_unmatched = {
        "_links": {
            "parentPackage": {
                "href": "/mdr/specializations/sdtm/packages/2024-09-27/datasetspecializations"
            }
        },
        "variables": [
            {"name": "VSORRES", "dataType": "string", "dataElementConceptId": "C99999"},
            {
                "name": "VSORRESU",
                "dataType": "string",
                "dataElementConceptId": "C25347",
            },
        ],
    }
    with (
        patch(
            "usdm.generate_biomedical_concept_properties._get_biomedical_concept_data",
            return_value=MOCK_BC_API,
        ),
        patch(
            "usdm.generate_biomedical_concept_properties._get_dss_href_for_bc",
            return_value=FAKE_DSS_HREF,
        ),
        patch(
            "usdm.generate_biomedical_concept_properties._fetch_dss_spec",
            return_value=sdtm_with_unmatched,
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
    conn.close()
    assert names == ["VSORRESU"]


def test_sdtm_skip_variable_without_dec_id():
    _seed_bc()
    sdtm_no_dec_id = {
        "_links": {
            "parentPackage": {
                "href": "/mdr/specializations/sdtm/packages/2024-09-27/datasetspecializations"
            }
        },
        "variables": [
            {"name": "VSORRES", "dataType": "string"},
            {
                "name": "VSORRESU",
                "dataType": "string",
                "dataElementConceptId": "C25347",
            },
        ],
    }
    with (
        patch(
            "usdm.generate_biomedical_concept_properties._get_biomedical_concept_data",
            return_value=MOCK_BC_API,
        ),
        patch(
            "usdm.generate_biomedical_concept_properties._get_dss_href_for_bc",
            return_value=FAKE_DSS_HREF,
        ),
        patch(
            "usdm.generate_biomedical_concept_properties._fetch_dss_spec",
            return_value=sdtm_no_dec_id,
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
    conn.close()
    assert names == ["VSORRESU"]


# ---------------------------------------------------------------------------
# USDM output shape
# ---------------------------------------------------------------------------


def test_build_returns_usdm_shape():
    _seed_bc()
    with (
        patch(
            "usdm.generate_biomedical_concept_properties._get_biomedical_concept_data",
            return_value=MOCK_BC_API,
        ),
        patch(
            "usdm.generate_biomedical_concept_properties._get_dss_href_for_bc",
            return_value="",
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

    sc = p["code"]["standardCode"]
    assert sc["code"] == "C25347"
    assert sc["codeSystem"] == "http://www.cdisc.org"
    assert sc["codeSystemVersion"] == "2024-09-27"
    assert sc["decode"] == "Height"


def test_build_sdtm_includes_assigned_term_response_code():
    _seed_bc()
    with (
        patch(
            "usdm.generate_biomedical_concept_properties._get_biomedical_concept_data",
            return_value=MOCK_BC_API,
        ),
        patch(
            "usdm.generate_biomedical_concept_properties._get_dss_href_for_bc",
            return_value=FAKE_DSS_HREF,
        ),
        patch(
            "usdm.generate_biomedical_concept_properties._fetch_dss_spec",
            return_value=MOCK_SDTM_WITH_ASSIGNED_TERM,
        ),
    ):
        populate_biomedical_concept_properties_for_bc(SOA_ID, BC_UID, CONCEPT_CODE)

    out = build_usdm_biomedical_concept_properties(SOA_ID, BC_UID)
    assert len(out) == 2

    vsstat = next(p for p in out if p["name"] == "VSSTAT")
    rcs = vsstat["responseCodes"]
    assert len(rcs) == 1
    rc = rcs[0]
    assert rc["instanceType"] == "ResponseCode"
    assert rc["name"] == "NOT DONE"
    assert rc["code"]["code"] == "C61585"

    vsorres = next(p for p in out if p["name"] == "VSORRES")
    assert vsorres["responseCodes"] == []


def test_build_usdm_for_soa_is_json_serialisable():
    _seed_bc()
    with (
        patch(
            "usdm.generate_biomedical_concept_properties._get_biomedical_concept_data",
            return_value=MOCK_BC_API,
        ),
        patch(
            "usdm.generate_biomedical_concept_properties._get_dss_href_for_bc",
            return_value="",
        ),
    ):
        populate_biomedical_concept_properties_for_bc(SOA_ID, BC_UID, CONCEPT_CODE)
        out = build_usdm_biomedical_concept_properties_for_soa(SOA_ID)

    serialised = json.dumps(out)
    assert isinstance(serialised, str)
    assert "BiomedicalConceptProperty" in serialised


# ---------------------------------------------------------------------------
# Issue #218: Validate BCP output correctness against Dave's reference
# ---------------------------------------------------------------------------

# Dave Iberson-Hurst's exact exclusion list from usdm_excel/cdisc_bc_library.py
# (_process_property method).  Our frozenset must stay in sync with it.
_DAVE_EXCLUDED_SUFFIXES = [
    "TEST",
    "STRESN",
    "STRESU",
    "STRESC",
    "CLASCD",
    "LOINC",
    "LOT",
    "CAT",
    "SCAT",
    "LLT",
    "LLTCD",
    "HLT",
    "HLTCD",
    "PTCD",
    "BODSYS",
    "BDSYCD",
    "SOC",
    "SOCCD",
    "RLDEV",
]
_DAVE_EXCLUDED_NAMES = ["EPOCH"]

MOCK_SDTM_WITH_DSS_DATATYPE = {
    "_links": {
        "parentPackage": {
            "href": (
                "/mdr/specializations/sdtm/packages/2024-09-27/datasetspecializations"
            )
        }
    },
    "shortName": "VS",
    "variables": [
        {
            "name": "VSORRES",
            # DSS gives a more specific type ("float") than the DEC ("decimal")
            "dataType": "float",
            "mandatoryValue": True,
            "dataElementConceptId": "C25347",
        },
        {
            "name": "VSORRESU",
            "dataType": "string",
            "mandatoryValue": False,
            "dataElementConceptId": "C49669",
        },
    ],
}


def test_exclusion_list_matches_dave_reference():
    """Our _EXCLUDED_PROPERTY_SUFFIXES must exactly match Dave's list."""
    from usdm.generate_biomedical_concept_properties import (
        _EXCLUDED_PROPERTY_NAMES,
        _EXCLUDED_PROPERTY_SUFFIXES,
    )

    assert _EXCLUDED_PROPERTY_SUFFIXES == frozenset(_DAVE_EXCLUDED_SUFFIXES), (
        "Exclusion suffix list has drifted from Dave Iberson-Hurst's "
        "cdisc_bc_library._process_property reference"
    )
    assert _EXCLUDED_PROPERTY_NAMES == frozenset(_DAVE_EXCLUDED_NAMES), (
        "Exclusion name list has drifted from Dave's reference"
    )


def test_all_excluded_suffixes_are_filtered():
    """Every suffix in Dave's list must cause _include_property to return False."""
    from usdm.generate_biomedical_concept_properties import _include_property

    for suffix in _DAVE_EXCLUDED_SUFFIXES:
        var_name = f"LB{suffix}"
        assert not _include_property(var_name), (
            f"Expected {var_name!r} to be excluded but _include_property returned True"
        )
    assert not _include_property("EPOCH")


def test_non_excluded_variables_are_included():
    """Key data-carrying variables must pass the filter."""
    from usdm.generate_biomedical_concept_properties import _include_property

    for var in ("VSORRES", "VSORRESU", "LBORRES", "LBORRESU", "LBSPEC", "LBDTC"):
        assert _include_property(var), f"Expected {var!r} to be included"


def test_all_bcp_usdm_attributes_present():
    """Every BCP in the USDM output must carry all required attributes."""
    _seed_bc()
    with (
        patch(
            "usdm.generate_biomedical_concept_properties._get_biomedical_concept_data",
            return_value=MOCK_BC_API,
        ),
        patch(
            "usdm.generate_biomedical_concept_properties._get_dss_href_for_bc",
            return_value="",
        ),
    ):
        populate_biomedical_concept_properties_for_bc(SOA_ID, BC_UID, CONCEPT_CODE)

    out = build_usdm_biomedical_concept_properties(SOA_ID, BC_UID)
    assert out, "Expected at least one BCP in output"

    required_keys = {
        "id",
        "extensionAttributes",
        "name",
        "label",
        "isRequired",
        "isEnabled",
        "datatype",
        "responseCodes",
        "code",
        "notes",
        "instanceType",
    }
    for bcp in out:
        missing = required_keys - bcp.keys()
        assert not missing, f"BCP {bcp['id']} missing keys: {missing}"
        assert bcp["instanceType"] == "BiomedicalConceptProperty"
        assert isinstance(bcp["isRequired"], bool)
        assert isinstance(bcp["isEnabled"], bool)
        assert isinstance(bcp["responseCodes"], list)
        assert isinstance(bcp["notes"], list)
        code_block = bcp["code"]
        assert "standardCode" in code_block
        assert code_block["instanceType"] == "AliasCode"
        sc = code_block["standardCode"]
        assert sc["instanceType"] == "Code"
        assert sc["codeSystem"] == "http://www.cdisc.org"


def test_is_enabled_always_true():
    """isEnabled must be True for all generated BCPs."""
    _seed_bc()
    with (
        patch(
            "usdm.generate_biomedical_concept_properties._get_biomedical_concept_data",
            return_value=MOCK_BC_API,
        ),
        patch(
            "usdm.generate_biomedical_concept_properties._get_dss_href_for_bc",
            return_value="",
        ),
    ):
        populate_biomedical_concept_properties_for_bc(SOA_ID, BC_UID, CONCEPT_CODE)

    out = build_usdm_biomedical_concept_properties(SOA_ID, BC_UID)
    for bcp in out:
        assert bcp["isEnabled"] is True, (
            f"BCP {bcp['id']} has isEnabled={bcp['isEnabled']!r}, expected True"
        )


def test_mandatory_value_false_sets_is_required_false():
    """mandatoryValue: false in DSS must produce isRequired: False in USDM."""
    _seed_bc()
    with (
        patch(
            "usdm.generate_biomedical_concept_properties._get_biomedical_concept_data",
            return_value=MOCK_BC_API,
        ),
        patch(
            "usdm.generate_biomedical_concept_properties._get_dss_href_for_bc",
            return_value=FAKE_DSS_HREF,
        ),
        patch(
            "usdm.generate_biomedical_concept_properties._fetch_dss_spec",
            return_value=MOCK_SDTM_API,
        ),
    ):
        populate_biomedical_concept_properties_for_bc(SOA_ID, BC_UID, CONCEPT_CODE)

    out = build_usdm_biomedical_concept_properties(SOA_ID, BC_UID)
    vsorresu = next(p for p in out if p["name"] == "VSORRESU")
    assert vsorresu["isRequired"] is False, (
        "mandatoryValue: false must map to isRequired: False"
    )
    vsorres = next(p for p in out if p["name"] == "VSORRES")
    assert vsorres["isRequired"] is True


def test_datatype_from_dss_variable_preferred_over_dec():
    """DSS variable dataType (specific) must take priority over DEC dataType."""
    _seed_bc()
    # MOCK_BC_API has Height DEC with dataType "decimal"
    # MOCK_SDTM_WITH_DSS_DATATYPE overrides VSORRES to "float"
    with (
        patch(
            "usdm.generate_biomedical_concept_properties._get_biomedical_concept_data",
            return_value=MOCK_BC_API,
        ),
        patch(
            "usdm.generate_biomedical_concept_properties._get_dss_href_for_bc",
            return_value=FAKE_DSS_HREF,
        ),
        patch(
            "usdm.generate_biomedical_concept_properties._fetch_dss_spec",
            return_value=MOCK_SDTM_WITH_DSS_DATATYPE,
        ),
    ):
        populate_biomedical_concept_properties_for_bc(SOA_ID, BC_UID, CONCEPT_CODE)

    out = build_usdm_biomedical_concept_properties(SOA_ID, BC_UID)
    vsorres = next(p for p in out if p["name"] == "VSORRES")
    assert vsorres["datatype"] == "float", (
        f"Expected DSS dataType 'float', got {vsorres['datatype']!r}"
    )


def test_backfill_skips_already_populated_bcs():
    """populate_biomedical_concept_properties_for_all_soas must not re-fetch
    BCs that already have property rows in the database."""
    _seed_bc()
    call_count = 0

    def _mock_fetch(code):
        nonlocal call_count
        call_count += 1
        return MOCK_BC_API

    # First call: nothing populated yet, so backfill should fetch.
    with (
        patch(
            "usdm.generate_biomedical_concept_properties._get_biomedical_concept_data",
            side_effect=_mock_fetch,
        ),
        patch(
            "usdm.generate_biomedical_concept_properties._get_dss_href_for_bc",
            return_value="",
        ),
    ):
        populate_biomedical_concept_properties_for_all_soas()

    assert call_count == 1, "Expected one API call on first backfill"
    first_run_calls = call_count

    # Second call: properties already exist, backfill must not fetch again.
    with (
        patch(
            "usdm.generate_biomedical_concept_properties._get_biomedical_concept_data",
            side_effect=_mock_fetch,
        ),
        patch(
            "usdm.generate_biomedical_concept_properties._get_dss_href_for_bc",
            return_value="",
        ),
    ):
        populate_biomedical_concept_properties_for_all_soas()

    assert call_count == first_run_calls, (
        "Backfill must not re-fetch BCs that are already populated"
    )
