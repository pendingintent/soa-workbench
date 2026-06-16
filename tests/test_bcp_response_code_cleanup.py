"""Tests for orphaned/duplicate response-code cleanup and cascade.

Covers:
- ``sweep_orphaned_bcp_rows`` removes orphaned RC/BCP rows and their owned
  code chains while leaving live rows untouched.
- ``delete_bc_cascade`` removes a BC's BCP + RC rows.
- The soa_bundle import filter drops orphaned RCs and dedups duplicates.
"""

from soa_builder.web.app import app  # noqa: F401  (triggers DB init)
from soa_builder.web.db import _connect
from soa_builder.web.routers.soa_bundle import (
    _import_bc_properties_and_response_codes,
)
from usdm.generate_biomedical_concept_properties import (
    delete_bc_cascade,
    sweep_orphaned_bcp_rows,
)

SWEEP_SOA = 9201
IMPORT_SOA = 9202


def _reset(soa_id):
    conn = _connect()
    cur = conn.cursor()
    for table in (
        "bcp_response_code",
        "biomedical_concept_property",
        "biomedical_concept",
        "activity_concept",
        "alias_code",
        "code",
    ):
        cur.execute(f"DELETE FROM {table} WHERE soa_id=?", (soa_id,))
    conn.commit()
    conn.close()


def _add_code_chain(cur, soa_id, code_uid, alias_uid, ncit):
    cur.execute(
        "INSERT INTO code (code_uid, soa_id, code, code_system,"
        " code_system_version, decode) VALUES (?,?,?,?,?,?)",
        (code_uid, soa_id, ncit, "http://www.cdisc.org", "v1", ncit),
    )
    cur.execute(
        "INSERT INTO alias_code (alias_code_uid, soa_id, standard_code) VALUES (?,?,?)",
        (alias_uid, soa_id, code_uid),
    )


def _add_bcp(cur, soa_id, bc_uid, bcp_uid, alias_uid):
    cur.execute(
        "INSERT INTO biomedical_concept_property"
        " (soa_id, biomedical_concept_uid, biomedical_concept_property_uid,"
        " name, label, isRequired, isEnabled, datatype, code)"
        " VALUES (?,?,?,?,?,?,?,?,?)",
        (soa_id, bc_uid, bcp_uid, bcp_uid, bcp_uid, 0, 1, "string", alias_uid),
    )


def _add_rc(cur, soa_id, bcp_uid, rc_uid, alias_uid):
    cur.execute(
        "INSERT INTO bcp_response_code"
        " (soa_id, biomedical_concept_property_uid, response_code_uid,"
        " name, label, is_enabled, code) VALUES (?,?,?,?,?,?,?)",
        (soa_id, bcp_uid, rc_uid, rc_uid, rc_uid, 1, alias_uid),
    )


def _seed_sweep_fixture():
    """Live BC/BCP/RC plus an orphaned BCP and an orphaned RC."""
    _reset(SWEEP_SOA)
    conn = _connect()
    cur = conn.cursor()
    # Live BC + its code chain
    _add_code_chain(cur, SWEEP_SOA, "Code_L", "Alias_L", "C1")
    cur.execute(
        "INSERT INTO biomedical_concept"
        " (biomedical_concept_uid, soa_id, name, code) VALUES (?,?,?,?)",
        ("BiomedicalConcept_L", SWEEP_SOA, "Live BC", "Alias_L"),
    )
    # Live BCP + live RC
    _add_code_chain(cur, SWEEP_SOA, "Code_BCP", "Alias_BCP", "C2")
    _add_bcp(cur, SWEEP_SOA, "BiomedicalConcept_L", "BCP_Live", "Alias_BCP")
    _add_code_chain(cur, SWEEP_SOA, "Code_RC", "Alias_RC", "C3")
    _add_rc(cur, SWEEP_SOA, "BCP_Live", "RC_Live", "Alias_RC")
    # Orphaned BCP (parent BC absent) + its owned code chain
    _add_code_chain(cur, SWEEP_SOA, "Code_OBCP", "Alias_OBCP", "C4")
    _add_bcp(cur, SWEEP_SOA, "BiomedicalConcept_GONE", "BCP_Orphan", "Alias_OBCP")
    # Orphaned RC (parent BCP absent) + its owned code chain
    _add_code_chain(cur, SWEEP_SOA, "Code_ORC", "Alias_ORC", "C5")
    _add_rc(cur, SWEEP_SOA, "BCP_GONE", "RC_Orphan", "Alias_ORC")
    conn.commit()
    conn.close()


def _ids(soa_id, table, col):
    conn = _connect()
    cur = conn.cursor()
    cur.execute(f"SELECT {col} FROM {table} WHERE soa_id=? ORDER BY {col}", (soa_id,))
    out = [r[0] for r in cur.fetchall()]
    conn.close()
    return out


def test_sweep_removes_orphans_keeps_live():
    _seed_sweep_fixture()
    result = sweep_orphaned_bcp_rows(SWEEP_SOA)

    assert result == {"response_codes": 1, "properties": 1}
    assert _ids(
        SWEEP_SOA, "biomedical_concept_property", "biomedical_concept_property_uid"
    ) == ["BCP_Live"]
    assert _ids(SWEEP_SOA, "bcp_response_code", "response_code_uid") == ["RC_Live"]
    # Orphan-owned code chains are gone; live + BC chains remain.
    alias_uids = _ids(SWEEP_SOA, "alias_code", "alias_code_uid")
    assert "Alias_OBCP" not in alias_uids
    assert "Alias_ORC" not in alias_uids
    assert {"Alias_L", "Alias_BCP", "Alias_RC"} <= set(alias_uids)


def test_sweep_is_idempotent():
    _seed_sweep_fixture()
    sweep_orphaned_bcp_rows(SWEEP_SOA)
    again = sweep_orphaned_bcp_rows(SWEEP_SOA)
    assert again == {"response_codes": 0, "properties": 0}


def test_delete_bc_cascade_removes_bcp_and_rc():
    _seed_sweep_fixture()
    conn = _connect()
    cur = conn.cursor()
    delete_bc_cascade(cur, SWEEP_SOA, "BiomedicalConcept_L")
    conn.commit()
    conn.close()

    # Live BC's BCP + RC are gone; the orphan rows are untouched here.
    assert "BCP_Live" not in _ids(
        SWEEP_SOA, "biomedical_concept_property", "biomedical_concept_property_uid"
    )
    assert "RC_Live" not in _ids(SWEEP_SOA, "bcp_response_code", "response_code_uid")


def test_import_filters_orphans_and_dedups():
    _reset(IMPORT_SOA)
    bundle = {
        "biomedical_concept": [
            {
                "id": 1,
                "biomedical_concept_uid": "BiomedicalConcept_1",
                "name": "BC1",
                "label": "BC1",
                "code": "AliasCode_1",
            },
        ],
        "biomedical_concept_property": [
            # valid: parent BC present
            {
                "id": 1,
                "biomedical_concept_uid": "BiomedicalConcept_1",
                "biomedical_concept_property_uid": "BiomedicalConceptProperty_1",
                "name": "P1",
                "label": "P1",
                "description": None,
                "isRequired": 0,
                "isEnabled": 1,
                "datatype": "string",
                "code": "AliasCode_2",
            },
            # orphan: parent BC absent -> dropped
            {
                "id": 2,
                "biomedical_concept_uid": "BiomedicalConcept_GONE",
                "biomedical_concept_property_uid": "BiomedicalConceptProperty_2",
                "name": "P2",
                "label": "P2",
                "description": None,
                "isRequired": 0,
                "isEnabled": 1,
                "datatype": "string",
                "code": "AliasCode_3",
            },
        ],
        "bcp_response_code": [
            # valid RC on kept BCP
            {
                "id": 1,
                "biomedical_concept_property_uid": "BiomedicalConceptProperty_1",
                "response_code_uid": "ResponseCode_1",
                "name": "R1",
                "label": "R1",
                "is_enabled": 1,
                "code": "AliasCode_10",
            },
            # duplicate of the valid RC (same property + resolved code)
            {
                "id": 2,
                "biomedical_concept_property_uid": "BiomedicalConceptProperty_1",
                "response_code_uid": "ResponseCode_2",
                "name": "R1dup",
                "label": "R1dup",
                "is_enabled": 1,
                "code": "AliasCode_11",
            },
            # orphan RC: parent BCP was dropped -> dropped
            {
                "id": 3,
                "biomedical_concept_property_uid": "BiomedicalConceptProperty_2",
                "response_code_uid": "ResponseCode_3",
                "name": "R3",
                "label": "R3",
                "is_enabled": 1,
                "code": "AliasCode_12",
            },
        ],
        "alias_code": [
            {"alias_code_uid": "AliasCode_10", "standard_code": "Code_10"},
            {"alias_code_uid": "AliasCode_11", "standard_code": "Code_11"},
            {"alias_code_uid": "AliasCode_12", "standard_code": "Code_12"},
        ],
        # AliasCode_10 and _11 resolve to the same NCIt code -> duplicate.
        "code": [
            {"code_uid": "Code_10", "code": "C999"},
            {"code_uid": "Code_11", "code": "C999"},
            {"code_uid": "Code_12", "code": "C111"},
        ],
    }

    conn = _connect()
    cur = conn.cursor()
    _import_bc_properties_and_response_codes(cur, bundle, IMPORT_SOA)
    conn.commit()
    conn.close()

    # Only the BCP whose BC exists is imported.
    assert _ids(
        IMPORT_SOA, "biomedical_concept_property", "biomedical_concept_property_uid"
    ) == ["BiomedicalConceptProperty_1"]
    # Orphan RC dropped; duplicate collapsed to one.
    assert _ids(IMPORT_SOA, "bcp_response_code", "response_code_uid") == [
        "ResponseCode_1"
    ]
