"""codeSystemVersion on cdisc.org codes is a CT release date (#296)."""

import re

from fastapi.testclient import TestClient

from soa_builder.web.app import app
from soa_builder.web.db import _connect
from usdm import generate_biomedical_concept_properties as bcp_mod
from usdm import generate_biomedical_concepts as bc_mod
from usdm import usdm_utils

SOA_ID = 987296
CT_DATE = "2026-03-27"
BC_PACKAGE_DATE = "2026-05-26"
ISO_DATE = re.compile(r"^\d{4}-\d{2}-\d{2}$")
client = TestClient(app)


def _clean(cur):
    for table in (
        "bcp_response_code",
        "biomedical_concept_property",
        "biomedical_concept",
        "activity_concept",
        "alias_code",
        "code",
        "code_association",
    ):
        cur.execute(f"DELETE FROM {table} WHERE soa_id=?", (SOA_ID,))


def _seed_bcp():
    client.get("/soa")  # ensure schema is initialized
    conn = _connect()
    cur = conn.cursor()
    _clean(cur)
    for num, code in ((1, "C82515"), (2, "C16809")):
        cur.execute(
            "INSERT INTO code (code_uid, soa_id, code, code_system, "
            "code_system_version, decode) VALUES (?,?,?,?,?,?)",
            (
                f"Code_{num}",
                SOA_ID,
                code,
                "http://www.cdisc.org",
                BC_PACKAGE_DATE,
                "decode",
            ),
        )
        cur.execute(
            "INSERT INTO alias_code (alias_code_uid, soa_id, "
            "standard_code) VALUES (?,?,?)",
            (f"AliasCode_{num}", SOA_ID, f"Code_{num}"),
        )
    cur.execute(
        "INSERT INTO biomedical_concept_property (soa_id, "
        "biomedical_concept_uid, biomedical_concept_property_uid, "
        "name, label, isRequired, isEnabled, datatype, code) "
        "VALUES (?,?,?,?,?,?,?,?,?)",
        (
            SOA_ID,
            "BiomedicalConcept_1",
            "BiomedicalConceptProperty_1",
            "n",
            "n",
            1,
            1,
            "string",
            "AliasCode_1",
        ),
    )
    cur.execute(
        "INSERT INTO bcp_response_code (soa_id, "
        "biomedical_concept_property_uid, response_code_uid, name, "
        "label, is_enabled, code) VALUES (?,?,?,?,?,?,?)",
        (
            SOA_ID,
            "BiomedicalConceptProperty_1",
            "ResponseCode_1",
            "n",
            "n",
            1,
            "AliasCode_2",
        ),
    )
    conn.commit()
    conn.close()


def test_bcp_codes_use_ct_release_date(monkeypatch):
    _seed_bcp()
    monkeypatch.setattr(bcp_mod, "_get_latest_sdtmct_version", lambda: CT_DATE)
    props = bcp_mod.build_usdm_biomedical_concept_properties(
        SOA_ID, "BiomedicalConcept_1"
    )
    code = props[0]["code"]["standardCode"]
    assert code["codeSystemVersion"] == CT_DATE
    rc_code = props[0]["responseCodes"][0]["code"]
    assert rc_code["codeSystemVersion"] == CT_DATE


def test_bc_standard_code_uses_ct_release_date(monkeypatch):
    _seed_bcp()
    conn = _connect()
    cur = conn.cursor()
    cur.execute(
        "INSERT INTO biomedical_concept (soa_id, "
        "biomedical_concept_uid, name, label, code) "
        "VALUES (?,?,?,?,?)",
        (SOA_ID, "BiomedicalConcept_1", "bc", "bc", "AliasCode_1"),
    )
    cur.execute(
        "INSERT INTO activity_concept (soa_id, activity_uid, "
        "concept_code, concept_uid) VALUES (?,?,?,?)",
        (SOA_ID, 1, "C82515", "BiomedicalConcept_1"),
    )
    conn.commit()
    conn.close()
    monkeypatch.setattr(bc_mod, "_get_latest_sdtmct_version", lambda: CT_DATE)
    monkeypatch.setattr(bc_mod, "_get_biomedical_concept_synonyms", lambda code: [])
    monkeypatch.setattr(bc_mod, "_get_biomedical_concept_reference", lambda code: "")
    monkeypatch.setattr(bc_mod, "_ensure_bcp_populated", lambda *a, **k: None)
    monkeypatch.setattr(bc_mod, "populate_extension_attributes", lambda *a, **k: None)
    monkeypatch.setattr(
        bc_mod, "populate_crf_extension_attributes", lambda *a, **k: None
    )
    out = bc_mod.build_usdm_biomedical_concepts(SOA_ID)
    version = out[0]["code"]["standardCode"]["codeSystemVersion"]
    assert version == CT_DATE


def test_level_code_without_dated_slug_falls_back(monkeypatch):
    client.get("/soa")
    conn = _connect()
    cur = conn.cursor()
    _clean(cur)
    cur.execute(
        "INSERT INTO code_association (soa_id, code_uid, codelist_table, "
        "codelist_code, code) VALUES (?,?,?,?,?)",
        (SOA_ID, "Code_9", "/mdr/ct/packages", "C188725", "C85826"),
    )
    conn.commit()
    conn.close()
    monkeypatch.setattr(usdm_utils, "_get_latest_ddfct_version", lambda: CT_DATE)
    out = usdm_utils._build_level_code(SOA_ID, "Code_9", "Objective")
    assert out["codeSystemVersion"] == CT_DATE
    assert ISO_DATE.match(out["codeSystemVersion"])


def test_level_code_keeps_dated_slug():
    client.get("/soa")
    conn = _connect()
    cur = conn.cursor()
    _clean(cur)
    cur.execute(
        "INSERT INTO code_association (soa_id, code_uid, codelist_table, "
        "codelist_code, code) VALUES (?,?,?,?,?)",
        (
            SOA_ID,
            "Code_9",
            "/mdr/ct/packages/ddfct-2025-09-26",
            "C188725",
            "C85826",
        ),
    )
    conn.commit()
    conn.close()
    out = usdm_utils._build_level_code(SOA_ID, "Code_9", "Objective")
    assert out["codeSystemVersion"] == "2025-09-26"
