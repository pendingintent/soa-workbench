"""Tests for Define-JSON concepts / conceptProperties population.

Covers `USDMDefineJSONProcessor.populate_concepts()` and the
itemGroup/item concept linkage, using an in-memory USDM fixture and a
mocked CDISC Library client (no network, no database).
"""

import json
import os
import tempfile
from unittest.mock import patch

from usdm.create_define_json import USDMDefineJSONProcessor

CDISC = "http://www.cdisc.org"


def _code(code, decode, version="2026-05-26"):
    return {
        "code": code,
        "codeSystem": CDISC,
        "codeSystemVersion": version,
        "decode": decode,
        "instanceType": "Code",
    }


def _alias(code, decode, version="2026-05-26"):
    return {"standardCode": _code(code, decode, version), "instanceType": "AliasCode"}


def _usdm_with_bcs(bcs):
    return {"study": {"versions": [{"biomedicalConcepts": bcs}]}}


def _make_processor(usdm_data):
    """Build a processor against an in-memory USDM doc (client mocked)."""
    tmp = tempfile.NamedTemporaryFile(mode="w", suffix=".json", delete=False)
    json.dump(usdm_data, tmp)
    tmp.close()
    out = tmp.name + ".define.json"
    with patch("usdm.create_define_json.CDISCLibraryClient"):
        proc = USDMDefineJSONProcessor(
            usdm_file=tmp.name,
            output_template=out,
            sdtmig="3.4",
            sdtmct="2025-03-28",
            studyversion=0,
            studydesign=0,
            docversion=0,
            cdisc_api_key="test",
            cosmosversion="v2",
            debug=False,
        )
    os.unlink(tmp.name)
    return proc


def _vs_bc():
    return {
        "id": "BiomedicalConcept_1",
        "name": "Vital Signs Result",
        "label": "VS Result",
        "synonyms": ["VS"],
        "reference": "/mdr/bc/biomedicalconcepts/C49672",
        "code": _alias("C49672", "Vital Signs", "2025-09-23"),
        "properties": [
            {
                "id": "BiomedicalConceptProperty_1",
                "name": "VSORRES",
                "label": "VSORRES",
                "isRequired": True,
                "code": _alias("C70856", "Observation Result"),
                "responseCodes": [
                    {"name": "TARGET", "code": _code("C94520", "TARGET")},
                    {"name": "BLOOD", "code": _code("C12434", "BLOOD")},
                ],
            },
            {
                "id": "BiomedicalConceptProperty_2",
                "name": "VSLOC",
                "label": "VSLOC",
                "isRequired": False,
                "code": _alias("C170500", "Anatomical Location"),
                "responseCodes": [],
            },
        ],
    }


def test_populate_concepts_maps_bcs():
    proc = _make_processor(_usdm_with_bcs([_vs_bc()]))
    proc.populate_concepts()

    assert len(proc.concepts) == 1
    concept = proc.concepts[0]
    assert concept["OID"] == "CONC.BiomedicalConcept_1"
    assert concept["name"] == "Vital Signs Result"
    assert concept["aliases"] == ["VS"]
    assert concept["href"] == "/mdr/bc/biomedicalconcepts/C49672"
    assert concept["coding"] == [
        {
            "code": "C49672",
            "codeSystem": CDISC,
            "codeSystemVersion": "2025-09-23",
            "decode": "Vital Signs",
        }
    ]
    assert len(concept["properties"]) == 2

    # flat conceptProperties list mirrors the inline ones
    assert len(proc.concept_properties) == 2

    p1, p2 = concept["properties"]
    assert p1["OID"] == "CONCPROP.BiomedicalConceptProperty_1"
    assert p1["mandatory"] is True
    assert p1["coding"][0]["code"] == "C70856"
    assert p2["mandatory"] is False


def test_response_codes_become_codelist_reference():
    proc = _make_processor(_usdm_with_bcs([_vs_bc()]))
    proc.populate_concepts()

    p1, p2 = proc.concepts[0]["properties"]
    # property with response codes -> codeList ref into code_lists_map
    assert "codeList" in p1
    cl_oid = p1["codeList"]
    assert cl_oid in proc.code_lists_map
    codelist = proc.code_lists_map[cl_oid]
    assert codelist["isNonStandard"] is True
    assert {i["codedValue"] for i in codelist["codeListItems"]} == {
        "TARGET",
        "BLOOD",
    }
    # property without response codes -> no codeList
    assert "codeList" not in p2


def test_identical_response_code_sets_dedupe():
    bc = _vs_bc()
    # second property carries the SAME response-code set as VSORRES
    bc["properties"].append(
        {
            "id": "BiomedicalConceptProperty_3",
            "name": "VSORRES2",
            "label": "VSORRES2",
            "isRequired": False,
            "code": _alias("C70856", "Observation Result"),
            "responseCodes": [
                {"name": "TARGET", "code": _code("C94520", "TARGET")},
                {"name": "BLOOD", "code": _code("C12434", "BLOOD")},
            ],
        }
    )
    proc = _make_processor(_usdm_with_bcs([bc]))
    proc.populate_concepts()

    props = {p["name"]: p for p in proc.concepts[0]["properties"]}
    assert props["VSORRES"]["codeList"] == props["VSORRES2"]["codeList"]
    # only one synthesized codelist for the shared value set
    shared_oid = props["VSORRES"]["codeList"]
    assert list(proc.code_lists_map).count(shared_oid) == 1


def test_itemgroup_and_item_reference_concepts():
    proc = _make_processor(_usdm_with_bcs([_vs_bc()]))
    proc.dataset_to_bc_id = {"VS": "BiomedicalConcept_1"}
    proc.datasets_dict = {"VS": {}}
    proc.vlm_lookup = {}
    proc.populate_concepts()

    dataset_data = {
        "label": "Vital Signs",
        "datasetStructure": "One record per vital sign per visit",
        "_links": {"parentClass": {"title": "Findings"}},
        "datasetVariables": [
            {
                "name": "VSORRES",
                "label": "Result",
                "role": "Result Qualifier",
                "core": "Req",
                "simpleDatatype": "Char",
            },
            {
                "name": "VSLOC",
                "label": "Location",
                "role": "Record Qualifier",
                "core": "Req",
                "simpleDatatype": "Char",
            },
        ],
    }
    proc._process_standard_dataset("VS", dataset_data)

    item_group = proc.item_groups[-1]
    assert item_group["implementsConcept"] == "CONC.BiomedicalConcept_1"
    items = {i["name"]: i for i in item_group["items"]}
    assert items["VSORRES"]["conceptProperty"] == (
        "CONCPROP.BiomedicalConceptProperty_1"
    )
    assert items["VSLOC"]["conceptProperty"] == ("CONCPROP.BiomedicalConceptProperty_2")
