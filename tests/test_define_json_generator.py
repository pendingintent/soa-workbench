"""Tests for the Define-JSON generator and its route."""

import json

from fastapi.testclient import TestClient

from soa_builder.web.app import app
from usdm.generate_define_json import build_define_json

client = TestClient(app)


def _create_soa(name: str) -> int:
    r = client.post("/soa", json={"name": name})
    return r.json()["id"]


def test_no_bcs_returns_valid_minimal_output():
    """SOA with no BCs returns a valid MetaDataVersion dict with required keys."""
    soa_id = _create_soa("DefineJSON No BCs")
    result = build_define_json(soa_id)

    assert isinstance(result, dict)
    assert "OID" in result
    assert "fileOID" in result
    assert "studyOID" in result
    assert result["odmVersion"] == "1.3.2"
    assert result["fileType"] == "Snapshot"
    assert result.get("itemGroups") is None or result.get("itemGroups") == []


def test_no_bcs_json_serialisable():
    """model_dump output round-trips through json.dumps / json.loads."""
    soa_id = _create_soa("DefineJSON Serialise Test")
    result = build_define_json(soa_id)
    serialised = json.dumps(result)
    assert json.loads(serialised) == result


def test_study_key_in_oids():
    """OIDs incorporate the soa_id when study_id is not set."""
    soa_id = _create_soa("DefineJSON OID Test")
    result = build_define_json(soa_id)

    assert str(soa_id) in result["OID"]
    assert str(soa_id) in result["fileOID"]
    assert str(soa_id) in result["studyOID"]


def test_route_returns_200_with_json_attachment():
    """GET /soa/{soa_id}/usdm_json/define_json returns 200 JSON attachment."""
    soa_id = _create_soa("DefineJSON Route Test")
    resp = client.get(f"/soa/{soa_id}/usdm_json/define_json")

    assert resp.status_code == 200
    assert "application/json" in resp.headers.get("content-type", "")
    assert "attachment" in resp.headers.get("content-disposition", "")
    assert "define.json" in resp.headers.get("content-disposition", "")


def test_route_404_nonexistent_soa():
    """GET /soa/999999/usdm_json/define_json returns 404."""
    resp = client.get("/soa/999999/usdm_json/define_json")
    assert resp.status_code == 404


def test_ui_define_json_page_200():
    """GET /ui/soa/{soa_id}/define_json returns 200 HTML."""
    soa_id = _create_soa("DefineJSON UI Test")
    resp = client.get(f"/ui/soa/{soa_id}/define_json")

    assert resp.status_code == 200
    assert "text/html" in resp.headers.get("content-type", "")
    assert "Define-JSON" in resp.text
