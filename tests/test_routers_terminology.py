"""Characterization tests for the CDISC CT terminology browser routers.

Each router wraps a cached ``get_*_ct_rows`` fetcher; the fetcher is
replaced with canned rows so these tests run offline.
"""

import pytest
from fastapi.testclient import TestClient

from soa_builder.web.app import app
from soa_builder.web.routers import (
    cdash_terminology,
    ddf_controlled_terminology,
    define_xml_terminology,
    protocol_controlled_terminology,
    sdtm_terminology,
)

client = TestClient(app)

ROUTERS = [
    (cdash_terminology, "get_cdash_ct_rows", "/cdash/terminology"),
    (ddf_controlled_terminology, "get_ddf_ct_rows", "/ddf-ct/terminology"),
    (define_xml_terminology, "get_define_xml_ct_rows", "/define-xml/terminology"),
    (
        protocol_controlled_terminology,
        "get_protocol_ct_rows",
        "/protocol-ct/terminology",
    ),
    (sdtm_terminology, "get_sdtm_ct_rows", "/sdtm/terminology"),
]
IDS = [path for _, _, path in ROUTERS]

ROWS = [
    {
        "code": "C1",
        "codelist_code": "CL1",
        "codelist_name": "Units",
        "submission_value": "mg",
        "definition": "milligram",
        "synonyms": "",
        "preferred_term": "Milligram",
    },
    {
        "code": "C2",
        "codelist_code": "CL1",
        "codelist_name": "Units",
        "submission_value": "kg",
        "definition": "kilogram",
        "synonyms": "",
        "preferred_term": "Kilogram",
    },
    {
        "code": "C3",
        "codelist_code": "CL2",
        "codelist_name": "Route",
        "submission_value": "ORAL",
        "definition": "by mouth",
        "synonyms": "PO",
        "preferred_term": "Oral Route",
    },
]


@pytest.fixture(params=ROUTERS, ids=IDS)
def router(request, monkeypatch):
    module, fetcher, path = request.param
    calls = []

    def fake_rows(force=False, **_kwargs):
        calls.append(force)
        return {"slug": "pkg-2026-01-01", "rows": ROWS, "error": None}

    monkeypatch.setattr(module, fetcher, fake_rows)
    return module, path, calls


def test_json_returns_all_rows(router):
    _, path, _ = router
    body = client.get(path).json()
    assert body["slug"] == "pkg-2026-01-01"
    assert body["total_count"] == 3
    assert body["matched_count"] == 3
    assert [r["code"] for r in body["rows"]] == ["C1", "C2", "C3"]
    assert body["error"] is None


def test_json_filters_by_codelist_and_search(router):
    _, path, _ = router
    body = client.get(path, params={"codelist_code": "CL1"}).json()
    assert [r["code"] for r in body["rows"]] == ["C1", "C2"]
    body = client.get(path, params={"search": "kilo"}).json()
    assert [r["code"] for r in body["rows"]] == ["C2"]
    body = client.get(path, params={"code": "C3", "search": "nomatch"}).json()
    assert [r["code"] for r in body["rows"]] == ["C3"]


def test_json_paginates_and_clamps(router):
    _, path, _ = router
    body = client.get(path, params={"limit": 1, "offset": 1}).json()
    assert [r["code"] for r in body["rows"]] == ["C2"]
    assert body["matched_count"] == 3
    body = client.get(path, params={"limit": 0, "offset": -5}).json()
    assert (body["limit"], body["offset"]) == (1, 0)


def test_refresh_forces_refetch(router):
    _, path, calls = router
    client.get(path, params={"refresh": "true"})
    assert calls[-1] is True


def test_json_passes_fetch_error_through(monkeypatch):
    def failing_rows(force=False, **_kwargs):
        return {"slug": None, "rows": [], "error": "CDISC Library unreachable"}

    monkeypatch.setattr(sdtm_terminology, "get_sdtm_ct_rows", failing_rows)
    body = client.get("/sdtm/terminology").json()
    assert body["error"] == "CDISC Library unreachable"
    assert body["rows"] == []


def test_ui_page_renders_rows(router):
    _, path, _ = router
    r = client.get(f"/ui{path}", params={"search": "Oral"})
    assert r.status_code == 200
    assert "text/html" in r.headers["content-type"]
    assert "ORAL" in r.text
