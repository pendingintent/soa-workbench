"""Tests for the Unassigned Biomedical Concepts report endpoints."""

import csv
import io
from unittest.mock import patch

from fastapi.testclient import TestClient

from soa_builder.web.app import app

client = TestClient(app)

_ALL_CONCEPTS = [
    {"code": "C001", "title": "Alpha Concept"},
    {"code": "C002", "title": "Beta Concept"},
    {"code": "C003", "title": "Gamma Concept"},
]

_CATEGORIES = [
    {"name": "Cat A", "title": "Category A", "href": "/mdr/bc/categories/cat-a"},
]

_CAT_A_CONCEPTS = [
    {"code": "C001", "title": "Alpha Concept", "href": "/mdr/bc/C001"},
]

_PATCH_ALL = "soa_builder.web.app.fetch_biomedical_concepts"
_PATCH_CATS = "soa_builder.web.app.fetch_biomedical_concept_categories"
_PATCH_BY_CAT = "soa_builder.web.app.fetch_biomedical_concepts_by_category"


def test_page_load_ok():
    r = client.get("/ui/bc/unassigned-concepts")
    assert r.status_code == 200
    assert "Unassigned Biomedical Concepts" in r.text


def test_generate_returns_unassigned():
    with (
        patch(_PATCH_ALL, return_value=_ALL_CONCEPTS),
        patch(_PATCH_CATS, return_value=_CATEGORIES),
        patch(_PATCH_BY_CAT, return_value=_CAT_A_CONCEPTS),
    ):
        r = client.post("/ui/bc/unassigned-concepts/generate")
    assert r.status_code == 200
    assert "C002" in r.text
    assert "C003" in r.text
    assert "C001" not in r.text
    assert "2 Unassigned Concepts" in r.text


def test_generate_all_assigned():
    with (
        patch(_PATCH_ALL, return_value=_ALL_CONCEPTS),
        patch(_PATCH_CATS, return_value=_CATEGORIES),
        patch(_PATCH_BY_CAT, return_value=_ALL_CONCEPTS),
    ):
        r = client.post("/ui/bc/unassigned-concepts/generate")
    assert r.status_code == 200
    assert "All concepts are assigned" in r.text
    assert "0 Unassigned Concepts" in r.text


def test_generate_empty_catalogue():
    with (
        patch(_PATCH_ALL, return_value=[]),
        patch(_PATCH_CATS, return_value=[]),
        patch(_PATCH_BY_CAT, return_value=[]),
    ):
        r = client.post("/ui/bc/unassigned-concepts/generate")
    assert r.status_code == 200
    assert "0 Unassigned Concepts" in r.text


def test_csv_export_headers_and_rows():
    with (
        patch(_PATCH_ALL, return_value=_ALL_CONCEPTS),
        patch(_PATCH_CATS, return_value=_CATEGORIES),
        patch(_PATCH_BY_CAT, return_value=_CAT_A_CONCEPTS),
    ):
        r = client.get("/ui/bc/unassigned-concepts/export/csv")
    assert r.status_code == 200
    assert "text/csv" in r.headers["content-type"]
    assert "bc_unassigned_concepts.csv" in r.headers["content-disposition"]
    rows = list(csv.DictReader(io.StringIO(r.text)))
    codes = {row["code"] for row in rows}
    assert "C002" in codes
    assert "C003" in codes
    assert "C001" not in codes
    assert len(rows) == 2


def test_csv_export_all_assigned():
    with (
        patch(_PATCH_ALL, return_value=_ALL_CONCEPTS),
        patch(_PATCH_CATS, return_value=_CATEGORIES),
        patch(_PATCH_BY_CAT, return_value=_ALL_CONCEPTS),
    ):
        r = client.get("/ui/bc/unassigned-concepts/export/csv")
    assert r.status_code == 200
    rows = list(csv.DictReader(io.StringIO(r.text)))
    assert rows == []
