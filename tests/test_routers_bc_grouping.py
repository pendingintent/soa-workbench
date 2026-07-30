"""Tests for the BC Grouping Explorer proxy routes on the Concept
Groups page (routers/concept_groups.py). These cover only the new
BC-grouping-service integration; the Custom Concept Groups feature
on the same page is covered by the regression smoke test below.
"""

from types import SimpleNamespace

import pytest
import requests
from fastapi.testclient import TestClient

from soa_builder.web.app import app
from soa_builder.web.routers.concept_groups import _bc_grouping_cache

client = TestClient(app)


class DummyResponse:
    def __init__(self, payload, status_code=200):
        self._payload = payload
        self.status_code = status_code

    def json(self):
        return self._payload

    def raise_for_status(self):
        if self.status_code >= 400:
            raise requests.exceptions.HTTPError(f"HTTP {self.status_code}")


@pytest.fixture(autouse=True)
def clear_bc_grouping_cache():
    _bc_grouping_cache["data"] = None
    _bc_grouping_cache["fetched_at"] = 0


def _paged_get(pages_by_path):
    """Build a fake requests.get that pages through pages_by_path.

    pages_by_path: {path: [ (items, total), ... ]} — one page per call,
    keyed by request-count for that path.
    """
    call_index = {}

    def fake_get(url, params=None, timeout=None):
        for path, pages in pages_by_path.items():
            if url.endswith(path):
                idx = call_index.get(path, 0)
                items, total = pages[idx]
                call_index[path] = idx + 1
                return DummyResponse({"items": items, "total": total})
        raise AssertionError(f"Unexpected URL: {url}")

    return fake_get


def _happy_path_get(monkeypatch, call_counter=None):
    bcs = [{"bc_id": "C1", "short_name": "Sex", "ncit_code": "C1"}]
    schemes = [
        {
            "scheme_id": "concept_group",
            "name": "Concept Group",
            "description": "",
            "purpose": "",
            "intended_use": "",
        }
    ]
    values = [
        {
            "value_id": "cg_demo",
            "scheme_id": "concept_group",
            "label": "Demographics",
            "description": "",
        }
    ]
    # Two pages of assignments to exercise the pagination loop.
    assignments_page1 = [
        {
            "assignment_id": "a1",
            "bc_id": "C1",
            "scheme_id": "concept_group",
            "value_id": "cg_demo",
        }
    ]
    assignments_page2 = [
        {
            "assignment_id": "a2",
            "bc_id": "C1",
            "scheme_id": "concept_group",
            "value_id": "cg_demo",
        }
    ]

    fake_get = _paged_get(
        {
            "/biomedical-concepts": [(bcs, 1)],
            "/classification-schemes": [(schemes, 1)],
            "/classification-values": [(values, 1)],
            "/classification-assignments": [
                (assignments_page1, 2),
                (assignments_page2, 2),
            ],
        }
    )

    def counting_get(url, params=None, timeout=None):
        if call_counter is not None:
            call_counter.n += 1
        return fake_get(url, params=params, timeout=timeout)

    monkeypatch.setattr("requests.get", counting_get)


def test_bc_explorer_happy_path(monkeypatch):
    _happy_path_get(monkeypatch)

    resp = client.get("/ui/concept-groups/bc-explorer")
    assert resp.status_code == 200
    assert "BC Grouping Explorer unavailable" not in resp.text
    assert "Demographics" in resp.text
    assert "Sex" in resp.text


def test_bc_explorer_connection_error(monkeypatch):
    def fake_get(url, params=None, timeout=None):
        raise requests.exceptions.ConnectionError("boom")

    monkeypatch.setattr("requests.get", fake_get)

    resp = client.get("/ui/concept-groups/bc-explorer")
    assert resp.status_code == 200
    assert "BC Grouping Explorer unavailable" in resp.text
    assert "not reachable" in resp.text


def test_bc_explorer_timeout(monkeypatch):
    def fake_get(url, params=None, timeout=None):
        raise requests.exceptions.Timeout("boom")

    monkeypatch.setattr("requests.get", fake_get)

    resp = client.get("/ui/concept-groups/bc-explorer")
    assert resp.status_code == 200
    assert "BC Grouping Explorer unavailable" in resp.text
    assert "timed out" in resp.text


def test_bc_explorer_cache_hit_and_refresh(monkeypatch):
    call_counter = SimpleNamespace(n=0)
    _happy_path_get(monkeypatch, call_counter=call_counter)

    resp1 = client.get("/ui/concept-groups/bc-explorer")
    assert resp1.status_code == 200
    calls_after_first = call_counter.n
    assert calls_after_first > 0

    # Second call within TTL should hit the cache — no new HTTP calls.
    resp2 = client.get("/ui/concept-groups/bc-explorer")
    assert resp2.status_code == 200
    assert call_counter.n == calls_after_first

    # Refresh forces a bypass.
    refresh_resp = client.post("/ui/concept-groups/bc-explorer/refresh")
    assert refresh_resp.status_code == 200
    assert call_counter.n > calls_after_first


def test_bc_grouping_pagination_helper_concatenates_all_pages(monkeypatch):
    """Focused unit test of the pagination loop: total exceeds one page."""
    from soa_builder.web.routers.concept_groups import _bc_grouping_fetch_all

    pages = [
        ({"items": [{"id": 1}, {"id": 2}], "total": 5}),
        ({"items": [{"id": 3}, {"id": 4}], "total": 5}),
        ({"items": [{"id": 5}], "total": 5}),
    ]
    call_count = SimpleNamespace(n=0)

    def fake_get(url, params=None, timeout=None):
        page = pages[call_count.n]
        call_count.n += 1
        return DummyResponse(page)

    monkeypatch.setattr("requests.get", fake_get)

    items = _bc_grouping_fetch_all("/classification-assignments", limit=2)
    assert call_count.n == 3
    assert [i["id"] for i in items] == [1, 2, 3, 4, 5]


def test_bc_status_ok(monkeypatch):
    def fake_get(url, timeout=None):
        return DummyResponse({"status": "ok"})

    monkeypatch.setattr("requests.get", fake_get)

    resp = client.get("/ui/concept-groups/bc-status")
    assert resp.status_code == 200
    assert "BC Grouping Service is connected" in resp.text


def test_bc_status_connection_error(monkeypatch):
    def fake_get(url, timeout=None):
        raise requests.exceptions.ConnectionError("boom")

    monkeypatch.setattr("requests.get", fake_get)

    resp = client.get("/ui/concept-groups/bc-status")
    assert resp.status_code == 200
    assert "BC Grouping Service unavailable" in resp.text


def test_concept_groups_page_smoke_and_custom_groups_unchanged():
    """GET /ui/concept-groups makes no bc-grouping-service call itself,
    so no monkeypatching is needed here. Regression guard: Custom
    Concept Groups markup must still be present and the placeholder
    must be gone.
    """
    resp = client.get("/ui/concept-groups")
    assert resp.status_code == 200
    assert "Custom Concept Groups" in resp.text
    assert 'action="/ui/concept-groups/create"' in resp.text
    assert "bc-explorer-root" in resp.text
    assert "bc-status-notice" in resp.text
    assert "__PLACEHOLDER__" not in resp.text
