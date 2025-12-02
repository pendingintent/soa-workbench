from types import SimpleNamespace

import pytest

from soa_builder.web.app import (
    fetch_biomedical_concept_categories,
    _bc_categories_cache,
)


class DummyResponse:
    def __init__(self, payload, status_code=200):
        self._payload = payload
        self.status_code = status_code

    def json(self):
        return self._payload


@pytest.fixture(autouse=True)
def clear_categories_cache():
    _bc_categories_cache.clear()


def test_categories_cache_hit(monkeypatch):
    call_count = SimpleNamespace(n=0)

    payload = {
        "_links": {
            "categories": [
                {
                    "name": "CategoryA",
                    "_links": {
                        "self": {"href": "/mdr/bc/categories/A", "title": "TitleA"}
                    },
                },
                {
                    "name": "CategoryB",
                    "_links": {
                        "self": {"href": "/mdr/bc/categories/B", "title": "TitleB"}
                    },
                },
            ]
        }
    }

    def fake_get(url, headers=None, timeout=None):
        call_count.n += 1
        return DummyResponse(payload)

    monkeypatch.setattr("requests.get", fake_get)

    # First call populates cache
    cats1 = fetch_biomedical_concept_categories(force=False)
    assert call_count.n == 1
    assert len(cats1) == 2

    # Second call within TTL should hit cache, no new HTTP call
    cats2 = fetch_biomedical_concept_categories(force=False)
    assert call_count.n == 1
    assert cats2 == cats1


def test_categories_cache_force_bypass(monkeypatch):
    call_count = SimpleNamespace(n=0)

    payload1 = {
        "_links": {
            "categories": [
                {
                    "name": "CategoryA",
                    "_links": {
                        "self": {"href": "/mdr/bc/categories/A", "title": "TitleA"}
                    },
                }
            ]
        }
    }
    payload2 = {
        "_links": {
            "categories": [
                {
                    "name": "CategoryA",
                    "_links": {
                        "self": {"href": "/mdr/bc/categories/A2", "title": "TitleA2"}
                    },
                },
                {
                    "name": "CategoryC",
                    "_links": {
                        "self": {"href": "/mdr/bc/categories/C", "title": "TitleC"}
                    },
                },
            ]
        }
    }

    def fake_get(url, headers=None, timeout=None):
        call_count.n += 1
        # Return payload1 first, payload2 thereafter
        return DummyResponse(payload1 if call_count.n == 1 else payload2)

    monkeypatch.setattr("requests.get", fake_get)

    # Populate cache
    cats1 = fetch_biomedical_concept_categories(force=False)
    assert call_count.n == 1
    assert [c["name"] for c in cats1] == ["CategoryA"]

    # Force bypass should trigger a new HTTP call and new content
    cats2 = fetch_biomedical_concept_categories(force=True)
    assert call_count.n == 2
    assert [c["name"] for c in cats2] == ["CategoryA", "CategoryC"]

    # Regular call after force should use cached latest content (no extra HTTP)
    cats3 = fetch_biomedical_concept_categories(force=False)
    assert call_count.n == 2
    assert cats3 == cats2
