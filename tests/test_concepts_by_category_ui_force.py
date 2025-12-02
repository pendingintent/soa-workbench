from types import SimpleNamespace

import pytest
from fastapi.testclient import TestClient

from soa_builder.web.app import app, _category_concepts_cache

client = TestClient(app)


class DummyResponse:
    def __init__(self, payload, status_code=200, text=""):
        self._payload = payload
        self.status_code = status_code
        self.text = text or ""

    def json(self):
        return self._payload


@pytest.fixture(autouse=True)
def clear_concepts_cache():
    _category_concepts_cache.clear()


def test_ui_concepts_by_category_force_bypass(monkeypatch):
    call_count = SimpleNamespace(n=0)

    # The app queries: /mdr/bc/biomedicalconcepts?category=CategoryA (encoded)
    # Provide payloads representing concepts in direct 'items' list form.
    payload1 = {
        "items": [
            {
                "code": "C100",
                "title": "Alpha",
                "href": "/mdr/bc/biomedicalconcepts/C100",
            },
        ]
    }
    payload2 = {
        "items": [
            {
                "code": "C100",
                "title": "Alpha v2",
                "href": "/mdr/bc/biomedicalconcepts/C100",
            },
            {
                "code": "C200",
                "title": "Beta",
                "href": "/mdr/bc/biomedicalconcepts/C200",
            },
        ]
    }

    def fake_get(url, headers=None, timeout=None):
        # Ensure we are mocking the concepts-by-category endpoint
        assert "/mdr/bc/biomedicalconcepts?category=" in url
        call_count.n += 1
        return DummyResponse(payload1 if call_count.n == 1 else payload2, text="ok")

    monkeypatch.setattr("requests.get", fake_get)

    # Initial request populates cache, shows Alpha only
    r1 = client.get("/ui/concept_categories/view", params={"name": "CategoryA"})
    assert r1.status_code == 200
    html1 = r1.text
    assert "Alpha" in html1
    assert "Alpha v2" not in html1
    assert "Beta" not in html1
    assert call_count.n == 1

    # Force bypass should fetch again and render updated concept titles
    r2 = client.get(
        "/ui/concept_categories/view", params={"name": "CategoryA", "force": 1}
    )
    assert r2.status_code == 200
    html2 = r2.text
    assert "Alpha v2" in html2
    assert "Beta" in html2
    assert call_count.n == 2

    # Subsequent non-force call should serve cached updated content (no new HTTP)
    r3 = client.get("/ui/concept_categories/view", params={"name": "CategoryA"})
    assert r3.status_code == 200
    html3 = r3.text
    assert "Alpha v2" in html3
    assert "Beta" in html3
    assert call_count.n == 2
