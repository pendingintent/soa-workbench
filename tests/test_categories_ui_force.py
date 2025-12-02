from types import SimpleNamespace

import pytest
from fastapi.testclient import TestClient

from soa_builder.web.app import app, _bc_categories_cache

client = TestClient(app)


class DummyResponse:
    def __init__(self, payload, status_code=200, text=""):
        self._payload = payload
        self.status_code = status_code
        self.text = text or ""

    def json(self):
        return self._payload


@pytest.fixture(autouse=True)
def clear_categories_cache():
    _bc_categories_cache.clear()


def test_ui_categories_force_bypass(monkeypatch):
    call_count = SimpleNamespace(n=0)

    payload1 = {
        "_links": {
            "categories": [
                {
                    "name": "CategoryA",
                    "_links": {
                        "self": {"href": "/mdr/bc/categories/A", "title": "TitleA"}
                    },
                },
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
        return DummyResponse(payload1 if call_count.n == 1 else payload2, text="ok")

    monkeypatch.setattr("requests.get", fake_get)

    # Initial request populates cache, shows TitleA only
    r1 = client.get("/ui/concept_categories")
    assert r1.status_code == 200
    html1 = r1.text
    assert "TitleA" in html1
    assert "TitleA2" not in html1
    assert "TitleC" not in html1
    assert call_count.n == 1

    # Force bypass should fetch again and render updated titles
    r2 = client.get("/ui/concept_categories?force=1")
    assert r2.status_code == 200
    html2 = r2.text
    assert "TitleA2" in html2
    assert "TitleC" in html2
    assert call_count.n == 2

    # Subsequent non-force call should serve cached updated content (no new HTTP)
    r3 = client.get("/ui/concept_categories")
    assert r3.status_code == 200
    html3 = r3.text
    assert "TitleA2" in html3
    assert "TitleC" in html3
    assert call_count.n == 2
