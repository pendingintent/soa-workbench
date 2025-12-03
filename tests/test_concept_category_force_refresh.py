import json
from typing import List
from fastapi.testclient import TestClient

from soa_builder.web.app import app, _category_concepts_cache

client = TestClient(app)


class DummyResp:
    def __init__(self, status_code: int, json_data=None, text: str = ""):
        self.status_code = status_code
        self._json = json_data
        self.text = text or json.dumps(json_data or {})

    def json(self):
        if self._json is None:
            raise ValueError("No JSON")
        return self._json


def test_ui_category_force_refresh(monkeypatch):
    _category_concepts_cache.clear()
    calls: List[str] = []
    payload1 = {
        "items": [
            {
                "code": "ALT",
                "title": "Alanine",
                "href": "/mdr/bc/biomedicalconcepts/ALT",
            }
        ]
    }
    payload2 = {
        "items": [
            {
                "code": "AST",
                "title": "Aspartate",
                "href": "/mdr/bc/biomedicalconcepts/AST",
            }
        ]
    }

    def fake_get(url, headers=None, timeout=0):
        # First call returns ALT, subsequent calls return AST
        if not calls:
            calls.append(url)
            return DummyResp(200, payload1)
        calls.append(url)
        return DummyResp(200, payload2)

    monkeypatch.setattr("requests.get", fake_get)
    # Initial request populates cache with ALT
    r1 = client.get("/ui/concept_categories/view", params={"name": "Force Test"})
    assert r1.status_code == 200
    assert "/ui/concepts/ALT" in r1.text
    # Second request without force still uses cache (ALT)
    r2 = client.get("/ui/concept_categories/view", params={"name": "Force Test"})
    assert r2.status_code == 200
    assert "/ui/concepts/ALT" in r2.text
    # Third request with force bypasses cache and shows AST
    r3 = client.get(
        "/ui/concept_categories/view", params={"name": "Force Test", "force": True}
    )
    assert r3.status_code == 200
    assert "/ui/concepts/AST" in r3.text
