"""Tests for usdm.generate_narrative_content.

Regression coverage for a bug where narrative content extracted for
NCT01797120 was built unconditionally for every SOA, meaning it could
leak into an unrelated study's USDM export.
"""

from fastapi.testclient import TestClient

from soa_builder.web.app import app
from usdm.generate_narrative_content import (
    build_usdm_narrative_content_items,
    build_usdm_narrative_contents,
    build_usdm_study_definition_document,
)

client = TestClient(app)


def _new_soa(name: str, study_id: str = None) -> int:
    payload = {"name": name}
    if study_id:
        payload["study_id"] = study_id
    return client.post("/soa", json=payload).json()["id"]


def test_narrative_content_empty_for_unrelated_study():
    soa_id = _new_soa("Unrelated Study", study_id="NCT88888888")
    assert build_usdm_narrative_content_items(soa_id) == []
    assert build_usdm_narrative_contents(soa_id) == []
    assert build_usdm_study_definition_document(soa_id) is None


def test_narrative_content_empty_when_study_id_unset():
    soa_id = _new_soa("No Study Id Set")
    assert build_usdm_narrative_content_items(soa_id) == []
    assert build_usdm_narrative_contents(soa_id) == []
    assert build_usdm_study_definition_document(soa_id) is None


def test_narrative_content_only_attempted_for_matching_study(monkeypatch):
    """NCT01797120's SOA is the only one that should read the data file."""
    calls = []

    def _fake_load_sections():
        calls.append(True)
        return []

    monkeypatch.setattr(
        "usdm.generate_narrative_content._load_sections", _fake_load_sections
    )

    matching_soa_id = _new_soa("PrE0102-like", study_id="NCT01797120")
    other_soa_id = _new_soa("Other Study", study_id="NCT00000001")

    build_usdm_narrative_content_items(other_soa_id)
    assert calls == []

    build_usdm_narrative_content_items(matching_soa_id)
    assert calls == [True]
