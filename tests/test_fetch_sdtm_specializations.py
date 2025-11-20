# ...existing code...
import os
import json
from unittest.mock import patch, Mock

from soa_builder.web.app import fetch_sdtm_specializations, _sdtm_specializations_cache


def test_fetch_sdtm_specializations_env_override_with_code(monkeypatch):
    # Reset cache
    _sdtm_specializations_cache.update(data=None, fetched_at=0)
    # Prepare override JSON
    override = [
        {
            "title": "Spec A",
            "href": "/mdr/specializations/sdtm/datasetspecializations/AAA",
        },
        {
            "datasetSpecializationName": "Spec B",
            "datasetSpecializationId": "BBB",
        },
    ]
    monkeypatch.setenv("CDISC_SDTM_SPECIALIZATIONS_JSON", json.dumps(override))

    result = fetch_sdtm_specializations(force=True, code="BC123")

    assert len(result) == 2
    hrefs = [p["href"] for p in result]
    # All hrefs must be absolute and have biomedicalconcept param
    for href in hrefs:
        assert href.startswith("https://api.library.cdisc.org/")
        assert "biomedicalconcept=BC123" in href


def test_fetch_sdtm_specializations_remote_no_code(monkeypatch):
    # Reset cache and ensure no override
    _sdtm_specializations_cache.update(data=None, fetched_at=0)
    monkeypatch.delenv("CDISC_SDTM_SPECIALIZATIONS_JSON", raising=False)
    monkeypatch.delenv("CDISC_SKIP_REMOTE", raising=False)

    fake_json = {
        "items": [
            {
                "title": "Remote Spec 1",
                "href": "/mdr/specializations/sdtm/datasetspecializations/R1",
            },
            {
                "title": "Remote Spec 2",
                "href": "/mdr/specializations/sdtm/datasetspecializations/R2",
            },
        ]
    }

    mock_resp = Mock()
    mock_resp.status_code = 200
    mock_resp.text = json.dumps(fake_json)
    mock_resp.json.return_value = fake_json

    with patch("soa_builder.web.app.requests.get", return_value=mock_resp) as mock_get:
        result = fetch_sdtm_specializations(force=True)

    mock_get.assert_called_once()
    assert len(result) == 2
    titles = sorted(p["title"] for p in result)
    assert titles == ["Remote Spec 1", "Remote Spec 2"]
    # When no code is passed, hrefs should NOT have biomedicalconcept query param
    for p in result:
        assert "biomedicalconcept=" not in p["href"]
