import json
from unittest.mock import Mock, patch

from soa_builder.web.app import (
    CDISC_CRF_API_BASE_URL,
    _crf_specializations_by_code_cache,
    _crf_specializations_cache,
    fetch_crf_specializations,
)


def test_fetch_crf_specializations_env_override(monkeypatch):
    _crf_specializations_cache.update(data=None, fetched_at=0)
    override = [
        {
            "title": "Spec A",
            "href": "/mdr/specializations/crf/specializations/AAA",
        },
        {
            "title": "Spec B",
            "href": "/mdr/specializations/crf/specializations/BBB",
        },
    ]
    monkeypatch.setenv("CDISC_CRF_SPECIALIZATIONS_JSON", json.dumps(override))

    result = fetch_crf_specializations(force=True)

    assert len(result) == 2
    titles = sorted(p["title"] for p in result)
    assert titles == ["Spec A", "Spec B"]
    for p in result:
        assert p["href"].startswith(CDISC_CRF_API_BASE_URL)


def test_fetch_crf_specializations_remote_no_code_uses_latest_endpoint(monkeypatch):
    _crf_specializations_cache.update(data=None, fetched_at=0)
    monkeypatch.delenv("CDISC_CRF_SPECIALIZATIONS_JSON", raising=False)
    monkeypatch.delenv("CDISC_SKIP_REMOTE", raising=False)

    fake_json = {
        "_links": {
            "specializations": [
                {
                    "href": "/mdr/specializations/crf/specializations/R1",
                    "title": "Remote Spec 1",
                },
                {
                    "href": "/mdr/specializations/crf/specializations/R2",
                    "title": "Remote Spec 2",
                },
            ]
        }
    }

    mock_resp = Mock()
    mock_resp.status_code = 200
    mock_resp.text = json.dumps(fake_json)
    mock_resp.json.return_value = fake_json

    with patch("soa_builder.web.app.requests.get", return_value=mock_resp) as mock_get:
        result = fetch_crf_specializations(force=True)

    # Only one request — no separate packages lookup.
    mock_get.assert_called_once()
    called_url = mock_get.call_args[0][0]
    assert called_url.endswith("/mdr/specializations/crf/specializations")

    assert len(result) == 2
    titles = sorted(p["title"] for p in result)
    assert titles == ["Remote Spec 1", "Remote Spec 2"]
    for p in result:
        assert "/mdr/specializations/crf/packages/" not in p["href"]


def test_fetch_crf_specializations_with_code_uses_generic_endpoint(monkeypatch):
    _crf_specializations_by_code_cache.clear()
    monkeypatch.delenv("CDISC_CRF_SPECIALIZATIONS_JSON", raising=False)

    hal_json = {
        "_links": {
            "datasetSpecializations": {
                "sdtm": [
                    {
                        "href": "/mdr/specializations/sdtm/datasetspecializations/AAA",
                        "title": "SDTM Spec A",
                    },
                ],
                "crf": [
                    {
                        "href": "/mdr/specializations/crf/specializations/AAA_DENORMALIZED",
                        "title": "CRF Spec A",
                        "type": "CRF Specialization",
                    },
                    {
                        "href": "/mdr/specializations/crf/specializations/BBB_DENORMALIZED",
                        "title": "CRF Spec B",
                        "type": "CRF Specialization",
                    },
                ],
            }
        }
    }

    mock_resp = Mock()
    mock_resp.status_code = 200
    mock_resp.text = json.dumps(hal_json)
    mock_resp.json.return_value = hal_json

    with patch("soa_builder.web.app.requests.get", return_value=mock_resp) as mock_get:
        result = fetch_crf_specializations(force=True, code="C179175")

    called_url = mock_get.call_args[0][0]
    assert "mdr/specializations/datasetspecializations" in called_url
    assert "biomedicalconcept=C179175" in called_url

    assert len(result) == 2
    titles = sorted(p["title"] for p in result)
    assert titles == ["CRF Spec A", "CRF Spec B"]
    for p in result:
        assert "/mdr/specializations/crf/specializations/" in p["href"]
