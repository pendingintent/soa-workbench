import os
from unittest.mock import patch, Mock

from soa_builder.web import utils
from soa_builder.web.utils import load_epoch_type_options


def test_load_epoch_type_options_parses_submission_values():
    # Ensure environment headers won't block test when absent
    os.environ.pop("CDISC_SUBSCRIPTION_KEY", None)
    os.environ.pop("CDISC_API_KEY", None)

    fake_json = {
        "_links": {
            "terms": [
                {"submissionValue": "SCREENING"},
                {"submissionValue": "TREATMENT"},
                {"submissionValue": "FOLLOW-UP"},
                {"submissionValue": "TREATMENT"},  # duplicate to test dedupe
            ]
        }
    }

    mock_resp = Mock()
    mock_resp.status_code = 200
    mock_resp.json.return_value = fake_json

    with patch("soa_builder.web.utils.requests.get", return_value=mock_resp) as pg:
        values = load_epoch_type_options(force=True)
        # Verify API called
        assert pg.called
        # Values should be deduplicated and sorted
        assert values == ["FOLLOW-UP", "SCREENING", "TREATMENT"]


def test_load_epoch_type_options_caches_results():
    os.environ.pop("CDISC_SUBSCRIPTION_KEY", None)
    os.environ.pop("CDISC_API_KEY", None)

    fake_json = {"_links": {"terms": [{"submissionValue": "A"}]}}
    mock_resp = Mock(status_code=200)
    mock_resp.json.return_value = fake_json

    with patch("soa_builder.web.utils.requests.get", return_value=mock_resp) as pg:
        first = load_epoch_type_options(force=True)
        second = load_epoch_type_options(force=False)
        # Only one API call due to cache on the second call
        assert pg.call_count == 1
        assert first == ["A"]
        assert second == ["A"]


def test_load_epoch_type_options_handles_error_status_code():
    mock_resp = Mock(status_code=500)
    mock_resp.json.return_value = {}
    with patch("soa_builder.web.utils.requests.get", return_value=mock_resp):
        values = load_epoch_type_options(force=True)
        assert values == []


def test_load_epoch_type_options_uses_configured_codelist(monkeypatch):
    """Overriding EPOCH_TYPE_CODELIST changes the fetched codelist URL."""
    monkeypatch.setattr(utils, "EPOCH_TYPE_CODELIST", "C99999")

    mock_resp = Mock(status_code=200)
    mock_resp.json.return_value = {"_links": {"terms": []}}

    with patch("soa_builder.web.utils.requests.get", return_value=mock_resp) as pg:
        load_epoch_type_options(force=True)
        called_url = pg.call_args.args[0]
        assert called_url.endswith("/codelists/C99999")
