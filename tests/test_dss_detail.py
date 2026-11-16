"""DSS detail page: /ui/soa/{soa_id}/dss/detail (linked from dss_cell.html)."""

import pytest
import requests
from fastapi.testclient import TestClient

from soa_builder.web.app import app

client = TestClient(app)
BASE = "https://api.library.cdisc.org/api/cosmos/v2"
DSS_HREF = f"{BASE}/mdr/specializations/sdtm/datasetspecializations/SYSBP"


class _Resp:
    status_code = 200
    text = '{"domain": "VS"}'

    def json(self):
        return {
            "datasetSpecializationId": "SYSBP",
            "domain": "VS",
            "shortName": "Systolic Blood Pressure",
            "variables": [{"name": "VSTESTCD", "dataType": "text"}],
        }


@pytest.fixture
def soa_id():
    return client.post("/soa", json={"name": "DSS detail"}).json()["id"]


@pytest.fixture
def requests_seen(monkeypatch):
    monkeypatch.setenv("CDISC_BC_API_BASE_URL", BASE)
    seen = []

    def fake_get(url, **_kwargs):
        seen.append(url)
        return _Resp()

    monkeypatch.setattr(requests, "get", fake_get)
    return seen


def test_invalid_href_shows_error_without_fetching(soa_id, requests_seen):
    r = client.get(
        f"/ui/soa/{soa_id}/dss/detail",
        params={"href": "https://evil.example.com/steal", "title": "Bad"},
    )
    assert r.status_code == 200
    assert "Invalid href: only CDISC Library API URLs are permitted." in r.text
    assert requests_seen == []


def test_missing_href_shows_error(soa_id, requests_seen):
    r = client.get(f"/ui/soa/{soa_id}/dss/detail")
    assert r.status_code == 200
    assert "No href provided." in r.text
    assert requests_seen == []


def test_valid_href_renders_specialization(soa_id, requests_seen):
    r = client.get(
        f"/ui/soa/{soa_id}/dss/detail",
        params={"href": DSS_HREF, "title": "Systolic BP"},
    )
    assert r.status_code == 200
    assert "Systolic BP" in r.text
    assert "VSTESTCD" in r.text
    assert requests_seen == [DSS_HREF]


def test_unknown_soa_404(requests_seen):
    r = client.get("/ui/soa/987654/dss/detail", params={"href": DSS_HREF})
    assert r.status_code == 404
