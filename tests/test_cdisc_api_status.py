"""Tests for the /ui/cdisc-api-status HTMX partial endpoint."""

import requests as _requests
from unittest.mock import MagicMock, patch

from fastapi.testclient import TestClient

from soa_builder.web.app import app

client = TestClient(app)


def test_cdisc_status_skip_remote(monkeypatch):
    monkeypatch.setenv("CDISC_SKIP_REMOTE", "1")
    monkeypatch.delenv("CDISC_API_KEY", raising=False)
    monkeypatch.delenv("CDISC_SUBSCRIPTION_KEY", raising=False)
    resp = client.get("/ui/cdisc-api-status")
    assert resp.status_code == 200
    assert "am-api-notice-info" in resp.text


def test_cdisc_status_no_key(monkeypatch):
    monkeypatch.delenv("CDISC_SKIP_REMOTE", raising=False)
    monkeypatch.delenv("CDISC_API_KEY", raising=False)
    monkeypatch.delenv("CDISC_SUBSCRIPTION_KEY", raising=False)
    resp = client.get("/ui/cdisc-api-status")
    assert resp.status_code == 200
    assert "am-api-notice-warn" in resp.text


def test_cdisc_status_ok(monkeypatch):
    monkeypatch.delenv("CDISC_SKIP_REMOTE", raising=False)
    monkeypatch.setenv("CDISC_API_KEY", "test-key")
    mock_resp = MagicMock()
    mock_resp.status_code = 200
    with patch("soa_builder.web.app.requests.get", return_value=mock_resp):
        resp = client.get("/ui/cdisc-api-status")
    assert resp.status_code == 200
    assert "am-api-notice-ok" in resp.text


def test_cdisc_status_connection_error(monkeypatch):
    monkeypatch.delenv("CDISC_SKIP_REMOTE", raising=False)
    monkeypatch.setenv("CDISC_API_KEY", "test-key")
    with patch(
        "soa_builder.web.app.requests.get",
        side_effect=_requests.exceptions.ConnectionError("unreachable"),
    ):
        resp = client.get("/ui/cdisc-api-status")
    assert resp.status_code == 200
    assert "am-api-notice-error" in resp.text


def test_cdisc_status_timeout(monkeypatch):
    monkeypatch.delenv("CDISC_SKIP_REMOTE", raising=False)
    monkeypatch.setenv("CDISC_API_KEY", "test-key")
    with patch(
        "soa_builder.web.app.requests.get",
        side_effect=_requests.exceptions.Timeout("timed out"),
    ):
        resp = client.get("/ui/cdisc-api-status")
    assert resp.status_code == 200
    assert "am-api-notice-error" in resp.text


def test_cdisc_status_server_error(monkeypatch):
    monkeypatch.delenv("CDISC_SKIP_REMOTE", raising=False)
    monkeypatch.setenv("CDISC_API_KEY", "test-key")
    mock_resp = MagicMock()
    mock_resp.status_code = 503
    with patch("soa_builder.web.app.requests.get", return_value=mock_resp):
        resp = client.get("/ui/cdisc-api-status")
    assert resp.status_code == 200
    assert "am-api-notice-error" in resp.text
