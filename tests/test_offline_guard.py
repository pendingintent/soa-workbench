"""The test suite must run fully offline (see conftest.py)."""

import os
import socket

import pytest
import requests

from conftest import _CDISC_CREDENTIAL_VARS, NetworkDisabledError


def test_requests_get_is_blocked():
    with pytest.raises(NetworkDisabledError):
        requests.get("https://library.cdisc.org/api/mdr/ct/packages", timeout=1)


def test_blocked_call_is_a_connection_error():
    """App code that handles ConnectionError keeps working offline."""
    with pytest.raises(requests.ConnectionError):
        requests.get("https://api.library.cdisc.org/api/cosmos/v2", timeout=1)


def test_raw_socket_connect_is_blocked():
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    try:
        with pytest.raises(NetworkDisabledError):
            sock.connect(("192.0.2.1", 443))
    finally:
        sock.close()


def test_mocked_requests_get_still_works(monkeypatch):
    class _Resp:
        status_code = 200

    monkeypatch.setattr(requests, "get", lambda *a, **k: _Resp())
    assert requests.get("https://library.cdisc.org").status_code == 200


def test_cdisc_credentials_are_stripped():
    assert not any(os.environ.get(var) for var in _CDISC_CREDENTIAL_VARS)
