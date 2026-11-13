"""Pytest configuration to isolate tests from the production SoA database.

- Forces tests to use a dedicated SQLite file: ``soa_builder_web_tests.db`` (or the
        value of ``SOA_BUILDER_DB`` if set) so local/prod data are never touched.
- Removes any stale WAL/SHM files at the start of the test session to avoid
        "disk I/O" errors from interrupted runs.
"""

import os
import socket
from pathlib import Path

import pytest
import requests


# Use a test-specific database file in the current working directory (where pytest is invoked)
TEST_DB_PATH = Path(
    os.environ.get("SOA_BUILDER_DB", "soa_builder_web_tests.db")
).absolute()
# Only set if not already overridden externally, so imports pick this path
os.environ.setdefault("SOA_BUILDER_DB", str(TEST_DB_PATH))

# Ensure directory exists (for absolute paths inside nested structures)
TEST_DB_PATH.parent.mkdir(parents=True, exist_ok=True)


def _remove_db_files(path: Path) -> None:
    """Remove the SQLite database and any WAL/SHM sidecar files if present."""
    for suffix in ("", "-wal", "-shm"):
        try:
            Path(str(path) + suffix).unlink()
        except FileNotFoundError:
            pass


# Clean the test DB immediately upon importing conftest, prior to app import
_remove_db_files(TEST_DB_PATH)


# --- Offline test suite -------------------------------------------------
# Tests must never reach the network (CDISC Library, NCIt, ...). Real
# HTTP is blocked at the transport layer, below ``requests.get`` and
# ``Session.get``, so tests that mock those still work. Unmocked calls
# fail fast with a ``requests.ConnectionError``, the same path the app
# already handles when the network is down. This is installed at import
# time so it also covers migrations that run when ``app`` is imported.

# Developer credentials would change which code paths run (e.g. "no API
# key configured" vs "remote call failed"). Strip them so local runs
# behave like CI; tests that need a key set one via ``monkeypatch``.
_CDISC_CREDENTIAL_VARS = (
    "CDISC_API_KEY",
    "CDISC_SUBSCRIPTION_KEY",
    "CDISC_CRF_API_KEY",
    "CDISC_CRF_SUBSCRIPTION_KEY",
    "CDISC_LIBRARY_API_KEY",
    "CDISC_LIBRARY_API_KEY_DEV",
)
for _var in _CDISC_CREDENTIAL_VARS:
    os.environ.pop(_var, None)

_NETWORK_DISABLED_MSG = (
    "Network access is disabled in tests; mock the call instead "
    "(e.g. monkeypatch requests.get or set CDISC_CONCEPTS_JSON)."
)


class NetworkDisabledError(requests.ConnectionError):
    """Raised when a test tries to make a real network request."""


def _blocked_adapter_send(self, request, **kwargs):
    raise NetworkDisabledError(
        f"{_NETWORK_DISABLED_MSG} Attempted: {request.method} {request.url}"
    )


requests.adapters.HTTPAdapter.send = _blocked_adapter_send

_LOOPBACK_HOSTS = {"127.0.0.1", "::1", "localhost"}
_original_socket_connect = socket.socket.connect


def _guarded_socket_connect(self, address):
    """Block non-loopback TCP/UDP connects from non-requests clients."""
    if self.family in (socket.AF_INET, socket.AF_INET6):
        host = address[0] if isinstance(address, tuple) else address
        if host not in _LOOPBACK_HOSTS:
            raise NetworkDisabledError(
                f"{_NETWORK_DISABLED_MSG} Attempted socket connect: {host}"
            )
    return _original_socket_connect(self, address)


socket.socket.connect = _guarded_socket_connect


@pytest.fixture(scope="session", autouse=True)
def clean_test_db_session():
    # Optionally clean after session; comment out to inspect DB after tests
    yield
    # _remove_db_files(TEST_DB_PATH)
