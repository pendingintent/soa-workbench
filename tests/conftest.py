"""Pytest configuration to isolate tests from the production SoA database.

- Forces tests to use a dedicated SQLite file: ``soa_builder_web_tests.db`` (or the
        value of ``SOA_BUILDER_DB`` if set) so local/prod data are never touched.
- Removes any stale WAL/SHM files at the start of the test session to avoid
        "disk I/O" errors from interrupted runs.
"""

import os
from pathlib import Path
import pytest


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


@pytest.fixture(scope="session", autouse=True)
def clean_test_db_session():
    # Optionally clean after session; comment out to inspect DB after tests
    yield
    # _remove_db_files(TEST_DB_PATH)
