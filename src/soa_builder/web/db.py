import os
import sqlite3
import sys

from dotenv import load_dotenv

# Load environment variables from .env early
load_dotenv()

_PRODUCTION_DB = "soa_builder_web.db"


def _resolve_db_path() -> str:
    """Resolve the database path at call time (not import time).

    Priority: SOA_BUILDER_DB env var > pytest detection > production default.
    Evaluated fresh on every call so import order does not affect test isolation.
    """
    env_db = os.environ.get("SOA_BUILDER_DB")
    if env_db:
        return env_db
    is_pytest = "PYTEST_CURRENT_TEST" in os.environ or "pytest" in sys.modules
    return "soa_builder_web_tests.db" if is_pytest else _PRODUCTION_DB


# Module-level snapshot kept for backward-compat (used only for logging in app.py).
# All actual connections go through _connect() which re-evaluates dynamically.
DB_PATH = _resolve_db_path()


def _connect():
    db_path = _resolve_db_path()
    # Hard guard: tests must never connect to the production database
    if db_path == _PRODUCTION_DB and (
        "PYTEST_CURRENT_TEST" in os.environ or "pytest" in sys.modules
    ):
        raise RuntimeError(
            f"Tests must not connect to the production database '{_PRODUCTION_DB}'. "
            "Set SOA_BUILDER_DB to a test-specific path."
        )
    conn = sqlite3.connect(db_path, timeout=5.0, check_same_thread=False)
    try:
        is_pytest = "PYTEST_CURRENT_TEST" in os.environ or "pytest" in sys.modules
        if is_pytest:
            conn.execute("PRAGMA journal_mode=DELETE")
            conn.execute("PRAGMA synchronous=OFF")
        else:
            conn.execute("PRAGMA journal_mode=WAL")
            conn.execute("PRAGMA synchronous=NORMAL")
        conn.execute("PRAGMA busy_timeout=3000")
    except Exception:
        pass
    return conn
