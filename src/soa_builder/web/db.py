import os
import sqlite3
import sys

from dotenv import load_dotenv

# Load environment variables from .env early
load_dotenv()

# Prefer explicit env var; otherwise, auto-select test DB under pytest
_env_db = os.environ.get("SOA_BUILDER_DB")
_running_pytest = "PYTEST_CURRENT_TEST" in os.environ or "pytest" in sys.modules
if _env_db:
    DB_PATH = _env_db
else:
    DB_PATH = "soa_builder_web_tests.db" if _running_pytest else "soa_builder_web.db"


def _connect():
    conn = sqlite3.connect(DB_PATH, timeout=5.0, check_same_thread=False)
    try:
        # Improve concurrency and reduce lock errors; favor simpler mode under pytest
        _is_pytest = "PYTEST_CURRENT_TEST" in os.environ or "pytest" in sys.modules
        if _is_pytest:
            conn.execute("PRAGMA journal_mode=DELETE")
            conn.execute("PRAGMA synchronous=OFF")
        else:
            conn.execute("PRAGMA journal_mode=WAL")
            conn.execute("PRAGMA synchronous=NORMAL")
        conn.execute("PRAGMA busy_timeout=3000")
    except Exception:
        pass
    return conn
