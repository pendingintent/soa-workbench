import os
import sqlite3

from dotenv import load_dotenv

load_dotenv()
DB_PATH = os.environ.get("SOA_BUILDER_DB", "soa_builder_web.db")


def _connect():
    conn = sqlite3.connect(DB_PATH, timeout=5.0, check_same_thread=False)
    try:
        # Improve concurrency and reduce lock errors
        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute("PRAGMA synchronous=NORMAL")
        conn.execute("PRAGMA busy_timeout=3000")
    except Exception:
        pass
    return conn
