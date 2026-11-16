"""Run database migrations once each, recording them in schema_migrations.

Baseline: on an existing database the first boot runs every migration
(all idempotent no-ops there, exactly as every boot did before the
registry) and records them; later boots skip the recorded ones.

Legacy migrations swallow their own errors, so they always "succeed"
and get recorded, as before. New migrations should raise on failure:
the runner logs the error, does not record the migration, and retries
it on the next boot.
"""

import logging
from datetime import datetime, timezone
from typing import Callable, Iterable, Optional

from ..db import _connect
from .registry import MIGRATIONS, RECURRING

logger = logging.getLogger("soa_builder.web.migrations")

_CREATE_TABLE = (
    "CREATE TABLE IF NOT EXISTS schema_migrations ("
    " name TEXT PRIMARY KEY,"
    " applied_at TEXT NOT NULL)"
)


def _applied_names(connect: Callable) -> set:
    conn = connect()
    try:
        conn.execute(_CREATE_TABLE)
        conn.commit()
        return {r[0] for r in conn.execute("SELECT name FROM schema_migrations")}
    finally:
        conn.close()


def _record(connect: Callable, name: str) -> None:
    conn = connect()
    try:
        conn.execute(
            "INSERT OR IGNORE INTO schema_migrations (name, applied_at) VALUES (?, ?)",
            (name, datetime.now(timezone.utc).isoformat()),
        )
        conn.commit()
    finally:
        conn.close()


def run_pending_migrations(
    migrations: Optional[Iterable] = None,
    recurring: Optional[Iterable[str]] = None,
    connect: Callable = _connect,
) -> list:
    """Run migrations not yet recorded, plus every recurring one.

    Returns the names that ran this time, in order.
    """
    migrations = MIGRATIONS if migrations is None else tuple(migrations)
    recurring = set(RECURRING if recurring is None else recurring)
    names = [name for name, _ in migrations]
    if len(names) != len(set(names)):
        raise ValueError("Duplicate migration names in registry")
    applied = _applied_names(connect)
    ran = []
    for name, migration in migrations:
        is_recurring = name in recurring
        if name in applied and not is_recurring:
            continue
        try:
            migration()
        except Exception:
            logger.exception("Migration %s failed; it will be retried", name)
            continue
        ran.append(name)
        if not is_recurring:
            _record(connect, name)
    return ran
