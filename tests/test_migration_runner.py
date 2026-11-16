"""Migration registry/runner: run once, record, skip; recurring always."""

import sqlite3

import pytest

from soa_builder.web.app import _connect  # noqa: F401  (boots the app DB)
from soa_builder.web.db import _connect as db_connect
from soa_builder.web.migrations import registry
from soa_builder.web.migrations.runner import run_pending_migrations


@pytest.fixture
def scratch_db(tmp_path):
    path = tmp_path / "runner.db"
    return lambda: sqlite3.connect(path)


def _recorded(connect) -> set:
    conn = connect()
    try:
        return {r[0] for r in conn.execute("SELECT name FROM schema_migrations")}
    finally:
        conn.close()


def test_runs_once_then_skips(scratch_db):
    calls = []
    migrations = [("a", lambda: calls.append("a")), ("b", lambda: calls.append("b"))]
    assert run_pending_migrations(migrations, (), scratch_db) == ["a", "b"]
    assert run_pending_migrations(migrations, (), scratch_db) == []
    assert calls == ["a", "b"]
    assert _recorded(scratch_db) == {"a", "b"}


def test_recurring_runs_every_time_and_is_not_recorded(scratch_db):
    calls = []
    migrations = [
        ("once", lambda: calls.append("once")),
        ("r", lambda: calls.append("r")),
    ]
    run_pending_migrations(migrations, {"r"}, scratch_db)
    run_pending_migrations(migrations, {"r"}, scratch_db)
    assert calls == ["once", "r", "r"]
    assert _recorded(scratch_db) == {"once"}


def test_failed_migration_is_retried_and_later_ones_still_run(scratch_db):
    state = {"fail": True}

    def flaky():
        if state["fail"]:
            raise RuntimeError("boom")

    migrations = [("flaky", flaky), ("after", lambda: None)]
    assert run_pending_migrations(migrations, (), scratch_db) == ["after"]
    assert _recorded(scratch_db) == {"after"}
    state["fail"] = False
    assert run_pending_migrations(migrations, (), scratch_db) == ["flaky"]


def test_new_migration_appended_later_runs_on_next_boot(scratch_db):
    base = [("a", lambda: None)]
    run_pending_migrations(base, (), scratch_db)
    assert run_pending_migrations(base + [("b", lambda: None)], (), scratch_db) == ["b"]


def test_duplicate_names_rejected(scratch_db):
    with pytest.raises(ValueError, match="Duplicate"):
        run_pending_migrations(
            [("a", lambda: None), ("a", lambda: None)], (), scratch_db
        )


def test_registry_names_unique_and_recurring_known():
    names = [name for name, _ in registry.MIGRATIONS]
    assert len(names) == len(set(names))
    assert set(registry.RECURRING) <= set(names)
    assert all(callable(fn) for _, fn in registry.MIGRATIONS)


def test_second_boot_runs_only_recurring_migrations():
    """The app's import already ran the registry against the test DB."""
    ran = run_pending_migrations()
    assert set(ran) == set(registry.RECURRING)
    conn = db_connect()
    try:
        recorded = {r[0] for r in conn.execute("SELECT name FROM schema_migrations")}
    finally:
        conn.close()
    assert recorded == {n for n, _ in registry.MIGRATIONS} - set(registry.RECURRING)
