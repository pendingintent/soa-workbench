# Database migrations

`app.py` calls `_init_db()` (base schema) at import time and then
`run_pending_migrations()`, which walks `registry.MIGRATIONS` in order:

- A migration not yet in the `schema_migrations` table runs and, if it
  returns normally, is recorded there. Recorded migrations are skipped
  on later boots.
- Migrations listed in `registry.RECURRING` run on every boot and are
  never recorded: they repair data that current code paths can still
  produce. Prefer fixing the write path and removing an entry over
  adding one.

## Adding a migration

1. Write it as a module-level function in a new module under this
   package (e.g. `m0114_add_widget_table.py`), not in
   `migrate_database.py`, which is frozen.
2. Make it idempotent (`CREATE TABLE IF NOT EXISTS`, check
   `PRAGMA table_info` before `ALTER TABLE`).
3. Let errors raise. The runner logs them, leaves the migration
   unrecorded and retries it next boot. Do not swallow exceptions the
   way the legacy functions do.
4. Append `("m0114_add_widget_table", fn)` to the end of `MIGRATIONS`.
   Never reorder or rename existing entries: names are the record keys.
5. Update `tests/snapshots/schema.json` with
   `UPDATE_SNAPSHOTS=1 pytest tests/test_schema_snapshot.py`.
