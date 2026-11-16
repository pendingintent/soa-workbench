# soa-workbench Improvement Plan

> **Revision 2, 2026-11-13.** Re-verified against `master` @ `e2fcbf9`
> (v1.6.2 + #371 + #383). Changes from revision 1 (2026-07-15): marked
> Workstream A done; added Workstream D (data integrity) ahead of the
> refactor; redesigned the MCP expansion (B.2) to call the FastAPI app
> in-process instead of reimplementing router SQL; refreshed every count;
> added exit criteria per phase.

## Context

The project is functionally healthy (79 test files, ruff + pytest
pre-commit, CI in `.github/workflows/ci.yml`), but it carries structural
debt that slows every change.

- **Oversized hotspots** (lines as of 2026-11-13):
  `app.py` 7,057; `create_define_json.py` 4,505; `migrate_database.py`
  3,570 (111 migration functions, 110 bare calls run at import time on
  every startup); `amendments.py` 2,617; `utils.py` 2,150 (8 copy-pasted
  TTL caches, plus the new `uid_counter` helpers);
  `routers/_freeze_helpers.py` 1,703; `generate_dair.py` 1,635;
  `activities.py` 1,615; `audit.py` 939 (33 copy-pasted `_record_*_audit`
  functions that swallow exceptions).
- **The recurring defect class is freeze/rollback coverage.** Fixes
  #326, #369–#372, #378 and #371 all fixed a table that freeze/rollback
  silently dropped or mishandled. Each soa-scoped table needs hand-written
  `_capture_*` and restore code, and nothing fails when a table is
  missed. A fresh DB has **88 tables with a `soa_id` column**.
- **Test gaps:** only the 5 `*_terminology` routers have no tests.
  Freeze/rollback gained three test files in #371.
- **MCP server** (`src/soa_builder/mcp/server.py`, 11 tools) covers a
  small fraction of the 165+ endpoint API. Its write tools reimplement
  router SQL and skip the audit trail. `_create_activity` computes
  `Activity_N` as max+1 over the live table only, so it **can reissue a
  deleted activity's UID**. The router's `_next_activity_uid()` also
  scans the audit trail to prevent this.
- **DB hygiene:** no per-connection `PRAGMA foreign_keys=ON`; most
  entity tables have a leading `soa_id` index, but the code/BC child
  tables and all `*_audit` tables do not; `migrate_database.py:14` holds
  a dead `DB_PATH`.

Ground rules: each workstream step is its own branch/PR off `master`.
Python commands use `source .venv/bin/activate &&`. Never touch
`soa_builder_web.db`; ad-hoc smoke tests must set
`SOA_BUILDER_DB=soa_builder_web_tests.db` (or a `$TMPDIR` path).

---

## Workstream A — Claude agents, skills & memory ✅ (essentially done)

Done before revision 2: USDM routing section in `CLAUDE.md`;
`dair-report-writer` skill; seeded `usdm-soa-protocol-expert` memory;
skill frontmatter; db-ops-reviewer memory re-verified (2026-11-13);
`CLAUDE.md` tooling text corrected from black/flake8 to ruff.

Leftovers:
- Re-verify `bcp_architecture.md` and `feedback_ncit_confidence.md` in
  the project memory dir and stamp them with a verification date.
- Optional cleanup: drop `black` from the dev dependencies in
  `pyproject.toml` and delete `.flake8`. Pre-commit and CI use ruff only.

## Workstream D — Data integrity (NEW, do first)

Small changes that remove the bug class that is actually causing
defects. They also shrink the later `_freeze_helpers.py` split.

1. **D.1 Snapshot table registry.** Replace the hand-maintained
   `_capture_*` and restore pairs in `routers/_freeze_helpers.py` and
   `routers/rollback.py` with one declarative `SNAPSHOT_TABLES` spec:
   table, key column, UID column (+ `uid_counter` prefix), parent/child
   relations and restore order. Regular tables go through the generic
   capture path and the existing `_restore_rows()`. Irregular ones
   (matrix cells incl. legacy visit-keyed cells, study cells, BC
   properties) keep explicit handlers, which the spec names. The freeze
   JSON format stays unchanged, so existing freezes still roll back.
2. **D.2 Coverage guard test**: `tests/test_snapshot_coverage.py`.
   On a fresh DB, enumerate every table with a `soa_id` column and
   assert it is either in `SNAPSHOT_TABLES` or in `SNAPSHOT_EXCLUDED`
   with a written reason. Seed the exclusions with `uid_counter` (UIDs
   are monotonic and must never roll back), every `*_audit` table, and
   `rollback_audit`. This turns the CLAUDE.md "Freeze and Rollback"
   warning into a failing test.
3. **D.3 Close the `soa_id` index gaps.** Extend the pattern in
   `_migrate_add_soa_id_indexes` (`migrate_database.py` ~1473) as a new
   migration covering `activity_concept_crf`, `activity_concept_dss`,
   `alias_code`, `bcp_response_code`, `biomedical_concept`,
   `biomedical_concept_property`, `biomedical_concept_surrogate`,
   `code`, `code_association`, `estimand_intervention`,
   `estimand_variable`, `governance_date_geographic_scope`,
   `indication_code`, `study_intervention_code`, and the `*_audit` tables
   that the audit-history views filter by `soa_id`. Confirm with
   `EXPLAIN QUERY PLAN` on the hot queries before and after.
4. **D.4 Foreign keys: investigate, then decide.** Run
   `PRAGMA foreign_key_check` against a *copy* of a real DB and record
   the orphan rows. If a cleanup migration can clear them, turn on
   `PRAGMA foreign_keys=ON` in `db.py` (with a test that an orphan
   insert fails). Otherwise, document it as an accepted limitation in
   the db-ops-reviewer memory.
5. **D.5** Delete the dead `DB_PATH` at `migrate_database.py:14`.

## Workstream B — MCP server

File: `src/soa_builder/mcp/server.py`; tests: `tests/test_mcp_server.py`.

1. **B.1 Immediate fixes to the existing tools**
   - `_create_activity`: allocate the UID through the router's
     `_next_activity_uid()` (`routers/activities.py:143`). Remove the
     silent try/except fallback that drops label/description. Add a
     regression test: create → delete → create returns a new UID.
   - `assign_instance_activity`: validate that `instance_id` and
     `activity_id` belong to `soa_id`.
   - Add `limit`/`offset` to `list_soas`, `list_visits` and
     `list_activities`.
   - Audit: done for free by B.2 when the write tools are ported.
     Until then, call the `audit.py` recorders directly.
2. **B.2 New tools call the FastAPI app in-process (design change)**
   - Each MCP tool sends a request to the existing API through
     `httpx.ASGITransport(app=app)` (or `TestClient`) instead of writing
     SQL. Validation, UID allocation (`uid_counter`), audit and the
     duplicate-cell protections all come from the routers, so the MCP
     server cannot drift from them. Port the 5 existing write tools the
     same way, then delete their SQL.
   - No dependency on Workstream C. The route-inventory test (C Phase 0)
     guarantees the refactor cannot break the URLs the tools call.
   - Choose tools by workflow, not one per endpoint:
     - **Freeze/rollback**: `list_freezes`, `create_freeze`,
       `rollback_freeze` (reports the safety freeze it creates),
       `diff_freezes`.
     - **Design entities**: create/update/delete for epochs, arms,
       elements and study cells.
     - **Instances**: `create_instance`, `update_instance`,
       `delete_instance`.
     - **Audit**: `get_audit_history(soa_id, entity_type?, limit,
       offset)`.
     - **Reports/exports**: `generate_dair(soa_id, base_freeze_id,
       revised_freeze_id)`, `export_excel(soa_id)`.
     - **Validation**: `validate_soa(soa_id)`.
   - Errors: map HTTP 4xx responses to structured MCP errors that carry
     the router's `detail` message.
   - Tests: keep calling `_dispatch()` directly; for every mutating
     tool, assert that the audit row exists.

## Workstream C — Structural refactor (phased; each phase lands green)

Facts that shape the design: migrations run at **module import time**
in `app.py` (`_init_db()` at line 320, then 110 bare calls at lines
324–439), not in the lifespan. `_connect` lives in `web/db.py` and is
re-exported through `initialize_database.py` into `app.py`. Many test
files (and `usdm/generate_study_titles.py`) import from
`soa_builder.web.app`, so `app.py` must remain a permanent compatibility
shim. Routers avoid circular imports by using deferred
`from ..app import ...` inside function bodies.

### Phase 0 — Remaining characterization tests
- Happy-path + 404 tests for the 5 `*_terminology` routers. Mock
  `requests.get`, following `tests/test_fetch_sdtm_specializations.py`.
- `_diff_entity_list` unit tests. The freeze/rollback round trips are
  already covered by `tests/test_freeze_rollback_*.py`, and D.2 covers
  table completeness.
- **Golden schema test** (`tests/test_schema_snapshot.py`): full
  `sqlite_master` inventory after a fresh init, compared with a
  checked-in snapshot. This is the safety net for Phases 1 and 4.
- **Route-inventory test** (`tests/test_route_inventory.py`): the
  `{(method, path)}` set compared with a checked-in snapshot. This is
  the safety net for Phases 3 and 5 and for the B.2 MCP tools.

### Phase 1 — Migration registry (hand-rolled, not Alembic)
The app uses raw `sqlite3` (no SQLAlchemy), and the migrations are
idempotent, imperative Python functions. A registry of about 70 lines
keeps them unchanged.
- New `src/soa_builder/web/migrations/` package:
  - `registry.py`: an ordered list
    `MIGRATIONS: list[tuple[name, callable]]` that imports the existing
    functions unchanged.
  - `runner.py`: `run_pending_migrations()`, backed by a
    `schema_migrations(name PRIMARY KEY, applied_at)` table; recorded
    entries are skipped.
- Baselining costs nothing. The first boot runs all of them (no-ops on
  an up-to-date DB, as every boot does today) and records them.
- In `app.py`, replace the import block and the call block with a
  single `run_pending_migrations()` call. It stays at import time,
  because tests rely on "import app ⇒ DB ready".
- **Forward convention:** new migrations are `m0112_<desc>` modules in
  `migrations/` that **raise on failure**. The runner records a
  migration only after it returns cleanly, so a failed new migration
  retries on the next boot. Legacy functions keep their
  swallow-and-log behavior, protected by idempotency.
  `migrate_database.py` is frozen.

### Phase 2 — `CachedResource` + generic audit recorder
- New `src/soa_builder/web/caching.py` with `CachedResource`, a
  `MutableMapping` subclass. Existing code and tests use the cache dicts
  directly, so the 8 replacements keep their names and dict behavior.
  Methods: `is_fresh(ttl)`, `store()`, `fail()`, `invalidate()`. Also
  move `_get_ct_rows()` onto it.
- Rewrite `audit.py` around one core function, `_record_audit(table,
  entity_col, soa_id, action, entity_id, before, after)`, plus
  declarative per-table specs. Irregular tables such as `reorder_audit`
  get explicit paths. The 33 `_record_*_audit` names stay as thin
  wrappers with identical signatures and identical INSERT columns.
- Keep the deliberate broad catch ("audit must never break the
  request"), but log at **error** level and increment a module counter.
  An autouse fixture in `conftest.py` asserts the counter is 0, so
  audit failures fail tests instead of passing silently.

### Phase 3 — `app.py` split (three sub-PRs)
Target layout:
- `application.py`: FastAPI instance, mounts, `include_router` calls.
- `bootstrap.py`: `_init_db()` + `run_pending_migrations()`, run at
  import.
- `lifespan.py` and `templating.py`.
- `services/concepts.py` and `services/matrix.py`: business logic.
- Four new routers for the roughly 90 inline endpoints:
  `routers/soa_core.py`, `routers/exports.py`,
  `routers/concepts_admin.py`, `routers/soa_ui.py`. They have no
  prefixes and are registered in the original order, so URLs are
  identical.

Sub-PRs:
- **3a: services extraction.** Move the concept and matrix logic to
  `services/`. Repoint the deferred `from ..app import ...` in
  `routers/activities.py`, `bc_categories.py`, `bc_surrogates.py`,
  `concept_groups.py`, `rollback.py` and `_freeze_helpers.py` to
  top-of-file `..services.*` imports.
- **3b: endpoint extraction** into the four routers. The
  route-inventory test proves the result is equivalent.
- **3c: composition root + permanent shim.** `app.py` becomes explicit
  re-exports. Build the list from a grep for
  `from soa_builder.web.app import` and `from ..app import` across
  `src/` and `tests/`. Do NOT migrate the test files. The
  `soa-builder-web = "soa_builder.web.app:main"` entry point stays.
- Guard: services modules never import `application` at module level.

### Phase 4 — Split `migrate_database.py` by domain
This is a pure code move once Phase 1 is in. Create
`migrations/legacy_core.py`, `legacy_concepts.py`,
`legacy_amendments.py`, `legacy_people_orgs.py` and
`legacy_terminology.py` with the bodies unchanged.
`migrate_database.py` becomes a re-export shim. Grep for direct test
imports first.

### Phase 5 — Shrink `amendments.py` / `activities.py`; split `_freeze_helpers.py`
- Turn each large router file into a package that shares the router
  instances.
  - `routers/amendments/` contains `_shared.py` (`router`, `ui_router`,
    `templates`, UID/code helpers), `crud.py`, `components.py`,
    `scopes.py` and `ui.py`. `__init__.py` re-exports
    `router`/`ui_router`, so URLs stay identical.
  - `routers/activities/` follows the same pattern, with `crud.py`,
    `concepts.py`, `dss.py`, `crf.py` and `ui.py`.
- Re-scope the `_freeze_helpers.py` split after D.1. Whatever is left
  goes into `routers/freeze_support/{capture,diff,rollback}.py`.
  `_freeze_helpers.py` stays as a re-export shim, because `usdm/`
  imports from it.
- The DAIR branch that blocked this phase in revision 1 has merged, so
  there is no longer a sequencing constraint.

### Phase 6 — Mechanical sweeps (last)
- Narrow the 85 router `except Exception` clauses to `sqlite3.Error`,
  `requests.RequestException` or `(KeyError, ValueError)`, as the code
  justifies. Explicitly keep the broad catches in `audit.py`,
  `lifespan.py` and the legacy migrations. Make one commit per router,
  then enable ruff `BLE001`.
- Add return-type hints module by module. Optionally enable ruff
  `ANN201` for `src/soa_builder/web/`. Avoid rules that conflict with
  `ruff-format` (`ISC001`, `COM812`, `W191`, `E111`).
- Out of scope for now: splitting `usdm/create_define_json.py` and
  `generate_dair.py` beyond repointing their `app` imports in Phase 3.
  That is a follow-up with its own characterization pass.

---

## Ordering

A leftovers → **D.1 + D.2** → D.3, D.5 → **B.1** → C0 → **B.2**
(needs the route-inventory test) → C1 → C2 → C3 → C4 → C5 → C6.

D.4 (foreign key investigation) runs in parallel and decides its own
follow-up.

## Exit criteria

| Step | Done when |
|------|-----------|
| D.1/D.2 | `test_snapshot_coverage.py` passes; adding a dummy soa-scoped table without registering it makes the test fail; all `test_freeze_rollback_*` tests pass; freezes from before D.1 still roll back |
| D.3 | `EXPLAIN QUERY PLAN` shows an index search on the listed tables |
| B.1 | UID regression test passes; FK validation tests pass |
| B.2 | Every MCP mutation has an audit-row assertion; `server.py` has no `INSERT`/`UPDATE`/`DELETE` SQL |
| C0 | Schema and route snapshots checked in; each terminology router has at least 2 tests |
| C1 | `app.py` has no `_migrate_*` calls; second boot runs 0 migrations; the schema snapshot is unchanged |
| C2 | `utils.py` has no hand-rolled TTL cache dicts; the audit failure counter fixture is active |
| C3 | `app.py` is under 200 lines; `/openapi.json` is byte-identical before and after; smoke test passes |
| C4–C5 | No source file in `web/` exceeds about 1,500 lines (except shims); route snapshot is unchanged |
| C6 | ruff `BLE001` is enabled with no errors |

## Verification (every step)

- `source .venv/bin/activate && pytest` passes.
- `pre-commit run --all-files` (ruff-check --fix, ruff-format, pytest)
  passes before each commit.
- Run the smoke test through the `run-soa-workbench` skill
  (`smoke.sh`) with `SOA_BUILDER_DB=soa_builder_web_tests.db`, after
  D.1, B.2 and C3.
- Test the migration registry against (a) a fresh empty DB and (b) a
  copy of an existing fully migrated DB, never the production DB
  itself.
