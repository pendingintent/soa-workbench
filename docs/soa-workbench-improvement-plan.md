# soa-workbench Improvement Plan

## Context

A full-codebase analysis (core Python, Claude agents/skills, MCP server) found that the
project is functionally healthy (~257 passing tests, clean pre-commit pipeline, active
DAIR feature work) but carries structural debt that slows every change:

- **Oversized hotspots**: `app.py` (7,039 lines), `migrate_database.py` (3,319 lines /
  104 unversioned migrations run on every startup), `create_define_json.py` (4,505),
  `amendments.py` (2,596), `utils.py` (1,848 with 8 copy-pasted TTL caches),
  `audit.py` (939 with 40+ copy-pasted recorders that swallow exceptions).
- **Test gaps**: 10 routers have no tests, including the 1,377-line
  `_freeze_helpers.py` that powers freeze/rollback.
- **MCP server** (`src/soa_builder/mcp/server.py`, 11 tools) covers ~8% of the 165+
  endpoint API: no freeze/rollback, no epoch/arm/element/instance CRUD, no audit
  access, no DAIR/exports, no pagination; its write tools bypass the audit trail.
- **Claude config**: db-ops-reviewer agent memory asserts a "rogue `_connect()` in
  freezes.py" that has since been fixed; usdm-soa-protocol-expert has empty memory;
  three overlapping USDM experts lack routing guidance; skill frontmatter is
  incomplete.

User decisions: full structural refactor; high-value MCP workflow tools; agent/skill
work = stale-memory fixes + USDM routing clarity + a new DAIR report-writer skill
(no migration-advisor agent).

All work happens on a new branch off `master` (not the active
`pi-add-impact-analysis-report` branch). Python commands use
`source .venv/bin/activate &&`. Never touch `soa_builder_web.db`; ad-hoc smoke tests
must set `SOA_BUILDER_DB=soa_builder_web_tests.db`.

---

## Workstream A — Claude agents, skills & memory (small, do first)

Low-risk documentation/config edits; restores trust in agent output before the agents
are used to review the refactor.

1. **Fix db-ops-reviewer stale memory**
   - `.claude/agent-memory/db-ops-reviewer/MEMORY.md` and `patterns.md`: remove the
     freezes.py "rogue `_connect()` / module-level DB_PATH" claim (fixed in code —
     `routers/freezes.py` now uses `from ..db import _connect`); re-verify the other
     cited issues/line numbers against current code and update or delete.
2. **Initialize usdm-soa-protocol-expert memory**
   - Create `.claude/agent-memory/usdm-soa-protocol-expert/MEMORY.md` seeded with the
     recurring SoA→USDM mapping patterns already documented in the agent definition.
3. **Skill frontmatter fixes**
   - `.claude/skills/code-review/SKILL.md`: add missing frontmatter fields to match
     the other skills.
   - `.claude/skills/run-soa-workbench/SKILL.md`: add `command` declaration.
4. **USDM routing guidance**
   - Add a short "Which USDM expert to use" section to `CLAUDE.md` (or a shared note
     linked from all three): usdm-sme skill = standard/spec questions;
     usdm-implementation-expert agent = IG-v4 compliance/validation;
     usdm-soa-protocol-expert agent = SoA mapping + live workbench data via MCP.
     Cross-link the two project agents' MEMORY.md files.
5. **New skill: `dair-report-writer`**
   - `.claude/skills/dair-report-writer/SKILL.md` encoding the DAIR conventions
     established on the current branch: decode values instead of `Code_{n}`,
     "snapshot" terminology (never "freeze" in report text), table header-row
     repetition across page breaks, section 3.4/3.5 intent explanations, explanatory
     paragraphs under section-2 subsections. Point it at
     `src/usdm/generate_dair.py` and `routers/dair.py`.
6. **Refresh dated project memory**
   - Re-verify `bcp_architecture.md` and `feedback_ncit_confidence.md` in the
     project memory dir; update or annotate with verification date.

## Workstream B — MCP server expansion & hardening

File: `src/soa_builder/mcp/server.py` (raw MCP SDK, stdio, dispatch pattern);
tests: `tests/test_mcp_server.py`. Reuse the web layer's logic — do not duplicate
SQL: call into the same helpers routers use (e.g. `routers/_freeze_helpers.py`,
`usdm.generate_dair.build_dair`) where practical.

1. **Quality fixes to existing tools (do first)**
   - Route all 5 write tools through the audit recorders in `soa_builder/web/audit.py`
     (same before/after pattern routers use).
   - `assign_instance_activity`: validate `instance_id`/`activity_id` exist before
     insert.
   - `_create_activity`: remove the silent try/except fallback that drops
     label/description; fail with a clear error.
   - Add `limit`/`offset` params to `list_soas`, `list_visits`, `list_activities`.
2. **New workflow tools** (~15-20 tools, grouped; each with schema, FK validation,
   audit integration, tests):
   - **Freeze/rollback**: `list_freezes`, `create_freeze`, `rollback_freeze`,
     `diff_freezes` (reuse `_freeze_helpers.py` — after Workstream C adds its
     characterization tests).
   - **Design entities**: CRUD for epochs, arms, elements, study cells
     (mirror the router logic; keep UID generation identical).
   - **Instances**: `create_instance`, `update_instance`, `delete_instance`.
   - **Audit access**: `get_audit_history(soa_id, entity_type?, limit, offset)`.
   - **Reports/exports**: `generate_dair(soa_id, base_freeze_id, revised_freeze_id)`
     returning the DOCX path; `export_excel(soa_id)`.
   - **Validation**: `validate_soa(soa_id)` wrapping the existing validation module.
3. **Error handling**: wrap sqlite constraint violations into structured error
   messages; keep the ValueError-for-validation convention.

## Workstream C — Structural refactor (phased; each phase lands green)

Verified facts that shape the design: migrations run at **module import time** in
`app.py` (lines ~305–412: `_init_db()` + 104 bare calls), not in the lifespan;
`_connect` lives in `web/db.py` and is re-exported through `initialize_database.py`
into `app.py`; ~60 test files (and `usdm/generate_study_titles.py`) import from
`soa_builder.web.app`, so `app.py` must remain a permanent compat shim. Routers
avoid circular imports today via deferred `from ..app import ...` inside function
bodies. Each phase is its own branch/PR off `master`, merged sequentially.

### Phase 0 — Characterization tests (pure test additions)
- `tests/test_freeze_helpers_characterization.py`: TestClient round-trips over
  create SoA → populate → freeze → mutate → diff → rollback → preview; plus direct
  unit tests for `_diff_entity_list` and representative `_capture_*` functions.
- Thin happy-path + 404 tests for the other 9 untested routers (mock `requests.get`
  for the terminology ones, following `tests/test_fetch_sdtm_specializations.py`).
- **Golden schema test** (`tests/test_schema_snapshot.py`): full `sqlite_master`
  inventory after fresh init vs checked-in snapshot — safety net for Phases 1/4.
- **Route-inventory test** (`tests/test_route_inventory.py`): `{(method, path)}`
  set vs checked-in snapshot — safety net for Phases 3/5.

### Phase 1 — Migration registry (hand-rolled, not Alembic)
Alembic is wrong here: app is raw `sqlite3` (no SQLAlchemy), and the 104 migrations
are imperative idempotent Python functions — a ~70-line registry preserves them
byte-for-byte.
- New `src/soa_builder/web/migrations/`: `registry.py` (ordered
  `MIGRATIONS: list[tuple[name, callable]]` importing existing functions unchanged)
  and `runner.py` (`run_pending_migrations()` with a
  `schema_migrations(name PRIMARY KEY, applied_at)` table; skip recorded entries).
- **Baselining is free**: first boot runs all 104 (no-ops on up-to-date DBs, exactly
  like every boot today) and records them; subsequent boots skip all. Idempotency
  retained as safety net.
- In `app.py`: replace the 106-line import block + 104-call block with one
  `run_pending_migrations()` call — kept at module import time (tests depend on
  `import app` ⇒ DB ready).
- Convention forward: new migrations named `m0105_<desc>` in `migrations/`,
  appended to the registry; `migrate_database.py` frozen.
- Accepted limitation (documented, not fixed): existing migration functions swallow
  their own exceptions, so a failed migration can be recorded as applied — same
  semantics as today, protected by idempotency.

### Phase 2 — `CachedResource` + generic audit recorder
- New `src/soa_builder/web/caching.py`: `CachedResource` as a **`MutableMapping`
  subclass** (existing code and tests poke the cache dicts directly with
  `.update()`/`["data"]`, and tests mutate module-level instances in place — so the
  8 replacements keep the same names and dict compatibility). Methods:
  `is_fresh(ttl)`, `store()`, `fail()`, `invalidate()`. Tighten `_get_ct_rows()` to
  use it.
- Rewrite `audit.py` around one `_record_audit(table, entity_col, soa_id, action,
  entity_id, before, after)` core + declarative per-table specs (irregular tables
  like `reorder_audit` get explicit paths). All 33 `_record_*_audit` names stay as
  thin wrappers with identical signatures; exact INSERT columns preserved. Keep the
  intentional broad catch ("audit must never break the request") but log at
  **error** level instead of warning.

### Phase 3 — `app.py` split (three sub-PRs)
Target layout: `application.py` (FastAPI instance, mounts, include_router calls),
`bootstrap.py` (`_init_db()` + `run_pending_migrations()` at import),
`lifespan.py`, `templating.py`, `services/concepts.py` + `services/matrix.py`
(business logic), and four new routers for the ~90 inline endpoints:
`routers/soa_core.py`, `routers/exports.py`, `routers/concepts_admin.py`,
`routers/soa_ui.py` (no prefixes; registered in original order → identical URLs).
- **3a — services extraction**: move concept/matrix logic to `services/`; repoint
  the deferred `from ..app import ...` in `routers/activities.py`,
  `bc_categories.py`, `bc_surrogates.py`, `concept_groups.py`, `rollback.py`,
  `_freeze_helpers.py` to top-of-file `..services.*` imports (circularity gone).
- **3b — endpoint extraction** into the four routers; route-inventory test proves
  equivalence.
- **3c — composition root + permanent shim**: `app.py` becomes explicit re-exports
  (`app`, `main`, `_connect`, `templates`, `fetch_biomedical_concepts`, caches,
  etc. — final list grep-driven from `from soa_builder.web.app import` +
  `from ..app import` across `src/` and `tests/`). Do NOT migrate the 60 test
  files. `soa-builder-web = "soa_builder.web.app:main"` entry point untouched.
- Guard: services modules never import `application` at module level; check with
  `python -c "from soa_builder.web.app import app, _connect"` and an
  `/openapi.json` diff before/after.

### Phase 4 — Split `migrate_database.py` by domain
Pure code move now that orchestration is in the registry: `migrations/legacy_core.py`,
`legacy_concepts.py`, `legacy_amendments.py`, `legacy_people_orgs.py`,
`legacy_terminology.py` (bodies unchanged); `migrate_database.py` becomes a
re-export shim (grep for direct test imports first).

### Phase 5 — Shrink `amendments.py` / `activities.py`; split `_freeze_helpers.py`
File→package pattern with shared router instances: `routers/amendments/` with
`_shared.py` (`router`, `ui_router`, `templates`, UID/code helpers) + `crud.py`,
`components.py`, `scopes.py`, `ui.py`; `__init__.py` re-exports `router`/`ui_router`
so `application.py` imports and all URL paths stay identical. Same for
`routers/activities/` (`crud.py`, `concepts.py`, `dss.py`, `crf.py`, `ui.py`).
Split `_freeze_helpers.py` (now under Phase 0 tests) into
`routers/freeze_support/{capture,diff,rollback}.py` with `_freeze_helpers.py` as a
re-export shim (`usdm/` imports from it).
**Sequencing**: do this after `pi-add-impact-analysis-report` merges (that branch is
active in amendments/DAIR territory), or accept one mechanical rebase.

### Phase 6 — Mechanical sweeps (last)
- Narrow the ~82 router `except Exception` clauses to `sqlite3.Error`,
  `requests.RequestException`, `(KeyError, ValueError)` as evidence dictates;
  explicitly keep broad catches in `audit.py`, `lifespan.py`, and migrations. One
  commit per router; enable ruff `BLE001` at the end.
- Add return-type hints module-by-module; optionally enable ruff `ANN201` for
  `src/soa_builder/web/`.
- Out of scope (flagged follow-up with its own characterization pass):
  splitting `usdm/create_define_json.py` (4,505) and `generate_dair.py` (1,635)
  beyond repointing their `app` imports in Phase 3.

### Cross-workstream ordering
A (agent/skill fixes) → B.1 (MCP quality fixes) → C Phase 0 → C Phase 1 → C Phase 2
→ B.2 freeze/audit MCP tools (needs Phase 0 freeze tests) → C Phases 3–4 → remaining
B.2 tools → C Phase 5 (after DAIR branch merges) → C Phase 6.

## Verification

- `source .venv/bin/activate && pytest` green after every phase/workstream step.
- `pre-commit run --all-files` (black/ruff + pytest) before each commit.
- Smoke test via `.claude/skills/run-soa-workbench` (`smoke.sh`, port 9877, 14
  checks) after app.py split and after MCP changes, with
  `SOA_BUILDER_DB=soa_builder_web_tests.db`.
- MCP: extend `tests/test_mcp_server.py` (direct `_dispatch()` calls) for every new
  tool; verify audit rows are written for MCP mutations.
- Migration registry: test against (a) a fresh empty DB and (b) a copy of an
  existing fully-migrated DB (baseline path) — never the production DB itself.