# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

Clinical trial Schedule of Activities (SoA) workbench: FastAPI web app + CLI tools for normalizing, expanding, and validating study visit matrices against USDM (Unified Study Definitions Model).

## Code Style (PEP 8)

### Indentation
- Use four spaces per indentation level
- Never mix tabs and spaces

### Line Length
- Limit lines to 79 characters for code
- Limit docstrings/comments to 72 characters
- Use parentheses for implicit line continuation
- This is a convention: `.flake8` sets `max-line-length = 127` and
  ignores E501, so tooling does not enforce 79

### Imports
- Place imports at the top of the file
- Group imports in order: standard library, third-party, local
- Use absolute imports over relative imports
- One import per line (except `from x import a, b`)

```python
# Good
import os
import sys

from typing import List, Optional

from mypackage import mymodule
```

### Naming conventions
- **Functions/variables**: `lowercase_with_underscores`
- **Classes**: `CapitalizedWords` (PascalCase)
- **Constants**: `UPPERCASE_WITH_UNDERSCORES`
- **Private**: prefix with single underscore `_private_method`
- **Avoid**: single character names except for counters/iterators


## Commands

### Development Setup
```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -e .
pre-commit install
```

All Python commands must run inside the venv, so prefix them with
`source .venv/bin/activate &&`.

### Running the Web Server
```bash
soa-builder-web                                           # starts uvicorn on 0.0.0.0:8000 with reload
uvicorn soa_builder.web.app:app --reload --port 8000     # manual alternative
soa-mcp                                                   # MCP stdio server (backs mcp__soa-workbench__* tools; registered in .mcp.json)
```

### Testing
```bash
pytest                                    # run all tests
pytest tests/test_routers_*.py           # router tests only
pytest tests/test_specific.py -v         # run single test file
pytest tests/test_file.py::test_name -v  # run single test
```

`pytest.ini` skips `old-tests/` and `cdisc-biomedical-concept-groupings/`.

The suite runs fully offline. `tests/conftest.py` blocks real HTTP
(at `requests`' transport layer and on non-loopback sockets) and
strips CDISC credential env vars, so local runs match CI. An
unmocked call raises `NetworkDisabledError` (a
`requests.ConnectionError`). New tests must mock remote calls
(`monkeypatch` `requests.get`, or set `CDISC_CONCEPTS_JSON`).

Tests use isolated database `soa_builder_web_tests.db`. `db.py` selects
it only when pytest is in `sys.modules`, so run tests exclusively via
`pytest`. Ad-hoc `python -c` smoke tests are NOT protected: set
`SOA_BUILDER_DB=soa_builder_web_tests.db` explicitly, or you will write
into the production DB. Manual cleanup if needed:
```bash
rm -f soa_builder_web_tests.db*
```

### CLI Tools (Legacy)
```bash
soa-builder normalize --input files/SoA.csv --out-dir normalized/
soa-builder expand --normalized-dir normalized/ --start-date 2025-01-01
soa-builder validate --normalized-dir normalized/
```

### Pre-commit Hooks
```bash
pre-commit run --all-files  # runs black + pytest + flake8
```

## Architecture

### Directory Structure
- `src/soa_builder/` - Core logic (normalization, schedule expansion, validation)
- `src/soa_builder/web/` - FastAPI app with SQLite persistence
- `src/soa_builder/web/routers/` - Domain-specific endpoints (visits, activities, epochs, arms, elements, etc.)
- `src/usdm/` - USDM JSON generators (transform database → USDM artifacts)
- `tests/` - Pytest tests with FastAPI TestClient

### Web Layer Patterns
- **`app.py` is large (~7,000 lines)** and still holds many routes and
  UI handlers (e.g. `/ui/soa/create`, `update_meta`) besides wiring and
  lifespan migrations. Put new domain endpoints in a router under
  `routers/`, not in `app.py`.
- **Router pattern**: `@router.post("/soa/{soa_id}/visits")` for API, `@router.post("/ui/soa/{soa_id}/visits/create")` for UI forms
- **Database**: Always use `from .db import _connect` (handles pytest detection, WAL mode, busy timeouts)
- **Schema migrations**: Lifespan event in `app.py` runs migrations—add new ones to `migrate_database.py`
- **HTMX UI**: Templates use `base.html` inheritance; form submissions return HTML partials for swaps

### Freeze and Rollback
`routers/freezes.py`, `rollback.py` and `_freeze_helpers.py` snapshot
and restore entity tables. A new entity table that hangs off `soa_id`
must be added to both snapshot and rollback, or rollback will silently
drop it (see recent fixes #371, #378). Rollback tests live in
`tests/test_freeze_rollback_*.py`.

### USDM Model Entities (Key Domain Knowledge)
- **StudyElement**: Structural design components (periods, cohorts). UID pattern: `StudyElement_N`
- **ScheduledActivityInstance**: Temporal visit/timepoint occurrences. UID pattern: `ScheduledActivityInstance_N`
- **StudyCell**: Junction entity (armId + epochId + elementIds[]). UID pattern: `StudyCell_N`
- **ScheduleTimeline**: Container for instances, timings, exits. Has `mainTimeline` flag
- **Encounter**: Physical/virtual visits (via `visit` table). UID pattern: `Encounter_N`

**Key distinction**: Elements = structural design (periods, cohorts) | Instances = temporal schedule (visits, timepoints)

### Which USDM Expert to Use
- **usdm-sme** (skill) - standard/spec questions: USDM class structure,
  attributes, relationships, controlled terminology, conformance rules
  in general.
- **usdm-implementation-expert** (agent) - USDM-IG v4 compliance and
  validation of a specific structure/JSON against the implementation
  guide.
- **usdm-soa-protocol-expert** (agent) - SoA→USDM temporal mapping
  specifically (StudyCell, ScheduleTimeline, ScheduledActivityInstance,
  Encounter, timings), with live workbench data access via the
  `mcp__soa-workbench__*` tools.

### Project Skills and Agents (`.claude/`)
- Skills: `ui-design` (templates/CSS), `run-soa-workbench` (start and
  smoke-test the server), `dair-report-writer`
  (`src/usdm/generate_dair.py`), `usdm-modeler` (USDM entity generators)
- Agent: `db-ops-reviewer` (connection handling, migrations, audit,
  UIDs, N+1 queries)

### UID Generation
- Auto-generated UIDs follow pattern: `{EntityName}_{incrementing_id}`
- UIDs are monotonic (max+1, never fills gaps after deletes)
- Once assigned, UIDs are immutable

### Audit Trail
All entity mutations log before/after state:
```python
from .audit import _record_element_audit
_record_element_audit(soa_id, "update", element_id, before=old_state, after=new_state)
```

### External API Integration
**CDISC Library API** requires `CDISC_SUBSCRIPTION_KEY` or `CDISC_API_KEY` env vars.
For tests, set `CDISC_CONCEPTS_JSON` to file path or inline JSON to bypass remote API.

**CDISC Library CRF Specializations API** uses a separate key from
the main CDISC Library API. Set `CDISC_CRF_API_KEY` /
`CDISC_CRF_SUBSCRIPTION_KEY`; these fall back to `CDISC_API_KEY` /
`CDISC_SUBSCRIPTION_KEY` when unset (see `get_cdisc_crf_api_key` and
`get_cdisc_crf_subscription_key` in `web/utils.py`).

## Environment Variables
- `SOA_BUILDER_DB` - Database path (defaults to `soa_builder_web.db`, tests use `soa_builder_web_tests.db`)
- `CDISC_API_KEY` - CDISC Library API authentication
- `CDISC_CRF_API_KEY` / `CDISC_CRF_SUBSCRIPTION_KEY` - CDISC Library
  CRF Specializations API authentication (falls back to
  `CDISC_API_KEY` / `CDISC_SUBSCRIPTION_KEY` when unset)
- `CODELIST_*` - CDISC CT codelist codes backing UI dropdowns (Epoch
  Type, Arm Type, Role Type, Timing Type, etc.). Read once at import
  time by `src/soa_builder/web/codelist_config.py`; see that module
  for the full list of variables and their defaults. Edit
  `config.env` and restart the server to point a dropdown at an
  alternate codelist.

## Reference Documentation
- `README_endpoints.md` - Complete API reference with curl examples (165+ endpoints)
- `docs/api_endpoints.csv` - Sortable endpoint catalog
- `README.md` for description of project


## Best Practices

### General
- Always use test driven development principles
- Follow "Explicit is better than implicit" (Zen of Python)
- Use list comprehensions for simple transformations
- Prefer `with` statements for resource management
- Use `pathlib` for file path operations

### Error Handling
- Be specific with exception types
- Don't use bare `except:` clauses
- Use `finally` or context managers for cleanup
- `import logging` and create a `logger` to collect and output messages