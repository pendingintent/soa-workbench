[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)



# SoA Workbench

This workspace provides a Python package `soa_builder` with APIs to:

1. Normalize a wide Schedule of Activities (SoA) matrix into relational tables.
2. Expand repeating schedule rules into projected calendar instances.


## Installation
Recommended: editable install for development.
```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -e .
pre-commit install
```

## Start web server
```bash
soa-builder-web  # starts uvicorn on 0.0.0.0:8000 with reload
```

Or manually:
```bash
uvicorn soa_builder.web.app:app --reload --port 8000
```
HTML UI:
- Open http://localhost:8000/ in a browser.
- Add visits and activities; click cells to toggle status (blank -> X -> blank). 'O' values are not surfaced in the UI; clearing removes the cell row.
- Use "Generate Normalized Summary" link to produce artifacts.
 - Use export buttons (to be added) or hit endpoints directly for XLSX output.
 - Delete a visit or activity using the ✕ button next to its name (confirmation dialog). Deletion cascades to associated cells and automatically reorders remaining items.
 - View biomedical concepts via the "Concepts" navigation link (`GET /ui/concepts`): renders a table of concept codes, titles and API links (cached; force refresh per study using `POST /ui/soa/{id}/concepts_refresh`).
 
Biomedical Concepts API Access:
- The concepts list and detail pages call the CDISC Library API.
- Set one (or both) of: `CDISC_SUBSCRIPTION_KEY`, `CDISC_API_KEY`.
- The server will send all of these headers when possible:
	- `Ocp-Apim-Subscription-Key: <key>`
	- `Authorization: Bearer <key>` (when `CDISC_API_KEY` provided)
	- `api-key: <key>` (legacy fallback)
- If only one key is defined it is reused across header variants.
- Directly opening the API URL in the browser will 401 because the browser does not attach the required headers; use the internal detail page or an API client (curl/Postman) with the headers above.

## Development & Testing
Run unit tests:
```bash
pytest
```

### Test database
- Tests run against a separate SQLite file to avoid touching your local/prod data.
- Default path: `soa_builder_web_tests.db` in the repo root. Override with env var `SOA_BUILDER_DB`.
- A pytest session fixture removes any stale test DB/WAL/SHM files at start to prevent I/O errors.
- Manually clear the test DB before a run if needed:
```bash
rm -f soa_builder_web_tests.db soa_builder_web_tests.db-wal soa_builder_web_tests.db-shm
```

> **Full API Documentation**: See `README_endpoints.md` for complete endpoint reference with curl examples, request/response schemas, and usage patterns.
>
> **Endpoint Catalog**: See `docs/api_endpoints.csv` for sortable/filterable list of all 165+ endpoints.

## USDM Export
Export USDM-compliant JSON for integration with external systems:
```bash
# Get normalized USDM JSON for a study
curl http://localhost:8000/soa/1/normalized

# Or use the USDM generator scripts directly
python -m usdm.generate_activities --soa-id 1 --output-file activities.json
python -m usdm.generate_encounters --soa-id 1 --output-file encounters.json
python -m usdm.generate_study_epochs --soa-id 1 --output-file epochs.json
# See src/usdm/ for all generator scripts
```

## CLI Tools (Legacy)
Command-line tools for CSV normalization and validation:
```bash
# Normalize wide CSV → relational tables
soa-builder normalize --input files/SoA.csv --out-dir normalized/

# Expand repeating rules → calendar instances
soa-builder expand --normalized-dir normalized/ --start-date 2025-01-01

# Validate imaging intervals
soa-builder validate --normalized-dir normalized/
```
See `.github/copilot-instructions.md` for detailed CLI usage patterns.

---

## Architecture Notes
- **Web UI**: HTMX loaded via CDN; no build step required
- **Database**: SQLite with WAL mode (production) or DELETE mode (tests)
- **Test Isolation**: Tests use `soa_builder_web_tests.db` (set via `SOA_BUILDER_DB` env var)
- **Production Config**: Set `SOA_BUILDER_DB` environment variable for persistent DB path
- **USDM Generators**: Python scripts in `src/usdm/` transform database state → USDM JSON artifacts

For detailed architectural patterns, USDM entity relationships, and development workflows, see `.github/copilot-instructions.md`.

