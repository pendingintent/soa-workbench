[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)

# SoA Workbench

This workspace provides a Python package `soa_builder` with APIs to create a Schedule of Activites for Clinical Studies.


## Cloning the repository
This project now includes a submodule for USDM JSON validation with the USDM_API_v4.0.0.json schema.

In order to clone the repository with the new submodule, use the command:

```bash
> git clone --recurse-submodules https://github.com/pendingintent/soa-workbench.git
```

Once the repository has been cloned locally, in order to ensure the submodule is up-to-date, use the commands:
```bash
> cd cdisc-json-validation
> git pull
# or use the command for updating all registered submodules
> git submodule update --remote
```

This will ensure the submodule is always up-to-date.


## Installation
Recommended: editable install for development.
```bash
> python3 -m venv .venv
> source .venv/bin/activate
> pip install -r requirements.txt
> pre-commit install
> pre-commit run --all-files
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
- Create a new Schedule of Activities for a study or access an existing one.
	- When a study is chosen, additional navigation links are available in the navigation menu that are unique to the Study context.
	- More options and parameters for configuring the USDM classes are available through these navigation links.
- Add Scheduled Activity instances (columns) and activities (rows) to create an SoA matrix on the edit page for a Study; click cells to toggle status (blank -> X -> blank). 'O' values are not surfaced in the UI; clearing removes the cell row.
 - Use export buttons (to be added) for XLSX output of the Matrix.
- View avialable biomedical concepts via the "Biomedical Concepts" navigation link to render a table of concept codes, titles and API links (cached; force refresh available).
- View available data set specializations via the "SDTM Dataset Specializations" navigation link to render a table of specializations and API links to view associated concepts (cached; force refresh available). 

CDISC Library API Access:
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
# Use the USDM generator scripts directly
python -m usdm.generate_usdm 1 -o study_usdm.json
python -m usdm.generate_activities 1 -o activities.json
python -m usdm.generate_encounters 1 -o encounters.json
python -m usdm.generate_study_epochs 1 -o epochs.json
# See src/usdm/ for all generator scripts
```

---

## Architecture Notes
- **Database**: SQLite with WAL mode (production) or DELETE mode (tests)
- **Test Isolation**: Tests use `soa_builder_web_tests.db` (set via `SOA_BUILDER_DB` env var)
- **Production Config**: Set `SOA_BUILDER_DB` environment variable for persistent DB path
- **USDM Generators**: Python scripts in `src/usdm/` transform database state → USDM JSON artifacts


