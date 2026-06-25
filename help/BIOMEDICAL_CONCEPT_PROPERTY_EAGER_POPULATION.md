# Plan: Eager BiomedicalConceptProperty Population

## Context

When an activity is linked to a CDISC BiomedicalConcept today, only the
top-level concept row is enriched in the background (`shortName`,
`definition`, CDISC code chain). The child `BiomedicalConceptProperty`
rows — sourced from `dataElementConcepts[]` in the CDISC Library API
response — are **only populated at USDM export time** inside
`build_usdm_biomedical_concepts()`.

This means the `biomedical_concept_property` table is empty until a
USDM export is triggered, and any partial inspection of the DB or
incremental USDM build will have no properties. The goal is to add an
**eager path** that populates properties as a background task the moment
a concept is assigned, with a flag to keep the current lazy path as the
default.

Additionally, the current implementation has three gaps against the
authoritative CDISC `cdisc_bc_library.py` reference mapping (from the
installed `usdm_excel` package):

1. `responseCodes` is always `[]` — `exampleSet[]` (generic BC) and
   `valueList[]` (SDTM variable) must map to `ResponseCode` entities.
2. SDTM specialization (`/mdr/specializations/sdtm/`) is not fetched —
   SDTM `variables` are the preferred BCP source when available.
3. No DB storage for `ResponseCode` rows — they are first-class USDM
   entities requiring their own UIDs.

---

## Official Mapping Rules (from `cdisc_bc_library.py`)

### Generic BC path (fallback — BC has no SDTM specialization)

Source: `GET /mdr/bc/biomedicalconcepts/{conceptCode}`

Each `dataElementConcepts[]` entry → one `BiomedicalConceptProperty`:

| API field | USDM / DB field |
|-----------|-----------------|
| `conceptId` | `code.standardCode.code` |
| `shortName` | `name`, `label`, `code.standardCode.decode` |
| `dataType` | `datatype` |
| `exampleSet[]` → CT lookup → `conceptId`, `preferredTerm` | `responseCodes[].code` |
| *(hardcoded)* | `isRequired: true`, `isEnabled: true` |

`exampleSet` values (e.g. `"Y"`, `"N"`, `"Brain"`) are **not** used
directly as codes — each is resolved via the CDISC CT Library API to get
a proper `conceptId` and `preferredTerm`.

### SDTM specialization path (preferred — when available)

Source 1: `GET /mdr/specializations/sdtm/packages/{pkg}/datasetspecializations/{id}`
Source 2: follow `_links.parentBiomedicalConcept.href` → generic BC

Each SDTM `variables[]` entry → one `BiomedicalConceptProperty`:

| API field | USDM / DB field |
|-----------|-----------------|
| `dataElementConceptId` → join to generic `dataElementConcepts[].conceptId` | `code.standardCode.code` |
| `name` (SDTM variable name) | `name`, `label` |
| `dataType` | `datatype` |
| `valueList[]` → CT lookup (with optional `codelist.conceptId`) | `responseCodes[].code` |
| `assignedTerm.conceptId` + `.value` | fallback concept code when no DEC match |
| *(hardcoded)* | `isRequired: true`, `isEnabled: true` |

### `ResponseCode` entity structure

```json
{
  "id": "ResponseCode_N",
  "instanceType": "ResponseCode",
  "name": "RC_C49488",
  "label": "",
  "isEnabled": true,
  "code": {
    "id": "Code_N",
    "instanceType": "Code",
    "code": "C49488",
    "codeSystem": "http://www.cdisc.org",
    "codeSystemVersion": "...",
    "decode": "Yes"
  }
}
```

---

## Current Data Flow (Lazy — Default)

```
POST /soa/{id}/activities/{id}/concepts
  → _upsert_biomedical_concept()          # creates BC row (name empty)
  → bg: _enrich_biomedical_concept_bg()   # fills name/label/description
  → bg: _enrich_code_bg()                 # fills code_system/version/decode
  # biomedical_concept_property table: EMPTY
  # bcp_response_code table: EMPTY

GET /usdm/...  (export)
  → build_usdm_biomedical_concepts()
      → populate_biomedical_concept_properties(soa_id)   # fills BCP rows (no responseCodes)
      → build_usdm_biomedical_concept_properties()       # returns responseCodes: []
```

## Proposed New Data Flow (Eager, opt-in)

```
POST /soa/{id}/activities/{id}/concepts
  → _upsert_biomedical_concept()
  → bg: _enrich_biomedical_concept_bg()
  → bg: _enrich_code_bg()
  → bg: _populate_biomedical_concept_properties_bg()  # NEW — if flag set
       fetches SDTM specialization (if available) + generic BC
       writes BCP rows + ResponseCode rows + Code rows

GET /usdm/...  (export)
  → build_usdm_biomedical_concepts()
      → _ensure_bcp_populated(soa_id)   # no-op if rows already present
      → build_usdm_biomedical_concept_properties()  # returns populated responseCodes
```

---

## Feature Flag

Environment variable: `SOA_EAGER_BCP_POPULATION`

| Value | Behaviour |
|-------|-----------|
| unset / `0` / `false` | **Current (lazy)** — properties populated only at USDM export |
| `1` / `true` | **New (eager)** — properties + response codes populated as background tasks on concept assignment |

The export-time populate call stays in place in **both modes** as a
safety-net (idempotent).

---

## Implementation Steps

### Step 1 — Add `bcp_response_code` table (migration)

**File:** `src/soa_builder/web/migrate_database.py`

Add a new migration function and register it in the lifespan migrations:

```python
def _migrate_add_bcp_response_code_table():
    conn = _connect()
    cur = conn.cursor()
    cur.execute("""
        CREATE TABLE IF NOT EXISTS bcp_response_code (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            soa_id INTEGER NOT NULL,
            biomedical_concept_property_uid TEXT NOT NULL,
            response_code_uid TEXT NOT NULL,
            name TEXT,
            label TEXT,
            is_enabled INTEGER NOT NULL DEFAULT 1,
            code TEXT,    -- FK to alias_code.alias_code_uid
            UNIQUE(response_code_uid, soa_id)
        )
    """)
    conn.commit()
    conn.close()
```

Add `get_next_response_code_uid(cur, soa_id)` to
`src/soa_builder/web/utils.py` following the existing monotonic UID
pattern (`ResponseCode_N`).

### Step 2 — Fetch SDTM specialization alongside generic BC

**File:** `src/usdm/usdm_utils.py`

Add a cached function alongside `_get_biomedical_concept_data`:

```python
@functools.lru_cache(maxsize=256)
def _get_sdtm_specialization_data(concept_code: str) -> Dict[str, Any]:
    """Fetch the SDTM dataset specialization for a BC, or {} if none."""
    # GET /mdr/specializations/sdtm/datasetspecializations?biomedicalconcept={code}
    url = URL_PREFIX + "mdr/specializations/sdtm/datasetspecializations"
    ...
```

This is used in Step 3 to prefer SDTM variables over generic
`dataElementConcepts`.

### Step 3 — Add scoped BCP + ResponseCode population function

**File:** `src/usdm/generate_biomedical_concept_properties.py`

Add `populate_biomedical_concept_properties_for_bc(soa_id, bc_uid, concept_code)`:

```python
def populate_biomedical_concept_properties_for_bc(
    soa_id: int, bc_uid: str, concept_code: str
) -> None:
    """Populate BCP + ResponseCode rows for one BC. Idempotent."""
```

Logic (matching `cdisc_bc_library.py` precedence):

1. Fetch generic BC via `_get_biomedical_concept_data(concept_code)`.
2. Attempt to fetch SDTM specialization via
   `_get_sdtm_specialization_data(concept_code)`.
3. If SDTM specialization found → iterate `variables[]`:
   - Resolve concept code by joining `dataElementConceptId` →
     generic BC `dataElementConcepts[].conceptId`.
   - Map `valueList[]` → `ResponseCode` rows (CT lookup via
     `get_protocol_ct_term` or direct C-code if no CT match).
4. Else → iterate `dataElementConcepts[]`:
   - Use `conceptId` directly as concept code.
   - Map `exampleSet[]` → `ResponseCode` rows (CT lookup).
5. For each property: insert `biomedical_concept_property` row (skip if
   already present by `bc_uid` + ncit code — existing idempotency check).
6. For each response code: insert `bcp_response_code` +
   `alias_code` + `code` rows (skip if already present).

Also update the existing `populate_biomedical_concept_properties(soa_id)`
to delegate to this function per BC, so both paths share the same logic.

### Step 4 — Update USDM JSON generator for BCP

**File:** `src/usdm/generate_biomedical_concept_properties.py`

Add `build_usdm_biomedical_concept_properties_for_soa(soa_id)` — a
flag-aware top-level generator:

```python
def build_usdm_biomedical_concept_properties_for_soa(
    soa_id: int,
) -> List[Dict[str, Any]]:
    """Return USDM BCP dicts for all BCs in the SOA.

    When SOA_EAGER_BCP_POPULATION is set rows are already present;
    otherwise triggers lazy populate first.
    """
    import os
    eager = os.environ.get(
        "SOA_EAGER_BCP_POPULATION", ""
    ).strip().lower() in ("1", "true")
    if not eager:
        populate_biomedical_concept_properties(soa_id)
    conn = _connect()
    cur = conn.cursor()
    cur.execute(
        "SELECT DISTINCT biomedical_concept_uid FROM biomedical_concept"
        " WHERE soa_id=? ORDER BY id",
        (soa_id,),
    )
    bc_uids = [r[0] for r in cur.fetchall()]
    conn.close()
    out: List[Dict[str, Any]] = []
    for bc_uid in bc_uids:
        out.extend(build_usdm_biomedical_concept_properties(soa_id, bc_uid))
    return out
```

Update `build_usdm_biomedical_concept_properties(soa_id, bc_uid)` to
join against `bcp_response_code` and return populated `responseCodes[]`:

```python
# For each BCP row, also query:
cur.execute(
    "SELECT rc.response_code_uid, rc.name, rc.label, rc.is_enabled,"
    " rc.code AS alias_uid, c.code_uid, c.code, c.decode,"
    " c.code_system, c.code_system_version"
    " FROM bcp_response_code rc"
    " LEFT JOIN alias_code ac ON rc.code = ac.alias_code_uid ..."
    " WHERE rc.soa_id=? AND rc.biomedical_concept_property_uid=?"
    " ORDER BY rc.id",
    (soa_id, bcp_uid),
)
# Build ResponseCode dicts instead of hardcoding []
```

### Step 5 — Update `generate_biomedical_concepts.py` to use new generator

**File:** `src/usdm/generate_biomedical_concepts.py`

Replace the direct `populate_biomedical_concept_properties(soa_id)` call
at line 35:

```python
# Before
populate_biomedical_concept_properties(soa_id)

# After — flag-aware, no API calls if rows already present
from .generate_biomedical_concept_properties import (
    build_usdm_biomedical_concept_properties_for_soa as _ensure_bcp_populated,
)
_ensure_bcp_populated(soa_id)
```

### Step 6 — Add background task wrapper

**File:** `src/soa_builder/web/app.py`

Add alongside `_enrich_biomedical_concept_bg` (around line 2165):

```python
def _populate_biomedical_concept_properties_bg(
    concept_code: str, bc_uid: str, soa_id: int
) -> None:
    """Background task: populate BCP + ResponseCode rows for one BC.

    Gated by SOA_EAGER_BCP_POPULATION env var.
    """
    import os
    if os.environ.get(
        "SOA_EAGER_BCP_POPULATION", ""
    ).strip().lower() not in ("1", "true"):
        return
    from usdm.generate_biomedical_concept_properties import (
        populate_biomedical_concept_properties_for_bc,
    )
    try:
        populate_biomedical_concept_properties_for_bc(
            soa_id, bc_uid, concept_code
        )
    except Exception:
        logger.exception(
            "_populate_biomedical_concept_properties_bg failed "
            "concept_code=%s bc_uid=%s soa_id=%s",
            concept_code, bc_uid, soa_id,
        )
```

### Step 7 — Schedule the task in activities and concept_groups routers

**File:** `src/soa_builder/web/routers/activities.py`

Extend the import in `set_activity_concepts()` (line ~508):

```python
from ..app import (
    ...,
    _populate_biomedical_concept_properties_bg,  # NEW
)
```

After existing enrichment tasks (line ~567):

```python
background_tasks.add_task(
    _populate_biomedical_concept_properties_bg,
    ccode, concept_uid, soa_id,
)
```

**File:** `src/soa_builder/web/routers/concept_groups.py`

Add the same background task after each of the two existing
`_enrich_biomedical_concept_bg` scheduling calls (lines ~812, ~817).

### Step 8 — Add tests

**File:** `tests/test_generate_biomedical_concept_properties.py`

- `populate_biomedical_concept_properties_for_bc`: mock API via
  `CDISC_CONCEPTS_JSON`, assert BCP rows created with correct
  `name`, `datatype`, `code`.
- Same call twice → no duplicate rows (idempotency).
- `exampleSet` present → `bcp_response_code` rows created.
- `exampleSet` absent → `bcp_response_code` is empty (no error).
- `build_usdm_biomedical_concept_properties_for_soa`: with pre-populated
  rows, returns correct USDM dicts with non-empty `responseCodes`.

**File:** `tests/test_routers_activities.py`

- With `SOA_EAGER_BCP_POPULATION=1`: after
  `POST /soa/{id}/activities/{id}/concepts`, assert `biomedical_concept_property`
  and `bcp_response_code` rows exist in the test DB.
- Without flag: same call leaves both tables empty until export.

---

## Critical Files

| File | Change |
|------|--------|
| `src/soa_builder/web/migrate_database.py` | Add `_migrate_add_bcp_response_code_table()` |
| `src/soa_builder/web/utils.py` | Add `get_next_response_code_uid()` |
| `src/usdm/usdm_utils.py` | Add `_get_sdtm_specialization_data()` |
| `src/usdm/generate_biomedical_concept_properties.py` | Add `populate_biomedical_concept_properties_for_bc()`, `build_usdm_biomedical_concept_properties_for_soa()`; update `build_usdm_biomedical_concept_properties()` to include `responseCodes` |
| `src/usdm/generate_biomedical_concepts.py` | Replace direct populate call with flag-aware delegate |
| `src/soa_builder/web/app.py` | Add `_populate_biomedical_concept_properties_bg()` |
| `src/soa_builder/web/routers/activities.py` | Import + schedule new bg task |
| `src/soa_builder/web/routers/concept_groups.py` | Import + schedule new bg task |
| `tests/test_generate_biomedical_concept_properties.py` | Tests for scoped function, ResponseCode mapping, USDM generator |
| `tests/test_routers_activities.py` | Integration test for eager flag |

---

## Reused Existing Functions

| Function | Location | Role |
|----------|----------|------|
| `populate_biomedical_concept_properties(soa_id)` | `generate_biomedical_concept_properties.py:31` | Delegates to new scoped function; stays as safety net |
| `_get_biomedical_concept_data(concept_code)` | `usdm/usdm_utils.py:168` | Generic BC fetch (lru_cache) |
| `get_next_code_uid` | `soa_builder/web/utils.py` | UID allocation |
| `get_next_alias_code_uid` | `soa_builder/web/utils.py` | UID allocation |
| `get_next_biomedical_concept_property_uid` | `soa_builder/web/utils.py` | UID allocation |
| `get_protocol_ct_term` | `soa_builder/web/utils.py` | CT lookup for `exampleSet`/`valueList` → ResponseCode concept IDs |

---

## Verification

```bash
# Lazy mode (default) — BCP + ResponseCode tables empty until export
# (no SOA_EAGER_BCP_POPULATION set)
curl -X POST /soa/1/activities/1/concepts -d '["C25347"]'
sqlite3 soa_builder_web.db \
  "SELECT COUNT(*) FROM biomedical_concept_property;
   SELECT COUNT(*) FROM bcp_response_code;"
# → 0 / 0

# Eager mode — populated right after assignment
export SOA_EAGER_BCP_POPULATION=1
curl -X POST /soa/1/activities/1/concepts -d '["C25347"]'
sleep 2   # allow background task
sqlite3 soa_builder_web.db \
  "SELECT COUNT(*) FROM biomedical_concept_property;
   SELECT COUNT(*) FROM bcp_response_code;"
# → > 0 / > 0 (for BCs with exampleSet/valueList)

# USDM export: responseCodes populated for relevant properties
curl /soa/1/usdm | jq \
  '.biomedicalConcepts[].properties[] | select(.responseCodes | length > 0) | .name'

# Run tests
pytest tests/test_generate_biomedical_concept_properties.py -v
pytest tests/test_routers_activities.py -v
```
