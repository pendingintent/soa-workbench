# Plan: Organizations Feature

## Context

USDM v4.0 `StudyVersion` supports an `organizations` array of `Organization-Output`
objects. Each organization has a name, label, identifier, identifierScheme, a
controlled type code (DDF CT C215480), and an optional `legalAddress` (with text,
lines[], city, district, state, postalCode, country). Currently `generate_usdm.py`
emits no organizations. This plan adds a fully database-backed 0..N Organization
entity that users can manage in the edit page below the study-meta-card.

---

## USDM Schema (from `schema/USDM_API_v4.0.0.json`)

**Organization-Output** required: `id`, `name`, `type` (Code-Output),
`identifierScheme`, `identifier`, `instanceType: "Organization"`.
Optional: `label`, `legalAddress` (Address-Output), `managedSites: []`,
`extensionAttributes: []`.

**Address-Output** required: `id`, `instanceType: "Address"`.
Optional: `text`, `lines` (array of strings), `city`, `district`, `state`,
`postalCode`, `country` (Code-Output).

---

## Data Model

### New DB tables (add as migrations in `migrate_database.py`)

```sql
CREATE TABLE IF NOT EXISTS organization (
    id                   INTEGER PRIMARY KEY AUTOINCREMENT,
    soa_id               INTEGER NOT NULL,
    organization_uid     TEXT NOT NULL,
    name                 TEXT NOT NULL,
    label                TEXT,
    identifier           TEXT,
    identifier_scheme    TEXT,
    type_code_uid        TEXT,   -- FK → code.code_uid (C215480 conceptId)
    addr_text            TEXT,
    addr_lines           TEXT,   -- JSON array of strings
    addr_city            TEXT,
    addr_district        TEXT,
    addr_state           TEXT,
    addr_postal_code     TEXT,
    addr_country_code_uid TEXT,  -- FK → code.code_uid (ISO 3166 numeric)
    order_index          INTEGER,
    UNIQUE(soa_id, organization_uid)
);

CREATE TABLE IF NOT EXISTS organization_audit (
    id           INTEGER PRIMARY KEY AUTOINCREMENT,
    soa_id       INTEGER NOT NULL,
    org_id       INTEGER,
    action       TEXT NOT NULL,
    before_json  TEXT,
    after_json   TEXT,
    performed_at TEXT NOT NULL
);
```

Address is stored inline in `organization` (0..1 per org). `addr_lines` is a
JSON-encoded `list[str]`; split the textarea input by `\n` on the server.

### Code table rows created per organization

| UID | code | decode | codeSystem | codeSystemVersion |
|-----|------|--------|------------|-------------------|
| Code_N | C215480 conceptId | preferredTerm | `http://www.cdisc.org` | DDF CT package date (e.g. `2022-09-30`) |
| Code_M | ISO numeric (e.g. `840`) | country name | `ISO 3166 1 Numeric Code` | `2026` |

Use `get_next_code_uid(cur, soa_id)` (utils.py) for each new Code_N. Insert into
the `code` table (same pattern as study_titles.py).

---

## UID Generation

Follow monotonic max+1 pattern checking both live table and audit JSON:

```python
def _next_org_uid(cur, soa_id: int) -> str:
    # scan organization.organization_uid and audit before/after JSON
    # same pattern as _next_study_title_uid in study_titles.py
    return f"Organization_{max_n + 1}"
```

Address UID: `Address_{organization_uid_suffix}` (e.g., org is `Organization_3`
→ address id is `Address_3`). This keeps address IDs deterministic without
a separate counter.

---

## New Files

### 1. `src/soa_builder/web/routers/organizations.py`

APIRouter (`prefix="/soa/{soa_id}"`) with routes:

| Method | Path | Purpose |
|--------|------|---------|
| `GET` | `/organizations` | List all orgs (JSON) |
| `POST` | `/organizations` | Create org (JSON) |
| `DELETE` | `/organizations/{org_id}` | Delete org (JSON) |
| `POST` | `/ui/soa/{soa_id}/organizations-add` | HTMX add → refreshed partial |
| `POST` | `/ui/soa/{soa_id}/organizations/{id}/delete` | HTMX delete → refreshed partial |

**JSON body for POST `/soa/{soa_id}/organizations`:**
```json
{
  "name": "CDISC",
  "label": null,
  "identifier": "12345",
  "identifier_scheme": "DUNS",
  "type_concept_id": "C...",
  "type_preferred_term": "Sponsor",
  "type_version": "2022-09-30",
  "addr_text": null,
  "addr_lines": [],
  "addr_city": null,
  "addr_district": null,
  "addr_state": null,
  "addr_postal_code": null,
  "addr_country_numeric": null,
  "addr_country_name": null
}
```

**Create handler logic (single transaction):**
1. Guard with `soa_exists`
2. If `type_concept_id`: `get_next_code_uid` → INSERT into `code` → save `type_code_uid`
3. If `addr_country_numeric`: `get_next_code_uid` → INSERT into `code` with
   `code=numeric`, `decode=country_name`, `codeSystem="ISO 3166 1 Numeric Code"`,
   `codeSystemVersion="2026"` → save `addr_country_code_uid`
4. `_next_org_uid` → INSERT into `organization`
5. Record audit

**Delete handler logic:**
1. Fetch org row (capture for audit)
2. Delete `type_code_uid` and `addr_country_code_uid` from `code` table
3. `DELETE FROM organization WHERE id=? AND soa_id=?`
4. Re-index remaining orgs
5. Record audit

**HTMX add handler** (`POST /ui/soa/{soa_id}/organizations-add`) receives form fields:
`name`, `label`, `identifier`, `identifier_scheme`, `type_code` (conceptId),
`type_decode` (preferredTerm), `addr_text`, `addr_lines` (newline-separated),
`addr_city`, `addr_district`, `addr_state`, `addr_postal_code`,
`addr_country_numeric`, `addr_country_name`. Returns refreshed
`organizations_partial.html`.

**DDF CT version extraction** (same as study_titles.py):
```python
slug = get_latest_ddf_ct_href() or ""
parts = slug.split("-")
type_version = "-".join(parts[-3:]) if len(parts) >= 3 else ""
```

### 2. `src/usdm/generate_organizations.py` (new)

```python
from soa_builder.web.app import _connect
import json

def build_usdm_organizations(soa_id: int) -> list[dict]:
    conn = _connect()
    cur = conn.cursor()
    cur.execute(
        "SELECT organization_uid, name, label, identifier, identifier_scheme, "
        "type_code_uid, addr_text, addr_lines, addr_city, addr_district, "
        "addr_state, addr_postal_code, addr_country_code_uid "
        "FROM organization WHERE soa_id=? ORDER BY order_index, id",
        (soa_id,),
    )
    rows = cur.fetchall()
    conn.close()
    return [_build_org(soa_id, r) for r in rows]


def _build_org(soa_id, row) -> dict:
    (org_uid, name, label, identifier, id_scheme,
     type_code_uid, addr_text, addr_lines_json, addr_city,
     addr_district, addr_state, addr_postal_code,
     addr_country_code_uid) = row
    return {
        "id": org_uid,
        "extensionAttributes": [],
        "name": name or "",
        "label": label or None,
        "identifier": identifier or "",
        "identifierScheme": id_scheme or "",
        "type": _read_code(soa_id, type_code_uid),
        "legalAddress": _build_address(
            soa_id, org_uid, addr_text, addr_lines_json,
            addr_city, addr_district, addr_state,
            addr_postal_code, addr_country_code_uid
        ),
        "managedSites": [],
        "instanceType": "Organization",
    }


def _build_address(soa_id, org_uid, text, lines_json, city,
                   district, state, postal_code,
                   country_code_uid) -> dict | None:
    """Return None if no address fields are populated."""
    lines = json.loads(lines_json) if lines_json else []
    has_data = any([text, lines, city, district, state,
                    postal_code, country_code_uid])
    if not has_data:
        return None
    suffix = org_uid.split("_")[-1]
    return {
        "id": f"Address_{suffix}",
        "extensionAttributes": [],
        "text": text or None,
        "lines": lines,
        "city": city or None,
        "district": district or None,
        "state": state or None,
        "postalCode": postal_code or None,
        "country": _read_code(soa_id, country_code_uid),
        "instanceType": "Address",
    }


def _read_code(soa_id, code_uid) -> dict | None:
    """Read a Code-Output dict from the `code` table."""
    if not code_uid:
        return None
    conn = _connect()
    cur = conn.cursor()
    cur.execute(
        "SELECT code, code_system, code_system_version, decode "
        "FROM code WHERE soa_id=? AND code_uid=? LIMIT 1",
        (soa_id, code_uid),
    )
    row = cur.fetchone()
    conn.close()
    if not row:
        return None
    code, cs, csv, decode = row
    return {
        "id": code_uid,
        "extensionAttributes": [],
        "code": code or "",
        "codeSystem": cs or "",
        "codeSystemVersion": csv or "",
        "decode": decode or "",
        "instanceType": "Code",
    }
```

### 3. `src/soa_builder/web/templates/organizations_partial.html` (new)

No `{% extends %}`. Wrapped in `<div id="organizations-section">` (HTMX target).

**Structure:**
- `fz-table` listing existing orgs: UID chip, Name, Type decode, Identifier, delete
  form per row (`hx-post="/ui/soa/{{ soa_id }}/organizations/{{ o.id }}/delete"
  hx-target="#organizations-section" hx-swap="outerHTML"`)
- Empty row: `fz-empty-row` when no orgs
- `<hr>` divider
- `<div class="study-meta-card">` add form with `am-grid-2`:
  - Row 1: name (required) + label
  - Row 2: identifierScheme + identifier
  - Row 3: type select (am-field-span, full-width) — options from `org_type_options`,
    plus hidden `type_decode` input, JS populates via `data-decode` on `<option>`
  - `<hr>` sub-divider "Legal Address"
  - Row 4: addr_text (am-field-span, full-width textarea)
  - Row 5: addr_lines (am-field-span, textarea, note "One address line per row")
  - Row 6: addr_city + addr_district
  - Row 7: addr_state + addr_postal_code
  - Row 8: addr_country select (am-field-span) from `countries_options`
    plus hidden `addr_country_name` input; JS populates from `data-name` on option
  - Submit: `am-btn am-btn-info am-btn-sm` "Add Organization"
    (`hx-post="/ui/soa/{{ soa_id }}/organizations-add"
     hx-target="#organizations-section" hx-swap="outerHTML"`)

### 4. New test file: `tests/test_routers_organizations.py`

8 tests following pattern from `tests/test_routers_study_titles.py`:

- `test_create_organization_returns_uid` — POST, uid starts with `Organization_`
- `test_create_organization_uid_monotonic` — create 2, delete 1, 3rd is `Organization_3`
- `test_list_organizations` — GET returns correct list
- `test_delete_organization` — delete, then list is empty
- `test_unknown_soa_returns_404` — POST to nonexistent soa_id
- `test_usdm_organizations_in_output` — created org appears in `study_version.organizations`
- `test_organization_with_address` — create org with address fields; USDM output has
  correct `legalAddress` with `city`, `country.code` (ISO numeric), etc.
- `test_usdm_empty_organizations` — no orgs → `study_version.organizations == []`

---

## Modified Files

### `src/soa_builder/web/migrate_database.py`

Add at the end:
- `_migrate_add_organization_table()` — CREATE TABLE organization (schema above)
- `_migrate_add_organization_audit_table()` — CREATE TABLE organization_audit
Register both in the `migrate_database(conn)` dispatcher.

### `src/soa_builder/web/audit.py`

Add `_record_organization_audit(soa_id, action, org_id, before, after)` following
exact same pattern as `_record_study_title_audit`.

### `src/soa_builder/web/app.py`

1. `from .routers import organizations as organizations_router` + `app.include_router`
2. Add helpers (called from ui_edit and partial renders):
   - `_get_org_type_options()` — filters `get_ddf_ct_rows()` for `codelist_code == "C215480"`,
     returns `[{code: conceptId, label: preferredTerm}]`
   - `_get_countries_options()` — queries `country_codes` table:
     `SELECT country_name, country_numeric_code FROM country_codes ORDER BY country_name`
     returns `[{name, code}]`
3. In `ui_edit` handler: load `org_type_options = _get_org_type_options()`,
   `countries_options = _get_countries_options()`, `organizations = _list_organizations(soa_id)`,
   pass all three to template context. `_list_organizations` queries org + LEFT JOIN code for
   type decode.
4. Import `_list_organizations` from `organizations_router` (same pattern as `_list_titles`).

### `src/soa_builder/web/templates/edit.html`

Add a second `study-meta-card` div **below** the existing study-meta-card (after its
closing `</div>`):

```html
<div class="study-meta-card" style="margin-top:16px;">
  <h3>Organizations</h3>
  <div id="organizations-section">
    {% include "organizations_partial.html" %}
  </div>
</div>
```

### `src/usdm/generate_usdm.py`

1. Add import: `from usdm.generate_organizations import build_usdm_organizations`
2. In `study_version` dict, add:
   ```python
   "organizations": _safe("organizations", build_usdm_organizations, soa_id),
   ```
   Place after `"titles"` line.

---

## Reused Patterns & Functions

| Function | File | Purpose |
|----------|------|---------|
| `get_next_code_uid(cur, soa_id)` | `utils.py` | Unique Code_N UIDs |
| `get_ddf_ct_rows()` | `utils.py` | Fetch DDF CT (cached) |
| `get_latest_ddf_ct_href()` | `utils.py` | DDF CT package date |
| `_next_study_title_uid` | `study_titles.py` | Template for `_next_org_uid` |
| `_record_study_title_audit` | `audit.py` | Template for `_record_organization_audit` |
| `_get_countries()` in `amendments.py` | `routers/amendments.py` | Country list from `country_codes` table |
| `soa_exists` | `utils.py` | Guard all routes |
| `study_titles_partial.html` | `templates/` | Template for `organizations_partial.html` |

---

## Verification

1. `pytest tests/test_routers_organizations.py -v` — all 8 new tests pass.
2. `pytest` — 425+ tests, no regressions.
3. Start `soa-builder-web`, open `/ui/soa/{soa_id}/edit`:
   - Below the study-meta-card an "Organizations" card is visible.
   - Type dropdown shows terms from C215480 (e.g., "Sponsor", "Contract Research
     Organization") with preferredTerm as display.
   - Country dropdown shows country names (ISO 3166); selecting one populates the
     hidden numeric code.
   - Fill name + identifier + type → click "Add Organization" → org row appears.
   - Click × → org removed.
4. `GET /soa/{soa_id}/usdm_json/full` → `study.versions[0].organizations` contains
   the saved org with correct `type.code` (conceptId), `type.decode` (preferredTerm),
   `legalAddress.country.code` (ISO numeric), `legalAddress.country.codeSystem`
   `"ISO 3166 1 Numeric Code"`.
5. With no orgs: `study.versions[0].organizations == []` (valid per USDM schema —
   no fallback required).
