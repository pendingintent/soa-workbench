# Changelog: biomedical-concept-grouping

**Branch:** `biomedical-concept-grouping`  
**Base commit:** `60ce0d1`

---

## New Feature: Biomedical Concept Groups

Introduces a global grouping mechanism for biomedical concepts. Groups are
not SoA-specific — once created they can be associated with activities in
any SoA.

### Database Migrations (`migrate_database.py`)

- `_migrate_add_concept_group_table()` — creates two new tables:
  - `concept_group` — stores `concept_group_uid`, `name`, `label`,
    `description`
  - `concept_group_concept` — stores the many-to-many relationship between
    a group and its assigned concept codes
- `_migrate_activity_concept_add_concept_group_uid()` — adds
  `concept_group_uid` column to `activity_concept`
- `_migrate_surrogate_add_concept_group_uid()` — adds `concept_group_uid`
  column to `biomedical_concept_surrogate`

### New Router (`routers/concept_groups.py`)

Full CRUD for concept groups, including:
- Create, read, update, delete concept groups (API + UI endpoints)
- Assign / unassign individual concept codes to a group
- Link a concept group to an activity (applies all group concepts at once)

### New Template (`templates/concept_groups.html`)

Management page for concept groups — create groups, assign concepts, and
view group membership.

### `app.py`

- Imports and runs the three new migration functions at startup
- Registers `concept_groups_router` (both API and UI routers)
- `_get_activity_concepts()` updated to `LEFT JOIN concept_group` and
  return `concept_group_uid` and `group_name` alongside existing fields
- New `_get_concept_groups_for_cell()` helper used when rendering the
  concepts cell partial

### Updated Router: `routers/activities.py`

- `ui_list_activities` query branches updated to `LEFT JOIN concept_group`
  when the `concept_group_uid` column is present
- `concept_group_uid` and `group_name` included in the per-activity concept
  dict
- `concept_groups` list fetched globally and passed to the template context

### Updated Router: `routers/bc_surrogates.py`

- `_render_concepts_cell` updated to detect the `concept_group_uid` column
  and include `concept_group_uid` / `group_name` in `selected_list`

### Updated Templates

- `templates/activities.html` — computes `activity_group_uids` (unique
  group UIDs currently assigned to the activity) for use inside the
  `concepts_cell.html` include
- `templates/base.html` — adds **Biomedical Concept Groups** navigation
  link; reorders BC-related nav items (Concepts → Categories → Groups →
  SDTM Specializations)
- `templates/concepts_cell.html` — updated to display assigned concept
  groups, render group badges, and expose group-assignment controls via
  HTMX

---

## CI / Tooling

### `pyproject.toml`

- Added `pytest-cov>=4.0.0` to the `[dev]` optional dependencies so
  coverage reporting works in local and CI environments.

### `.github/workflows/ci.yml`

- Added `# yaml-language-server: $schema=…` modeline to pin the GitHub
  Actions schema and suppress false-positive CDISC schema diagnostics
  from the VS Code YAML extension.
