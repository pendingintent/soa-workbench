# Plan: Per-Concept DSS Multi-Select & Remove Auto-Assignment

## Context

The current DSS assignment design has two problems:
1. `_lookup_and_save_dss` blindly picks `sdtm_links[0]`, even when multiple specializations are
   available for a concept.
2. A single `dss_title`/`dss_href` column pair on `activity_concept` only supports one DSS per
   concept — but a concept may legitimately map to multiple specializations.

This plan removes all auto-assignment, introduces a new `activity_concept_dss` table for
one-to-many DSS assignments, and changes the dropdown to show only specializations available
for that specific concept (filtered via the CDISC API).

---

## 1. New table + migration

**File:** `src/soa_builder/web/migrate_database.py`

Add a new migration function `_migrate_add_activity_concept_dss_table()`:

```python
def _migrate_add_activity_concept_dss_table():
    conn = _connect()
    cur = conn.cursor()
    cur.execute("""
        CREATE TABLE IF NOT EXISTS activity_concept_dss (
            id        INTEGER PRIMARY KEY AUTOINCREMENT,
            soa_id    INTEGER NOT NULL,
            activity_id INTEGER NOT NULL,
            concept_code TEXT NOT NULL,
            dss_title TEXT NOT NULL,
            dss_href  TEXT NOT NULL,
            dss_domain TEXT
        )
    """)
    # Migrate existing single assignments
    cur.execute("PRAGMA table_info(activity_concept)")
    cols = {r[1] for r in cur.fetchall()}
    if "dss_title" in cols and "dss_href" in cols:
        cur.execute("""
            INSERT INTO activity_concept_dss
                (soa_id, activity_id, concept_code, dss_title, dss_href, dss_domain)
            SELECT soa_id, activity_id, concept_code, dss_title, dss_href, dss_domain
            FROM activity_concept
            WHERE dss_title IS NOT NULL AND dss_title != ''
        """)
    conn.commit()
    conn.close()
```

Register it in the lifespan migration sequence in `app.py`.

---

## 2. Remove auto-assignment infrastructure

### `activities.html` — remove button
**File:** `src/soa_builder/web/templates/activities.html` lines 18–23

Remove the entire `<div>` block containing the "Auto-assign DSS" form.

### `activities.py` — remove endpoint
**File:** `src/soa_builder/web/routers/activities.py`

Remove `ui_dss_auto_assign` (lines 793–829).

### `app.py` — remove function and callers
**File:** `src/soa_builder/web/app.py`

- Remove `_lookup_and_save_dss` function (lines 2473–2534).
- Remove `background_tasks.add_task(_lookup_and_save_dss, ...)` at line 3246.
- Remove `background_tasks.add_task(_lookup_and_save_dss, ...)` at line 5733.

### `concept_groups.py` — remove caller
**File:** `src/soa_builder/web/routers/concept_groups.py`

- Remove `background_tasks.add_task(_lookup_and_save_dss, ...)` at line 797.
- Remove the import of `_lookup_and_save_dss` if it becomes unused.

---

## 3. Update DSS save endpoint

**File:** `src/soa_builder/web/routers/activities.py` — `ui_save_dss_assignment` (line 961)

Change from `UPDATE activity_concept SET dss_title=...` to `INSERT INTO activity_concept_dss`.
The `dss_selection` form value remains `"dss_id||dss_href"` format (unchanged from current).

- Parse `dss_id` and `dss_href` from `dss_selection`.
- Skip if this `dss_title`/`concept_code`/`soa_id` combination already exists in
  `activity_concept_dss` (prevent duplicates).
- After insert, trigger `_populate_bc_properties_bg` background task with the new href.
- Keep audit logging (update field names to reflect new table).
- Return `_render_dss_cell(...)` as before.

---

## 4. Add DSS delete endpoint

**File:** `src/soa_builder/web/routers/activities.py`

Add a new endpoint after `ui_save_dss_assignment`:

```python
@ui_router.post(
    "/ui/soa/{soa_id}/activity/{activity_id}/concept/{concept_code}/dss/{dss_row_id}/delete",
    response_class=HTMLResponse,
)
def ui_delete_dss_assignment(
    request: Request,
    soa_id: int,
    activity_id: int,
    concept_code: str,
    dss_row_id: int,
    background_tasks: BackgroundTasks,
):
```

- DELETE the row from `activity_concept_dss` by `id=dss_row_id AND soa_id=soa_id`.
- After delete, check if any assignments remain for this `concept_code`/`soa_id`.
- If none remain: run the existing cascade-delete of BC properties (move the cascade block
  from `ui_save_dss_assignment` into a shared helper `_cascade_delete_bc_properties`).
- Return `_render_dss_cell(...)`.

---

## 5. Update data loading in `activities.py`

### `ui_list_activities` and `_render_dss_cell`

Replace queries against `activity_concept.dss_title/dss_href` with queries against
`activity_concept_dss`.

Each concept entry in `activity_concepts` changes to include `assigned_dss` list:

```python
# Query activity_concept_dss for this SOA
cur.execute(
    "SELECT activity_id, concept_code, id, dss_title, dss_href, dss_domain"
    " FROM activity_concept_dss WHERE soa_id=?",
    (soa_id,)
)
# Group by activity_id -> concept_code -> [assignments]
```

Merge into the `activity_concepts` dict so each concept dict has:
```python
{
    "code": ...,
    "title": ...,
    "concept_group_uid": ...,
    "group_name": ...,
    "assigned_dss": [
        {"id": ..., "dss_title": ..., "dss_href": ..., "dss_domain": ...},
        ...
    ]
}
```

Replace `sdtm_specializations = _app_fetch_dss()` with per-concept map:

```python
unique_codes = {
    ac["code"]
    for concepts in activity_concepts.values()
    for ac in concepts
}
concept_dss_map = {
    code: _app_fetch_dss(code=code)
    for code in unique_codes
}
```

Pass `concept_dss_map` (not `sdtm_specializations`) to the template.

---

## 6. Update `dss_cell.html` template

**File:** `src/soa_builder/web/templates/dss_cell.html`

Restructure to support multiple assignments per concept:

**Count pill** — count total assigned across all concepts:
```jinja2
{% set ns = namespace(total=0) %}
{% for ac in dss_concepts %}
  {% set ns.total = ns.total + (ac.assigned_dss | length) %}
{% endfor %}
{% if ns.total > 0 %}
  <span class="concept-count-pill">{{ ns.total }}</span>
{% endif %}
```

**Per-concept row** — for each assigned DSS, show a pill + delete form + properties link:
```jinja2
{% for assigned in ac.assigned_dss %}
  {# resolve display title from concept_dss_map #}
  {% set ns2 = namespace(display=assigned.dss_title) %}
  {% for d in concept_dss_map.get(ac.code, []) %}
    {% if d.href.rstrip('/').split('/')|last == assigned.dss_title %}
      {% set ns2.display = d.title %}
    {% endif %}
  {% endfor %}
  <span title="{{ assigned.dss_title }}">{{ ns2.display }}</span>
  <form hx-post="/ui/soa/{{ soa_id }}/activity/{{ activity_id }}/concept/{{ ac.code }}/dss/{{ assigned.id }}/delete"
        hx-target="#dss-cell-{{ activity_id }}" hx-swap="outerHTML" style="...">
    <button type="submit" title="Remove DSS">&minus;</button>
  </form>
  <a href="/ui/soa/{{ soa_id }}/dss/detail?href={{ assigned.dss_href|urlencode }}&title={{ assigned.dss_title|urlencode }}">properties</a>
{% endfor %}
```

**Dropdown** — filter out already-assigned DSS:
```jinja2
{% set assigned_ids = ac.assigned_dss | map(attribute='dss_title') | list %}
{% set available = concept_dss_map.get(ac.code, []) %}
{% if available %}
  <form hx-post="/ui/soa/{{ soa_id }}/activity/{{ activity_id }}/concept/{{ ac.code }}/dss"
        hx-target="#dss-cell-{{ activity_id }}" hx-swap="outerHTML" ...>
    <select name="dss_selection">
      <option value="" disabled selected>Select DSS...</option>
      {% for d in available %}
        {% set dss_id = d.href.rstrip('/').split('/')|last %}
        {% if dss_id not in assigned_ids %}
          <option value="{{ dss_id }}||{{ d.href }}">{{ d.title }}</option>
        {% endif %}
      {% endfor %}
    </select>
    <button type="submit">+</button>
  </form>
{% endif %}
```

---

## 7. Update USDM generators and BC property helpers

### `app.py` — `_populate_bc_properties_bg`
**Lines 2544–2561**

Change the lookup from `activity_concept.dss_href` to `activity_concept_dss`:
```python
cur.execute(
    "SELECT dss_href FROM activity_concept_dss"
    " WHERE soa_id=? AND activity_id=? AND concept_code=?",
    (soa_id, activity_id, concept_code),
)
rows = cur.fetchall()
```
Run the property population loop for each row's `dss_href`.

### `usdm/usdm_utils.py` — `_get_dss_response_codes`
**Line 151**

Change query to JOIN `activity_concept_dss` via `activity_concept` (new table has no
`concept_uid` column, so join is required):
```python
cur.execute(
    "SELECT acd.dss_href FROM activity_concept_dss acd"
    " JOIN activity_concept ac ON ac.soa_id=acd.soa_id"
    "   AND ac.activity_id=acd.activity_id"
    "   AND ac.concept_code=acd.concept_code"
    " WHERE ac.concept_uid=? AND acd.soa_id=?"
    " LIMIT 1",
    (biomedical_concept_uid, soa_id),
)
```

### `usdm/generate_biomedical_concepts.py`
**Line 37**

Change `ac.dss_href reference` to a correlated subquery against `activity_concept_dss`:
```sql
(SELECT acd.dss_href FROM activity_concept_dss acd
 WHERE acd.soa_id=ac.soa_id AND acd.activity_id=ac.activity_id
   AND acd.concept_code=ac.concept_code LIMIT 1) reference
```

---

## Verification

1. Load `/ui/soa/3/activities` — "Auto-assign DSS" button is gone.
2. Expand the DSS cell for a concept with code `C200145` — dropdown shows only the
   specializations available for that concept (~11 options).
3. Select one specialization and click `+` — the assignment appears as a pill. The dropdown
   now shows the remaining unassigned options only.
4. Select a second specialization and click `+` — two pills appear; count pill updates.
5. Click `−` on one pill — that assignment is removed; count updates.
6. Click `−` on the last remaining pill — assignment removed; cascade-delete fires for BC
   properties.
7. Run `pytest tests/test_routers_activities.py` — no regressions.
