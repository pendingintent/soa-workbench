# Plan: Extend Diff Report to Cover All USDM Entities

## Context

The freeze diff report (`/ui/soa/{soa_id}/freezes` Compare button and XLSX export) currently only tracks four entity types: visits (added/removed), activities (added/removed), matrix cells (added/removed/changed), and biomedical concept codes per activity. However, the application generates 15+ USDM entity classes from multiple DB tables — most of which are captured in freeze snapshots but never compared, or not even captured at all. The user requires that every USDM entity class produced by this application be included in the diff report, now and in the future.

---

## Current State

### Entities in snapshot but NOT diffed
| Entity | Snapshot key | uid_key for diff |
|---|---|---|
| Epoch | `epochs` | `epoch_uid` |
| Arm | `arms` | `arm_uid` |
| Element | `elements` | `name` (no uid col) |
| Timing | `timings` | `timing_uid` |
| Objective | `objectives` | `objective_uid` |
| Endpoint | `endpoints` | `endpoint_uid` |

### Entities NOT in snapshot (and therefore not diffed)
| Entity | DB table | uid_key |
|---|---|---|
| Encounter (full) | `visit` | `encounter_uid` |
| StudyCell | `study_cell` | `study_cell_uid` |
| ScheduleTimeline | `schedule_timelines` | `schedule_timeline_uid` |
| ScheduledActivityInstance | `instances` | `instance_uid` |
| ScheduledDecisionInstance | `decision_instances` | `instance_uid` |
| BiomedicalConceptSurrogate | `biomedical_concept_surrogate` | `surrogate_uid` |
| BiomedicalConcept (entity) | `biomedical_concept` | `biomedical_concept_uid` |
| BiomedicalConceptProperty | `biomedical_concept_property` | `biomedical_concept_property_uid` |
| StudyAmendment | `study_amendment` | `amendment_uid` |
| ExtensionAttribute | `activity_concept_dss` | `extension_attribute_uid` |

---

## Implementation Plan

### File: `src/soa_builder/web/routers/_freeze_helpers.py`

#### 1. Add a generic diff helper (after existing `_cell_key` helpers)

```python
def _diff_entity_list(l_list, r_list, uid_key, display_fields=None):
    """Return added/removed/changed for two lists of entity dicts.

    uid_key: field used as identity (e.g. 'epoch_uid').
    display_fields: list of field names to compare for changes.
    """
    l_map = {e[uid_key]: e for e in (l_list or [])
             if isinstance(e, dict) and e.get(uid_key)}
    r_map = {e[uid_key]: e for e in (r_list or [])
             if isinstance(e, dict) and e.get(uid_key)}
    added = [r_map[k] for k in r_map.keys() - l_map.keys()]
    removed = [l_map[k] for k in l_map.keys() - r_map.keys()]
    changed = []
    for k in r_map.keys() & l_map.keys():
        fields = display_fields or list(
            set(l_map[k]) | set(r_map[k])
        )
        diffs = {
            f: {"old": l_map[k].get(f), "new": r_map[k].get(f)}
            for f in fields
            if l_map[k].get(f) != r_map[k].get(f)
        }
        if diffs:
            changed.append({"uid": k, "changes": diffs})
    return added, removed, changed
```

#### 2. Add new capture functions

Add these after `_capture_endpoints`:

- **`_capture_encounters_full(cur, soa_id)`** — reads `visit` table selecting all columns including `encounter_uid`, `type`, `environmentalSettings`, `contactModes`, `transitionStartRule`, `transitionEndRule`, `scheduledAtId`. Uses `_table_has_columns` guard for newer columns.

- **`_capture_study_cells(cur, soa_id)`** — reads `study_cell` table: `study_cell_uid`, `arm_uid`, `epoch_uid`, `order_index`. Guards with `_table_has_columns`.

- **`_capture_schedule_timelines(cur, soa_id)`** — reads `schedule_timelines` table: `schedule_timeline_uid`, `name`, `label`, `description`, `main_timeline`, `entry_condition`, `entry_id`, `exit_id`. Guards with `_table_has_columns`.

- **`_capture_instances(cur, soa_id)`** — reads `instances` table: `instance_uid`, `name`, `label`, `description`, `epoch_uid`, `timeline_id`, `encounter_uid`, `member_of_timeline`. Guards with `_table_has_columns`.

- **`_capture_decision_instances(cur, soa_id)`** — reads `decision_instances` table: `instance_uid`, `name`, `label`, `description`, `epoch_uid`, `member_of_timeline`. Guards with `_table_has_columns`.

- **`_capture_bc_surrogates(cur, soa_id)`** — reads `biomedical_concept_surrogate` table: `surrogate_uid`, `name`, `label`, `description`, `reference`. Guards with `_table_has_columns`.

- **`_capture_biomedical_concepts(cur, soa_id)`** — reads `biomedical_concept` table: `biomedical_concept_uid`, `name`, `label`, `code`. Guards with `_table_has_columns`.

- **`_capture_bc_properties(cur, soa_id)`** — reads `biomedical_concept_property` table: `biomedical_concept_property_uid`, `name`, `label`, `isRequired`, `isEnabled`, `datatype`. Guards with `_table_has_columns`.

- **`_capture_amendments(cur, soa_id)`** — reads `study_amendment` table: `amendment_uid`, `name`, `number`, `summary`, `label`, `description`. Guards with `_table_has_columns`.

- **`_capture_extension_attributes(cur, soa_id)`** — reads `activity_concept_dss` table: `extension_attribute_uid`, `concept_code`, `dss_href`, `dss_domain`, `dss_display`. Guards with `_table_has_columns`.

#### 3. Update `_create_freeze` snapshot dict

Call all new capture functions using the existing `cur` connection. Add results to the `snapshot` dict under these keys:

```
encounters_full, study_cells, schedule_timelines, instances,
decision_instances, bc_surrogates, biomedical_concepts,
bc_properties, amendments, extension_attributes
```

#### 4. Extend `_diff_freezes_limited`

After the existing concept changes block, add diff computation for each entity using `_diff_entity_list`:

| Snapshot key | uid_key | display_fields |
|---|---|---|
| `epochs` | `epoch_uid` | `name`, `label`, `type` |
| `arms` | `arm_uid` | `name`, `label`, `type` |
| `elements` | `name` | `label`, `description`, `testrl`, `teenrl` |
| `timings` | `timing_uid` | `name`, `value`, `window_lower`, `window_upper`, `type` |
| `objectives` | `objective_uid` | `text`, `level_code_uid` |
| `endpoints` | `endpoint_uid` | `text`, `objective_uid`, `level_code_uid` |
| `encounters_full` | `encounter_uid` | `name`, `label`, `type`, `contactModes` |
| `study_cells` | `study_cell_uid` | `arm_uid`, `epoch_uid` |
| `schedule_timelines` | `schedule_timeline_uid` | `name`, `main_timeline`, `entry_condition` |
| `instances` | `instance_uid` | `name`, `epoch_uid`, `encounter_uid`, `member_of_timeline` |
| `decision_instances` | `instance_uid` | `name`, `epoch_uid`, `member_of_timeline` |
| `bc_surrogates` | `surrogate_uid` | `name`, `label`, `reference` |
| `biomedical_concepts` | `biomedical_concept_uid` | `name`, `label`, `code` |
| `bc_properties` | `biomedical_concept_property_uid` | `name`, `isRequired`, `isEnabled`, `datatype` |
| `amendments` | `amendment_uid` | `name`, `number`, `summary` |
| `extension_attributes` | `extension_attribute_uid` | `dss_href`, `dss_domain`, `dss_display` |

Apply same `_truncate` logic to each new diff list.

#### 5. Update `_diff_freezes_limited` return dict

Add entries for each new entity diff under a top-level `"entities"` key:
```python
"entities": {
    "epochs": {"added": ..., "removed": ..., "changed": ...},
    "arms": {...},
    "elements": {...},
    "timings": {...},
    "objectives": {...},
    "endpoints": {...},
    "encounters": {...},
    "study_cells": {...},
    "schedule_timelines": {...},
    "instances": {...},
    "decision_instances": {...},
    "bc_surrogates": {...},
    "biomedical_concepts": {...},
    "bc_properties": {...},
    "amendments": {...},
    "extension_attributes": {...},
}
```

Add corresponding counts to `meta["entities"]`.

---

### File: `src/soa_builder/web/templates/freeze_modal.html`

In the `{% elif mode == 'diff' %}` block:

1. Update the summary stats grid to include a count for entities with changes.
2. Add a new `<details>` section **"USDM Entities"** below the existing Concept Changes section. Inside, render one sub-`<details>` per entity type, each showing Added / Removed / Changed sub-lists. Only render a section if there are changes (use `{% if %}` guards). Changed items show `uid: field old → new`.

---

### File: `src/soa_builder/web/app.py`

In the XLSX export handler (around line 3016):

After the existing `ConceptDiff` sheet generation, add an **`EntityDiff`** sheet. The sheet uses a flat format:

| EntityType | UID | ChangeType | FieldName | LeftValue | RightValue |
|---|---|---|---|---|---|
| Epoch | Epoch_1 | changed | label | "Screening" | "Screen" |
| Arm | Arm_3 | added | | | |
| StudyCell | StudyCell_7 | removed | | | |

Generate rows by iterating `diff["entities"]`. For added/removed rows, `FieldName`/`LeftValue`/`RightValue` are blank. For changed rows, emit one row per changed field.

Keep the existing `ConceptDiff` sheet unchanged.

---

### File: `tests/test_routers_freezes.py`

Add test functions following the existing pattern (`client.post("/soa", ...)` etc.):

1. **`test_diff_epochs_captured`** — Creates SOA, adds an epoch, freezes as v1, updates epoch label, freezes as v2, compares: assert `diff["entities"]["epochs"]["changed"]` is non-empty.
2. **`test_diff_arms_captured`** — Same pattern for arms.
3. **`test_diff_timings_captured`** — Same pattern for timings.
4. **`test_diff_objectives_captured`** — Same pattern for objectives.
5. **`test_diff_endpoints_captured`** — Same pattern for endpoints.
6. **`test_diff_study_cells_captured`** — Creates study cell, freezes, compares.
7. **`test_diff_schedule_timelines_captured`** — Creates timeline, freezes, compares.
8. **`test_diff_instances_captured`** — Creates instance, freezes, compares.
9. **`test_diff_all_entities_in_xlsx`** — Creates SOA with multiple entity types, compares two freezes, downloads XLSX, asserts `EntityDiff` sheet exists and has rows.
10. **`test_diff_entities_empty_when_no_changes`** — Two identical freezes: all entity diffs are empty.

---

## Backward Compatibility

All new capture functions guard with `_table_has_columns` before querying, and return `[]` if the table or column doesn't exist. `_diff_freezes_limited` reads new snapshot keys with `.get("key", []) or []`, so old freeze snapshots (missing the new keys) produce empty diffs for new entity types rather than errors.

---

## Verification

1. Run `pytest tests/test_routers_freezes.py -v` — all tests pass.
2. Run `pytest` (full suite, ~257 tests) — no regressions.
3. Start server (`soa-builder-web`), create a study with epochs/arms/timings/objectives, freeze as v1, modify entities, freeze as v2, click Compare: all entity sections visible in modal.
4. Download XLSX with ConceptDiff: verify `EntityDiff` sheet is present and contains rows for each changed entity type.
