# SoA Builder API & UI Endpoints

Complete documentation for all 165+ API and UI endpoints in the SoA Workbench application.

> **Quick Reference**: See `docs/api_endpoints.csv` for a sortable/filterable spreadsheet of all endpoints.
>
> **Conventions**
> - `{soa_id}`, `{visit_id}`, etc. denote path parameters (integers)
> - JSON endpoints return `application/json` unless noted
> - UI endpoints return `text/html` (HTMX partials for partial page updates)
> - Time values are ISO-8601 UTC
> - UIDs follow pattern: `EntityName_N` (e.g., `StudyElement_1`, `ScheduledActivityInstance_5`)
> - Errors use FastAPI default: `{"detail": "message"}`, HTTP status codes: 400, 404, 422
>
> **Authentication**: Not implemented (all endpoints open). Add auth (API keys / OAuth2) before production use.
>
> **Server**: Default runs at `http://localhost:8000` (start via `soa-builder-web` or `uvicorn soa_builder.web.app:app --reload`)

---
## Table of Contents
1. [SoA (Study Container)](#soa-study-container)
2. [Visits](#visits)
3. [Activities](#activities)
4. [Epochs](#epochs)
5. [Arms](#arms)
6. [Elements](#elements)
7. [Instances (ScheduledActivityInstance)](#instances-scheduledactivityinstance)
8. [Schedule Timelines](#schedule-timelines)
9. [Timings](#timings)
10. [Transition Rules](#transition-rules)
11. [Matrix Cells](#matrix-cells)
12. [Study Cells](#study-cells)
13. [Freezes & Rollback](#freezes--rollback)
14. [Audits](#audits)
15. [Biomedical Concepts (CDISC)](#biomedical-concepts-cdisc)
16. [SDTM Specializations](#sdtm-specializations)
17. [Terminology (DDF & Protocol)](#terminology-ddf--protocol)
18. [Curl Examples](#curl-examples)

---
## SoA (Study Container)

| Method | Path | Type | Description |
| ------ | ---- | ---- | ----------- |
| GET | `/` | UI | Index page - lists all studies with create form |
| POST | `/soa` | API | Create new SoA. Body: `{"name": str, "study_id"?: str, "study_label"?: str, "study_description"?: str}` |
| GET | `/soa/{soa_id}` | API | Get SoA summary (visits, activities, epochs, arms counts) |
| POST | `/soa/{soa_id}/metadata` | API | Update study metadata. Body: `{"study_id"?: str, "study_label"?: str, "study_description"?: str}` |
| GET | `/soa/{soa_id}/normalized` | API | Generate normalized USDM-compatible JSON |
| GET | `/soa/{soa_id}/matrix` | API | Get raw matrix data (visits, activities, cells) |
| POST | `/soa/{soa_id}/matrix/import` | API | Bulk import matrix. Body: `{"instances": [...], "activities": [...], "reset": bool}` |
| GET | `/soa/{soa_id}/export/xlsx` | API | Download Excel workbook |
| GET | `/soa/{soa_id}/export/pdf` | API | Download PDF report |
| POST | `/ui/soa/create` | UI | Create SoA via form |
| POST | `/ui/soa/{soa_id}/update_meta` | UI | Update study metadata via form |
| GET | `/ui/soa/{soa_id}/edit` | UI | Primary editing interface (matrix view) |

### Example: Create SoA
```bash
curl -X POST http://localhost:8000/soa \
  -H 'Content-Type: application/json' \
  -d '{"name":"Phase II Trial","study_id":"STUDY-2024-001","study_label":"Phase 2"}'
```
Response:
```json
{"id": 3, "name": "Phase II Trial", "created_at": "2026-01-20T10:30:00.000000+00:00"}
```

---
## Visits

Visits are **Encounters** in USDM terms - they represent physical or virtual visits where activities occur.

| Method | Path | Type | Description |
| ------ | ---- | ---- | ----------- |
| GET | `/soa/{soa_id}/visits` | API | List all visits for SoA (ordered by sequence_index) |
| GET | `/ui/soa/{soa_id}/visits` | UI | Visits management page |
| GET | `/soa/visits/{visit_id}` | API | Get visit detail (includes encounter_uid) |
| POST | `/soa/{soa_id}/visits` | API | Create visit. Body: `{"name": str, "label"?: str, "epoch_id"?: int, "encounter_uid"?: str}` |
| PATCH | `/soa/{soa_id}/visits/{visit_id}` | API | Update visit (partial). Returns `{"updated_fields": [...]}` |
| DELETE | `/soa/{soa_id}/visits/{visit_id}` | API | Delete visit (cascades to matrix_cells) |
| POST | `/soa/{soa_id}/visits/reorder` | API | Reorder visits. Body: `[visit_id1, visit_id2, ...]` |
| POST | `/ui/soa/{soa_id}/visits/create` | UI | Create visit via form |
| POST | `/ui/soa/{soa_id}/visits/{visit_id}/update` | UI | Update visit via form |
| POST | `/ui/soa/{soa_id}/visits/{visit_id}/delete` | UI | Delete visit via form |
| POST | `/ui/soa/{soa_id}/reorder_visits` | UI | Reorder visits via drag-drop form |
| POST | `/ui/soa/{soa_id}/set_visit_epoch` | UI | Assign/clear visit epoch |
| POST | `/ui/soa/{soa_id}/set_visit_transition_end_rule` | UI | Set transition end rule |
| POST | `/visits/reorder` | API | Reorder visits (router version) |

---
## Activities

Activities are **USDM Activity** entities linked to biomedical concepts via `activity_concept` table.

| Method | Path | Type | Description |
| ------ | ---- | ---- | ----------- |
| GET | `/activities` | API | List all activities |
| GET | `/activities/{activity_id}` | API | Get activity detail (includes activity_uid) |
| POST | `/activities` | API | Create activity. Body: `{"name": str, "activity_uid"?: str}` |
| PATCH | `/activities/{activity_id}` | API | Update activity (partial) |
| DELETE | `/soa/{soa_id}/activities/{activity_id}` | API | Delete activity (cascades to matrix_cells, activity_concept) |
| POST | `/activities/bulk` | API | Bulk add activities. Body: `{"names": [str, ...]}` (deduplicates, skips blanks) |
| POST | `/soa/{soa_id}/activities/{activity_id}/concepts` | API | Set biomedical concepts. Body: `{"concept_codes": [str, ...]}` |
| POST | `/activities/{activity_id}/concepts` | API | Set concepts (router version) |
| POST | `/soa/{soa_id}/activities/reorder` | API | Reorder activities. Body: `[activity_id1, ...]` |
| POST | `/activities/reorder` | API | Reorder activities (router version) |
| POST | `/activities/add` | UI | Add activity via form (router) |
| POST | `/activities/{activity_id}/update` | UI | Update activity via form (router) |
| POST | `/ui/soa/{soa_id}/add_activity` | UI | Add activity via form |
| POST | `/ui/soa/{soa_id}/delete_activity` | UI | Delete activity via form |
| POST | `/ui/soa/{soa_id}/reorder_activities` | UI | Reorder activities via drag-drop |

---
## Epochs

Epochs are **USDM StudyEpoch** entities representing high-level study phases (e.g., Screening, Treatment, Follow-up).

| Method | Path | Type | Description |
| ------ | ---- | ---- | ----------- |
| GET | `/soa/{soa_id}/epochs` | API | List epochs (ordered by epoch_seq) |
| GET | `/ui/soa/{soa_id}/epochs` | UI | Epochs management page |
| GET | `/soa/{soa_id}/epochs/{epoch_id}` | API | Get epoch detail (includes epoch_uid) |
| POST | `/soa/{soa_id}/epochs/{epoch_id}/metadata` | API | Update epoch metadata. Body: `{"name"?: str, "epoch_label"?: str, "epoch_description"?: str, "type"?: str}` |
| PATCH | `/soa/{soa_id}/epochs/{epoch_id}` | API | Update epoch (partial). Returns `{"updated_fields": [...]}` |
| DELETE | `/soa/{soa_id}/epochs/{epoch_id}` | API | Delete epoch |
| POST | `/soa/{soa_id}/epochs/reorder` | API | Reorder epochs. Body: `[epoch_id1, ...]` |
| POST | `/ui/soa/{soa_id}/epochs/create` | UI | Create epoch via form |
| POST | `/ui/soa/{soa_id}/epochs/{epoch_id}/update` | UI | Update epoch via form |
| POST | `/ui/soa/{soa_id}/epochs/{epoch_id}/delete` | UI | Delete epoch via form |
| POST | `/ui/soa/{soa_id}/reorder_epochs` | UI | Reorder epochs via drag-drop |

---
## Arms

Arms are **USDM StudyArm** entities. Each has immutable `arm_uid` (format: `StudyArm_N`).

| Method | Path | Type | Description |
| ------ | ---- | ---- | ----------- |
| GET | `/soa/{soa_id}/arms` | API | List arms (ordered) |
| GET | `/ui/soa/{soa_id}/arms` | UI | Arms management page |
| POST | `/soa/{soa_id}/arms` | API | Create arm. Body: `{"name": str, "label"?: str, "description"?: str, "type"?: str, "origin"?: str}`. Auto-assigns `arm_uid` |
| PATCH | `/soa/{soa_id}/arms/{arm_id}` | API | Update arm (partial). Returns `{"updated_fields": [...]}`. `arm_uid` immutable |
| POST | `/arms/reorder` | API | Reorder arms. Body: `[arm_id1, ...]` |
| POST | `/ui/soa/{soa_id}/arms/create` | UI | Create arm via form |
| POST | `/ui/soa/{soa_id}/arms/{arm_id}/update` | UI | Update arm via form |
| POST | `/ui/soa/{soa_id}/arms/{arm_id}/delete` | UI | Delete arm via form |
| POST | `/ui/soa/{soa_id}/reorder_arms` | UI | Reorder arms via drag-drop |

---
## Elements

Elements are **USDM StudyElement** entities representing structural design components (e.g., treatment periods, cohorts). Each has immutable `element_id` (format: `StudyElement_N`).

| Method | Path | Type | Description |
| ------ | ---- | ---- | ----------- |
| GET | `/soa/{soa_id}/elements` | API | List elements (ordered) |
| GET | `/ui/soa/{soa_id}/elements` | UI | Elements management page |
| GET | `/soa/{soa_id}/elements/{element_id}` | API | Get element detail |
| POST | `/elements` | API | Create element. Body: `{"name": str, "label"?: str, "description"?: str, "testrl"?: str, "teenrl"?: str}`. Auto-assigns `element_id` |
| PATCH | `/soa/{soa_id}/elements/{element_id}` | API | Update element (partial) |
| PATCH | `/elements/{element_id}` | API | Update element (router version) |
| DELETE | `/elements/{element_id}` | API | Delete element |
| POST | `/elements/reorder` | API | Reorder elements. Body: `[element_id1, ...]` |
| GET | `/soa/{soa_id}/element_audit` | API | Get element audit log |
| POST | `/ui/soa/{soa_id}/elements/create` | UI | Create element via form |
| POST | `/ui/soa/{soa_id}/elements/{element_id}/update` | UI | Update element via form |
| POST | `/ui/soa/{soa_id}/elements/{element_id}/delete` | UI | Delete element via form |

### Example: Element Operations
```bash
# Create element
curl -X POST http://localhost:8000/elements \
  -H 'Content-Type: application/json' \
  -d '{"name":"Screening Period","label":"SCR","description":"Initial screening"}'

# Reorder elements
curl -X POST http://localhost:8000/elements/reorder \
  -H 'Content-Type: application/json' \
  -d '[3,1,2]'
```

---
## Instances (ScheduledActivityInstance)

Instances are **USDM ScheduledActivityInstance** entities - temporal visit/timepoint occurrences where activities happen. Each has `instance_uid` (format: `ScheduledActivityInstance_N`).

| Method | Path | Type | Description |
| ------ | ---- | ---- | ----------- |
| GET | `/soa/{soa_id}/instances` | API | List instances (ordered) |
| GET | `/ui/soa/{soa_id}/instances` | UI | Instances management page |
| POST | `/ui/soa/{soa_id}/instances/create` | UI | Create instance via form. Fields: name, label, description, epoch_uid, encounter_uid, timeline_id, etc. |
| POST | `/ui/soa/{soa_id}/instances/{instance_id}/update` | UI | Update instance via form |
| POST | `/ui/soa/{soa_id}/instances/{instance_id}/delete` | UI | Delete instance via form |

---
## Schedule Timelines

Schedule Timelines are **USDM ScheduleTimeline** containers holding instances, timings, and exits. Each has `schedule_timeline_uid`.

| Method | Path | Type | Description |
| ------ | ---- | ---- | ----------- |
| GET | `/ui/soa/{soa_id}/schedule_timelines` | UI | Schedule timelines management page |
| POST | `/ui/soa/{soa_id}/schedule_timelines/create` | UI | Create timeline via form. Fields: name, label, main_timeline (bool), entry_condition, entry_id |
| POST | `/ui/soa/{soa_id}/schedule_timelines/{schedule_timeline_id}/update` | UI | Update timeline via form |
| POST | `/ui/soa/{soa_id}/schedule_timelines/{schedule_timeline_id}/delete` | UI | Delete timeline via form |

---
## Timings

Timings are **USDM Timing** definitions for schedule references. Each has `timing_uid` (format: `Timing_N`).

| Method | Path | Type | Description |
| ------ | ---- | ---- | ----------- |
| GET | `/soa/{soa_id}/timings` | API | List timings (ordered) |
| GET | `/ui/soa/{soa_id}/timings` | UI | Timings management page |
| GET | `/soa/{soa_id}/timing_audit` | API | Get timing audit log |
| POST | `/ui/soa/{soa_id}/timings/create` | UI | Create timing via form. Fields: name, label, type, value, window_upper, window_lower, relative_to_from, etc. |
| POST | `/ui/soa/{soa_id}/timings/{timing_id}/update` | UI | Update timing via form |
| POST | `/ui/soa/{soa_id}/timings/{timing_id}/delete` | UI | Delete timing via form |

---
## Transition Rules

Transition rules define **USDM TransitionRule** entities for element entry/exit conditions.

| Method | Path | Type | Description |
| ------ | ---- | ---- | ----------- |
| GET | `/soa/{soa_id}/rules` | API | List transition rules |
| GET | `/ui/soa/{soa_id}/rules` | UI | Transition rules management page |
| PATCH | `/soa/{soa_id}/rules/{rule_id}` | API | Update rule (partial) |
| POST | `/ui/soa/{soa_id}/rules/create` | UI | Create rule via form |
| POST | `/ui/soa/{soa_id}/rules/{rule_id}/update` | UI | Update rule via form |
| POST | `/ui/soa/{soa_id}/rules/{rule_id}/delete` | UI | Delete rule via form |

---
## Matrix Cells

Matrix cells (`matrix_cells` table) link visits/instances to activities with status markers.

| Method | Path | Type | Description |
| ------ | ---- | ---- | ----------- |
| POST | `/soa/{soa_id}/cells` | API | Create/update matrix cell. Body: `{"visit_id": int, "activity_id": int, "status": str}` |
| POST | `/soa/{soa_id}/cells_instance` | API | Create cell with instance_id. Body: `{"instance_id": int, "activity_id": int, "status": str}` |
| POST | `/ui/soa/{soa_id}/set_cell` | UI | Set cell status via form |
| POST | `/ui/soa/{soa_id}/toggle_cell` | UI | Toggle cell status (blank → X → O → blank) |
| POST | `/ui/soa/{soa_id}/toggle_cell_instance` | UI | Toggle cell instance status |

**Status values**: Blank (empty), `"X"` (required), `"O"` (optional)

---
## Study Cells

Study Cells are **USDM StudyCell** junction entities combining `armId + epochId + elementIds[]`. Each has `study_cell_uid` (format: `StudyCell_N`).

| Method | Path | Type | Description |
| ------ | ---- | ---- | ----------- |
| POST | `/ui/soa/{soa_id}/add_study_cell` | UI | Add study cell. Form fields: arm_uid, epoch_uid, element_uid |
| POST | `/ui/soa/{soa_id}/update_study_cell` | UI | Update study cell |
| POST | `/ui/soa/{soa_id}/delete_study_cell` | UI | Delete study cell |

---
## Freezes & Rollback

Freezes create immutable snapshots of SoA state for versioning. Rollback restores from a freeze.

| Method | Path | Type | Description |
| ------ | ---- | ---- | ----------- |
| POST | `/ui/soa/{soa_id}/freeze` | UI | Create freeze snapshot. Form field: `version_label` (optional) |
| GET | `/soa/{soa_id}/freeze/{freeze_id}` | API | Get freeze snapshot JSON (visits, activities, cells, epochs, arms, elements, concepts) |
| GET | `/ui/soa/{soa_id}/freeze/{freeze_id}/view` | UI | View freeze modal (HTML) |
| GET | `/ui/soa/{soa_id}/freeze/diff` | UI | Compare two freezes. Query params: `?left=freeze_id&right=freeze_id` |
| GET | `/soa/{soa_id}/freeze/diff.json` | API | Get freeze diff JSON. Query params: `?left=&right=` |

**Freeze includes**: epochs, elements, visits, activities, matrix_cells, activity_concepts, study metadata

---
## Audits

Comprehensive audit trails for all entity mutations and bulk operations.

| Method | Path | Type | Description |
| ------ | ---- | ---- | ----------- |
| GET | `/soa/{soa_id}/rollback_audit` | API | Rollback audit log (freeze restores) |
| GET | `/ui/soa/{soa_id}/rollback_audit` | UI | View rollback audit modal |
| GET | `/soa/{soa_id}/rollback_audit/export/xlsx` | API | Export rollback audit as Excel |
| GET | `/soa/{soa_id}/reorder_audit` | API | Reorder audit log (visits, activities, epochs, elements, arms) |
| GET | `/ui/soa/{soa_id}/reorder_audit` | UI | View reorder audit modal |
| GET | `/soa/{soa_id}/reorder_audit/export/csv` | API | Export reorder audit as CSV |
| GET | `/soa/{soa_id}/reorder_audit/export/xlsx` | API | Export reorder audit as Excel |
| GET | `/soa/{soa_id}/element_audit` | API | Element-specific audit (create/update/delete/reorder) |
| GET | `/soa/{soa_id}/timing_audit` | API | Timing-specific audit |
| GET | `/ui/soa/{soa_id}/audits` | UI | Combined audits page |

### Audit Entry Structure
All entity audits follow this pattern:
```json
{
  "id": 42,
  "soa_id": 1,
  "{entity}_id": 7,
  "action": "create|update|delete|reorder",
  "before": {"id": 7, "name": "Old Value"},
  "after": {"id": 7, "name": "New Value"},
  "performed_at": "2026-01-20T10:30:00.000000+00:00",
  "updated_fields": ["name", "label"]
}
```
- `before` is null for creates
- `after` is null for deletes
- `updated_fields` present only for updates

---
## Biomedical Concepts (CDISC)

Integration with CDISC Library API for biomedical concept assignment to activities.

| Method | Path | Type | Description |
| ------ | ---- | ---- | ----------- |
| GET | `/concepts/status` | API | Get concepts cache status (TTL, count, last refresh) |
| GET | `/ui/concepts` | UI | List all biomedical concepts (cached) |
| GET | `/ui/concepts/{code}` | UI | View concept detail page |
| POST | `/ui/soa/{soa_id}/concepts_refresh` | UI | Force refresh concepts cache from CDISC API |
| GET | `/ui/concept_categories` | UI | List concept categories |
| GET | `/ui/concept_categories/view` | UI | View concepts by category. Query param: `?name=category_name` |

**Environment Variables Required**:
- `CDISC_SUBSCRIPTION_KEY` or `CDISC_API_KEY` - for CDISC Library API access
- `CDISC_CONCEPTS_JSON` - (optional) for test overrides

**Test Override**: Set `CDISC_CONCEPTS_JSON` to file path or inline JSON to bypass remote API

---
## SDTM Specializations

SDTM controlled terminology codelists.

| Method | Path | Type | Description |
| ------ | ---- | ---- | ----------- |
| GET | `/sdtm/specializations/status` | API | Get SDTM specializations status |
| GET | `/ui/sdtm/specializations/status` | UI | View SDTM status page |
| POST | `/ui/sdtm/specializations/refresh` | UI | Refresh SDTM specializations from API |
| GET | `/ui/sdtm/specializations` | UI | List SDTM specializations |
| GET | `/ui/sdtm/specializations/{idx}` | UI | View SDTM specialization detail |

---
## Terminology (DDF & Protocol)

Two parallel terminology domains are supported: DDF Terminology and Protocol Terminology. Each provides identical capabilities: load Excel sheet, query with filters, upload new sheet via UI, audit loads (with filtering and CSV/JSON export).

| Domain | Method | Path | Purpose |
|--------|--------|------|---------|
| DDF | POST | `/admin/load_ddf_terminology` | (Re)load default DDF Excel from `files/DDF_Terminology_2025-09-26.xls` (captures `dataset_date=2025-09-26`) |
| DDF | GET | `/ddf/terminology` | Query rows (filters: `search`, `code`, `codelist_name`, `codelist_code`, pagination `limit`,`offset`). Rows include synthetic `dataset_date` extracted only from the sheet name. |
| DDF | GET | `/ui/ddf/terminology` | HTML UI page (same filters + upload status) including `dataset_date`. |
| DDF | POST | `/ui/ddf/terminology/upload` | Upload Excel (.xls/.xlsx) and reload table; `dataset_date` auto-derived strictly from sheet name (file name ignored). |
| DDF | GET | `/ddf/terminology/audit` | List audit entries (filters: `source`, `start`, `end`) each with `dataset_date`. |
| DDF | GET | `/ddf/terminology/audit/export.csv` | Export filtered audit rows (includes `dataset_date`) as CSV |
| DDF | GET | `/ddf/terminology/audit/export.json` | Export filtered audit rows (includes `dataset_date`) as JSON |
| DDF | GET | `/ui/ddf/terminology/audit` | HTML audit listing with filters + export links showing `dataset_date`. |
| Protocol | POST | `/admin/load_protocol_terminology` | (Re)load default Protocol Excel from `files/Protocol_Terminology_2025-09-26.xls` (captures `dataset_date=2025-09-26`) |
| Protocol | GET | `/protocol/terminology` | Query rows (filters: `search`, `code`, `codelist_name`, `codelist_code`, pagination). Rows include `dataset_date` extracted only from the sheet name. |
| Protocol | GET | `/ui/protocol/terminology` | HTML UI page (same filters + upload status) including `dataset_date`. |
| Protocol | POST | `/ui/protocol/terminology/upload` | Upload & reload Protocol terminology; `dataset_date` auto-derived strictly from sheet name. |
| Protocol | GET | `/protocol/terminology/audit` | Audit entries (filters: `source`, `start`, `end`) each with `dataset_date`. |
| Protocol | GET | `/protocol/terminology/audit/export.csv` | CSV export (includes `dataset_date`). |
| Protocol | GET | `/protocol/terminology/audit/export.json` | JSON export (includes `dataset_date`). |
| Protocol | GET | `/ui/protocol/terminology/audit` | HTML audit listing + export links showing `dataset_date`. |

### Terminology Query Parameters
`search` performs case-insensitive substring across key text columns (`code`, `cdisc_submission_value`, `cdisc_definition`, `cdisc_synonym_s`, `nci_preferred_term`, `codelist_name`, `codelist_code`). Exact-match filters (`code`, `codelist_name`, `codelist_code`) narrow before search is applied. Pagination: `limit` (1–200), `offset` (>=0).

### Audit Entry Fields
`id, loaded_at (UTC ISO), file_path, original_filename, sheet_name, row_count, column_count, columns_json, source (admin|upload), file_hash (sha256), error (nullable), dataset_date`

`dataset_date` is extracted via regex `YYYY-MM-DD` only from the sheet name. If the sheet name does not contain a date substring (e.g. `DDF Terminology 2025-09-26`) the load will fail with HTTP 400. The file name is intentionally ignored to enforce explicit versioning in worksheet naming. An index exists on this field (`idx_ddf_audit_dataset_date`, `idx_protocol_audit_dataset_date`) enabling efficient future filtering/grouping.

Error rows have `row_count=0` and `error` populated (e.g. read or missing file). Successful loads have `error=null`.

### Sample Terminology Queries
```bash
# DDF: find by codelist_code
curl -s --get 'http://localhost:8000/ddf/terminology' \
  --data-urlencode 'codelist_code=C139020' | jq '.matched_count'

# Protocol: search within definition text
curl -s --get 'http://localhost:8000/protocol/terminology' \
  --data-urlencode 'search=trial' \
  --data-urlencode 'limit=5' | jq '.rows[].code'

# Audit export (Protocol, last 7 days)
curl -s --get 'http://localhost:8000/protocol/terminology/audit/export.csv' \
  --data-urlencode 'start=2025-11-05' \
  --data-urlencode 'end=2025-11-12' > protocol_audit.csv
# Show dataset_date values (Protocol audit)
curl -s --get 'http://localhost:8000/protocol/terminology/audit' | jq '.rows[].dataset_date' | head
```

### Upload via UI Forms
- DDF: `/ui/ddf/terminology` (sheet name default: `DDF Terminology 2025-09-26`)
- Protocol: `/ui/protocol/terminology` (sheet name default: `Protocol Terminology 2025-09-26`)

Both accept `.xls` or `.xlsx`. A SHA-256 hash is computed and stored in audit for integrity tracking.

---
## Curl Examples

### Basic Workflow
```bash
# 1. Create a study
RESPONSE=$(curl -s -X POST http://localhost:8000/soa \
  -H 'Content-Type: application/json' \
  -d '{"name":"Phase II Trial","study_id":"TRIAL-2026-001"}')
SOA_ID=$(echo $RESPONSE | jq -r '.id')
echo "Created SoA ID: $SOA_ID"

# 2. Add visits
curl -s -X POST http://localhost:8000/soa/$SOA_ID/visits \
  -H 'Content-Type: application/json' \
  -d '{"name":"Screening"}'

curl -s -X POST http://localhost:8000/soa/$SOA_ID/visits \
  -H 'Content-Type: application/json' \
  -d '{"name":"Baseline"}'

# 3. Add activities
curl -s -X POST http://localhost:8000/activities \
  -H 'Content-Type: application/json' \
  -d '{"name":"Physical Exam"}'

curl -s -X POST http://localhost:8000/activities \
  -H 'Content-Type: application/json' \
  -d '{"name":"Vital Signs"}'

# 4. Bulk add activities
curl -s -X POST http://localhost:8000/activities/bulk \
  -H 'Content-Type: application/json' \
  -d '{"names":["ECG","Labs","Imaging"]}'

# 5. Create epochs
curl -s -X POST http://localhost:8000/soa/$SOA_ID/epochs \
  -H 'Content-Type: application/json' \
  -d '{"name":"Screening","epoch_label":"SCR"}'

# 6. Create arms
curl -s -X POST http://localhost:8000/soa/$SOA_ID/arms \
  -H 'Content-Type: application/json' \
  -d '{"name":"Treatment A","type":"Experimental"}'

# 7. Create elements
curl -s -X POST http://localhost:8000/elements \
  -H 'Content-Type: application/json' \
  -d '{"name":"Screening Period","label":"SCR_PERIOD"}'

# 8. Get matrix
curl -s http://localhost:8000/soa/$SOA_ID/matrix | jq

# 9. Export to Excel
curl -O http://localhost:8000/soa/$SOA_ID/export/xlsx

# 10. Create freeze
curl -s -X POST http://localhost:8000/ui/soa/$SOA_ID/freeze \
  -d 'version_label=v1.0'
```

### Advanced Operations
```bash
# Reorder elements
curl -X POST http://localhost:8000/elements/reorder \
  -H 'Content-Type: application/json' \
  -d '[3,1,2]'

# Assign biomedical concepts to activity
curl -X POST http://localhost:8000/soa/1/activities/5/concepts \
  -H 'Content-Type: application/json' \
  -d '{"concept_codes":["C25473","C16960"]}'

# Update epoch metadata
curl -X POST http://localhost:8000/soa/1/epochs/2/metadata \
  -H 'Content-Type: application/json' \
  -d '{"name":"Treatment Phase","epoch_label":"TRT","type":"TREATMENT"}'

# Get audit logs
curl -s http://localhost:8000/soa/1/element_audit | jq
curl -s http://localhost:8000/soa/1/reorder_audit | jq

# Export audits
curl -O http://localhost:8000/soa/1/rollback_audit/export/xlsx
curl -O http://localhost:8000/soa/1/reorder_audit/export/csv

# Compare freezes
curl -s 'http://localhost:8000/soa/1/freeze/diff.json?left=5&right=7' | jq

# Query DDF terminology
curl -s --get 'http://localhost:8000/ddf/terminology' \
  --data-urlencode 'codelist_code=C139020' | jq

# Search protocol terminology
curl -s --get 'http://localhost:8000/protocol/terminology' \
  --data-urlencode 'search=trial' \
  --data-urlencode 'limit=5' | jq
```

---
## Quick Reference Card

### Most Common Endpoints
| Operation | Method | Endpoint |
|-----------|--------|----------|
| List studies | GET | `/` |
| Create study | POST | `/soa` |
| Edit study | GET | `/ui/soa/{id}/edit` |
| Get matrix | GET | `/soa/{id}/matrix` |
| Export Excel | GET | `/soa/{id}/export/xlsx` |
| Create visit | POST | `/soa/{id}/visits` |
| Create activity | POST | `/activities` |
| Create epoch | POST | `/soa/{id}/epochs` |
| Create arm | POST | `/soa/{id}/arms` |
| Create element | POST | `/elements` |
| Reorder entities | POST | `/elements/reorder`, `/soa/{id}/visits/reorder`, etc. |
| Create freeze | POST | `/ui/soa/{id}/freeze` |
| View audits | GET | `/ui/soa/{id}/audits` |
| Concepts list | GET | `/ui/concepts` |

### Response Patterns
- **Success**: HTTP 200/201 with JSON body
- **Create**: Returns `{"id": N, ...}` with entity ID
- **Update**: Returns `{"updated_fields": [...]}` for partial updates
- **Reorder**: Returns `{"message": "...", "new_order": [...]}` 
- **Delete**: Returns `{"message": "...deleted"}` with cascade info
- **Error**: HTTP 400/404/422 with `{"detail": "..."}`

---
## Full Endpoint Inventory

See **`docs/api_endpoints.csv`** for complete sortable/filterable list of all 165 endpoints with:
- Method (GET/POST/PATCH/DELETE)
- Path with parameters
- Type (API/UI/Admin)
- Description
- Response type (JSON/HTML/Binary)

---

*Last Updated: January 20, 2026*
*Version: 4.0*
