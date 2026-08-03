import json
import math

RESULTS = "/Users/dmoreland/projects/soa-workbench/src/soa_builder/.ua/tmp/ua-file-extract-results-1.json"
OUTDIR = "/Users/dmoreland/projects/soa-workbench/src/soa_builder/.ua/intermediate"

with open(RESULTS) as f:
    data = json.load(f)

results = {r["path"]: r for r in data["results"]}

# ---- Skip lists (trivial helper functions, <10 lines, not exported meaningfully) ----
SKIP = {
    "web/routers/activities.py": {"ui_get_dss_cell", "ui_get_crf_cell"},
    "web/routers/bc_surrogates.py": {"_nz"},
    "web/routers/cells.py": {"_nz"},
    "web/routers/elements.py": {"_nz", "_get_element_interventions"},
    "web/routers/persons.py": {"_delete_role_assignments", "list_persons"},
    "web/routers/rules.py": {"_nz", "ui_delete_rule"},
    "web/routers/schedule_timelines.py": {"_nz", "_to_bool"},
    "web/routers/study_identifiers.py": {"list_study_identifiers"},
    "web/routers/timings.py": {"_nz", "ui_delete_timing"},
    "web/routers/visits.py": {"_nz"},
}

FILE_META = {
    "web/routers/activities.py": {"entity": "activity", "tag": "activities"},
    "web/routers/bc_surrogates.py": {
        "entity": "BC surrogate",
        "tag": "biomedical-concepts",
    },
    "web/routers/cells.py": {"entity": "study cell", "tag": "study-cells"},
    "web/routers/condition_assignments.py": {
        "entity": "condition assignment",
        "tag": "conditional-logic",
    },
    "web/routers/decision_instances.py": {
        "entity": "decision instance",
        "tag": "schedule-timeline",
    },
    "web/routers/elements.py": {"entity": "element", "tag": "study-elements"},
    "web/routers/footnotes.py": {"entity": "footnote", "tag": "footnotes"},
    "web/routers/instances.py": {"entity": "instance", "tag": "instances"},
    "web/routers/persons.py": {"entity": "person", "tag": "persons"},
    "web/routers/rules.py": {"entity": "transition rule", "tag": "transition-rules"},
    "web/routers/schedule_timelines.py": {
        "entity": "schedule timeline",
        "tag": "schedule-timeline",
    },
    "web/routers/study_identifiers.py": {
        "entity": "study identifier",
        "tag": "study-identifiers",
    },
    "web/routers/timings.py": {"entity": "timing", "tag": "timings"},
    "web/routers/visits.py": {"entity": "visit", "tag": "visits"},
}

# ---- File-level nodes ----
FILE_NODES = {
    "web/audit.py": {
        "summary": "Central audit-trail module providing before/after change-logging helper "
        "functions for every mutable entity type in the SoA workbench (visits, "
        "activities, elements, timings, schedule timelines, instances, amendments, "
        "organizations, roles, persons, and more), each writing a row to the "
        "entity's audit table using the supplied database cursor.",
        "tags": ["audit-trail", "utility", "database", "logging"],
        "complexity": "complex",
        "languageNotes": "Highly repetitive by design: each of the 30+ functions follows the "
        "identical (soa_id, action, entity_id, before, after) signature so "
        "callers can log mutations uniformly across the whole domain model.",
    },
    "web/routers/activities.py": {
        "summary": "FastAPI router for Activity CRUD, bulk creation, and reordering, plus "
        "CDISC Biomedical Concept fetching/association and the DSS (Dataset "
        "Specialization) and CRF specialization cell rendering used by the SoA "
        "matrix UI.",
        "tags": ["api-handler", "router", "activities", "biomedical-concepts"],
        "complexity": "complex",
    },
    "web/routers/bc_surrogates.py": {
        "summary": "FastAPI router managing BC (Biomedical Concept) Surrogate entities and "
        "their links to activities, including the concepts-cell HTMX partial that "
        "renders surrogate/BC assignments in the SoA matrix.",
        "tags": ["api-handler", "router", "biomedical-concepts"],
        "complexity": "complex",
    },
    "web/routers/cells.py": {
        "summary": "FastAPI router for StudyCell CRUD (arm/epoch/element junction entities) "
        "and cell reordering, backing the API and HTMX UI endpoints of the SoA "
        "design matrix.",
        "tags": ["api-handler", "router", "study-cells"],
        "complexity": "moderate",
    },
    "web/routers/condition_assignments.py": {
        "summary": "FastAPI router for ConditionAssignment CRUD, mapping decision-instance "
        "branch conditions to their target instances, with matching API and HTMX "
        "UI endpoints.",
        "tags": ["api-handler", "router", "conditional-logic"],
        "complexity": "moderate",
    },
    "web/routers/decision_instances.py": {
        "summary": "FastAPI router for DecisionInstance CRUD and reordering — the branch "
        "points in a ScheduleTimeline that route subjects based on "
        "ConditionAssignments.",
        "tags": ["api-handler", "router", "schedule-timeline"],
        "complexity": "moderate",
    },
    "web/routers/elements.py": {
        "summary": "FastAPI router for StudyElement CRUD (structural design periods/"
        "cohorts), transition-rule and study-intervention associations, and the "
        "element audit-history endpoint.",
        "tags": ["api-handler", "router", "study-elements"],
        "complexity": "complex",
    },
    "web/routers/footnotes.py": {
        "summary": "FastAPI router for Footnote CRUD used to annotate SoA matrix cells and "
        "activities with reference text.",
        "tags": ["api-handler", "router", "footnotes"],
        "complexity": "moderate",
    },
    "web/routers/instances.py": {
        "summary": "FastAPI router for ScheduledActivityInstance CRUD and reordering — the "
        "temporal visit/timepoint occurrences within a ScheduleTimeline.",
        "tags": ["api-handler", "router", "instances"],
        "complexity": "moderate",
    },
    "web/routers/persons.py": {
        "summary": "FastAPI router for Person CRUD plus role/organization assignment-"
        "conflict validation, backing the combined Organizations/Roles/Persons UI "
        "page.",
        "tags": ["api-handler", "router", "persons"],
        "complexity": "moderate",
    },
    "web/routers/rules.py": {
        "summary": "FastAPI router for transition-rule (reusable business-rule text) CRUD "
        "used as start/end rule references by elements and visits.",
        "tags": ["api-handler", "router", "transition-rules"],
        "complexity": "moderate",
    },
    "web/routers/schedule_timelines.py": {
        "summary": "FastAPI router for ScheduleTimeline CRUD (main/branch timeline "
        "containers) plus the consolidated Study Timing overview page aggregating "
        "instances, decision instances, timings, and condition assignments.",
        "tags": ["api-handler", "router", "schedule-timeline"],
        "complexity": "complex",
    },
    "web/routers/study_identifiers.py": {
        "summary": "FastAPI router for StudyIdentifier CRUD, linking scoping organizations "
        "to study identifier values such as the NCT number.",
        "tags": ["api-handler", "router", "study-identifiers"],
        "complexity": "moderate",
    },
    "web/routers/timings.py": {
        "summary": "FastAPI router for StudyTiming CRUD, including ISO 8601 duration/"
        "window validation and relative-to-instance scheduling used to compute "
        "visit timing windows.",
        "tags": ["api-handler", "router", "timings"],
        "complexity": "complex",
    },
    "web/routers/visits.py": {
        "summary": "FastAPI router for Visit (Encounter) CRUD, reordering, transition "
        "rules, and environmental-setting/contact-mode code-list associations.",
        "tags": ["api-handler", "router", "visits", "encounters"],
        "complexity": "complex",
    },
    "web/schemas.py": {
        "summary": "Central Pydantic request-validation schema module defining Create/"
        "Update payload models for every mutable USDM entity in the application "
        "(activities, elements, visits, timings, arms, epochs, amendments, and "
        "more), including ISO 8601 duration and timing-window consistency "
        "validators.",
        "tags": ["type-definition", "validation", "schemas", "pydantic"],
        "complexity": "moderate",
        "languageNotes": "Uses Pydantic v2 field_validator/model_validator decorators "
        "(_validate_iso8601_duration, _validate_window_all_or_none) shared "
        "across TimingCreate/TimingUpdate, and per-class validators for "
        "conditional 'other reason' text fields.",
    },
}


def complexity_for(lines):
    if lines <= 15:
        return "simple"
    if lines <= 50:
        return "moderate"
    return "complex"


# ---- Hand-crafted overrides for unique/complex functions ----
OVERRIDES = {
    ("web/audit.py", None): None,  # placeholder, audit handled generically below
    ("web/routers/activities.py", "fetch_biomedical_concepts"): (
        "Fetches and caches the CDISC Library Biomedical Concepts list, falling back to a "
        "local CDISC_CONCEPTS_JSON override file when set, and normalizes the various API "
        "response shapes into a flat list of concept dicts.",
        ["api-integration", "cdisc-library", "caching"],
    ),
    ("web/routers/activities.py", "_next_activity_uid"): (
        "Computes the next monotonic Activity_N UID by scanning existing activity rows "
        "for the SoA.",
        ["utility", "uid-generation"],
    ),
    ("web/routers/activities.py", "set_activity_concepts"): (
        "API endpoint that replaces an activity's associated Biomedical Concept codes, "
        "upserting Code/BiomedicalConcept rows and scheduling background enrichment tasks.",
        ["api-handler", "biomedical-concepts", "background-task"],
    ),
    ("web/routers/activities.py", "ui_list_activities"): (
        "Renders the full Activities page, including CDISC concept status, DSS/CRF cell "
        "state, and drag-to-reorder controls.",
        ["api-handler", "htmx", "ui"],
    ),
    ("web/routers/activities.py", "ui_refresh_concepts_activities"): (
        "Triggers a manual refresh of the cached CDISC Biomedical Concepts list from the "
        "Activities UI and re-renders the page.",
        ["api-handler", "htmx", "cdisc-library"],
    ),
    ("web/routers/activities.py", "_render_dss_cell"): (
        "Renders the Dataset Specialization (DSS) assignment cell HTMX partial for an "
        "activity, showing currently linked DSS entries.",
        ["api-handler", "htmx", "render"],
    ),
    ("web/routers/activities.py", "ui_dss_options"): (
        "Returns DSS option choices for a given Biomedical Concept code to populate the "
        "DSS assignment dropdown.",
        ["api-handler", "htmx", "cdisc-library"],
    ),
    ("web/routers/activities.py", "ui_save_dss_assignment"): (
        "Saves a DSS (Dataset Specialization) selection for an activity/concept pair and "
        "records an audit entry.",
        ["api-handler", "htmx", "create"],
    ),
    ("web/routers/activities.py", "ui_delete_dss_assignment"): (
        "Removes a DSS assignment row from an activity and records an audit entry.",
        ["api-handler", "htmx", "delete"],
    ),
    ("web/routers/activities.py", "ui_dss_detail"): (
        "Renders the full CDISC Library DSS specialization detail page for a given href, "
        "fetching and formatting variable-level metadata.",
        ["api-handler", "htmx", "cdisc-library"],
    ),
    ("web/routers/activities.py", "_render_crf_cell"): (
        "Renders the CRF specialization assignment cell HTMX partial for an activity.",
        ["api-handler", "htmx", "render"],
    ),
    ("web/routers/activities.py", "ui_crf_options"): (
        "Returns CRF specialization option choices for a Biomedical Concept code.",
        ["api-handler", "htmx", "cdisc-library"],
    ),
    ("web/routers/activities.py", "ui_save_crf_assignment"): (
        "Saves a CRF specialization selection for an activity/concept pair and records an "
        "audit entry.",
        ["api-handler", "htmx", "create"],
    ),
    ("web/routers/activities.py", "ui_delete_crf_assignment"): (
        "Removes a CRF specialization assignment from an activity and records an audit "
        "entry.",
        ["api-handler", "htmx", "delete"],
    ),
    ("web/routers/activities.py", "ui_crf_detail_from_activity"): (
        "Renders the CRF specialization detail page reached from an activity's assignment "
        "cell.",
        ["api-handler", "htmx", "cdisc-library"],
    ),
    ("web/routers/activities.py", "add_activities_bulk"): (
        "API endpoint that creates multiple activities in one request from a newline-"
        "delimited names payload.",
        ["api-handler", "crud", "create"],
    ),
    ("web/routers/activities.py", "_reindex_activities"): (
        "Renumbers activity display_order values contiguously after a deletion or "
        "reorder.",
        ["utility", "reorder"],
    ),
    ("web/routers/bc_surrogates.py", "_render_concepts_cell"): (
        "Renders the Concepts/Surrogates HTMX cell partial for an activity, listing "
        "linked Biomedical Concepts and BC Surrogates in the SoA matrix.",
        ["api-handler", "htmx", "render"],
    ),
    ("web/routers/bc_surrogates.py", "_next_surrogate_uid"): (
        "Computes the next monotonic BCSurrogate_N UID by scanning existing surrogate "
        "rows for the SoA.",
        ["utility", "uid-generation"],
    ),
    ("web/routers/persons.py", "_assert_no_person_org_role_org_conflict"): (
        "Validates that assigning a person to an organization would not conflict with an "
        "existing role assignment scoped to a different organization.",
        ["validation", "business-rule"],
    ),
    ("web/routers/persons.py", "ui_orgs_roles_persons"): (
        "Renders the combined Organizations/Roles/Persons management page.",
        ["api-handler", "htmx", "ui"],
    ),
    ("web/routers/persons.py", "_parse_lines"): (
        "Splits a raw textarea string into a list of non-blank, trimmed lines.",
        ["utility", "parsing"],
    ),
    ("web/routers/schedule_timelines.py", "ui_study_timing"): (
        "Renders the consolidated Study Timing overview page aggregating schedule "
        "timelines, instances, decision instances, timings, and condition assignments for "
        "the SoA.",
        ["api-handler", "htmx", "ui"],
    ),
    ("web/routers/schedule_timelines.py", "_assert_main_unique"): (
        "Ensures only one ScheduleTimeline is flagged as the study's main timeline.",
        ["validation", "business-rule"],
    ),
    ("web/routers/elements.py", "_get_element_interventions_by_element"): (
        "Loads a map of element_id to associated study-intervention UIDs for all elements "
        "in the SoA.",
        ["utility", "database"],
    ),
    ("web/routers/elements.py", "_set_element_interventions"): (
        "Replaces the set of study-intervention associations for an element.",
        ["utility", "database"],
    ),
    ("web/routers/elements.py", "list_element_audit"): (
        "API endpoint returning the audit-history rows for elements on the SoA.",
        ["api-handler", "audit-trail"],
    ),
    ("web/routers/footnotes.py", "_next_footnote_uid"): (
        "Computes the next monotonic Footnote_N UID for the SoA.",
        ["utility", "uid-generation"],
    ),
    ("web/routers/footnotes.py", "_row_to_dict"): (
        "Converts a footnote database row into a plain response dict.",
        ["utility", "serialization"],
    ),
    ("web/routers/study_identifiers.py", "_list_identifiers"): (
        "Loads all study identifier rows for the SoA joined with their scoping "
        "organization.",
        ["utility", "database"],
    ),
    ("web/routers/study_identifiers.py", "_list_orgs"): (
        "Loads the organizations available to scope a study identifier to.",
        ["utility", "database"],
    ),
    ("web/routers/study_identifiers.py", "_render_partial"): (
        "Renders the study identifiers section partial for the SoA UI page.",
        ["api-handler", "htmx", "render"],
    ),
    ("web/routers/timings.py", "list_timing_audit"): (
        "API endpoint returning the audit-history rows for timings on the SoA.",
        ["api-handler", "audit-trail"],
    ),
    ("web/routers/visits.py", "_load_code_value_map"): (
        "Loads controlled-terminology submission-value lookup maps used to resolve visit "
        "environmental-setting and contact-mode codes.",
        ["utility", "controlled-terminology"],
    ),
    ("web/routers/visits.py", "get_visit"): (
        "API endpoint returning a single visit's details by ID.",
        ["api-handler", "crud", "read"],
    ),
}


def default_summary(path, fname, params, lines):
    meta = FILE_META.get(path, {"entity": "record", "tag": "domain"})
    entity = meta["entity"]
    tag = meta["tag"]
    if fname.startswith("ui_list_"):
        return (
            f"Renders the {entity}s list/section HTMX partial for the SoA UI page.",
            ["api-handler", "htmx", "ui"],
        )
    if fname.startswith("list_"):
        return (
            f"API endpoint returning all {entity}s for the given SoA as JSON.",
            ["api-handler", "crud", tag],
        )
    if fname.startswith("ui_create_") or fname.startswith("ui_add_"):
        return (
            f"HTMX form handler that creates a new {entity} from submitted form "
            f"fields and returns the refreshed partial.",
            ["api-handler", "htmx", "create"],
        )
    if fname.startswith("create_") or fname.startswith("add_"):
        return (
            f"API endpoint that validates and inserts a new {entity} for the SoA, "
            f"assigning its UID and recording an audit entry.",
            ["api-handler", "crud", "create"],
        )
    if fname.startswith("ui_update_"):
        return (
            f"HTMX form handler that updates an existing {entity} from submitted "
            f"form fields and returns the refreshed partial.",
            ["api-handler", "htmx", "update"],
        )
    if fname.startswith("update_"):
        return (
            f"API endpoint that validates and applies changes to an existing "
            f"{entity}, recording before/after audit state.",
            ["api-handler", "crud", "update"],
        )
    if fname.startswith("ui_delete_") or fname.startswith("ui_del_"):
        return (
            f"HTMX handler that deletes a {entity} and returns the updated partial.",
            ["api-handler", "htmx", "delete"],
        )
    if fname.startswith("delete_"):
        return (
            f"API endpoint that deletes a {entity} and records an audit entry.",
            ["api-handler", "crud", "delete"],
        )
    if fname.startswith("reorder_"):
        return (
            f"API endpoint that persists a new display order for {entity}s and "
            f"records a reorder audit entry.",
            ["api-handler", "reorder"],
        )
    if fname.startswith("get_"):
        return (
            f"API endpoint returning a single {entity} by ID.",
            ["api-handler", "crud", "read"],
        )
    if fname.startswith("_next_") and fname.endswith("_uid"):
        return (
            f"Computes the next monotonic UID for a new {entity} row.",
            ["utility", "uid-generation"],
        )
    if fname.startswith("_render"):
        return (
            f"Renders an HTMX cell/partial related to {entity}s.",
            ["api-handler", "htmx", "render"],
        )
    return (
        f"Helper function supporting {entity} handling in this router.",
        ["utility"],
    )


nodes = []
edges = []


def add_contains_exports(file_id, node_id, is_export=True, export_weight=0.8):
    edges.append(
        {
            "source": file_id,
            "target": node_id,
            "type": "contains",
            "direction": "forward",
            "weight": 1.0,
        }
    )
    if is_export:
        edges.append(
            {
                "source": file_id,
                "target": node_id,
                "type": "exports",
                "direction": "forward",
                "weight": export_weight,
            }
        )


# ---- audit.py ----
path = "web/audit.py"
file_id = f"file:{path}"
fmeta = FILE_NODES[path]
nodes.append(
    {"id": file_id, "type": "file", "name": "audit.py", "filePath": path, **fmeta}
)
for fn in results[path]["functions"]:
    name = fn["name"]
    node_id = f"function:{path}:{name}"
    entity_word = name.replace("_record_", "").replace("_audit", "").replace("_", " ")
    article = "an" if entity_word[:1].lower() in "aeiou" else "a"
    summary = (
        f"Records a before/after audit-trail entry for {article} {entity_word} "
        f"mutation ({', '.join(fn['params'][:2])}, ...), inserting a row into the "
        f"corresponding audit table."
    )
    lines = fn["endLine"] - fn["startLine"] + 1
    nodes.append(
        {
            "id": node_id,
            "type": "function",
            "name": name,
            "filePath": path,
            "lineRange": [fn["startLine"], fn["endLine"]],
            "summary": summary,
            "tags": ["audit-trail", "utility", "database"],
            "complexity": complexity_for(lines),
        }
    )
    add_contains_exports(file_id, node_id, is_export=True, export_weight=0.8)

# ---- router files ----
for path, meta in FILE_META.items():
    file_id = f"file:{path}"
    fmeta = FILE_NODES[path]
    nodes.append(
        {
            "id": file_id,
            "type": "file",
            "name": path.split("/")[-1],
            "filePath": path,
            **fmeta,
        }
    )
    skip = SKIP.get(path, set())
    for fn in results[path]["functions"]:
        name = fn["name"]
        if name in skip:
            continue
        lines = fn["endLine"] - fn["startLine"] + 1
        override = OVERRIDES.get((path, name))
        if override:
            summary, tags = override
        else:
            summary, tags = default_summary(path, name, fn["params"], lines)
        node_id = f"function:{path}:{name}"
        nodes.append(
            {
                "id": node_id,
                "type": "function",
                "name": name,
                "filePath": path,
                "lineRange": [fn["startLine"], fn["endLine"]],
                "summary": summary,
                "tags": tags,
                "complexity": complexity_for(lines),
            }
        )
        # export edge only for public (non-underscore) route-handler style funcs
        is_export = not name.startswith("_")
        add_contains_exports(file_id, node_id, is_export=is_export, export_weight=0.6)

# ---- schemas.py ----
path = "web/schemas.py"
file_id = f"file:{path}"
fmeta = FILE_NODES[path]
nodes.append(
    {"id": file_id, "type": "file", "name": "schemas.py", "filePath": path, **fmeta}
)

# functions
func_overrides = {
    "_validate_iso8601_duration": (
        "Validator helper that checks a string matches ISO 8601 duration syntax "
        "(e.g. P1D, P2W).",
        ["validation", "utility"],
    ),
    "_validate_window_all_or_none": (
        "Validator helper enforcing that timing window_lower, window_upper, and "
        "window_label are either all specified or all absent together.",
        ["validation", "utility"],
    ),
}
for fn in results[path]["functions"]:
    name = fn["name"]
    lines = fn["endLine"] - fn["startLine"] + 1
    summary, tags = func_overrides.get(
        name,
        ("Validator helper used by Create/Update schemas.", ["validation", "utility"]),
    )
    node_id = f"function:{path}:{name}"
    nodes.append(
        {
            "id": node_id,
            "type": "function",
            "name": name,
            "filePath": path,
            "lineRange": [fn["startLine"], fn["endLine"]],
            "summary": summary,
            "tags": tags,
            "complexity": complexity_for(lines),
        }
    )
    add_contains_exports(file_id, node_id, is_export=True, export_weight=0.7)

# classes: derive entity + create/update
CLASS_ENTITY_OVERRIDE = {
    "InstanceUpdate": "a ScheduledActivityInstance",
    "InstanceCreate": "a new ScheduledActivityInstance",
    "TimingCreate": "a new StudyTiming",
    "TimingUpdate": "a StudyTiming",
    "ScheduleTimelineCreate": "a new ScheduleTimeline",
    "ScheduleTimelineUpdate": "a ScheduleTimeline",
    "ActivityCreate": "a new Activity",
    "ActivityUpdate": "an Activity",
    "BulkActivities": "a batch of Activities from a newline-delimited names list",
    "ElementCreate": "a new StudyElement",
    "ElementUpdate": "a StudyElement",
    "EpochCreate": "a new StudyEpoch",
    "EpochUpdate": "a StudyEpoch",
    "VisitCreate": "a new Visit/Encounter",
    "VisitUpdate": "a Visit/Encounter",
    "ArmCreate": "a new StudyArm",
    "ArmUpdate": "a StudyArm",
    "RuleCreate": "a new transition rule",
    "RuleUpdate": "a transition rule",
    "SOACreate": "a new Schedule of Activities (study)",
    "SOAMetadataUpdate": "a study's top-level metadata",
    "ConceptsUpdate": "an activity's associated Biomedical Concept codes",
    "ObjectiveCreate": "a new study Objective",
    "ObjectiveUpdate": "a study Objective",
    "EndpointCreate": "a new study Endpoint",
    "EndpointUpdate": "a study Endpoint",
    "FreezeCreate": "a version freeze/snapshot of the SoA",
    "CellCreate": "a legacy SoA matrix cell (visit x activity) status",
    "MatrixInstance": "an instance row within a bulk matrix import",
    "MatrixActivity": "an activity row (with per-instance statuses) within a bulk matrix "
    "import",
    "MatrixImport": "a full wide-format SoA matrix import payload",
    "StudyCellCreate": "a new StudyCell (arm x epoch x element junction)",
    "StudyCellUpdate": "a StudyCell",
    "DecisionInstanceCreate": "a new DecisionInstance",
    "DecisionInstanceUpdate": "a DecisionInstance",
    "ConditionAssignmentCreate": "a new ConditionAssignment",
    "ConditionAssignmentUpdate": "a ConditionAssignment",
    "BCSurrogateCreate": "a new BC Surrogate",
    "BCSurrogateUpdate": "a BC Surrogate",
    "FootnoteCreate": "a new Footnote",
    "FootnoteUpdate": "a Footnote",
    "StudyAmendmentCreate": "a new StudyAmendment, requiring primary_reason_other text "
    "when primary_reason_code indicates 'Other'",
    "StudyAmendmentUpdate": "a StudyAmendment",
    "StudyAmendmentReasonCreate": "a secondary amendment reason, requiring free-text when "
    "the code is 'Other'",
    "StudyAmendmentImpactCreate": "an amendment impact record",
    "StudyChangeCreate": "an amendment change/rationale record",
    "DocumentContentReferenceCreate": "a protocol document section reference linked to an "
    "amendment change",
    "GeographicScopeCreate": "a geographic scope (country/region) on an amendment",
    "SubjectEnrollmentCreate": "a subject enrollment/quantity record scoped to a "
    "geographic scope, cohort, or site",
    "GovernanceDateCreate": "a governance/regulatory milestone date on an amendment",
}
for cls in results[path]["classes"]:
    name = cls["name"]
    start = cls["startLine"]
    end = cls["endLine"]
    methods = cls.get("methods") or []
    props = cls.get("properties") or []
    lines = end - start + 1
    desc = CLASS_ENTITY_OVERRIDE.get(name, f"a {name} payload")
    if name.endswith("Update"):
        summary = f"Pydantic request-validation schema for updating {desc}."
    elif "Create" in name or name in ("MatrixInstance", "MatrixActivity"):
        summary = f"Pydantic request-validation schema for creating {desc}."
    else:
        summary = f"Pydantic schema modeling {desc}."
    tags = ["type-definition", "validation", "schema"]
    if methods:
        tags.append("custom-validator")
    node_id = f"class:{path}:{name}"
    nodes.append(
        {
            "id": node_id,
            "type": "class",
            "name": name,
            "filePath": path,
            "lineRange": [start, end],
            "summary": summary,
            "tags": tags,
            "complexity": complexity_for(lines),
        }
    )
    add_contains_exports(file_id, node_id, is_export=True, export_weight=0.7)

# ---- imports edges ----
IMPORT_DATA = {
    "web/audit.py": ["web/db.py"],
    "web/routers/activities.py": [
        "web/audit.py",
        "web/db.py",
        "web/schemas.py",
        "web/utils.py",
    ],
    "web/routers/bc_surrogates.py": [
        "web/audit.py",
        "web/db.py",
        "web/schemas.py",
        "web/utils.py",
    ],
    "web/routers/cells.py": [
        "web/audit.py",
        "web/db.py",
        "web/schemas.py",
        "web/utils.py",
    ],
    "web/routers/condition_assignments.py": [
        "web/audit.py",
        "web/db.py",
        "web/schemas.py",
        "web/utils.py",
    ],
    "web/routers/decision_instances.py": [
        "web/audit.py",
        "web/db.py",
        "web/schemas.py",
        "web/utils.py",
    ],
    "web/routers/elements.py": [
        "web/audit.py",
        "web/db.py",
        "web/schemas.py",
        "web/utils.py",
    ],
    "web/routers/footnotes.py": [
        "web/audit.py",
        "web/db.py",
        "web/schemas.py",
        "web/utils.py",
    ],
    "web/routers/instances.py": [
        "web/audit.py",
        "web/db.py",
        "web/schemas.py",
        "web/utils.py",
    ],
    "web/routers/persons.py": ["web/audit.py", "web/db.py", "web/utils.py"],
    "web/routers/rules.py": [
        "web/audit.py",
        "web/db.py",
        "web/schemas.py",
        "web/utils.py",
    ],
    "web/routers/schedule_timelines.py": [
        "web/audit.py",
        "web/codelist_config.py",
        "web/db.py",
        "web/routers/condition_assignments.py",
        "web/routers/decision_instances.py",
        "web/routers/instances.py",
        "web/routers/timings.py",
        "web/schemas.py",
        "web/utils.py",
    ],
    "web/routers/study_identifiers.py": ["web/audit.py", "web/db.py", "web/utils.py"],
    "web/routers/timings.py": [
        "web/audit.py",
        "web/codelist_config.py",
        "web/db.py",
        "web/schemas.py",
        "web/utils.py",
    ],
    "web/routers/visits.py": [
        "web/audit.py",
        "web/codelist_config.py",
        "web/db.py",
        "web/schemas.py",
        "web/utils.py",
    ],
    "web/schemas.py": [],
}
import_edge_count = 0
for src_path, targets in IMPORT_DATA.items():
    for tgt_path in targets:
        edges.append(
            {
                "source": f"file:{src_path}",
                "target": f"file:{tgt_path}",
                "type": "imports",
                "direction": "forward",
                "weight": 0.7,
            }
        )
        import_edge_count += 1

# ---- calls edges: router functions -> audit.py functions ----
audit_func_names = {fn["name"] for fn in results["web/audit.py"]["functions"]}
calls_edge_count = 0
for path in FILE_META:
    cg = results[path].get("callGraph", [])
    skip = SKIP.get(path, set())
    for c in cg:
        if c["callee"] in audit_func_names and c["caller"] not in skip:
            edges.append(
                {
                    "source": f"function:{path}:{c['caller']}",
                    "target": f"function:web/audit.py:{c['callee']}",
                    "type": "calls",
                    "direction": "forward",
                    "weight": 0.8,
                }
            )
            calls_edge_count += 1

print("total nodes:", len(nodes))
print("total edges:", len(edges))
print("import edges:", import_edge_count)
print("calls edges:", calls_edge_count)

# ---- partition into parts by alphabetical file chunks ----
all_files = sorted(
    [
        "web/audit.py",
        "web/routers/activities.py",
        "web/routers/bc_surrogates.py",
        "web/routers/cells.py",
        "web/routers/condition_assignments.py",
        "web/routers/decision_instances.py",
        "web/routers/elements.py",
        "web/routers/footnotes.py",
        "web/routers/instances.py",
        "web/routers/persons.py",
        "web/routers/rules.py",
        "web/routers/schedule_timelines.py",
        "web/routers/study_identifiers.py",
        "web/routers/timings.py",
        "web/routers/visits.py",
        "web/schemas.py",
    ]
)

node_count = len(nodes)
edge_count = len(edges)

parts = max(1, math.ceil(max(node_count / 60, edge_count / 120)))
chunk_size = math.ceil(len(all_files) / parts)
chunks = [all_files[i : i + chunk_size] for i in range(0, len(all_files), chunk_size)]
print("parts:", parts, "chunk_size:", chunk_size, "num_chunks:", len(chunks))
for i, c in enumerate(chunks):
    print(f"chunk {i + 1}: {c}")


# map node id -> file path (for file nodes use filePath, for func/class nodes use filePath too)
def node_path(n):
    return n.get("filePath")


for i, chunk_files in enumerate(chunks, start=1):
    chunk_set = set(chunk_files)
    part_nodes = [n for n in nodes if node_path(n) in chunk_set]
    part_node_ids = {n["id"] for n in part_nodes}
    part_edges = [
        e
        for e in edges
        if e["source"] in part_node_ids
        or (e["source"].startswith("file:") and e["source"][5:] in chunk_set)
    ]
    # dedupe just in case
    out = {"nodes": part_nodes, "edges": part_edges}
    fname = (
        f"{OUTDIR}/batch-1-part-{i}.json"
        if len(chunks) > 1
        else f"{OUTDIR}/batch-1.json"
    )
    with open(fname, "w") as f:
        json.dump(out, f, indent=2)
    print(f"wrote {fname}: {len(part_nodes)} nodes, {len(part_edges)} edges")
