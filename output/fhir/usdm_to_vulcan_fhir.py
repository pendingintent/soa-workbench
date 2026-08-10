"""One-off USDM -> FHIR mapper following the HL7 Vulcan Schedule of
Activities Implementation Guide (StudyProtocolSoa / StudyVisitSoa /
StudyActivitySoa profiles), grounded against the IG's own published
H2Q-MC-LZZT example resources at
http://hl7.org/fhir/uv/vulcan-schedule/.

Reads a USDM v4 JSON export from soa-workbench and emits a FHIR Bundle
containing:
  - one top-level PlanDefinition (StudyProtocolSoa) with one action per
    planned visit, carrying day offsets (action.relatedAction.offsetDuration)
    and acceptable visit windows (AcceptableOffsetRangeSoa extension)
  - one per-visit PlanDefinition (StudyVisitSoa) with one action per
    activity performed at that visit
  - one ActivityDefinition (StudyActivitySoa) per unique activity
    referenced by the main schedule timeline

Scope: main schedule timeline only. Branch timelines (adverse event,
early termination, vital-sign sub-profiles) are not represented -- see
the summary printed at the end of the run.
"""

import json
import re
import sys
from pathlib import Path

IG_BASE = "http://hl7.org/fhir/uv/vulcan-schedule"
OFFSET_RANGE_EXT_URL = f"{IG_BASE}/StructureDefinition/AcceptableOffsetRangeSoa"

ISO_DURATION_RE = re.compile(
    r"^(?P<sign>-)?P"
    r"(?:(?P<weeks>\d+(?:\.\d+)?)W)?"
    r"(?:(?P<days>\d+(?:\.\d+)?)D)?"
    r"(?:T"
    r"(?:(?P<hours>\d+(?:\.\d+)?)H)?"
    r"(?:(?P<minutes>\d+(?:\.\d+)?)M)?"
    r")?$"
)


def parse_iso_duration_to_days(value):
    """Parse an ISO 8601 duration (e.g. '-P2W', 'P2D', 'PT4H') into a
    single signed value in days. The Vulcan IG's own example
    (AcceptableOffsetRangeSoa, action.relatedAction.offsetDuration)
    always expresses offsets/windows in UCUM 'd', so we normalize to
    days here rather than carrying mixed units through the mapping.
    """
    if not value:
        return 0.0
    m = ISO_DURATION_RE.match(value)
    if not m:
        raise ValueError(f"Unrecognized ISO 8601 duration: {value!r}")
    sign = -1 if m.group("sign") else 1
    weeks = float(m.group("weeks") or 0)
    days = float(m.group("days") or 0)
    hours = float(m.group("hours") or 0)
    minutes = float(m.group("minutes") or 0)
    total_days = weeks * 7 + days + hours / 24.0 + minutes / 1440.0
    return sign * total_days


def quantity(value, unit):
    return {"value": value, "system": "http://unitsofmeasure.org", "code": unit}


def slugify(text):
    text = re.sub(r"[^A-Za-z0-9]+", "-", text.strip())
    return re.sub(r"-+", "-", text).strip("-")


def load_usdm(path):
    with open(path) as f:
        return json.load(f)


def get_study_design(usdm):
    version = usdm["study"]["versions"][0]
    return version, version["studyDesigns"][0]


def index_by_id(items):
    return {item["id"]: item for item in items}


def find_main_timeline(study_design):
    for tl in study_design["scheduleTimelines"]:
        if tl.get("mainTimeline"):
            return tl
    raise ValueError("No main schedule timeline found")


def walk_instance_order(timeline, instances_by_id):
    """Follow entryId -> defaultConditionId to get true visit order,
    skipping ScheduledDecisionInstance nodes (no encounterId)."""
    order = []
    seen = set()
    current_id = timeline["entryId"]
    while current_id and current_id not in seen:
        seen.add(current_id)
        inst = instances_by_id.get(current_id)
        if inst is None:
            break
        if inst.get("instanceType") == "ScheduledActivityInstance" and inst.get(
            "encounterId"
        ):
            order.append(inst)
        current_id = inst.get("defaultConditionId")
    return order


def build_offset_range_extension(offset_days, window_lower, window_upper):
    if window_lower is None and window_upper is None:
        return None
    lower_days = abs(parse_iso_duration_to_days(window_lower)) if window_lower else 0.0
    upper_days = abs(parse_iso_duration_to_days(window_upper)) if window_upper else 0.0
    low = offset_days - lower_days
    high = offset_days + upper_days
    return {
        "url": OFFSET_RANGE_EXT_URL,
        "valueRange": {
            "low": quantity(round(low, 3), "d"),
            "high": quantity(round(high, 3), "d"),
        },
    }


def build_related_action(anchor_action_id, timing):
    """Build PlanDefinition.action.relatedAction for a visit relative
    to its anchor, or return None if this visit IS the anchor
    (Timing.type == 'Fixed Reference', matching the IG's
    Index-Activity-Event pattern where the anchor visit has no
    relatedAction at all)."""
    type_decode = timing["type"]["decode"]
    if type_decode == "Fixed Reference":
        return None

    relationship = {"Before": "before", "After": "after"}.get(type_decode)
    if relationship is None:
        raise ValueError(f"Unrecognized timing type: {type_decode!r}")

    offset_days = abs(parse_iso_duration_to_days(timing["value"]))

    related_action = {
        "actionId": anchor_action_id,
        "relationship": relationship,
        "offsetDuration": quantity(round(offset_days, 3), "d"),
    }
    ext = build_offset_range_extension(
        offset_days, timing.get("windowLower"), timing.get("windowUpper")
    )
    if ext:
        related_action["extension"] = [ext]
    return related_action


def build_activity_definition(protocol_slug, activity, bc_by_id):
    slug = f"{protocol_slug}-Activity-{slugify(activity['name'])}"
    resource = {
        "resourceType": "ActivityDefinition",
        "id": slug,
        "meta": {"profile": [f"{IG_BASE}/StructureDefinition/StudyActivitySoa"]},
        "url": f"{IG_BASE}/ActivityDefinition/{slug}",
        "identifier": [{"use": "usual", "value": activity["name"]}],
        "name": slugify(activity["name"]).replace("-", "_"),
        "title": activity.get("label") or activity["name"],
        "status": "active",
        "description": activity.get("description")
        or activity.get("label")
        or activity["name"],
    }

    bc_ids = activity.get("biomedicalConceptIds") or []
    if bc_ids:
        bc = bc_by_id.get(bc_ids[0])
        if bc and bc.get("reference"):
            code_value = bc["reference"].rsplit("/", 1)[-1]
            resource["code"] = {
                "coding": [
                    {
                        "system": "https://ncithesaurus.nci.nih.gov",
                        "code": code_value,
                        "display": bc.get("name") or activity["name"],
                    }
                ],
                "text": bc.get("name") or activity["name"],
            }
    return resource


def build_visit_plan_definition(
    protocol_slug, instance, encounter, activities_by_id, activity_def_ids
):
    action_slug = f"{protocol_slug}-Study-Visit-{slugify(instance['name'])}"
    display_label = instance.get("label") or encounter.get("label") or instance["name"]
    display_description = (
        instance.get("description") or encounter.get("description") or display_label
    )

    actions = []
    seen_activity_ids = set()
    duplicate_count = 0
    for activity_id in instance.get("activityIds", []):
        if activity_id in seen_activity_ids:
            duplicate_count += 1
            continue
        seen_activity_ids.add(activity_id)
        activity = activities_by_id.get(activity_id)
        if activity is None:
            continue
        activity_def_id = activity_def_ids[activity_id]
        actions.append(
            {
                "title": activity.get("label") or activity["name"],
                "definitionUri": f"ActivityDefinition/{activity_def_id}",
            }
        )

    return (
        {
            "resourceType": "PlanDefinition",
            "id": action_slug,
            "meta": {"profile": [f"{IG_BASE}/StructureDefinition/StudyVisitSoa"]},
            "url": f"{IG_BASE}/PlanDefinition/{action_slug}",
            "identifier": [{"use": "usual", "value": action_slug}],
            "version": "1.0.0",
            "title": f"{protocol_slug} {display_label}",
            "type": {
                "coding": [
                    {
                        "system": "http://terminology.hl7.org/CodeSystem/plan-definition-type",
                        "code": "clinical-protocol",
                    }
                ]
            },
            "status": "active",
            "description": display_description,
            "action": actions,
        },
        action_slug,
        duplicate_count,
    )


def main(usdm_path, output_path):
    usdm = load_usdm(usdm_path)
    version, study_design = get_study_design(usdm)
    protocol_slug = study_design["name"]
    study_name = f"{protocol_slug} {usdm['study'].get('label') or ''}".strip()

    encounters_by_id = index_by_id(study_design["encounters"])
    activities_by_id = index_by_id(study_design["activities"])
    bc_by_id = index_by_id(version.get("biomedicalConcepts", []))

    main_timeline = find_main_timeline(study_design)
    instances_by_id = index_by_id(main_timeline["instances"])
    timings_by_from_id = {
        t["relativeFromScheduledInstanceId"]: t for t in main_timeline["timings"]
    }

    ordered_visits = walk_instance_order(main_timeline, instances_by_id)

    # Pass 1: assign each visit its FHIR action id, so relatedAction
    # references (which point at another visit's action id) resolve
    # regardless of iteration order.
    visit_action_ids = {}
    for inst in ordered_visits:
        visit_action_ids[inst["id"]] = f"Study-Visit-{slugify(inst['name'])}"

    # Pass 2: build the top-level protocol actions with relatedAction
    # offsets/windows, and the per-visit PlanDefinitions.
    protocol_actions = []
    visit_plan_definitions = []
    activity_def_ids = {
        activity_id: f"{protocol_slug}-Activity-{slugify(activities_by_id[activity_id]['name'])}"
        for activity_id in {
            aid for inst in ordered_visits for aid in inst.get("activityIds", [])
        }
    }

    total_duplicate_activities = 0
    for inst in ordered_visits:
        encounter = encounters_by_id[inst["encounterId"]]
        action_id = visit_action_ids[inst["id"]]
        visit_pd, visit_pd_id, dup_count = build_visit_plan_definition(
            protocol_slug, inst, encounter, activities_by_id, activity_def_ids
        )
        visit_plan_definitions.append(visit_pd)
        total_duplicate_activities += dup_count

        display_label = inst.get("label") or encounter.get("label") or inst["name"]
        protocol_action = {
            "id": action_id,
            "title": display_label,
            "description": f"Planned Visit [{display_label}]",
            "definitionUri": f"PlanDefinition/{visit_pd_id}",
        }

        timing = timings_by_from_id.get(inst["id"])
        if timing is not None:
            anchor_inst_id = timing["relativeToScheduledInstanceId"]
            anchor_action_id = visit_action_ids.get(anchor_inst_id)
            if anchor_action_id and anchor_action_id != action_id:
                related_action = build_related_action(anchor_action_id, timing)
                if related_action is not None:
                    protocol_action["relatedAction"] = [related_action]
        protocol_actions.append(protocol_action)

    protocol_plan_definition = {
        "resourceType": "PlanDefinition",
        "id": f"{protocol_slug}-ProtocolDesign",
        "meta": {"profile": [f"{IG_BASE}/StructureDefinition/StudyProtocolSoa"]},
        "url": f"{IG_BASE}/PlanDefinition/{protocol_slug}-ProtocolDesign",
        "identifier": [{"use": "usual", "value": f"{protocol_slug}-ProtocolDesign-1"}],
        "version": "1.0.0",
        "title": f"{study_name} Protocol Schedule of Activities",
        "type": {
            "coding": [
                {
                    "system": "http://terminology.hl7.org/CodeSystem/plan-definition-type",
                    "code": "clinical-protocol",
                }
            ]
        },
        "status": "active",
        "purpose": "The purpose of this PlanDefinition is to illustrate the planned study encounters.",
        "action": protocol_actions,
    }

    activity_definitions = [
        build_activity_definition(protocol_slug, activities_by_id[aid], bc_by_id)
        for aid in activity_def_ids
    ]

    all_resources = (
        [protocol_plan_definition] + visit_plan_definitions + activity_definitions
    )
    bundle = {
        "resourceType": "Bundle",
        "type": "collection",
        "entry": [{"fullUrl": r["url"], "resource": r} for r in all_resources],
    }

    Path(output_path).parent.mkdir(parents=True, exist_ok=True)
    with open(output_path, "w") as f:
        json.dump(bundle, f, indent=2)

    print(f"Study: {study_name}")
    print(f"Protocol slug: {protocol_slug}")
    print(
        f"Main timeline: {main_timeline['name']} ({len(ordered_visits)} visits walked)"
    )
    print(f"Generated {len(all_resources)} resources:")
    print(
        f"  - 1 top-level PlanDefinition (StudyProtocolSoa): {protocol_plan_definition['id']}"
    )
    print(
        f"  - {len(visit_plan_definitions)} per-visit PlanDefinitions (StudyVisitSoa)"
    )
    print(f"  - {len(activity_definitions)} ActivityDefinitions (StudyActivitySoa)")
    coded = sum(1 for a in activity_definitions if "code" in a)
    print(
        f"    ({coded}/{len(activity_definitions)} carry a code, from linked BiomedicalConcepts)"
    )
    if total_duplicate_activities:
        print(
            f"NOTE: skipped {total_duplicate_activities} duplicate activity "
            f"references found in the source USDM data (several instances "
            f"list every activityId twice) -- deduplicated per visit so "
            f"each activity appears once in its PlanDefinition"
        )
    other_timelines = [
        tl["name"]
        for tl in study_design["scheduleTimelines"]
        if not tl.get("mainTimeline")
    ]
    if other_timelines:
        print(f"NOT mapped (out of scope): {', '.join(other_timelines)}")
    print(f"\nWrote: {output_path}")


if __name__ == "__main__":
    usdm_path = (
        sys.argv[1]
        if len(sys.argv) > 1
        else (
            "/Users/dmoreland/projects/soa-workbench/output/json/NCT12345678-latest.json"
        )
    )
    output_path = (
        sys.argv[2]
        if len(sys.argv) > 2
        else (
            "/Users/dmoreland/projects/soa-workbench/output/fhir/NCT12345678-vulcan-soa-latest.json"
        )
    )
    main(usdm_path, output_path)
