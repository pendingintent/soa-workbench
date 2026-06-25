# USDM Validation Report: NCT01797120-20260618T1511.json

**File:** `output/json/NCT01797120-20260618T1511.json`  
**Study:** PrE0102 — Phase II HR+ Metastatic Breast Cancer (Fulvestrant ± Everolimus)  
**USDM Version:** 4.0  
**Generator:** SOA Workbench v1.4.0  
**Validated:** 2026-06-23  
**Overall Confidence:** 52%

This export is a **skeleton SoA** — the study structure is partially defined but the
schedule matrix has no procedural content (no activities linked to any visit instance,
no timing windows). Not suitable for regulatory submission or SDTM derivation.

---

## Entity Class Inventory

35 entity classes present across ~1,286 total instances (vs. ~7,074 for H2Q-MC-LZZT).
Notable: `Estimand` (6) and `IntercurrentEvent` (15) are populated; `StudyAmendment` is not.

| Missing Class | Severity | Notes |
|---|---|---|
| `EligibilityCriterion` | Critical | Required by USDM-IG §4.18 |
| `EligibilityCriterionItem` | Critical | Text carrier for criteria |
| `ScheduleTimelineExit` | Critical | Required exit per §4.14 |
| `StudyAmendment` | Info | None recorded for this version |
| `ScheduledDecisionInstance` | Info | No branching logic in this design |
| `BiomedicalConceptCategory` | Minor | Optional groupings |
| `Range` | Major | Needed for `plannedAge`, `plannedEnrollmentNumber` |

---

## Critical Findings (8)

**C-01 · Study.id is null**
The root `Study` entity carries no identifier. USDM-IG §4.4/§6.3 require a UUID for
global uniqueness and SDR primary key use. All other entity IDs follow `{EntityName}_N`
correctly.

**C-02 · studyType, studyPhase, blindingSchema all null; model Code is empty**
For a randomized, double-blind, parallel 2-arm Phase II trial these are required
characterizing fields per §4.8. Expected values:
- `studyType` → `{code=C93988, decode=INTERVENTIONAL}`
- `studyPhase` → `{code=C15602, decode=PHASE II TRIAL}`
- `blindingSchema` → `{code=C49660, decode=DOUBLE BLIND}`
- `model.code/decode` → `{code=C82639, decode=PARALLEL}`

**C-03 · EligibilityCriteria entirely absent**
`InterventionalStudyDesign.eligibilityCriteria = []`,
`StudyVersion.eligibilityCriterionItems = []`,
`StudyDesignPopulation.eligibilityCriteriaId = null`.
PrE0102 has well-defined inclusion/exclusion criteria. Required for SDTM IE domain
mapping per §4.18 and §7.1.

**C-04 · ScheduleTimelineExit absent**
`ScheduleTimeline_1.exits = []`. Per §4.14 a timeline requires 1..* exits.
Without an exit the timeline is topologically open.

**C-05 · Dangling foreign key: ScheduledActivityInstance_3.defaultConditionId = "ScheduledActivityInstance_4"**
No instance with ID `ScheduledActivityInstance_4` exists in the timeline.
Instance IDs present: 1, 2, 3, 6, 7, 8, 9, 10, 11. The sequence chain breaks at
C1D15 — the next scheduled visit cannot be resolved.

**C-06 · All 9 ScheduledActivityInstances have activityIds = []**
No activities are linked to any visit instance. The entire SoA matrix has topology but
no procedural content. This is the most significant completeness gap in this export.

**C-07 · StudyVersion.dateValues is empty**
No protocol approval or study start date at the version level. Required for SDR
provenance per §4.6.

**C-08 · Encounter_1.scheduledAtId references Timing_1 which does not exist**
`studyDesignTimings = []` at the design level — no `Timing` objects exist — yet
`Encounter_1` references one. This is a dangling FK at the schedule entry point.

---

## Major Findings (10)

**M-01 · All 12 StudyRole instances have organizationId=null and personId=null**
Role codes are present but the links to organizations and persons are broken. Breaks
§6.3 API Required Content rule (sponsor role must scope the sponsor identifier).

**M-02 · StudyDesignPopulation has no populated fields**
`description`, `plannedEnrollmentNumber`, `plannedAge`, `plannedSex=[]`,
`criterionIds=[]` all null/empty. PrE0102 targets ~120 post-menopausal women.

**M-03 · StudyEpoch_4 (Continuation Phase) has no StudyCells**
2 arms × 4 epochs = 8 cells required; only 6 exist. The Continuation Phase epoch is
referenced in the epoch chain but excluded from the StudyCell matrix.

**M-04 · studyDesignTimings = [] — no Timing objects at design level**
No visit days, cycle lengths, or windows encoded in the schedule. (One `Timing`
instance exists elsewhere but is not wired into the design timings list.)

**M-05 · All 4 StudyElement.studyInterventionIds = []**
Treatment elements not linked to interventions; SDTM TE domain generation will fail.

**M-06 · All 6 Estimand.treatmentId = null; both StudyIntervention.codes = []**
The treatment being estimated is unspecified. No ATC/UNII codes for Fulvestrant or
Everolimus.

**M-07 · All 14 BiomedicalConceptSurrogate have bcId=null and activityId=null**
Surrogates are defined but all reverse links are broken.

**M-08 · All 5 Organization.identifier and identifierScheme are empty strings**
Per §4.9/§6.3, organizations must carry DUNS or equivalent identifiers.

**M-09 · ScheduledActivityInstance_10 ("RANDOMIZE") has encounterId=null**
The randomization step cannot be placed in the SoA timeline.

**M-10 · 16 of 22 activities (73%) have no BC or BCSurrogate assignment**
50 BiomedicalConcepts are defined in the export but most are not linked back to
activities.

---

## Minor Findings (5)

| ID | Finding |
|---|---|
| N-01 | `Objective.text` empty for both objectives; `Endpoint.text` empty for all 6 endpoints |
| N-02 | All 6 `Estimand.summaryMeasure = null` (e.g., "Hazard Ratio", "Median PFS") |
| N-03 | Some `Code.decode` fields equal the bare code string rather than the CT preferred term |
| N-04 | `ScheduleTimeline.entryCondition=null`, `plannedDuration=null` |
| N-05 | Typo: `Encounter_8.description = "Foolow-up"` instead of "Follow-up" |

---

## Passing Checks (9)

| Check | Result |
|---|---|
| Wrapper shape (`usdmVersion="4.0"`, `systemName`, `systemVersion`) | Pass |
| `instanceType` on all entities | Pass |
| StudyCell cross-references (armId, epochId) | Pass — all 6 resolve |
| StudyArm type codes (CDISC CT) | Pass |
| StudyEpoch codes (SDTM CT) | Pass |
| Organization type codes | Pass |
| Indication coded with NCIt | Pass |
| Encounter linked-list chain | Pass — all 8 encounters linked |
| StudyEpoch chain | Pass — 4 epochs linked (order: 1→2→4→3) |

---

## Confidence Breakdown

| Dimension | Score | Notes |
|---|---|---|
| Schema validity | 65% | Null required fields, empty model Code, dangling FK |
| Entity class completeness | 83% | 35/42 expected classes; 3 critical missing |
| Cross-reference integrity | 72% | SAI_3→SAI_4 broken; Encounter_1→Timing_1 broken; all StudyRoles unlinked |
| SoA schedule completeness | 25% | All 22 activities orphaned; no Timing objects wired |
| Regulatory metadata | 30% | Study.id null; eligibility absent; studyType/phase/blinding null; dateValues empty |
| **Overall** | **52%** | |

---

## Comparison with H2Q-MC-LZZT (same generator version)

| Dimension | H2Q-MC-LZZT | NCT01797120 |
|---|---|---|
| Overall confidence | 73% | 52% |
| Activities linked to SAIs | 33/40 (82%) | 0/22 (0%) |
| Timing objects | 24 | 0 (wired) |
| studyType/studyPhase/blinding | null | null |
| Study.id | null | null |
| Eligibility criteria | absent | absent |
| ScheduleTimelineExit | absent | absent |
| studyDesign.name | "Darren" (placeholder) | valid |
| Dangling FK | SDI_2 defaultConditionId | SAI_3 defaultConditionId |

The `Study.id` null, eligibility absent, `studyType`/`studyPhase` null, and
`ScheduleTimelineExit` absent appear to be **systemic generator-level gaps** common
to both exports. The zero activity-to-instance linkage in this export is an additional
data-entry gap specific to this SoA.

---

## Remediation Notes (Future Work)

All systemic fixes shared with H2Q-MC-LZZT are already logged in
`docs/usdm-validation-h2q-mc-lzzt-20260618.md`. Additional items specific to this study:

| Priority | Fix |
|---|---|
| 1 | Wire `activityIds` on all `ScheduledActivityInstance` entries |
| 1 | Fix `ScheduledActivityInstance_3.defaultConditionId` (SAI_4 does not exist) |
| 1 | Add `Timing` objects and wire to `studyDesignTimings` and `Encounter.scheduledAtId` |
| 2 | Add `StudyCell` for `StudyEpoch_4` (Continuation Phase) × both arms |
| 2 | Populate `StudyRole.organizationId` and `personId` |
| 2 | Add `Organization.identifier` / `identifierScheme` (DUNS/ROR) |
| 2 | Link `StudyElement.studyInterventionIds` → `StudyIntervention` |
| 2 | Set `Estimand.treatmentId`; add ATC/UNII codes to `StudyIntervention.codes` |
| 2 | Populate `Objective.text` and `Endpoint.text` |
| 3 | Fix typo `Encounter_8.description` ("Foolow-up") |
| 3 | Set `Estimand.summaryMeasure` for all 6 estimands |
