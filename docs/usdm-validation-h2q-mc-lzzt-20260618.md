# USDM Validation Report: H2Q-MC-LZZT-20260618T1509.json

**File:** `output/json/H2Q-MC-LZZT-20260618T1509.json`  
**USDM Version:** 4.0  
**Generator:** SOA Workbench v1.4.0  
**Validated:** 2026-06-23  
**Overall Confidence:** 73%

---

## Entity Class Inventory

47 entity classes present across ~7,074 total instances. Three expected classes are absent:

| Missing Class | Severity | Notes |
|---|---|---|
| `EligibilityCriterion` | Critical | Required by USDM-IG §4.18 |
| `EligibilityCriterionItem` | Critical | Text carrier for criteria |
| `ScheduleTimelineExit` | Major | Required exit markers per §4.14 |
| `BiomedicalConceptCategory` | Minor | Grouping only; optional |
| `Range` | Minor | Needed for `plannedAge` |

---

## Critical Findings (4)

**C-01 · Eligibility criteria missing**
`InterventionalStudyDesign.eligibilityCriteria = []` and
`StudyVersion.eligibilityCriterionItems = []`. No eligibility criterion text exists
anywhere in the export. USDM-IG §4.18 requires each criterion to be referenced from
the study design population or its cohorts.

**C-02 · Dangling foreign key on ScheduledDecisionInstance_2**
`defaultConditionId = "blahblahblah"` — no entity with that ID exists across all 27
scheduled instances in the four timelines. The SDI also has zero `conditionAssignments`,
making it an inoperable decision node. This is a placeholder that was not cleaned up
before export.

**C-03 · Study.id is null**
The root `Study` entity carries no identifier. USDM-IG §4.4 requires a UUID for global
uniqueness. All other entity IDs follow `{EntityName}_N` correctly.

**C-04 · EligibilityCriterion / EligibilityCriterionItem entity classes entirely absent**
Consequence of C-01 — listed separately because the entity classes themselves are missing
from the output, not just empty arrays.

---

## Major Findings (6)

**M-01 · No ScheduleTimelineExit on any timeline**
All four `ScheduleTimeline.exits[] = []`. §4.14 requires exit markers linked from the
last SAI of each timeline. Zero SAIs have `timelineExitId` set.

**M-02 · 7 of 40 activities (17.5%) linked to no ScheduledActivityInstance**
`Activity_34–40`: Adverse Events, Check Adverse Events, Patient Summary, Supine,
Vital Signs Supine, Stand, Vital Signs Standing — all orphaned with no timeline
assignment.

**M-03 · studyType, studyPhase, blindingSchema all null**
For a randomized, double-blind, parallel 3-arm trial these are the primary
characterizing fields per §4.8.

**M-04 · Organization_2 (ClinicalTrials.gov) has wrong type code**
Code C142578 = "Independent Data Monitoring Committee". Should be the clinical study
registry type code (C93453 or equivalent) per §4.7.

**M-05 · StudyDesignPopulation missing criterionIds, plannedEnrollmentNumber, plannedAge**
Planned enrollment of ~300 is in the description string only. No `Range` entity encodes
an age window.

**M-06 · StudyVersion.dateValues empty**
At least one approval/effective date at the version level is expected per §4.6. A single
`GovernanceDate` entity exists elsewhere in the document but is not wired here.

---

## Minor Findings (5)

| ID | Finding |
|---|---|
| N-01 | No `BiomedicalConceptCategory` groupings for 168 BCs (§4.13 recommends groupings: Vital Signs, Labs, Efficacy, Safety) |
| N-02 | `Range` entity absent entirely — direct consequence of M-05 |
| N-03 | `StudyVersion.rationale` and `StudyDesign.rationale` are empty strings `""` |
| N-04 | `StudyDesign.name = "Darren"` — developer placeholder; requires a meaningful value |
| N-05 | `abbreviations`, `narrativeContentItems`, `businessTherapeuticAreas` empty (all optional per schema) |

---

## Passing Checks (9)

| Check | Result |
|---|---|
| Wrapper shape (`usdmVersion`, `systemName`, `systemVersion`) | Pass |
| StudyCell matrix | Pass — perfect 3 arms × 5 epochs = 15 cells |
| Cross-reference integrity | Pass — zero dangling IDs (except C-02) |
| `mainTimeline` flag | Pass — exactly 1 of 4 timelines carries it |
| ScheduleTimeline `entryIds` | Pass — all resolve to valid SAI IDs |
| Entity UID format | Pass — all follow `{EntityName}_N` monotonic pattern |
| Global ID uniqueness | Pass — zero duplicates across ~7,074 instances |
| BiomedicalConcept-to-Activity linkage | Pass — 161 of 168 BCs linked |
| Code / AliasCode coverage | Pass — 1,276 codes, 994 aliases |

---

## Confidence Breakdown

| Dimension | Score | Notes |
|---|---|---|
| Schema validity | 95% | OpenAPI schema passes; no type/enum violations |
| Entity class completeness | 78% | 47/50 classes present; 3 missing (2 critical) |
| Cross-reference integrity | 90% | 1 dangling FK (C-02) in ~333 references |
| SoA schedule completeness | 82% | 7 orphan activities, no timeline exits |
| Regulatory metadata | 55% | Study.id null, eligibility absent, studyType null |
| **Overall** | **73%** | |

The file is safe for internal development and SoA review. It is **not ready for
regulatory submission** without resolving C-01 through C-04.

---

## Remediation Notes (Future Work)

All fixes are generator-side in `src/usdm/`:

| Priority | Generator | Fix |
|---|---|---|
| 1 | `generate_studydesign.py` | Wire `eligibilityCriteria` from DB |
| 1 | `generate_studyversion.py` | Wire `eligibilityCriterionItems`; assign UUID to `Study.id` |
| 1 | `generate_schedule_timeline.py` | Fix `ScheduledDecisionInstance_2.defaultConditionId` |
| 2 | `generate_schedule_timeline.py` | Add `ScheduleTimelineExit` per timeline |
| 2 | SoA DB / activities | Investigate 7 orphaned activities |
| 2 | `generate_studydesign.py` | Populate `studyType`, `studyPhase`, `blindingSchema` |
| 2 | `generate_organizations.py` | Correct ClinicalTrials.gov type code to C93453 |
| 2 | `generate_studydesign_population.py` | Add `plannedEnrollmentNumber` and `Range` for age |
| 3 | `StudyDesign.name` | Replace "Darren" placeholder |
| 3 | `rationale` fields | Populate from protocol synopsis |
