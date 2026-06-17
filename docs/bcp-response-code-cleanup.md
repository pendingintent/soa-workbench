# BiomedicalConceptProperty Response Code Cleanup

## Summary

A review of `BiomedicalConcept_74` ("Tumor Identification") in a USDM
export revealed corrupt `bcp_response_code` (RC) data. Investigation
showed the corruption was **systemic across the database**, not isolated
to one concept. This document records the issue, how it was diagnosed,
and the fix that was implemented and applied to production.

## The Issue

Three distinct classes of bad data were found in `bcp_response_code`:

1. **Orphaned response codes** — RC rows whose parent
   `biomedical_concept_property` (BCP) or `biomedical_concept` (BC) no
   longer existed. Present in every populated SOA (≈153 rows total; e.g.
   soa 60 had 63 of 142 RCs orphaned).
2. **Duplicate response codes** — a single live property carrying multiple
   RCs (e.g. `TUORRES` with 2, `LBFAST` with 4) where the generator emits
   at most one per variable.
3. **Semantically wrong response codes** — codes unrelated to the property,
   such as `TULOC` (tumor location) → `RANDOM` and an "Arm Span" concept →
   `INFORMED CONSENT OBTAINED`. These carried `code_system_version` values
   that differed from the rest of the concept (e.g. `2025-07-01` vs
   `2026-05-26`), a tell-tale sign of data accreted across multiple
   populate runs.

The net effect was invalid response codes appearing in USDM exports.

## Investigation

1. **Traced the export back to source.** Confirmed `BiomedicalConcept_74`
   (soa 60) is populated through the SDTM specialization path
   (`TUMERGE`), where each variable's `assignedTerm` yields a single
   response code. Two RCs on one property therefore could not have come
   from the documented path.
2. **Queried the database directly.** Counting orphaned, duplicate, and
   mixed-version RCs across all SOAs showed the problem was widespread,
   not a one-off.
3. **Identified the entry points.** Reading the deletion, re-population,
   and import code paths revealed three root causes:
   - **BC/activity-concept deletion did not cascade** to
     `biomedical_concept_property` / `bcp_response_code`. The freeze
     rebuild and the orphaned-concept cleanup deleted (and later
     re-created) BCs with new UIDs, leaving the old BCP/RC rows behind as
     orphans.
   - **The SOA bundle import copied BCP/RC rows verbatim** with no
     referential-integrity check or de-duplication, so a corrupt source
     bundle propagated into every imported study (soa 60 was imported).
   - **No orphan sweep existed.** The existing per-BC delete only removed
     RCs reachable from a BC's current properties, so accumulated orphans
     were never reclaimed.

## The Fix

### Code changes (prevent recurrence)

- **Cascade helpers** in
  `src/usdm/generate_biomedical_concept_properties.py`:
  - `delete_bc_cascade()` — removes a BC's BCP + RC rows and their owned
    code chains.
  - `sweep_orphaned_bcp_rows()` — per-SOA, idempotent removal of orphaned
    RC/BCP rows and their dangling code/alias rows.
- **Cascade on deletion** — BC deletes in `_cleanup_orphaned_concept_rows`
  (`app.py`) and the freeze rebuild (`_freeze_helpers.py`) now remove the
  dependent BCP/RC rows so nothing orphans.
- **Import integrity** — `soa_bundle.py` now drops BCPs whose parent BC is
  absent, drops RCs whose parent BCP is absent, and de-duplicates RCs that
  resolve to the same code on the same property.
- **Startup safety net** — the app lifespan runs the orphan sweep on
  startup.

### Cleanup tooling (remove existing bad data)

`scripts/cleanup_bcp_response_codes.py`:

- Defaults to `--dry-run`; `--apply` performs changes; `--soa-id` scopes
  the run. The default scope is **every SOA that has response codes**, so
  anomalies are never missed because a SOA happens to lack orphans.
- **Step 1** sweeps orphaned rows (offline, always safe).
- **Step 2** re-populates anomalous BCs (those with duplicate RCs or mixed
  code-system versions) from the CDISC Library source, regenerating
  correct, single, version-consistent response codes.

### Tests

`tests/test_bcp_response_code_cleanup.py` covers the orphan sweep
(removes orphans, preserves live rows, idempotent), `delete_bc_cascade`,
and the import filtering/de-duplication.

## Outcome

The cleanup script was validated against copies of the production
database and then applied. After the run, all SOAs report **0 orphaned
response codes, 0 properties with duplicate response codes, and 0
mixed-version concepts**. `BiomedicalConcept_74` now exposes only its
correct response code (`TUORRES` → `TARGET`), and the previously
nonsensical codes (e.g. `TULOC` → `RANDOM`, "Arm Span" → `INFORMED
CONSENT OBTAINED`) are gone. The code changes ensure the corruption
cannot re-accumulate through deletion, re-population, or import.
