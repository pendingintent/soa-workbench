# Define-JSON Generation in the SoA Workbench

This document explains how the workbench produces a Define-JSON document
(a JSON serialization of CDISC Define-XML 2.1 / ODM), how it uses the
CDISC Library API to do so, and which Define-XML classes are not yet
generated together with what would be required to complete them.

## What Define-JSON is

Define-JSON is the workbench's representation of dataset-level metadata
(Define-XML v2.1, `defineVersion": "2.1.0"`). The top-level document
contains these sections:

| Section | Meaning |
|---|---|
| `itemGroups` | Datasets (domains) and their variables (ItemDefs) |
| `codeLists` | Controlled terminology codelists and terms |
| `whereClauses` | Value-level metadata (VLM) selection criteria |
| `conditions` | VLM range-check conditions referenced by where clauses |
| `standards` | The SDTM IG and CT packages the define is built against |
| `methods` | Computational/derivation method definitions |
| `comments` | Free-text comments attached to items/datasets |
| `annotatedCRF` | References to the annotated CRF document |
| `concepts` / `conceptProperties` | Embedded CDISC Biomedical Concepts |

## Generation pipeline

### Entry points

- **Web download route**:
  [`GET /soa/{soa_id}/usdm_json/define_json`](src/soa_builder/web/routers/usdm_json.py#L157)
  (`download_define_json`), with a UI page at
  [`/ui/soa/{soa_id}/define_json`](src/soa_builder/web/routers/usdm_json.py#L132).
- **MCP tool**: `get_define_json`
  ([server.py](src/soa_builder/mcp/server.py#L710)).

Both require an **`sdtmct`** argument (the SDTM Controlled Terminology
package date, `yyyy-mm-dd`) and accept an optional **`sdtmig`** version
(default `3.4`). The UI pre-fills `sdtmct` from the most recent CT package
slug.

### Flow

1. The route calls
   [`build_define_json(soa_id, sdtmct, sdtmig, …)`](src/usdm/generate_define_json.py#L10).
2. `build_define_json` first calls `build_usdm(soa_id)` to materialize the
   study's **USDM JSON** from the database (this is the same USDM export
   used elsewhere, including the BiomedicalConcept/BiomedicalConceptProperty
   data). The USDM JSON is written to a temp file.
3. It then constructs
   [`USDMDefineJSONProcessor`](src/usdm/create_define_json.py#L71) and calls
   `process()`.
4. `process()` returns the assembled `template` dict, which the route
   streams back as a `define.json` download.

So the chain is: **DB → USDM JSON (`build_usdm`) → Define-JSON
(`USDMDefineJSONProcessor`)**. Define-JSON is derived from the USDM
artifact plus live CDISC Library lookups; it is not stored.

### `process()` orchestration

[`USDMDefineJSONProcessor.process()`](src/usdm/create_define_json.py#L4156)
runs these steps in order:

1. `process_biomedical_concepts()` — read BCs/DSS specializations from the
   USDM data and CDISC Library; seed `datasets_dict`.
2. `build_vlm_lookup()` — build value-level-metadata lookups.
3. `update_datasets_dict()` — merge VLM data into datasets.
4. `_build_global_codelist_terms()` — index codelist terms.
5. `populate_study_elements()` — study/document metadata into `template`.
6. `process_datasets()` — build `itemGroups` (datasets + ItemDefs),
   `whereClauses`, `conditions`.
7. `_update_subset_codelist_names()` — tidy subset codelist names.
8. `add_standards()` — populate `standards`.
9. `save_output()` — assemble the template.

[`save_output()`](src/usdm/create_define_json.py#L3387) explicitly writes
only `itemGroups`, `whereClauses`, `conditions`, and `codeLists`;
`standards` is set by `add_standards()`. The remaining sections
(`methods`, `comments`, `annotatedCRF`, `concepts`, `conceptProperties`)
are left at the empty-list values from the template initializer in
[`__init__`](src/usdm/create_define_json.py#L150).

## How the CDISC Library API is used

The processor talks to the CDISC Library through the third-party
**`cdisc_library_client.CDISCLibraryClient`**
([import](src/usdm/create_define_json.py#L23), constructed in
[`__init__`](src/usdm/create_define_json.py#L133)). Authentication uses the
`cdisc_api_key` argument or, when that is `None` (the web/MCP path always
passes `None`), the **`CDISC_API_KEY`** environment variable.

The client methods used:

| Client call | Purpose | Path / package |
|---|---|---|
| `get_api_json(path)` | Generic CDISC Library GET | see paths below |
| `get_codelist_terms(...)` | Resolve a codelist's terms | CT package |
| `get_biomedicalconcept_latest_datasetspecializations(...)` | Latest DSS for a BC | COSMUS BC |
| `get_sdtm_latest_sdtm_datasetspecialization(...)` | Latest SDTM DSS | COSMUS SDTM |

Representative `get_api_json` paths (all parameterized by `sdtmct` /
`sdtmig`):

- `/mdr/ct/packages/sdtmct-{sdtmct}/codelists/{codelist_id}` — codelist and
  terms, including the hard-referenced No/Yes (`C66738`), `C67152`, and
  `C171445` codelists.
- `/mdr/sdtmig/{sdtmig}/datasets/{dataset}` — SDTM IG dataset variable
  metadata used to complete standard datasets.

Notes and constraints:

- `sdtmct` is **required** and must match a real CT package date; an
  invalid format raises `ValueError`.
- The processor relies on the client's own HTTP/caching behavior. (The
  separate BC export path in
  [`usdm_utils.py`](src/usdm/usdm_utils.py) uses `functools.lru_cache` and
  also supports an `Ocp-Apim-Subscription-Key`; the define processor does
  not add subscription-key handling itself.)
- Generation requires network access and a valid key; without them, code
  list and dataset lookups fail.

## What is generated today

For a representative export the document is well populated for:

- **`itemGroups`** — datasets with ItemDefs carrying `OID`, `name`,
  `role`, `dataType`, `mandatory`, `keySequence`, and an **`origin`**
  (`type` + `source`). `origin.type` is inferred by
  [`_infer_origin`](src/usdm/create_define_json.py#L238) and includes
  `Assigned`, `Collected`, `Protocol`, and `Derived`.
- **`whereClauses` / `conditions`** — value-level metadata.
- **`codeLists`** — pulled from the CDISC Library CT package.
- **`standards`** — SDTM IG + CT entries.

## Gaps and missing classes

Five Define-XML sections are always emitted **empty**, and there are a few
secondary data-quality gaps.

### 1. `methods` (MethodDef) — empty

Variables with `origin.type = "Derived"` are emitted, but no `MethodDef`
exists to describe the derivation, and items carry **no `method` OID
reference** (verified: zero method references across all items). A
compliant Define-XML expects derived variables to reference a method that
describes the computation.

**To complete:** for each derived variable, create a `MethodDef` (OID,
name, type=`Computation`, description, optional formal expression) and add
a `method` reference on the ItemDef. Derivation text would need a source —
either authored in the workbench (a new per-variable "derivation" field)
or sourced from SDTM IG assumptions / sponsor input. `_infer_origin`
already identifies derived variables, so the hook point exists.

### 2. `comments` (CommentDef) — empty

No comments are produced and no items reference comments.

**To complete:** add a `CommentDef` store plus a `comment` reference on
datasets/variables. Requires a workbench field to capture comment text
(e.g. per dataset/variable notes) and OID wiring in `process_datasets()`.

### 3. `annotatedCRF` — empty

No annotated-CRF document leaf or page references are emitted. This is
notable because the workbench now records **CRF specializations** (the
`…/specializations/crf` extension attributes on Biomedical Concepts).

**To complete:** model the annotated CRF document (a `leaf`/document
reference with location + page refs) and, ideally, drive page/where
linkage from the CRF specialization data already attached to BCs. Requires
a CRF document reference (filename/URL + page map) to be captured in the
workbench.

### 4. `concepts` / `conceptProperties` — empty

Define-XML 2.1 can embed CDISC Biomedical Concepts and their properties
(CDISC 360 alignment). The workbench **already holds this data** —
`biomedical_concept` and `biomedical_concept_property` flow into the USDM
JSON consumed by the processor — but it is not copied into the define's
`concepts` / `conceptProperties` sections.

**To complete:** map the USDM `BiomedicalConcept` /
`BiomedicalConceptProperty` objects (already in `self.usdm_data` and used
by `process_biomedical_concepts()`) into Define-JSON `concepts` and
`conceptProperties` entries, and reference them from the relevant
ItemGroups/Items. This is the lowest-effort gap because the source data is
present in-process; it is mostly a mapping/serialization step.

### 5. Secondary data-quality gaps

- **Unknown data types** — [`_convert_data_type`](src/usdm/create_define_json.py#L218)
  emits the literal `"????"` when a SDTM `simpleDatatype` is not `Char`/`Num`
  and the name has no recognized suffix. These should map to a valid
  Define data type.
- **Placeholder key sequences** — [`_derive_key_sequence`](src/usdm/create_define_json.py#L254)
  can emit `["__PLACEHOLDER__"]` when keys cannot be inferred.
- **Variable `length`** — emitted as `null` for many items; Define-XML
  expects a length for character/numeric variables.

## Priority summary

| Missing class | Source data available? | Effort | Notes |
|---|---|---|---|
| `concepts` / `conceptProperties` | Yes (USDM BCs in-process) | Low | Mapping only |
| `annotatedCRF` | Partial (CRF specializations) | Medium | Needs CRF document reference + page map |
| `methods` | No (needs derivation text) | Medium | Hook exists via `_infer_origin`; needs authored derivations |
| `comments` | No (needs comment text) | Medium | Needs a capture field + OID wiring |
| `length` / data-type / key-sequence fixes | Partial | Low–Medium | Data-quality clean-ups |
