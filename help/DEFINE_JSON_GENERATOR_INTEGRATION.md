# Plan: Define-JSON Generator Integration

## Context

The soa-workbench already models *what activities happen at what visits* (the SoA) and exports USDM JSON.
The define-json library models *what datasets/variables those activities produce* (the data contract).
The integration adds a `generate_define_json.py` generator that reads a study's activities and their
Biomedical Concepts from the soa-workbench DB and emits a valid Define-JSON `MetaDataVersion` — enabling
a complete regulatory submission package (USDM + Define) from one tool.

---

## Files Modified

| Action | Path |
|--------|------|
| EDIT | `pyproject.toml` — add `define-json` path dependency |
| CREATE | `src/usdm/generate_define_json.py` — new generator |
| EDIT | `src/soa_builder/web/routers/usdm_json.py` — register new component |
| CREATE | `tests/test_define_json_generator.py` — unit + integration tests |
| CREATE | `docs/define_json_integration.md` — design doc (user requested) |

---

## Step 1 — Add Dependency

In `pyproject.toml`, add to `[tool.poetry.dependencies]`:

```toml
define-json = { path = "../define-json", develop = true }
```

This assumes both repos are siblings on disk (`~/projects/soa-workbench` and `~/projects/define-json`).

---

## Step 2 — Create `src/usdm/generate_define_json.py`

### Data mapping

```
soa table              → MetaDataVersion header (OID, studyOID, studyName)
biomedical_concept     → ItemGroup  (one per BC, OID = "IG.{bc_uid}")
  bc.name              → itemGroup.name
  bc.label             → itemGroup.label
  bc.code (via joins)  → itemGroup.coding  (CDISC code from alias_code → code tables)

biomedical_concept_property → Item  (one per property = one SDTM variable)
  bcp.name             → item.name  (e.g., "VSORRES")
  bcp.label            → item.label
  bcp.datatype         → item.dataType
  bcp.isRequired       → item.mandatory
  bcp.code (via joins) → item.coding  (NCI code from alias_code → code tables)
```

### DB queries needed

```sql
-- 1. Study metadata
SELECT name, study_id, study_label FROM soa WHERE id = ?

-- 2. BCs with their semantic codes (one row per BC)
SELECT
    bc.biomedical_concept_uid, bc.name, bc.label,
    c.code ncit_code, c.decode, c.code_system, c.code_system_version
FROM biomedical_concept bc
JOIN alias_code a ON bc.code = a.alias_code_uid
JOIN code c ON a.standard_code = c.code_uid
WHERE bc.soa_id = ?
ORDER BY bc.id

-- 3. BC properties with semantic codes (all in one query, keyed by bc_uid)
SELECT
    bcp.biomedical_concept_uid parent_bc_uid,
    bcp.biomedical_concept_property_uid, bcp.name, bcp.label,
    bcp.datatype, bcp.isRequired, bcp.isEnabled,
    c.code ncit_code, c.decode, c.code_system, c.code_system_version
FROM biomedical_concept_property bcp
JOIN alias_code a ON bcp.code = a.alias_code_uid
JOIN code c ON a.standard_code = c.code_uid
WHERE bcp.soa_id = ?
ORDER BY bcp.id
```

### Function signature and outline

```python
from datetime import datetime, timezone
from typing import Any, Dict, List
from define_json.schema.define import (
    MetaDataVersion, ItemGroup, Item, Coding
)
from soa_builder.web.db import _connect


def build_define_json(soa_id: int) -> Dict[str, Any]:
    conn = _connect()
    cur = conn.cursor()

    study = _query_study(cur, soa_id)
    bcs = _query_bcs(cur, soa_id)           # list of bc rows
    props = _query_bc_props(cur, soa_id)    # dict: bc_uid → [prop rows]
    conn.close()

    item_groups = [
        _build_item_group(bc, props.get(bc["biomedical_concept_uid"], []))
        for bc in bcs
    ]

    mdv = MetaDataVersion(
        OID=f"MDV.{study['study_id'] or soa_id}.001",
        fileOID=f"FILE.{study['study_id'] or soa_id}.001",
        creationDateTime=datetime.now(timezone.utc),
        odmVersion="1.3.2",
        fileType="Snapshot",
        studyOID=f"STUDY.{study['study_id'] or soa_id}",
        name=study["name"],
        studyName=study["study_label"] or study["name"],
        sourceSystem="SOA Workbench",
        itemGroups=item_groups if item_groups else None,
    )

    return mdv.model_dump(mode="json", exclude_none=True)


def _build_item_group(bc: Dict, props: List[Dict]) -> ItemGroup:
    coding = [Coding(
        code=bc["ncit_code"],
        codeSystem=bc["code_system"],
        codeSystemVersion=bc["code_system_version"],
        decode=bc["decode"],
    )] if bc.get("ncit_code") else None

    items = [_build_item(p) for p in props if p["isEnabled"]]

    return ItemGroup(
        OID=f"IG.{bc['biomedical_concept_uid']}",
        name=bc["name"],
        label=bc.get("label"),
        type="DatasetSpecialization",
        coding=coding,
        items=items if items else None,
    )


def _build_item(prop: Dict) -> Item:
    coding = [Coding(
        code=prop["ncit_code"],
        codeSystem=prop["code_system"],
        codeSystemVersion=prop["code_system_version"],
        decode=prop["decode"],
    )] if prop.get("ncit_code") else None

    return Item(
        OID=f"IT.{prop['biomedical_concept_property_uid']}",
        name=prop["name"],
        label=prop.get("label"),
        dataType=prop["datatype"] or "text",
        mandatory=bool(prop["isRequired"]),
        coding=coding,
    )
```

**Guard**: If `biomedical_concept_property` rows have not yet been populated for a SOA
(they are lazily upserted by `populate_biomedical_concept_properties()`), call it first —
same pattern as `generate_biomedical_concepts.py`.

---

## Step 3 — Register in Router

File: `src/soa_builder/web/routers/usdm_json.py`

1. Add to `_COMPONENTS` tuple list:
   ```python
   ("define_json", "Define-JSON", "define.json"),
   ```

2. Add case to `_build()` dispatch function:
   ```python
   if component == "define_json":
       from usdm.generate_define_json import build_define_json
       return build_define_json(soa_id)
   ```

This makes the export available at `GET /soa/{soa_id}/usdm_json/define_json`.

---

## Step 4 — Tests (`tests/test_define_json_generator.py`)

```python
# Test 1: SOA with no BCs returns a valid minimal MetaDataVersion dict
# Test 2: SOA with BCs but no populated properties still returns valid output
# Test 3: SOA with BCs + properties → ItemGroups contain correct Items
# Test 4: Coding OIDs in output match code table values from DB
# Test 5: model_dump output is JSON-serialisable (json.dumps() round-trip)
# Test 6: Route GET /soa/{soa_id}/usdm_json/define_json returns 200
```

Use the existing TestClient + test DB fixture pattern from `tests/test_routers_*.py`.

---

## Step 5 — Documentation (`docs/define_json_integration.md`)

Write a concise integration guide covering:
- Why this exists (demand contract use case)
- The data mapping table (BC → ItemGroup, BCProperty → Item)
- How to invoke the endpoint
- How to extend (adding WhereClauses for vertical domains in future)
- Dependency install note

---

## Verification

```bash
# 1. Install dependency
pip install -e ../define-json

# 2. Run tests
pytest tests/test_define_json_generator.py -v

# 3. Manual smoke test (replace 1 with a real soa_id)
SOA_BUILDER_DB=soa_builder_web_tests.db python -c "
from usdm.generate_define_json import build_define_json
import json; print(json.dumps(build_define_json(1), indent=2)[:500])
"

# 4. Via API
curl http://localhost:8000/soa/1/usdm_json/define_json
```

---

## Known Limitations (future work, not in scope)

- Does not yet group ItemGroups by SDTM domain (e.g., all VS BCs → one IG.VS table)
- Does not yet emit WhereClauses for vertical SDTM structures (e.g., one WhereClause per VSTESTCD)
- Does not yet include epoch/visit structure as SDMX DataStructureDefinition
