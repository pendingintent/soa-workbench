"""Export a SoA as a self-contained JSON bundle and import it back.

A bundle carries every piece of study content for one SoA, so it can
be moved to another workbench or duplicated. It never carries audit
history or freezes: those describe how this copy of the study evolved,
not the study itself.

Every table with a ``soa_id`` column must be in ``_EXPORT_TABLES``,
in ``_NEVER_EXPORTED``, or end in ``_AUDIT_SUFFIX``;
``tests/test_soa_bundle_policy.py`` enforces this, and also that every
integer ``*_id`` column of an exported table is remapped on import.
"""

import io
import json
import logging
import sqlite3
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Tuple

from fastapi import APIRouter, File, Form, HTTPException, UploadFile
from fastapi.responses import JSONResponse, RedirectResponse, StreamingResponse

from ..db import _connect

router = APIRouter()
ui_router = APIRouter()
logger = logging.getLogger("soa_builder.web.routers.soa_bundle")

_FORMAT_VERSION = "1.0"

# Every soa_id-scoped table a bundle carries, in import order: a table
# whose integer ids are remapped (see _ID_REMAPS) comes after its parent.
_EXPORT_TABLES = [
    "code",
    "alias_code",
    "code_association",
    "epoch",
    "arm",
    "timing",
    "visit",
    "activity",
    "element",
    "study_cell",
    "schedule_timelines",
    "instances",
    "decision_instances",
    "condition_assignment",
    "transition_rule",
    "matrix_cells",
    "biomedical_concept",
    "biomedical_concept_property",
    "bcp_response_code",
    "biomedical_concept_surrogate",
    "activity_concept",
    "activity_concept_dss",
    "activity_concept_crf",
    "activity_surrogate",
    "activity_grouping_extension",
    "objective",
    "endpoint",
    "study_intervention",
    "study_intervention_code",
    "element_intervention",
    "estimand",
    "estimand_intervention",
    "estimand_variable",
    "intercurrent_event",
    "indication",
    "indication_code",
    "eligibility_criterion",
    "study_identifier",
    "study_title",
    "organization",
    "role",
    "person",
    "role_person",
    "footnote",
    "study_amendment",
    "study_amendment_reason",
    "study_amendment_impact",
    "study_change",
    "document_content_reference",
    "amendment_geographic_scope",
    "amendment_governance_date",
    "governance_date_geographic_scope",
    "amendment_subject_enrollment",
    "soa_tool_extension",
    "uid_counter",
]

# soa_id-scoped tables a bundle must never carry.
_NEVER_EXPORTED = {
    "soa_freeze": "freeze snapshots are version history, not study content",
    "wizard_state": "UI onboarding progress of this copy of the study",
}
# Every ``*_audit`` table is never exported either: audit history
# belongs to the workbench copy it was recorded in.
_AUDIT_SUFFIX = "_audit"

# Columns stripped from every exported row. freeze_id points into
# soa_freeze, which is never exported.
_STRIPPED_COLUMNS = frozenset({"freeze_id"})

# Values set on import for NOT NULL columns stripped from the export.
# freeze_id 0 means "imported, no associated freeze".
_IMPORT_DEFAULTS: Dict[str, Dict[str, Any]] = {
    "study_amendment": {"freeze_id": 0},
}

# soa row columns that describe the database row, not the study.
_EXCLUDED_SOA_COLUMNS = {
    "id": "assigned by the importing database",
    "created_at": "set to the import time",
    "migrated_bcp_truncate": "one-off migration bookkeeping",
}

# What to do with a row whose referenced parent id is not in the bundle.
_DROP = "drop"  # the row is meaningless without its parent
_NULL = "null"  # the reference is optional; keep the row, clear it

# Columns that hold another exported table's integer row id. Row ids
# are reassigned on import, so these are rewritten to the new ids:
# {table: {column: (parent_table, on_missing)}}.
_ID_REMAPS: Dict[str, Dict[str, Tuple[str, str]]] = {
    "visit": {
        "epoch_id": ("epoch", _NULL),
        # TEXT column holding timing.id
        "scheduledAtId": ("timing", _NULL),
    },
    "matrix_cells": {
        "visit_id": ("visit", _DROP),
        "activity_id": ("activity", _DROP),
        "instance_id": ("instances", _DROP),
    },
    "activity_concept": {"activity_id": ("activity", _DROP)},
    "activity_concept_dss": {"activity_id": ("activity", _DROP)},
    "activity_concept_crf": {"activity_id": ("activity", _DROP)},
    "study_intervention_code": {
        "intervention_id": ("study_intervention", _DROP),
    },
    "element_intervention": {"element_id": ("element", _DROP)},
    "estimand_intervention": {"estimand_id": ("estimand", _DROP)},
    "estimand_variable": {"estimand_id": ("estimand", _DROP)},
    "intercurrent_event": {"estimand_id": ("estimand", _DROP)},
    "indication_code": {"indication_id": ("indication", _DROP)},
    "role_person": {
        "role_id": ("role", _DROP),
        "person_id": ("person", _DROP),
    },
}


def _table_exists(cur, table: str) -> bool:
    cur.execute(
        "SELECT name FROM sqlite_master WHERE type='table' AND name=?",
        (table,),
    )
    return cur.fetchone() is not None


def _table_columns(cur, table: str) -> List[str]:
    cur.execute(f"PRAGMA table_info({table})")
    return [row[1] for row in cur.fetchall()]


def _export_table(cur, table: str, soa_id: int) -> List[Dict[str, Any]]:
    """Return all rows of table for soa_id, minus stripped columns."""
    if not _table_exists(cur, table):
        return []
    cols = _table_columns(cur, table)
    keep = [i for i, c in enumerate(cols) if c not in _STRIPPED_COLUMNS]
    cur.execute(f"SELECT * FROM {table} WHERE soa_id=?", (soa_id,))
    return [{cols[i]: row[i] for i in keep} for row in cur.fetchall()]


def _export_soa_bundle(soa_id: int) -> Dict[str, Any]:
    conn = _connect()
    try:
        cur = conn.cursor()
        cols = _table_columns(cur, "soa")
        cur.execute("SELECT * FROM soa WHERE id=?", (soa_id,))
        row = cur.fetchone()
        if not row:
            raise HTTPException(404, "SOA not found")
        soa_meta = {
            col: val for col, val in zip(cols, row) if col not in _EXCLUDED_SOA_COLUMNS
        }
        bundle: Dict[str, Any] = {
            "format_version": _FORMAT_VERSION,
            "exported_at": datetime.now(timezone.utc).isoformat(),
            "soa": soa_meta,
        }
        for table in _EXPORT_TABLES:
            bundle[table] = _export_table(cur, table, soa_id)
        return bundle
    finally:
        conn.close()


def _filter_bc_property_rows(
    bundle: Dict[str, Any],
) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]]]:
    """Return (property rows, response code rows) safe to import.

    Keeps only properties whose parent BC exists in the bundle and only
    response codes whose parent property survives, deduping RCs that
    resolve to the same code on the same property. This prevents a
    corrupt bundle from re-introducing orphaned or duplicate rows.
    """
    bc_uids = {
        r.get("biomedical_concept_uid") for r in bundle.get("biomedical_concept", [])
    }
    bcp_rows = [
        r
        for r in bundle.get("biomedical_concept_property", [])
        if r.get("biomedical_concept_uid") in bc_uids
    ]
    kept_bcp_uids = {r.get("biomedical_concept_property_uid") for r in bcp_rows}

    # Resolve an RC's alias_code → standard code value using bundle data so
    # duplicates can be detected by (property, code) rather than alias UID.
    alias_by_uid = {a.get("alias_code_uid"): a for a in bundle.get("alias_code", [])}
    code_by_uid = {c.get("code_uid"): c for c in bundle.get("code", [])}

    def _resolved_code(alias_uid: Any) -> Any:
        alias = alias_by_uid.get(alias_uid)
        if not alias:
            return alias_uid
        code_row = code_by_uid.get(alias.get("standard_code"))
        return code_row.get("code") if code_row else alias_uid

    rc_rows = []
    seen = set()
    for r in bundle.get("bcp_response_code", []):
        bcp_uid = r.get("biomedical_concept_property_uid")
        if bcp_uid not in kept_bcp_uids:
            continue
        key = (bcp_uid, _resolved_code(r.get("code")))
        if key in seen:
            continue
        seen.add(key)
        rc_rows.append(r)
    return bcp_rows, rc_rows


def _as_row_id(value: Any) -> Optional[int]:
    """Parse a stored row id (INTEGER, or TEXT like scheduledAtId)."""
    if isinstance(value, int):
        return value
    try:
        return int(str(value).strip())
    except ValueError:
        return None


def _remap_row(
    table: str,
    values: Dict[str, Any],
    id_maps: Dict[str, Dict[int, int]],
) -> bool:
    """Rewrite values' parent row ids in place; False to drop the row."""
    for col, (parent, on_missing) in _ID_REMAPS.get(table, {}).items():
        old = values.get(col)
        if old is None or old == "":
            continue
        old_id = _as_row_id(old)
        new_id = id_maps.get(parent, {}).get(old_id) if old_id is not None else None
        if new_id is None:
            if on_missing == _DROP:
                return False
            values[col] = None
        else:
            values[col] = str(new_id) if isinstance(old, str) else new_id
    return True


def _import_table(
    cur,
    table: str,
    rows: List[Dict[str, Any]],
    new_soa_id: int,
    id_maps: Dict[str, Dict[int, int]],
) -> Dict[int, int]:
    """Insert rows into table under new_soa_id; return {old_id: new_id}."""
    id_map: Dict[int, int] = {}
    if not rows or not _table_exists(cur, table):
        return id_map
    target_cols = set(_table_columns(cur, table)) - {"id", "soa_id"}
    defaults = {
        col: val
        for col, val in _IMPORT_DEFAULTS.get(table, {}).items()
        if col in target_cols
    }
    dropped = 0
    for row in rows:
        values = {c: v for c, v in row.items() if c in target_cols}
        if not _remap_row(table, values, id_maps):
            dropped += 1
            continue
        values.update(defaults)
        cols = ["soa_id", *values]
        cur.execute(
            f"INSERT INTO {table} ({','.join(cols)}) "
            f"VALUES ({','.join('?' * len(cols))})",
            [new_soa_id, *values.values()],
        )
        old_id = row.get("id")
        if old_id is not None:
            id_map[old_id] = cur.lastrowid
    if dropped:
        logger.warning(
            "Bundle import: dropped %d %s row(s) whose parent is not in the bundle",
            dropped,
            table,
        )
    return id_map


def _insert_soa_row(cur, soa_meta: Dict[str, Any], soa_name: str) -> int:
    """Insert the soa row from bundle metadata; return its new id."""
    soa_cols = set(_table_columns(cur, "soa")) - set(_EXCLUDED_SOA_COLUMNS)
    values = {k: v for k, v in soa_meta.items() if k in soa_cols}
    values["name"] = soa_name
    values["created_at"] = datetime.now(timezone.utc).isoformat()

    def _insert() -> int:
        cols = list(values)
        cur.execute(
            f"INSERT INTO soa ({','.join(cols)}) VALUES ({','.join('?' * len(cols))})",
            list(values.values()),
        )
        return cur.lastrowid

    try:
        return _insert()
    except sqlite3.IntegrityError:
        if values.get("study_id") is None:
            raise
        # study_id conflict — import without it; user can set it later
        logger.info(
            "study_id %r already taken; imported with study_id=NULL",
            values.pop("study_id"),
        )
        return _insert()


def _import_soa_bundle(
    bundle: Dict[str, Any], name: Optional[str] = None
) -> Dict[str, Any]:
    if bundle.get("format_version") != _FORMAT_VERSION:
        raise HTTPException(
            422,
            f"Unsupported format_version: {bundle.get('format_version')!r}",
        )
    soa_meta = bundle.get("soa", {})
    soa_name = (name or soa_meta.get("name") or "Imported SOA").strip()

    bcp_rows, rc_rows = _filter_bc_property_rows(bundle)
    filtered = {
        "biomedical_concept_property": bcp_rows,
        "bcp_response_code": rc_rows,
    }

    conn = _connect()
    try:
        cur = conn.cursor()
        new_soa_id = _insert_soa_row(cur, soa_meta, soa_name)
        id_maps: Dict[str, Dict[int, int]] = {}
        for table in _EXPORT_TABLES:
            rows = filtered.get(table, bundle.get(table) or [])
            id_maps[table] = _import_table(cur, table, rows, new_soa_id, id_maps)
        conn.commit()
    except (sqlite3.Error, HTTPException):
        conn.rollback()
        raise
    finally:
        conn.close()
    return {"soa_id": new_soa_id, "name": soa_name}


@router.get("/soa/{soa_id}/export/bundle")
def export_bundle(soa_id: int):
    bundle = _export_soa_bundle(soa_id)
    safe_name = (bundle["soa"]["name"] or "soa").replace(" ", "_").replace("/", "_")
    filename = f"{safe_name}.soa-bundle.json"
    payload = json.dumps(bundle, indent=2) + "\n"
    buf = io.BytesIO(payload.encode("utf-8"))
    return StreamingResponse(
        buf,
        media_type="application/json",
        headers={"Content-Disposition": f'attachment; filename="{filename}"'},
    )


@router.post("/soa/import/bundle", status_code=201)
async def import_bundle(
    file: UploadFile = File(...),
    name: Optional[str] = Form(None),
):
    content = await file.read()
    try:
        bundle = json.loads(content)
    except json.JSONDecodeError as exc:
        logger.debug("Bundle upload JSON decode error: %s", exc)
        raise HTTPException(
            422, "Invalid JSON: uploaded file is not valid JSON"
        ) from exc
    result = _import_soa_bundle(bundle, name)
    return JSONResponse(result, status_code=201)


@ui_router.post("/ui/soa/import/bundle")
async def ui_import_bundle(
    file: UploadFile = File(...),
    name: Optional[str] = Form(None),
):
    content = await file.read()
    try:
        bundle = json.loads(content)
    except json.JSONDecodeError as exc:
        logger.debug("Bundle upload JSON decode error: %s", exc)
        raise HTTPException(
            422, "Invalid JSON: uploaded file is not valid JSON"
        ) from exc
    result = _import_soa_bundle(bundle, name or None)
    new_soa_id = int(result["soa_id"])
    return RedirectResponse(f"/ui/soa/{new_soa_id}/edit", status_code=303)
