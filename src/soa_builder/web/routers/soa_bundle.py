import io
import json
import logging
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

from fastapi import APIRouter, File, Form, HTTPException, UploadFile
from fastapi.responses import JSONResponse, RedirectResponse, StreamingResponse

from ..db import _connect

router = APIRouter()
ui_router = APIRouter()
logger = logging.getLogger("soa_builder.web.routers.soa_bundle")

_FORMAT_VERSION = "1.0"

# All soa_id-scoped data tables, in safe import order
_EXPORT_TABLES = [
    "code",
    "alias_code",
    "code_association",
    "epoch",
    "arm",
    "visit",
    "activity",
    "element",
    "study_cell",
    "schedule_timelines",
    "instances",
    "decision_instances",
    "condition_assignment",
    "timing",
    "transition_rule",
    "matrix_cells",
    "biomedical_concept",
    "biomedical_concept_property",
    "bcp_response_code",
    "biomedical_concept_surrogate",
    "activity_concept",
    "activity_concept_dss",
    "activity_surrogate",
    "objective",
    "endpoint",
    "study_title",
    "organization",
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
]


def _table_exists(cur, table: str) -> bool:
    cur.execute(
        "SELECT name FROM sqlite_master WHERE type='table' AND name=?",
        (table,),
    )
    return cur.fetchone() is not None


def _export_table(cur, table: str, soa_id: int) -> List[Dict[str, Any]]:
    """Return all rows from table as list of column dicts."""
    if not _table_exists(cur, table):
        return []
    cur.execute(f"PRAGMA table_info({table})")
    cols = [row[1] for row in cur.fetchall()]
    cur.execute(f"SELECT * FROM {table} WHERE soa_id=?", (soa_id,))
    return [dict(zip(cols, row)) for row in cur.fetchall()]


def _export_soa_bundle(soa_id: int) -> Dict[str, Any]:
    conn = _connect()
    cur = conn.cursor()
    cur.execute(
        "SELECT name, study_id, study_label, study_description FROM soa WHERE id=?",
        (soa_id,),
    )
    row = cur.fetchone()
    if not row:
        conn.close()
        raise HTTPException(404, "SOA not found")
    bundle: Dict[str, Any] = {
        "format_version": _FORMAT_VERSION,
        "exported_at": datetime.now(timezone.utc).isoformat(),
        "soa": {
            "name": row[0],
            "study_id": row[1],
            "study_label": row[2],
            "study_description": row[3],
        },
    }
    for table in _EXPORT_TABLES:
        bundle[table] = _export_table(cur, table, soa_id)
    conn.close()
    return bundle


def _insert_and_map(
    cur,
    table: str,
    rows: List[Dict[str, Any]],
    new_soa_id: int,
    extra_skip: Optional[set] = None,
    col_overrides: Optional[Dict[str, Any]] = None,
) -> Dict[int, int]:
    """Insert rows into table; return {old_id: new_id} mapping."""
    id_map: Dict[int, int] = {}
    if not rows or not _table_exists(cur, table):
        return id_map
    skip = {"id", "soa_id"} | (extra_skip or set())
    col_names = [c for c in rows[0].keys() if c not in skip]
    placeholders = ",".join(["?"] * len(col_names))
    cols_sql = ",".join(col_names)
    for row in rows:
        values = []
        for col in col_names:
            if col_overrides and col in col_overrides:
                values.append(col_overrides[col])
            else:
                values.append(row.get(col))
        cur.execute(
            f"INSERT INTO {table} (soa_id,{cols_sql}) VALUES (?,{placeholders})",
            [new_soa_id] + values,
        )
        old_id = row.get("id")
        if old_id is not None:
            id_map[old_id] = cur.lastrowid
    return id_map


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
    now = datetime.now(timezone.utc).isoformat()

    conn = _connect()
    cur = conn.cursor()
    try:
        study_id = soa_meta.get("study_id")
        try:
            cur.execute(
                "INSERT INTO soa "
                "(name, created_at, study_id, study_label, study_description) "
                "VALUES (?,?,?,?,?)",
                (
                    soa_name,
                    now,
                    study_id,
                    soa_meta.get("study_label"),
                    soa_meta.get("study_description"),
                ),
            )
        except Exception:
            # study_id conflict — import without it; user can set it later
            cur.execute(
                "INSERT INTO soa "
                "(name, created_at, study_label, study_description) "
                "VALUES (?,?,?,?)",
                (
                    soa_name,
                    now,
                    soa_meta.get("study_label"),
                    soa_meta.get("study_description"),
                ),
            )
            logger.info(
                "study_id %r already taken; imported with study_id=NULL",
                study_id,
            )
        new_soa_id = cur.lastrowid

        # Tables with only string-UID foreign keys — insert directly
        for table in [
            "code",
            "alias_code",
            "code_association",
            "arm",
            "element",
            "study_cell",
            "schedule_timelines",
            "decision_instances",
            "condition_assignment",
            "timing",
            "transition_rule",
            "biomedical_concept",
            "biomedical_concept_property",
            "bcp_response_code",
            "biomedical_concept_surrogate",
            "activity_surrogate",
            "objective",
            "endpoint",
            "study_title",
            "organization",
            "footnote",
            "study_amendment_reason",
            "study_amendment_impact",
            "study_change",
            "document_content_reference",
            "amendment_geographic_scope",
            "amendment_governance_date",
            "governance_date_geographic_scope",
            "amendment_subject_enrollment",
        ]:
            _insert_and_map(cur, table, bundle.get(table, []), new_soa_id)

        # study_amendment: freeze_id references soa_freeze which is not
        # imported; use 0 as sentinel for "imported, no associated freeze"
        _insert_and_map(
            cur,
            "study_amendment",
            bundle.get("study_amendment", []),
            new_soa_id,
            extra_skip={"freeze_id"},
            col_overrides={"freeze_id": 0},
        )

        # epoch: build id map for visit.epoch_id remapping
        epoch_id_map = _insert_and_map(
            cur, "epoch", bundle.get("epoch", []), new_soa_id
        )

        # visit: remap epoch_id, build visit_id_map
        visit_rows = bundle.get("visit", [])
        visit_id_map: Dict[int, int] = {}
        if visit_rows and _table_exists(cur, "visit"):
            skip_v = {"id", "soa_id", "epoch_id"}
            col_names_v = [c for c in visit_rows[0].keys() if c not in skip_v]
            placeholders_v = ",".join(["?"] * len(col_names_v))
            cols_sql_v = ",".join(col_names_v)
            for row in visit_rows:
                new_epoch_id = epoch_id_map.get(row.get("epoch_id"))
                values = [row.get(c) for c in col_names_v]
                cur.execute(
                    f"INSERT INTO visit (soa_id,epoch_id,{cols_sql_v}) "
                    f"VALUES (?,?,{placeholders_v})",
                    [new_soa_id, new_epoch_id] + values,
                )
                old_id = row.get("id")
                if old_id is not None:
                    visit_id_map[old_id] = cur.lastrowid

        # activity: build activity_id_map
        activity_id_map = _insert_and_map(
            cur, "activity", bundle.get("activity", []), new_soa_id
        )

        # instances: build instance_id_map (timeline_id is a string UID)
        instance_id_map = _insert_and_map(
            cur, "instances", bundle.get("instances", []), new_soa_id
        )

        # matrix_cells: remap visit_id, activity_id, instance_id
        cell_rows = bundle.get("matrix_cells", [])
        if cell_rows and _table_exists(cur, "matrix_cells"):
            has_superscript = "superscript" in cell_rows[0]
            for row in cell_rows:
                new_visit_id = visit_id_map.get(row.get("visit_id"))
                new_act_id = activity_id_map.get(row.get("activity_id"))
                old_inst_id = row.get("instance_id")
                new_inst_id = (
                    instance_id_map.get(old_inst_id)
                    if old_inst_id is not None
                    else None
                )
                if has_superscript:
                    cur.execute(
                        "INSERT INTO matrix_cells "
                        "(soa_id,visit_id,activity_id,instance_id,"
                        "status,superscript) VALUES (?,?,?,?,?,?)",
                        (
                            new_soa_id,
                            new_visit_id,
                            new_act_id,
                            new_inst_id,
                            row.get("status", ""),
                            row.get("superscript"),
                        ),
                    )
                else:
                    cur.execute(
                        "INSERT INTO matrix_cells "
                        "(soa_id,visit_id,activity_id,instance_id,status) "
                        "VALUES (?,?,?,?,?)",
                        (
                            new_soa_id,
                            new_visit_id,
                            new_act_id,
                            new_inst_id,
                            row.get("status", ""),
                        ),
                    )

        # activity_concept: remap activity_id
        ac_rows = bundle.get("activity_concept", [])
        if ac_rows and _table_exists(cur, "activity_concept"):
            skip_ac = {"id", "soa_id", "activity_id"}
            col_names_ac = [c for c in ac_rows[0].keys() if c not in skip_ac]
            placeholders_ac = ",".join(["?"] * len(col_names_ac))
            cols_sql_ac = ",".join(col_names_ac)
            for row in ac_rows:
                new_act_id = activity_id_map.get(row.get("activity_id"))
                values = [row.get(c) for c in col_names_ac]
                cur.execute(
                    f"INSERT INTO activity_concept "
                    f"(soa_id,activity_id,{cols_sql_ac}) "
                    f"VALUES (?,?,{placeholders_ac})",
                    [new_soa_id, new_act_id] + values,
                )

        # activity_concept_dss: remap activity_id
        acd_rows = bundle.get("activity_concept_dss", [])
        if acd_rows and _table_exists(cur, "activity_concept_dss"):
            skip_acd = {"id", "soa_id", "activity_id"}
            col_names_acd = [c for c in acd_rows[0].keys() if c not in skip_acd]
            placeholders_acd = ",".join(["?"] * len(col_names_acd))
            cols_sql_acd = ",".join(col_names_acd)
            for row in acd_rows:
                new_act_id = activity_id_map.get(row.get("activity_id"))
                values = [row.get(c) for c in col_names_acd]
                cur.execute(
                    f"INSERT INTO activity_concept_dss "
                    f"(soa_id,activity_id,{cols_sql_acd}) "
                    f"VALUES (?,?,{placeholders_acd})",
                    [new_soa_id, new_act_id] + values,
                )

        conn.commit()
    except Exception:
        conn.rollback()
        conn.close()
        raise
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
        raise HTTPException(422, f"Invalid JSON: {exc}") from exc
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
        raise HTTPException(422, f"Invalid JSON: {exc}") from exc
    result = _import_soa_bundle(bundle, name or None)
    return RedirectResponse(f"/ui/soa/{result['soa_id']}/edit", status_code=303)
