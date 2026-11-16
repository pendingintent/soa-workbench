"""MCP server exposing soa-workbench Schedule of Activities data as tools.

Run via ``soa-mcp`` (console script) or ``python -m soa_builder.mcp.server``.
The server communicates over stdio and is registered in ``.mcp.json`` for
automatic pickup by Claude Code.

Write tools call the FastAPI app in-process (see ``_call_api``) so they
share the routers' validation, UID allocation and audit trail.

Tools:
  list_soas                 List all Schedules of Activities
  create_soa                Create a new SoA
  get_soa                   Get SoA metadata by id
  list_visits               List visit definitions for a SoA
  create_visit              Add a new visit definition
  list_activities           List study activities for a SoA
  create_activity           Add a new study activity
  assign_instance_activity  Mark an activity as scheduled at a SAI
  get_soa_matrix            Return the visits-x-activities matrix
  get_usdm_json             Generate a USDM component as JSON
  get_define_json           Generate a Define-JSON document
  list_freezes / create_freeze / rollback_preview / rollback_freeze /
  diff_freezes              Versioned snapshots and rollback
  list/create/update/delete_design_entity
                            Epochs, arms, elements, study cells, instances
  get_audit_history         Audit trail across entity types
  generate_dair             Digital Amendment Impact Report (DOCX file)
  export_excel              SoA matrix workbook (XLSX file)
"""

import asyncio
import json
import logging
import re
import tempfile
from pathlib import Path
from typing import Any

import mcp.types as types
from mcp.server import Server
from mcp.server.stdio import stdio_server

from soa_builder.web.db import _connect

logger = logging.getLogger("soa_builder.mcp")

server = Server("soa-workbench")

_USDM_COMPONENTS = [
    "full",
    "arms",
    "activities",
    "biomedical_concepts",
    "bc_surrogates",
    "elements",
    "encounters",
    "epochs",
    "schedule_timelines",
    "timings",
    "instances",
    "study_cells",
    "objectives",
    "endpoints",
    "amendments",
]

_TOOLS = [
    types.Tool(
        name="list_soas",
        description=(
            "List all Schedules of Activities in the workbench. "
            "Returns id, name, study_id, study_label, created_at for each SoA."
        ),
        inputSchema={
            "type": "object",
            "properties": {
                "limit": {
                    "type": "integer",
                    "description": "Maximum rows to return (optional; default all)",
                    "minimum": 0,
                },
                "offset": {
                    "type": "integer",
                    "description": "Rows to skip before returning (optional)",
                    "minimum": 0,
                },
            },
            "required": [],
        },
    ),
    types.Tool(
        name="create_soa",
        description="Create a new Schedule of Activities. Returns the new soa_id.",
        inputSchema={
            "type": "object",
            "properties": {
                "name": {"type": "string", "description": "Short study name"},
                "study_id": {
                    "type": "string",
                    "description": "Unique study protocol ID (optional)",
                },
                "study_label": {
                    "type": "string",
                    "description": "Full study title (optional)",
                },
                "study_description": {
                    "type": "string",
                    "description": "Study description (optional)",
                },
            },
            "required": ["name"],
        },
    ),
    types.Tool(
        name="get_soa",
        description=(
            "Return metadata for a single Schedule of Activities "
            "(name, study_id, study_label, study_description)."
        ),
        inputSchema={
            "type": "object",
            "properties": {
                "soa_id": {"type": "integer", "description": "SoA identifier"},
            },
            "required": ["soa_id"],
        },
    ),
    types.Tool(
        name="list_visits",
        description=(
            "List all visit (encounter) definitions for a SoA, ordered by "
            "order_index. Returns id, name, label, type, encounter_uid."
        ),
        inputSchema={
            "type": "object",
            "properties": {
                "soa_id": {"type": "integer", "description": "SoA identifier"},
                "limit": {
                    "type": "integer",
                    "description": "Maximum rows to return (optional; default all)",
                    "minimum": 0,
                },
                "offset": {
                    "type": "integer",
                    "description": "Rows to skip before returning (optional)",
                    "minimum": 0,
                },
            },
            "required": ["soa_id"],
        },
    ),
    types.Tool(
        name="create_visit",
        description=(
            "Add a new visit (encounter) definition to a SoA. The encounter "
            "type code is assigned by the workbench."
        ),
        inputSchema={
            "type": "object",
            "properties": {
                "soa_id": {"type": "integer", "description": "SoA identifier"},
                "name": {
                    "type": "string",
                    "description": "Visit name (e.g. 'Screening', 'Day 1')",
                },
                "label": {
                    "type": "string",
                    "description": "Short display label (optional)",
                },
                "description": {
                    "type": "string",
                    "description": "Visit description (optional)",
                },
            },
            "required": ["soa_id", "name"],
        },
    ),
    types.Tool(
        name="list_activities",
        description=(
            "List all study activities for a SoA, ordered by order_index. "
            "Returns id, name, label, description, activity_uid."
        ),
        inputSchema={
            "type": "object",
            "properties": {
                "soa_id": {"type": "integer", "description": "SoA identifier"},
                "limit": {
                    "type": "integer",
                    "description": "Maximum rows to return (optional; default all)",
                    "minimum": 0,
                },
                "offset": {
                    "type": "integer",
                    "description": "Rows to skip before returning (optional)",
                    "minimum": 0,
                },
            },
            "required": ["soa_id"],
        },
    ),
    types.Tool(
        name="create_activity",
        description="Add a new study activity (e.g. a lab test or assessment) to a SoA.",
        inputSchema={
            "type": "object",
            "properties": {
                "soa_id": {"type": "integer", "description": "SoA identifier"},
                "name": {
                    "type": "string",
                    "description": "Activity name (e.g. 'Vital Signs', 'CBC')",
                },
                "label": {
                    "type": "string",
                    "description": "Short display label (optional)",
                },
                "description": {
                    "type": "string",
                    "description": "Detailed description (optional)",
                },
            },
            "required": ["soa_id", "name"],
        },
    ),
    types.Tool(
        name="assign_instance_activity",
        description=(
            "Mark a study activity as scheduled at a Scheduled Activity Instance "
            "(SAI). Use get_soa_matrix to discover instance_ids and activity_ids. "
            "Passing status='' removes the assignment."
        ),
        inputSchema={
            "type": "object",
            "properties": {
                "soa_id": {"type": "integer", "description": "SoA identifier"},
                "instance_id": {
                    "type": "integer",
                    "description": "ScheduledActivityInstance id (from get_soa_matrix)",
                },
                "activity_id": {
                    "type": "integer",
                    "description": "Activity id (from list_activities)",
                },
                "status": {
                    "type": "string",
                    "description": "Cell status — 'X' to schedule, '' to remove",
                    "default": "X",
                },
            },
            "required": ["soa_id", "instance_id", "activity_id"],
        },
    ),
    types.Tool(
        name="get_soa_matrix",
        description=(
            "Return the full visits-x-activities matrix for a SoA. "
            "Includes instances (ScheduledActivityInstances), activities, and cells "
            "(instance_id + activity_id pairs where an activity is scheduled). "
            "Use instance_id values with assign_instance_activity."
        ),
        inputSchema={
            "type": "object",
            "properties": {
                "soa_id": {"type": "integer", "description": "SoA identifier"},
            },
            "required": ["soa_id"],
        },
    ),
    types.Tool(
        name="get_usdm_json",
        description=(
            "Generate a USDM component as JSON for a SoA. "
            f"Valid components: {', '.join(_USDM_COMPONENTS)}."
        ),
        inputSchema={
            "type": "object",
            "properties": {
                "soa_id": {"type": "integer", "description": "SoA identifier"},
                "component": {
                    "type": "string",
                    "description": (
                        "USDM component to generate. One of: "
                        + ", ".join(_USDM_COMPONENTS)
                    ),
                },
            },
            "required": ["soa_id", "component"],
        },
    ),
    types.Tool(
        name="get_define_json",
        description=(
            "Generate a Define-JSON document for a SoA. "
            "Requires an SDTM-CT package date (sdtmct) in yyyy-mm-dd format. "
            "This calls the CDISC Library API to resolve controlled terminology — "
            "ensure CDISC_API_KEY is set in the environment."
        ),
        inputSchema={
            "type": "object",
            "properties": {
                "soa_id": {"type": "integer", "description": "SoA identifier"},
                "sdtmct": {
                    "type": "string",
                    "description": ("SDTM-CT package date, e.g. '2025-12-20'"),
                },
                "sdtmig": {
                    "type": "string",
                    "description": "SDTM-IG version (default '3.4')",
                    "default": "3.4",
                },
            },
            "required": ["soa_id", "sdtmct"],
        },
    ),
]


# Design entities share one URL convention in the web API:
#   GET/POST /soa/{soa_id}/{entity}, PATCH/DELETE /soa/{soa_id}/{entity}/{id}
# Field lists in the tool descriptions come from the API's request models.
_DESIGN_ENTITIES = {
    "epochs": ("EpochCreate", "EpochUpdate"),
    "arms": ("ArmCreate", "ArmUpdate"),
    "elements": ("ElementCreate", "ElementUpdate"),
    "study_cells": ("StudyCellCreate", "StudyCellUpdate"),
    "instances": ("InstanceCreate", "InstanceUpdate"),
    "decision_instances": ("DecisionInstanceCreate", "DecisionInstanceUpdate"),
}


def _entity_fields_doc(model_index: int) -> str:
    from soa_builder.web import schemas

    parts = []
    for entity, models in _DESIGN_ENTITIES.items():
        fields = getattr(schemas, models[model_index]).model_fields
        parts.append(f"{entity}: {', '.join(fields)}")
    return "; ".join(parts)


_ENTITY_PROP = {
    "type": "string",
    "enum": sorted(_DESIGN_ENTITIES),
    "description": "Design entity collection",
}
_SOA_PROP = {"type": "integer", "description": "SoA identifier"}
_FREEZE_PROP = {"type": "integer", "description": "Freeze (snapshot) id"}
_OUTPUT_DIR_PROP = {
    "type": "string",
    "description": (
        "Directory to write the file to (optional; defaults to a "
        "soa-workbench-exports folder in the system temp directory)"
    ),
}

_TOOLS += [
    types.Tool(
        name="list_freezes",
        description="List freezes (versioned snapshots) of a SoA, newest first.",
        inputSchema={
            "type": "object",
            "properties": {"soa_id": _SOA_PROP},
            "required": ["soa_id"],
        },
    ),
    types.Tool(
        name="create_freeze",
        description=(
            "Freeze the current state of a SoA as a named version. Omit "
            "version_label to get the next vN label."
        ),
        inputSchema={
            "type": "object",
            "properties": {
                "soa_id": _SOA_PROP,
                "version_label": {
                    "type": "string",
                    "description": "Unique label for the freeze (optional)",
                },
            },
            "required": ["soa_id"],
        },
    ),
    types.Tool(
        name="rollback_preview",
        description=(
            "Show what rolling back to a freeze would restore, without "
            "changing anything."
        ),
        inputSchema={
            "type": "object",
            "properties": {"soa_id": _SOA_PROP, "freeze_id": _FREEZE_PROP},
            "required": ["soa_id", "freeze_id"],
        },
    ),
    types.Tool(
        name="rollback_freeze",
        description=(
            "Replace the SoA's current design with a freeze. Destructive: "
            "changes made after the freeze are removed. A safety freeze of "
            "the current state is taken first and returned as "
            "safety_freeze_id so the rollback can itself be undone. Run "
            "rollback_preview first."
        ),
        inputSchema={
            "type": "object",
            "properties": {"soa_id": _SOA_PROP, "freeze_id": _FREEZE_PROP},
            "required": ["soa_id", "freeze_id"],
        },
    ),
    types.Tool(
        name="diff_freezes",
        description="Compare two freezes of a SoA (added/removed/changed).",
        inputSchema={
            "type": "object",
            "properties": {
                "soa_id": _SOA_PROP,
                "left_freeze_id": _FREEZE_PROP,
                "right_freeze_id": _FREEZE_PROP,
                "full": {
                    "type": "boolean",
                    "description": "Return every difference (default caps at 1000)",
                },
            },
            "required": ["soa_id", "left_freeze_id", "right_freeze_id"],
        },
    ),
    types.Tool(
        name="list_design_entities",
        description="List a SoA's epochs, arms, elements, study cells or instances.",
        inputSchema={
            "type": "object",
            "properties": {"soa_id": _SOA_PROP, "entity": _ENTITY_PROP},
            "required": ["soa_id", "entity"],
        },
    ),
    types.Tool(
        name="create_design_entity",
        description=(
            "Create an epoch, arm, element, study cell, instance or decision "
            "instance. UIDs are assigned by the workbench and the change is "
            "audited. Accepted fields per entity: " + _entity_fields_doc(0)
        ),
        inputSchema={
            "type": "object",
            "properties": {
                "soa_id": _SOA_PROP,
                "entity": _ENTITY_PROP,
                "fields": {"type": "object", "description": "Field values"},
            },
            "required": ["soa_id", "entity", "fields"],
        },
    ),
    types.Tool(
        name="update_design_entity",
        description=(
            "Update fields of an existing design entity (only the fields "
            "given change). Accepted fields per entity: " + _entity_fields_doc(1)
        ),
        inputSchema={
            "type": "object",
            "properties": {
                "soa_id": _SOA_PROP,
                "entity": _ENTITY_PROP,
                "entity_id": {"type": "integer", "description": "Row id"},
                "fields": {"type": "object", "description": "Field values"},
            },
            "required": ["soa_id", "entity", "entity_id", "fields"],
        },
    ),
    types.Tool(
        name="delete_design_entity",
        description="Delete a design entity. The deletion is audited.",
        inputSchema={
            "type": "object",
            "properties": {
                "soa_id": _SOA_PROP,
                "entity": _ENTITY_PROP,
                "entity_id": {"type": "integer", "description": "Row id"},
            },
            "required": ["soa_id", "entity", "entity_id"],
        },
    ),
    types.Tool(
        name="get_audit_history",
        description=(
            "Audit trail entries (create/update/delete with before/after "
            "values), newest first. entity_type is e.g. 'activity', "
            "'visit', 'epoch', 'arm', 'element', 'instance'."
        ),
        inputSchema={
            "type": "object",
            "properties": {
                "soa_id": _SOA_PROP,
                "entity_type": {
                    "type": "string",
                    "description": "Filter to one entity type (optional)",
                },
                "limit": {"type": "integer", "minimum": 0, "default": 50},
                "offset": {"type": "integer", "minimum": 0, "default": 0},
            },
            "required": ["soa_id"],
        },
    ),
    types.Tool(
        name="generate_dair",
        description=(
            "Generate the Digital Amendment Impact Report (DOCX) comparing "
            "two freezes. Returns the path of the written file."
        ),
        inputSchema={
            "type": "object",
            "properties": {
                "soa_id": _SOA_PROP,
                "base_freeze_id": _FREEZE_PROP,
                "revised_freeze_id": _FREEZE_PROP,
                "output_dir": _OUTPUT_DIR_PROP,
            },
            "required": ["soa_id", "base_freeze_id", "revised_freeze_id"],
        },
    ),
    types.Tool(
        name="export_excel",
        description=(
            "Export the SoA matrix to an Excel workbook. Returns the path "
            "of the written file."
        ),
        inputSchema={
            "type": "object",
            "properties": {"soa_id": _SOA_PROP, "output_dir": _OUTPUT_DIR_PROP},
            "required": ["soa_id"],
        },
    ),
]


@server.list_tools()
async def list_tools() -> list[types.Tool]:
    return _TOOLS


@server.call_tool()
async def call_tool(name: str, arguments: dict) -> list[types.TextContent]:
    loop = asyncio.get_event_loop()
    result = await loop.run_in_executor(None, _dispatch, name, arguments)
    return [types.TextContent(type="text", text=json.dumps(result, indent=2))]


# ---------------------------------------------------------------------------
# Dispatch
# ---------------------------------------------------------------------------


def _dispatch(name: str, args: dict) -> Any:
    handlers = {
        "list_soas": _list_soas,
        "create_soa": _create_soa,
        "get_soa": _get_soa,
        "list_visits": _list_visits,
        "create_visit": _create_visit,
        "list_activities": _list_activities,
        "create_activity": _create_activity,
        "assign_instance_activity": _assign_instance_activity,
        "get_soa_matrix": _get_soa_matrix,
        "get_usdm_json": _get_usdm_json,
        "get_define_json": _get_define_json,
        "list_freezes": _list_freezes,
        "create_freeze": _create_freeze,
        "rollback_preview": _rollback_preview,
        "rollback_freeze": _rollback_freeze,
        "diff_freezes": _diff_freezes,
        "list_design_entities": _list_design_entities,
        "create_design_entity": _create_design_entity,
        "update_design_entity": _update_design_entity,
        "delete_design_entity": _delete_design_entity,
        "get_audit_history": _get_audit_history,
        "generate_dair": _generate_dair,
        "export_excel": _export_excel,
    }
    fn = handlers.get(name)
    if fn is None:
        raise ValueError(f"Unknown tool: {name!r}")
    return fn(args)


# ---------------------------------------------------------------------------
# Tool handlers
# ---------------------------------------------------------------------------


def _page(args: dict) -> tuple:
    """Return (limit, offset) from tool args; limit -1 means no limit."""
    limit = args.get("limit")
    offset = args.get("offset") or 0
    limit = -1 if limit is None else int(limit)
    offset = int(offset)
    if limit < 0 and args.get("limit") is not None:
        raise ValueError("limit must be zero or greater")
    if offset < 0:
        raise ValueError("offset must be zero or greater")
    return limit, offset


# Write tools call the web API in-process so they share its validation,
# UID allocation (uid_counter / audit-aware allocators) and audit trail
# instead of re-implementing them in SQL here.
_api_client = None


def _api():
    global _api_client
    if _api_client is None:
        from fastapi.testclient import TestClient

        from soa_builder.web.app import app

        _api_client = TestClient(app)
    return _api_client


def _api_response(method: str, path: str, **kwargs):
    """Call the web API; raise ValueError carrying the API's detail."""
    response = _api().request(method, path, **kwargs)
    if response.status_code >= 400:
        try:
            detail = response.json().get("detail", response.text)
        except ValueError:
            detail = response.text
        raise ValueError(f"{detail} (HTTP {response.status_code})")
    return response


def _call_api(method: str, path: str, **kwargs) -> Any:
    return _api_response(method, path, **kwargs).json()


def _require_name(args: dict) -> str:
    name = (args.get("name") or "").strip()
    if not name:
        raise ValueError("name is required")
    return name


def _list_soas(args: dict) -> list:
    limit, offset = _page(args)
    conn = _connect()
    cur = conn.cursor()
    try:
        cur.execute(
            "SELECT id,name,created_at,study_id,study_label,study_description"
            " FROM soa ORDER BY id DESC LIMIT ? OFFSET ?",
            (limit, offset),
        )
        rows = cur.fetchall()
    finally:
        conn.close()
    return [
        {
            "id": r[0],
            "name": r[1],
            "created_at": r[2],
            "study_id": r[3],
            "study_label": r[4],
            "study_description": r[5],
        }
        for r in rows
    ]


def _create_soa(args: dict) -> dict:
    payload = {"name": _require_name(args)}
    for key in ("study_id", "study_label", "study_description"):
        value = (args.get(key) or "").strip()
        if value:
            payload[key] = value
    return _call_api("POST", "/soa", json=payload)


def _get_soa(args: dict) -> dict:
    soa_id = int(args["soa_id"])
    conn = _connect()
    cur = conn.cursor()
    try:
        cur.execute(
            "SELECT id,name,study_id,study_label,study_description,created_at"
            " FROM soa WHERE id=?",
            (soa_id,),
        )
        row = cur.fetchone()
    finally:
        conn.close()
    if row is None:
        raise ValueError(f"SoA {soa_id} not found")
    return {
        "id": row[0],
        "name": row[1],
        "study_id": row[2],
        "study_label": row[3],
        "study_description": row[4],
        "created_at": row[5],
    }


def _list_visits(args: dict) -> list:
    soa_id = int(args["soa_id"])
    limit, offset = _page(args)
    conn = _connect()
    cur = conn.cursor()
    try:
        cur.execute(
            "SELECT id,encounter_uid,name,label,description,type,order_index"
            " FROM visit WHERE soa_id=? ORDER BY order_index,id"
            " LIMIT ? OFFSET ?",
            (soa_id, limit, offset),
        )
        rows = cur.fetchall()
    finally:
        conn.close()
    return [
        {
            "id": r[0],
            "encounter_uid": r[1],
            "name": r[2],
            "label": r[3],
            "description": r[4],
            "type": r[5],
            "order_index": r[6],
        }
        for r in rows
    ]


def _create_visit(args: dict) -> dict:
    soa_id = int(args["soa_id"])
    payload = {"name": _require_name(args)}
    for key in ("label", "description"):
        value = (args.get(key) or "").strip()
        if value:
            payload[key] = value
    created = _call_api("POST", f"/soa/{soa_id}/visits", json=payload)
    conn = _connect()
    cur = conn.cursor()
    try:
        cur.execute(
            "SELECT encounter_uid, order_index, type FROM visit WHERE id=?",
            (created["id"],),
        )
        encounter_uid, order_index, visit_type = cur.fetchone()
    finally:
        conn.close()
    return {
        "id": created["id"],
        "soa_id": soa_id,
        "name": created["name"],
        "label": created.get("label"),
        "description": created.get("description"),
        "type": visit_type,
        "encounter_uid": encounter_uid,
        "order_index": order_index,
    }


def _list_activities(args: dict) -> list:
    soa_id = int(args["soa_id"])
    limit, offset = _page(args)
    conn = _connect()
    cur = conn.cursor()
    try:
        cur.execute(
            "SELECT id,name,order_index,activity_uid,label,description"
            " FROM activity WHERE soa_id=? ORDER BY order_index,id"
            " LIMIT ? OFFSET ?",
            (soa_id, limit, offset),
        )
        rows = cur.fetchall()
    finally:
        conn.close()
    return [
        {
            "id": r[0],
            "name": r[1],
            "order_index": r[2],
            "activity_uid": r[3],
            "label": r[4],
            "description": r[5],
        }
        for r in rows
    ]


def _create_activity(args: dict) -> dict:
    soa_id = int(args["soa_id"])
    name = _require_name(args)
    label = (args.get("label") or "").strip() or None
    description = (args.get("description") or "").strip() or None
    created = _call_api(
        "POST",
        f"/soa/{soa_id}/activities",
        json={"name": name, "label": label, "description": description},
    )
    return {
        "id": created["activity_id"],
        "soa_id": soa_id,
        "name": name,
        "label": label,
        "description": description,
        "activity_uid": created["activity_uid"],
        "order_index": created["order_index"],
    }


def _assign_instance_activity(args: dict) -> dict:
    soa_id = int(args["soa_id"])
    instance_id = int(args["instance_id"])
    activity_id = int(args["activity_id"])
    raw_status = args.get("status")
    status = "X" if raw_status is None else str(raw_status).strip()
    result = _call_api(
        "POST",
        f"/soa/{soa_id}/cells_instance",
        json={
            "instance_id": instance_id,
            "activity_id": activity_id,
            "status": status,
        },
    )
    if status == "":
        return {
            "deleted": result.get("deleted", False),
            "instance_id": instance_id,
            "activity_id": activity_id,
        }
    return {
        "cell_id": result["cell_id"],
        "soa_id": soa_id,
        "instance_id": instance_id,
        "activity_id": activity_id,
        "status": status,
    }


def _get_soa_matrix(args: dict) -> dict:
    soa_id = int(args["soa_id"])
    conn = _connect()
    cur = conn.cursor()
    try:
        cur.execute(
            "SELECT id,name,epoch_uid,encounter_uid,instance_uid,member_of_timeline"
            " FROM instances WHERE soa_id=? ORDER BY member_of_timeline,id",
            (soa_id,),
        )
        instances = [
            {
                "id": r[0],
                "name": r[1],
                "epoch_uid": r[2],
                "encounter_uid": r[3],
                "instance_uid": r[4],
                "member_of_timeline": r[5],
            }
            for r in cur.fetchall()
        ]

        cur.execute(
            "SELECT id,name,order_index,activity_uid,label,description"
            " FROM activity WHERE soa_id=? ORDER BY order_index",
            (soa_id,),
        )
        activities = [
            {
                "id": r[0],
                "name": r[1],
                "order_index": r[2],
                "activity_uid": r[3],
                "label": r[4],
                "description": r[5],
            }
            for r in cur.fetchall()
        ]

        cur.execute(
            "SELECT instance_id,activity_id,status"
            " FROM matrix_cells WHERE soa_id=? AND instance_id IS NOT NULL",
            (soa_id,),
        )
        cells = [
            {"instance_id": r[0], "activity_id": r[1], "status": r[2]}
            for r in cur.fetchall()
        ]
    finally:
        conn.close()
    return {"instances": instances, "activities": activities, "cells": cells}


def _get_usdm_json(args: dict) -> Any:
    soa_id = int(args["soa_id"])
    component = str(args.get("component") or "").strip()
    if component not in _USDM_COMPONENTS:
        raise ValueError(
            f"Unknown component {component!r}. "
            f"Valid values: {', '.join(_USDM_COMPONENTS)}"
        )
    from soa_builder.web.routers.usdm_json import _build

    return _build(component, soa_id)


def _get_define_json(args: dict) -> Any:
    soa_id = int(args["soa_id"])
    sdtmct = str(args.get("sdtmct") or "").strip()
    if not sdtmct:
        raise ValueError("sdtmct is required (yyyy-mm-dd format)")
    sdtmig = str(args.get("sdtmig") or "3.4").strip()

    from usdm.generate_define_json import build_define_json

    return build_define_json(soa_id, sdtmct=sdtmct, sdtmig=sdtmig)


def _list_freezes(args: dict) -> list:
    return _call_api("GET", f"/soa/{int(args['soa_id'])}/freezes")


def _create_freeze(args: dict) -> dict:
    label = (args.get("version_label") or "").strip() or None
    return _call_api(
        "POST",
        f"/soa/{int(args['soa_id'])}/freezes",
        json={"version_label": label},
    )


def _rollback_preview(args: dict) -> dict:
    soa_id, freeze_id = int(args["soa_id"]), int(args["freeze_id"])
    return _call_api("GET", f"/soa/{soa_id}/freeze/{freeze_id}/rollback_preview")


def _rollback_freeze(args: dict) -> dict:
    soa_id, freeze_id = int(args["soa_id"]), int(args["freeze_id"])
    return _call_api("POST", f"/soa/{soa_id}/freeze/{freeze_id}/rollback")


def _diff_freezes(args: dict) -> dict:
    return _call_api(
        "GET",
        f"/soa/{int(args['soa_id'])}/freeze/diff.json",
        params={
            "left": int(args["left_freeze_id"]),
            "right": int(args["right_freeze_id"]),
            "full": 1 if args.get("full") else 0,
        },
    )


def _entity_path(args: dict, with_id: bool = False) -> str:
    entity = str(args.get("entity") or "")
    if entity not in _DESIGN_ENTITIES:
        raise ValueError(
            f"Unknown entity {entity!r}. "
            f"Valid values: {', '.join(sorted(_DESIGN_ENTITIES))}"
        )
    path = f"/soa/{int(args['soa_id'])}/{entity}"
    if with_id:
        path += f"/{int(args['entity_id'])}"
    return path


def _entity_fields(args: dict) -> dict:
    fields = args.get("fields")
    if not isinstance(fields, dict):
        raise ValueError("fields must be an object of field values")
    return fields


def _list_design_entities(args: dict) -> Any:
    return _call_api("GET", _entity_path(args))


def _create_design_entity(args: dict) -> Any:
    return _call_api("POST", _entity_path(args), json=_entity_fields(args))


def _update_design_entity(args: dict) -> Any:
    return _call_api(
        "PATCH", _entity_path(args, with_id=True), json=_entity_fields(args)
    )


def _delete_design_entity(args: dict) -> Any:
    return _call_api("DELETE", _entity_path(args, with_id=True))


def _get_audit_history(args: dict) -> list:
    params = {
        "limit": int(args.get("limit", 50)),
        "offset": int(args.get("offset", 0)),
    }
    if args.get("entity_type"):
        params["entity_type"] = str(args["entity_type"])
    return _call_api("GET", f"/soa/{int(args['soa_id'])}/audit", params=params)


def _write_export(response, args: dict, fallback_name: str) -> dict:
    """Save an API file response and return where it went."""
    output_dir = Path(
        args.get("output_dir") or Path(tempfile.gettempdir()) / "soa-workbench-exports"
    )
    output_dir.mkdir(parents=True, exist_ok=True)
    disposition = response.headers.get("content-disposition", "")
    match = re.search(r'filename="?([^";]+)"?', disposition)
    name = Path(match.group(1)).name if match else fallback_name
    path = output_dir / name
    path.write_bytes(response.content)
    return {
        "path": str(path),
        "bytes": len(response.content),
        "content_type": response.headers.get("content-type"),
    }


def _generate_dair(args: dict) -> dict:
    soa_id = int(args["soa_id"])
    base, revised = int(args["base_freeze_id"]), int(args["revised_freeze_id"])
    response = _api_response(
        "GET",
        f"/soa/{soa_id}/dair/download",
        params={"base_freeze_id": base, "revised_freeze_id": revised},
    )
    return _write_export(response, args, f"dair_soa{soa_id}_{base}_{revised}.docx")


def _export_excel(args: dict) -> dict:
    soa_id = int(args["soa_id"])
    response = _api_response("GET", f"/soa/{soa_id}/export/xlsx")
    return _write_export(response, args, f"soa_{soa_id}_matrix.xlsx")


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------


def main() -> None:
    logging.basicConfig(level=logging.WARNING)
    asyncio.run(_run())


async def _run() -> None:
    async with stdio_server() as streams:
        await server.run(*streams, server.create_initialization_options())


if __name__ == "__main__":
    main()
