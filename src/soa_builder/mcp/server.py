"""MCP server exposing soa-workbench Schedule of Activities data as tools.

Run via ``soa-mcp`` (console script) or ``python -m soa_builder.mcp.server``.
The server communicates over stdio and is registered in ``.mcp.json`` for
automatic pickup by Claude Code.

Write tools call the FastAPI app in-process (see ``_call_api``) so they
share the routers' validation, UID allocation and audit trail.

Tools (11 total):
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
"""

import asyncio
import json
import logging
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


def _call_api(method: str, path: str, **kwargs) -> Any:
    """Call the web API; raise ValueError carrying the API's detail."""
    response = _api().request(method, path, **kwargs)
    if response.status_code >= 400:
        try:
            detail = response.json().get("detail", response.text)
        except ValueError:
            detail = response.text
        raise ValueError(f"{detail} (HTTP {response.status_code})")
    return response.json()


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
