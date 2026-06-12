"""MCP server exposing soa-workbench Schedule of Activities data as tools.

Run via ``soa-mcp`` (console script) or ``python -m soa_builder.mcp.server``.
The server communicates over stdio and is registered in ``.mcp.json`` for
automatic pickup by Claude Code.

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
from datetime import datetime, timezone
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
            "properties": {},
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
            },
            "required": ["soa_id"],
        },
    ),
    types.Tool(
        name="create_visit",
        description="Add a new visit (encounter) definition to a SoA.",
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
                "type": {
                    "type": "string",
                    "description": (
                        "Visit type (optional), e.g. 'SCHEDULED', 'UNSCHEDULED'"
                    ),
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


def _list_soas(_args: dict) -> list:
    conn = _connect()
    cur = conn.cursor()
    try:
        cur.execute(
            "SELECT id,name,created_at,study_id,study_label,study_description"
            " FROM soa ORDER BY id DESC"
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
    name = args.get("name", "").strip()
    if not name:
        raise ValueError("name is required")
    study_id = (args.get("study_id") or "").strip() or None
    study_label = (args.get("study_label") or "").strip() or None
    study_description = (args.get("study_description") or "").strip() or None
    conn = _connect()
    cur = conn.cursor()
    try:
        if study_id:
            cur.execute("SELECT 1 FROM soa WHERE study_id=?", (study_id,))
            if cur.fetchone():
                raise ValueError(f"study_id {study_id!r} already exists")
        cur.execute(
            "INSERT INTO soa"
            " (name, created_at, study_id, study_label, study_description)"
            " VALUES (?,?,?,?,?)",
            (
                name,
                datetime.now(timezone.utc).isoformat(),
                study_id,
                study_label,
                study_description,
            ),
        )
        soa_id = cur.lastrowid
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()
    return {
        "id": soa_id,
        "name": name,
        "study_id": study_id,
        "study_label": study_label,
        "study_description": study_description,
    }


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
    conn = _connect()
    cur = conn.cursor()
    try:
        cur.execute(
            "SELECT id,encounter_uid,name,label,description,type,order_index"
            " FROM visit WHERE soa_id=? ORDER BY order_index,id",
            (soa_id,),
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
    name = args.get("name", "").strip()
    if not name:
        raise ValueError("name is required")
    label = (args.get("label") or "").strip() or None
    visit_type = (args.get("type") or "").strip() or None

    conn = _connect()
    cur = conn.cursor()
    try:
        cur.execute(
            "SELECT COALESCE(MAX(order_index),0) FROM visit WHERE soa_id=?",
            (soa_id,),
        )
        order_index = cur.fetchone()[0] + 1

        cur.execute(
            "SELECT encounter_uid FROM visit"
            " WHERE soa_id=? AND encounter_uid LIKE 'Encounter_%'",
            (soa_id,),
        )
        existing_uids = [r[0] for r in cur.fetchall()]
        next_n = 1
        if existing_uids:
            nums = []
            for uid in existing_uids:
                try:
                    nums.append(int(uid.split("_")[1]))
                except (IndexError, ValueError):
                    pass
            if nums:
                next_n = max(nums) + 1
        encounter_uid = f"Encounter_{next_n}"

        cur.execute(
            "INSERT INTO visit"
            " (soa_id, name, label, order_index, encounter_uid, type)"
            " VALUES (?,?,?,?,?,?)",
            (soa_id, name, label, order_index, encounter_uid, visit_type),
        )
        visit_id = cur.lastrowid
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()
    return {
        "id": visit_id,
        "soa_id": soa_id,
        "name": name,
        "label": label,
        "type": visit_type,
        "encounter_uid": encounter_uid,
        "order_index": order_index,
    }


def _list_activities(args: dict) -> list:
    soa_id = int(args["soa_id"])
    conn = _connect()
    cur = conn.cursor()
    try:
        cur.execute(
            "SELECT id,name,order_index,activity_uid,label,description"
            " FROM activity WHERE soa_id=? ORDER BY order_index",
            (soa_id,),
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
    name = args.get("name", "").strip()
    if not name:
        raise ValueError("name is required")
    label = (args.get("label") or "").strip() or None
    description = (args.get("description") or "").strip() or None

    conn = _connect()
    cur = conn.cursor()
    try:
        cur.execute(
            "SELECT COALESCE(MAX(order_index),0) FROM activity WHERE soa_id=?",
            (soa_id,),
        )
        order_index = cur.fetchone()[0] + 1

        cur.execute(
            "SELECT activity_uid FROM activity"
            " WHERE soa_id=? AND activity_uid LIKE 'Activity_%'",
            (soa_id,),
        )
        existing_uids = [r[0] for r in cur.fetchall()]
        next_n = 1
        if existing_uids:
            nums = []
            for uid in existing_uids:
                try:
                    nums.append(int(uid.split("_")[1]))
                except (IndexError, ValueError):
                    pass
            if nums:
                next_n = max(nums) + 1
        activity_uid = f"Activity_{next_n}"

        try:
            cur.execute(
                "INSERT INTO activity"
                " (soa_id, name, order_index, activity_uid, label, description)"
                " VALUES (?,?,?,?,?,?)",
                (soa_id, name, order_index, activity_uid, label, description),
            )
        except Exception:
            cur.execute(
                "INSERT INTO activity"
                " (soa_id, name, order_index, activity_uid)"
                " VALUES (?,?,?,?)",
                (soa_id, name, order_index, activity_uid),
            )
        activity_id = cur.lastrowid
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()
    return {
        "id": activity_id,
        "soa_id": soa_id,
        "name": name,
        "label": label,
        "description": description,
        "activity_uid": activity_uid,
        "order_index": order_index,
    }


def _assign_instance_activity(args: dict) -> dict:
    soa_id = int(args["soa_id"])
    instance_id = int(args["instance_id"])
    activity_id = int(args["activity_id"])
    raw_status = args.get("status")
    status = "X" if raw_status is None else str(raw_status).strip()

    conn = _connect()
    cur = conn.cursor()
    try:
        cur.execute(
            "SELECT id FROM matrix_cells"
            " WHERE soa_id=? AND instance_id=? AND activity_id=?",
            (soa_id, instance_id, activity_id),
        )
        existing = cur.fetchone()
        if status == "":
            if existing:
                cur.execute("DELETE FROM matrix_cells WHERE id=?", (existing[0],))
                conn.commit()
                return {
                    "deleted": True,
                    "instance_id": instance_id,
                    "activity_id": activity_id,
                }
            return {
                "deleted": False,
                "instance_id": instance_id,
                "activity_id": activity_id,
            }
        if existing:
            cur.execute(
                "UPDATE matrix_cells SET status=? WHERE id=?", (status, existing[0])
            )
            cell_id = existing[0]
        else:
            cur.execute(
                "INSERT INTO matrix_cells"
                " (soa_id, instance_id, activity_id, status)"
                " VALUES (?,?,?,?)",
                (soa_id, instance_id, activity_id, status),
            )
            cell_id = cur.lastrowid
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()
    return {
        "cell_id": cell_id,
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
