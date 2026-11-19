"""Shared imports, routers and constants for the soa_ui package."""

# Re-export hub for the sibling submodules, so imports here are
# intentionally unused locally.
# ruff: noqa: F401

import logging

from ...services import concepts as concepts_service
from ...audit import _record_element_audit
from fastapi import APIRouter
from ...initialize_database import _connect
from ...routers import activities as activities_router
from ...routers import elements as elements_router
from ...routers import instances as instances_router
from ...routers.organizations import _get_countries_options
from ...routers.organizations import _get_org_type_options
from ...routers.organizations import _list_organizations
from ...routers.roles import _get_role_type_options
from ...routers.roles import _list_roles
from ...schemas import CellCreate
from ...schemas import ElementCreate
from ...schemas import ElementUpdate
from ...utils import find_visit_cell
from ...utils import get_cdisc_api_key as _get_cdisc_api_key
from ...utils import get_concepts_override as _get_concepts_override
from ...utils import get_encounter_id
from ...utils import get_epoch_uid
from ...utils import get_next_code_uid as _get_next_code_uid
from ...utils import get_next_concept_uid as _get_next_concept_uid
from ...utils import get_schedule_timeline
from ...utils import get_scheduled_activity_instance
from ...utils import iso_duration_to_days
from ...utils import load_epoch_type_map
from ...utils import load_epoch_type_options
from ...utils import soa_exists
from ...utils import table_has_columns as _table_has_columns
from datetime import datetime
from datetime import timezone
from fastapi import BackgroundTasks
from fastapi import Form
from fastapi import HTTPException
from fastapi import Request
from fastapi.responses import HTMLResponse
from typing import List
from typing import Optional
import html as _html
import os
from ...audit import _record_activity_audit
from ...audit import _record_reorder_audit
from ...audit import _record_visit_audit
from ...routers.soa_core import delete_activity
from ...routers.soa_core import set_cell
from ...services.concepts import _cleanup_orphaned_concept_rows
from ...services.concepts import _concept_cache
from ...services.concepts import _enrich_biomedical_concept_bg
from ...services.concepts import _enrich_code_bg
from ...services.concepts import _get_activity_concepts
from ...services.concepts import _get_activity_surrogates
from ...services.concepts import _get_bc_categories_for_cell
from ...services.concepts import _get_concept_groups_for_cell
from ...services.concepts import _upsert_biomedical_concept
from ...services.matrix import _fetch_arms_for_edit
from ...services.matrix import _fetch_matrix
from ...services.matrix import _list_study_cells
from ...services.matrix import _render_activity_td
from ...services.matrix import _render_cell_td
from ...services.matrix import _render_encounter_th
from ...services.matrix import _render_epoch_th
from ...templating import templates

router = APIRouter()


def _soa_scoped_tables(cur) -> list:
    """Every table with a soa_id column, discovered from the schema.

    Deleting a study clears all of them, including audit, freeze and
    legacy tables, so a table added later can never keep rows of a
    deleted study (a hand-kept list used to miss new tables).
    """
    cur.execute(
        "SELECT name FROM sqlite_master WHERE type='table'"
        " AND name NOT LIKE 'sqlite_%' ORDER BY name"
    )
    tables = []
    for (name,) in cur.fetchall():
        cur.execute(f"PRAGMA table_info({name})")
        if any(row[1] == "soa_id" for row in cur.fetchall()):
            tables.append(name)
    return tables


logger = logging.getLogger("soa_builder.concepts")
