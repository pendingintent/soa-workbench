"""Shared imports, routers and constants for the _freeze_helpers package."""

# Re-export hub for the sibling submodules, so imports here are
# intentionally unused locally.
# ruff: noqa: F401

import json
import logging
from datetime import datetime, timezone
from typing import Optional
from fastapi import HTTPException
from ...db import _connect
from ...utils import (
    soa_exists,
    get_next_concept_uid as _get_next_concept_uid,
    reserve_encounter_uid_floor,
    reserve_decision_uid_floor,
    reserve_instance_uid_floor,
    reserve_timeline_uid_floor,
    table_has_columns as _table_has_columns,
)

logger = logging.getLogger("soa_builder.web.routers._freeze_helpers")
# Bumped when the snapshot gains data that rollback depends on:
# 2 = element_id on elements and element_uid on study cells.
CAPTURE_VERSION = 2
# Tables whose rows rollback deletes and rebuilds; references from them
# do not keep a code row alive.
_BC_CODE_CHAIN_TABLES = (
    "biomedical_concept",
    "biomedical_concept_property",
    "bcp_response_code",
)
# Tables keyed on the integer activity.id that freeze does not capture.
# Rollback re-inserts activities under new ids, so these rows are
# re-pointed (or dropped when their activity is not restored).
ACTIVITY_ID_CHILD_TABLES = ("activity_concept_dss", "activity_concept_crf")
# Columns returned by _list_rollback_audit, in order; also the header
# row for the audit exports when there are no rows.
ROLLBACK_AUDIT_COLUMNS = (
    "id",
    "freeze_id",
    "performed_at",
    "visits_restored",
    "activities_restored",
    "cells_restored",
    "concepts_restored",
    "elements_restored",
    "cells_skipped",
    "encounters_restored",
    "instances_restored",
    "timelines_restored",
    "decision_instances_restored",
    "study_cells_restored",
    "safety_freeze_id",
)
