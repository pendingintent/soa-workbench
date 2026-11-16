"""Shared imports, routers and constants for the activities package."""

# Re-export hub for the sibling submodules, so imports here are
# intentionally unused locally.
# ruff: noqa: F401

from ...templating import TEMPLATES_DIR

import html
import json
import logging
import os
import time
from typing import List
from fastapi import APIRouter, BackgroundTasks, Body, HTTPException, Request, Form
from fastapi.responses import JSONResponse, HTMLResponse, RedirectResponse
from fastapi.templating import Jinja2Templates
from ...audit import _record_activity_audit, _record_reorder_audit
from ...db import _connect
from ...schemas import ActivityCreate, ActivityUpdate, BulkActivities
from ...utils import (
    soa_exists,
    table_has_columns as _table_has_columns,
    get_next_concept_uid as _get_next_concept_uid,
    get_cdisc_api_key as _get_cdisc_api_key,
    get_cdisc_crf_api_key as _get_cdisc_crf_api_key,
    get_cdisc_crf_subscription_key as _get_cdisc_crf_subscription_key,
)
import html as _html

_ACT_CONCEPT_CACHE = {"data": None, "fetched_at": 0}
_ACT_CONCEPT_TTL = int(os.environ.get("SOA_BUILDER_CACHE_TTL", "3600"))
router = APIRouter(prefix="/soa/{soa_id}")
ui_router = APIRouter()
logger = logging.getLogger("soa_builder.web.routers.activities")
templates = Jinja2Templates(directory=TEMPLATES_DIR)
