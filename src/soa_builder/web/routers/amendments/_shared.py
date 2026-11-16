"""Shared imports, routers and constants for the amendments package."""

# Re-export hub for the sibling submodules, so imports here are
# intentionally unused locally.
# ruff: noqa: F401

from ...templating import TEMPLATES_DIR

import html
import json
import logging
import os
from typing import List, Optional
from fastapi import APIRouter, Form, HTTPException, Request
from fastapi.responses import HTMLResponse, JSONResponse
from fastapi.templating import Jinja2Templates
from ...audit import (
    _record_amendment_audit,
    _record_change_audit,
    _record_enrollment_audit,
    _record_geo_scope_audit,
    _record_gov_date_audit,
    _record_impact_audit,
    _record_reason_audit,
    _record_ref_audit,
)
from ...codelist_config import (
    AMENDMENT_IMPACT_TYPE_CODELIST,
    AMENDMENT_REASON_CODELIST,
    GEOGRAPHIC_SCOPE_TYPE_CODELIST,
    GOVERNANCE_DATE_TYPE_CODELIST,
)
from ...db import _connect
from ...schemas import (
    DocumentContentReferenceCreate,
    GeographicScopeCreate,
    GovernanceDateCreate,
    StudyAmendmentCreate,
    StudyAmendmentImpactCreate,
    StudyAmendmentReasonCreate,
    StudyAmendmentUpdate,
    StudyChangeCreate,
    SubjectEnrollmentCreate,
)
from ...utils import (
    get_ddf_ct_codelist_map,
    get_latest_ddf_ct_href,
    get_next_code_uid,
    soa_exists,
)

router = APIRouter()
ui_router = APIRouter()
logger = logging.getLogger("soa_builder.web.routers.amendments")
templates = Jinja2Templates(directory=TEMPLATES_DIR)
_COUNTRY_SUBMISSION_VALUE = "Country"
_REGION_SUBMISSION_VALUE = "Region"
