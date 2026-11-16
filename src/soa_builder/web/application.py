"""Composition root: the FastAPI app, its routers and startup.

Importing this module initialises the database and runs pending
migrations (see migrations/README.md), then registers routers. Order
matters: Starlette matches routes in registration order, and the
routers moved out of app.py are registered last, as their routes were
before (tests/test_route_shadowing.py guards this).
"""

import logging
import os

from dotenv import load_dotenv
from fastapi import FastAPI
from fastapi.staticfiles import StaticFiles

from .db import DB_PATH as _DB_PATH
from .initialize_database import _init_db
from .lifespan import lifespan
from .migrations.runner import run_pending_migrations
from .routers import activities as activities_router
from .routers import arms as arms_router
from .routers import elements as elements_router
from .routers import epochs as epochs_router
from .routers import freezes as freezes_router
from .routers import rollback as rollback_router
from .routers import visits as visits_router
from .routers import audits as audits_router
from .routers import rules as rules_router
from .routers import timings as timings_router
from .routers import schedule_timelines as schedule_timelines_router
from .routers import cells as cells_router
from .routers import instances as instances_router
from .routers import usdm_json as usdm_json_router
from .routers import tdd as tdd_router
from .routers import dair as dair_router
from .routers import decision_instances as decision_instances_router
from .routers import condition_assignments as condition_assignments_router
from .routers import footnotes as footnotes_router
from .routers import bc_surrogates as bc_surrogates_router
from .routers import concept_groups as concept_groups_router
from .routers import bc_categories as bc_categories_router
from .routers import sdtm_terminology as sdtm_terminology_router
from .routers import cdash_terminology as cdash_terminology_router
from .routers import define_xml_terminology as define_xml_terminology_router
from .routers import (
    protocol_controlled_terminology as protocol_controlled_terminology_router,
)
from .routers import (
    ddf_controlled_terminology as ddf_controlled_terminology_router,
)
from .routers import objectives as objectives_router
from .routers import study_titles as study_titles_router
from .routers import endpoints as endpoints_router
from .routers import amendments as amendments_router
from .routers import organizations as organizations_router
from .routers import roles as roles_router
from .routers import study_interventions as study_interventions_router
from .routers import estimands as estimands_router
from .routers import indications as indications_router
from .routers import persons as persons_router
from .routers import soa_bundle as soa_bundle_router
from .routers import study_identifiers as study_identifiers_router
from .routers import concepts_admin as concepts_admin_router
from .routers import exports as exports_router
from .routers import soa_core as soa_core_router
from .routers import soa_ui as soa_ui_router
from .templating import STATIC_DIR


def _configure_logging():
    level = logging.INFO
    if os.environ.get("SOA_BUILDER_DEBUG") == "1":
        level = logging.DEBUG
    logging.basicConfig(
        level=level, format="%(asctime)s %(name)s %(levelname)s: %(message)s"
    )
    # Quiet noisy libraries if present
    logging.getLogger("sqlalchemy").setLevel(logging.WARNING)
    return logging.getLogger("soa_builder")


_configure_logging()

load_dotenv("config.env")  # must come BEFORE reading env-based config
# Use the DB path resolved by db.py to keep consistency across modules
DB_PATH = _DB_PATH

# Server bind address / port (override via config.env)
HTTP_LISTEN_PORT = int(os.environ.get("SOA_BUILDER_PORT", "8008"))
HTTP_LISTEN_IP = os.environ.get("SOA_BUILDER_HOST", "0.0.0.0")

app = FastAPI(title="SoA Builder API", version="0.1.0")
logger = logging.getLogger("soa_builder.concepts")
if not logger.handlers:
    _h = logging.StreamHandler()
    _h.setFormatter(logging.Formatter("[%(asctime)s] %(levelname)s %(message)s"))
    logger.addHandler(_h)
logger.setLevel(logging.INFO)

app.mount("/static", StaticFiles(directory=STATIC_DIR), name="static")

# Database bootstrap
_init_db()

# Database migrations: see migrations/README.md
run_pending_migrations()

# Include routers
app.include_router(arms_router.router)
app.include_router(elements_router.router)
app.include_router(visits_router.router)
app.include_router(activities_router.router)
app.include_router(activities_router.ui_router)
app.include_router(epochs_router.router)
app.include_router(freezes_router.router)
app.include_router(rollback_router.router)
app.include_router(timings_router.router)
app.include_router(instances_router.router)
app.include_router(audits_router.router)
app.include_router(schedule_timelines_router.router)
app.include_router(rules_router.router)
app.include_router(cells_router.router)
app.include_router(usdm_json_router.router)
app.include_router(tdd_router.router)
app.include_router(dair_router.router)
app.include_router(decision_instances_router.router)
app.include_router(condition_assignments_router.router)
app.include_router(footnotes_router.router)
app.include_router(footnotes_router.ui_router)
app.include_router(bc_surrogates_router.router)
app.include_router(bc_surrogates_router.ui_router)
app.include_router(concept_groups_router.router)
app.include_router(concept_groups_router.ui_router)
app.include_router(bc_categories_router.ui_router)
app.include_router(sdtm_terminology_router.router)
app.include_router(cdash_terminology_router.router)
app.include_router(define_xml_terminology_router.router)
app.include_router(protocol_controlled_terminology_router.router)
app.include_router(ddf_controlled_terminology_router.router)
app.include_router(objectives_router.router)
app.include_router(objectives_router.ui_router)
app.include_router(study_titles_router.router)
app.include_router(endpoints_router.router)
app.include_router(endpoints_router.ui_router)
app.include_router(amendments_router.router)
app.include_router(amendments_router.ui_router)
app.include_router(organizations_router.router)
app.include_router(organizations_router.ui_router)
app.include_router(roles_router.router)
app.include_router(roles_router.ui_router)
app.include_router(study_interventions_router.router)
app.include_router(study_interventions_router.ui_router)
app.include_router(estimands_router.router)
app.include_router(estimands_router.ui_router)
app.include_router(indications_router.router)
app.include_router(indications_router.ui_router)
app.include_router(persons_router.router)
app.include_router(persons_router.ui_router)
app.include_router(soa_bundle_router.router)
app.include_router(soa_bundle_router.ui_router)
app.include_router(study_identifiers_router.router)
app.include_router(study_identifiers_router.ui_router)
# Routes that used to be defined in app.py, registered after the
# routers above exactly as before.
app.include_router(soa_core_router.router)
app.include_router(exports_router.router)
app.include_router(concepts_admin_router.router)
app.include_router(soa_ui_router.router)

app.router.lifespan_context = lifespan


def main():
    import uvicorn

    uvicorn.run(
        "soa_builder.web.app:app",
        host=HTTP_LISTEN_IP,
        port=HTTP_LISTEN_PORT,
        reload=True,
    )
