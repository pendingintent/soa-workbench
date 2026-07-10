"""Configurable CDISC CT codelist codes for UI dropdowns.

Each constant identifies the CDISC controlled terminology codelist
whose terms populate a specific dropdown in the UI. Override any of
these via the corresponding ``CODELIST_*`` variable in ``config.env``
to point that dropdown at an alternate/custom codelist.

This module calls ``load_dotenv("config.env")`` itself (mirroring
``db.py``/``migrate_database.py``) so the configured values are
available regardless of module import order relative to
``app.py``'s own ``load_dotenv`` call.
"""

import os

from dotenv import load_dotenv

load_dotenv("config.env")

EPOCH_TYPE_CODELIST = os.environ.get("CODELIST_EPOCH_TYPE", "C99079")
ARM_TYPE_CODELIST = os.environ.get("CODELIST_ARM_TYPE", "C174222")
ARM_DATA_ORIGIN_TYPE_CODELIST = os.environ.get(
    "CODELIST_ARM_DATA_ORIGIN_TYPE", "C188727"
)
ROLE_TYPE_CODELIST = os.environ.get("CODELIST_ROLE_TYPE", "C215480")
ORGANIZATION_TYPE_CODELIST = os.environ.get("CODELIST_ORGANIZATION_TYPE", "C215480")
AMENDMENT_REASON_CODELIST = os.environ.get("CODELIST_AMENDMENT_REASON", "C207415")
AMENDMENT_IMPACT_TYPE_CODELIST = os.environ.get(
    "CODELIST_AMENDMENT_IMPACT_TYPE", "C215481"
)
GEOGRAPHIC_SCOPE_TYPE_CODELIST = os.environ.get(
    "CODELIST_GEOGRAPHIC_SCOPE_TYPE", "C207412"
)
GOVERNANCE_DATE_TYPE_CODELIST = os.environ.get(
    "CODELIST_GOVERNANCE_DATE_TYPE", "C207413"
)
STUDY_TITLE_TYPE_CODELIST = os.environ.get("CODELIST_STUDY_TITLE_TYPE", "C207419")
INTERVENTION_ROLE_CODELIST = os.environ.get("CODELIST_INTERVENTION_ROLE", "C207417")
INTERVENTION_TYPE_CODELIST = os.environ.get("CODELIST_INTERVENTION_TYPE", "C99078")
INTERVENTION_UNIT_CODELIST = os.environ.get("CODELIST_INTERVENTION_UNIT", "C66781")
OBJECTIVE_LEVEL_CODELIST = os.environ.get("CODELIST_OBJECTIVE_LEVEL", "C188725")
ENDPOINT_LEVEL_CODELIST = os.environ.get("CODELIST_ENDPOINT_LEVEL", "C188726")
ENCOUNTER_ENVIRONMENTAL_SETTING_CODELIST = os.environ.get(
    "CODELIST_ENCOUNTER_ENVIRONMENTAL_SETTING", "C127262"
)
ENCOUNTER_CONTACT_MODE_CODELIST = os.environ.get(
    "CODELIST_ENCOUNTER_CONTACT_MODE", "C171445"
)
TIMING_TYPE_CODELIST = os.environ.get("CODELIST_TIMING_TYPE", "C201264")
TIMING_RELATIVE_TO_FROM_CODELIST = os.environ.get(
    "CODELIST_TIMING_RELATIVE_TO_FROM", "C201265"
)
