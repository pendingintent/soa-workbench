#!/usr/bin/env python3
"""Compatibility shim for ``soa_builder.web.app``.

The FastAPI app now lives in ``application.py``; business logic in
``services/``; the endpoints formerly defined here in
``routers/soa_core.py``, ``exports.py``, ``concepts_admin.py`` and
``soa_ui.py``. This module keeps the import path stable: the
``soa-builder-web`` entry point (``soa_builder.web.app:main``), uvicorn
(``soa_builder.web.app:app``), tests and usdm generators import from
here. Add new code to the modules above, not here.
"""

from .application import DB_PATH, app, main  # noqa: F401
from .initialize_database import _connect  # noqa: F401
from .routers._freeze_helpers import _list_rollback_audit  # noqa: F401
from .services.concepts import (  # noqa: F401
    CDISC_BC_API_BASE_URL,
    CDISC_CRF_API_BASE_URL,
    _bc_categories_cache,
    _category_concepts_cache,
    _cleanup_orphaned_concept_rows,
    _crf_specializations_by_code_cache,
    _crf_specializations_cache,
    _enrich_biomedical_concept_bg,
    _enrich_code_bg,
    _get_concept_group_sections,
    _populate_biomedical_concept_properties_bg,
    _sdtm_specializations_cache,
    _upsert_biomedical_concept,
    fetch_biomedical_concept_categories,
    fetch_biomedical_concepts,
    fetch_biomedical_concepts_by_category,
    fetch_crf_specializations,
    fetch_sdtm_specializations,
)
from .services.matrix import _fetch_matrix, _list_reorder_audit  # noqa: F401
from .utils import get_next_code_uid as _get_next_code_uid  # noqa: F401

if __name__ == "__main__":
    main()
