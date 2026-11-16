"""FastAPI lifespan: startup tasks run in the background."""

import logging
from .services import concepts as concepts_service
from .initialize_database import _connect
from contextlib import asynccontextmanager
from fastapi import FastAPI
from .services.concepts import _enrich_code_bg

logger = logging.getLogger("soa_builder.concepts")


@asynccontextmanager
async def lifespan(app: FastAPI):
    """FastAPI lifespan context replacing deprecated startup event.

    Preloads cached terminology datasets (biomedical concepts and SDTM dataset
    specializations) so first request uses warm caches. Errors are logged but
    never raised to avoid blocking application startup.
    """
    try:
        concepts = concepts_service.fetch_biomedical_concepts(force=True)
        logger.info("Lifespan preload concepts count=%d", len(concepts))
    except Exception as e:
        logger.error("Lifespan concept preload failed: %s", e)
    try:
        sdtm_specs = concepts_service.fetch_sdtm_specializations(force=True)
        logger.info("Lifespan preload SDTM specializations count=%d", len(sdtm_specs))
    except Exception as e:
        logger.error("Lifespan SDTM specializations preload failed: %s", e)
    try:
        crf_specs = concepts_service.fetch_crf_specializations(force=True)
        logger.info("Lifespan preload CRF specializations count=%d", len(crf_specs))
    except Exception as e:
        logger.error("Lifespan CRF specializations preload failed: %s", e)
    try:
        from .routers.concept_groups import sync_cdisc_concept_groups

        summary = sync_cdisc_concept_groups(force=True)
        logger.info("Lifespan CDISC concept-group sync: %s", summary)
    except Exception as e:
        logger.error("Lifespan CDISC concept-group sync failed: %s", e)
    try:
        import threading
        from concurrent.futures import ThreadPoolExecutor

        _conn = _connect()
        _cur = _conn.cursor()
        _cur.execute(
            "SELECT code, soa_id FROM code"
            " WHERE code_system IS NULL AND code IS NOT NULL"
        )
        _unenriched = _cur.fetchall()
        _conn.close()
        if _unenriched:
            logger.info(
                "Lifespan scheduling enrichment for %d unenriched code rows",
                len(_unenriched),
            )

            def _run_enrichment_pool(_rows=_unenriched):
                with ThreadPoolExecutor(
                    max_workers=1
                ) as _pool:  # concurrency was reduced for rate-limiting
                    for _concept_code, _soa_id in _rows:
                        _pool.submit(_enrich_code_bg, _concept_code, _soa_id)

            threading.Thread(target=_run_enrichment_pool, daemon=True).start()
    except Exception as e:
        logger.error("Lifespan code enrichment startup failed: %s", e)
    try:
        from usdm.generate_biomedical_concept_properties import (
            sweep_orphaned_bcp_rows,
        )

        swept = sweep_orphaned_bcp_rows()
        logger.info("Lifespan orphaned BCP sweep removed %s", swept)
    except Exception as e:
        logger.error("Lifespan orphaned BCP sweep failed: %s", e)
    yield
