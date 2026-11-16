"""Biomedical concept, category and specialization browser pages."""

from ..errors import REMOTE_PAYLOAD_ERRORS
import logging
from ..services import concepts as concepts_service
from fastapi import APIRouter
from ..utils import get_cdisc_api_key as _get_cdisc_api_key
from ..utils import get_concepts_override as _get_concepts_override
from ..utils import soa_exists
from fastapi import HTTPException
from fastapi import Request
from fastapi.responses import HTMLResponse
from fastapi.responses import StreamingResponse
from typing import Optional
import csv
import io
import json
import os
import requests
import time
from ..services.concepts import CDISC_BC_API_BASE_URL
from ..services.concepts import _HTTP_TIMEOUT
from ..services.concepts import _compute_unassigned_concepts
from ..services.concepts import _concept_cache
from ..services.concepts import _crf_specializations_cache
from ..services.concepts import _sdtm_specializations_cache
from ..templating import templates

logger = logging.getLogger("soa_builder.concepts")
router = APIRouter()


# UI endpoint to refrech the biomedical concepts cache
@router.post("/ui/soa/{soa_id}/concepts_refresh")
def ui_refresh_concepts(request: Request, soa_id: int):
    """Fetch Biomedical Concepts; refresh cache"""
    if not soa_exists(soa_id):
        raise HTTPException(404, "SOA not found")
    concepts_service.fetch_biomedical_concepts(force=True)
    # If HTMX request, use HX-Redirect header for clean redirect without injecting script
    if request.headers.get("HX-Request") == "true":
        return HTMLResponse("", headers={"HX-Redirect": f"/ui/soa/{soa_id}/edit"})
    # Fallback: plain form POST non-htmx redirect via script
    return HTMLResponse(
        f"<script>window.location='/ui/soa/{int(soa_id)}/edit';</script>"
    )


@router.get("/concepts/status")
def concepts_status():
    """Return diagnostics for Biomedical Concepts fetch/cache."""
    return {
        "count": len(_concept_cache.get("data") or []),
        "fetched_at": _concept_cache.get("fetched_at"),
        "cache_age_sec": (
            (time.time() - _concept_cache.get("fetched_at", 0))
            if _concept_cache.get("fetched_at")
            else None
        ),
        "last_status": _concept_cache.get("last_status"),
        "last_error": _concept_cache.get("last_error"),
        "api_key_present": bool(_get_cdisc_api_key()),
        "override_present": bool(_get_concepts_override()),
        "skip_remote": os.environ.get("CDISC_SKIP_REMOTE") == "1",
    }


@router.get("/sdtm/specializations/status")
def sdtm_specializations_status():
    """Return diagnostics for SDTM dataset specializations fetch/cache."""
    data = _sdtm_specializations_cache.get("data") or []
    fetched_at = _sdtm_specializations_cache.get("fetched_at")
    age = (time.time() - fetched_at) if fetched_at else None
    sample = data[:3]
    return {
        "count": len(data),
        "fetched_at": fetched_at,
        "cache_age_sec": age,
        "last_status": _sdtm_specializations_cache.get("last_status"),
        "last_error": _sdtm_specializations_cache.get("last_error"),
        "last_url": _sdtm_specializations_cache.get("last_url"),
        "api_key_present": bool(_get_cdisc_api_key()),
        "skip_remote": os.environ.get("CDISC_SKIP_REMOTE") == "1",
        "override_present": bool(os.environ.get("CDISC_SDTM_SPECIALIZATIONS_JSON")),
        "sample": sample,
    }


@router.get("/ui/sdtm/specializations/status", response_class=HTMLResponse)
def ui_sdtm_specializations_status(request: Request):
    """HTML wrapper for SDTM specializations diagnostics."""
    data = sdtm_specializations_status()
    return templates.TemplateResponse(
        request,
        "sdtm_specializations_status.html",
        data,
    )


@router.post("/ui/sdtm/specializations/refresh", response_class=HTMLResponse)
def ui_sdtm_specializations_refresh(request: Request):
    """Force refresh of SDTM specializations cache and redirect back to list."""
    concepts_service.fetch_sdtm_specializations(force=True)
    # HX redirect support
    if request.headers.get("HX-Request") == "true":
        return HTMLResponse("", headers={"HX-Redirect": "/ui/sdtm/specializations"})
    return HTMLResponse("<script>window.location='/ui/sdtm/specializations';</script>")


@router.get("/ui/cdisc-api-status", response_class=HTMLResponse)
def ui_cdisc_api_status(request: Request):
    """Return HTML partial indicating CDISC Library API availability."""
    api_key = os.environ.get("CDISC_API_KEY")
    subscription_key = os.environ.get("CDISC_SUBSCRIPTION_KEY")
    skip_remote = os.environ.get("CDISC_SKIP_REMOTE") == "1"
    has_key = bool(api_key or subscription_key)

    if skip_remote:
        status, detail = (
            "offline",
            (
                "CDISC API calls are disabled (CDISC_SKIP_REMOTE=1). "
                "Local overrides are active."
            ),
        )
    elif not has_key:
        status, detail = (
            "no_key",
            (
                "No CDISC API key configured. "
                "Set CDISC_API_KEY or CDISC_SUBSCRIPTION_KEY in your "
                "shell environment to enable Biomedical Concept features."
            ),
        )
    else:
        unified = subscription_key or api_key
        headers = {}
        if unified:
            headers["Ocp-Apim-Subscription-Key"] = unified
        if api_key:
            headers["Authorization"] = f"Bearer {api_key}"
            headers["api-key"] = api_key
        try:
            resp = requests.get(CDISC_BC_API_BASE_URL, headers=headers, timeout=5)
            if resp.status_code < 500:
                status, detail = "ok", "CDISC Library API is connected."
            else:
                status = "error"
                detail = f"CDISC Library API returned HTTP {resp.status_code}."
        except requests.exceptions.Timeout:
            status, detail = (
                "error",
                (
                    "CDISC Library API timed out. "
                    "The service may be temporarily unavailable."
                ),
            )
        except requests.exceptions.ConnectionError:
            status, detail = (
                "error",
                ("CDISC Library API is not reachable. Check your network connection."),
            )
        except requests.RequestException as exc:
            logger.warning("CDISC API connectivity check failed: %s", exc)
            status, detail = (
                "error",
                "CDISC API check failed. See server logs for details.",
            )

    return templates.TemplateResponse(
        request,
        "cdisc_api_status.html",
        {"status": status, "detail": detail},
    )


# UI endpoint for listing BCs
@router.get("/ui/concepts", response_class=HTMLResponse)
def ui_concepts_list(request: Request):
    """Render table listing biomedical concepts (title + href)."""
    concepts = concepts_service.fetch_biomedical_concepts(force=True) or []
    rows = []
    for c in concepts:
        code = c.get("concept_code") or c.get("code")
        title = c.get("title") or c.get("concept_title") or c.get("name") or code
        href = (
            f"{CDISC_BC_API_BASE_URL}/mdr/bc/biomedicalconcepts/{code}"
            if code
            else None
        )
        rows.append({"code": code, "title": title, "href": href})
    subscription_key = os.environ.get("CDISC_SUBSCRIPTION_KEY") or _get_cdisc_api_key()
    return templates.TemplateResponse(
        request,
        "concepts_list.html",
        {
            "rows": rows,
            "count": len(rows),
            "missing_key": subscription_key is None,
        },
    )


# UI endpoints for Unassigned Biomedical Concepts report
@router.get("/ui/bc/unassigned-concepts", response_class=HTMLResponse)
def ui_bc_unassigned_concepts(request: Request):
    """Page for the Unassigned Biomedical Concepts report."""
    return templates.TemplateResponse(
        request,
        "bc_unassigned_concepts.html",
        {},
    )


@router.post("/ui/bc/unassigned-concepts/generate", response_class=HTMLResponse)
def ui_bc_unassigned_concepts_generate(request: Request):
    """HTMX endpoint — compute and return inline results partial."""
    concepts = _compute_unassigned_concepts()
    return templates.TemplateResponse(
        request,
        "bc_unassigned_results.html",
        {"concepts": concepts, "count": len(concepts)},
    )


@router.get("/ui/bc/unassigned-concepts/export/csv")
def ui_bc_unassigned_concepts_export_csv():
    """Download unassigned concepts as CSV."""
    concepts = _compute_unassigned_concepts()
    out = io.StringIO()
    writer = csv.DictWriter(out, fieldnames=["code", "title"])
    writer.writeheader()
    writer.writerows({"code": c["code"], "title": c.get("title", "")} for c in concepts)
    buf = io.BytesIO(out.getvalue().encode("utf-8"))
    return StreamingResponse(
        buf,
        media_type="text/csv",
        headers={
            "Content-Disposition": ('attachment; filename="bc_unassigned_concepts.csv"')
        },
    )


# UI endpoint for listing BC Categories
@router.get("/ui/concept_categories", response_class=HTMLResponse)
def ui_categories_list(request: Request, force: bool = False):
    """Render table listing biomedical concept categories (name + title + href)."""
    categories = concepts_service.fetch_biomedical_concept_categories(force=force) or []
    rows = [
        {
            "name": c.get("name"),
            "title": c.get("title") or c.get("name"),
            "href": c.get("href"),
        }
        for c in categories
    ]
    subscription_key = os.environ.get("CDISC_SUBSCRIPTION_KEY") or _get_cdisc_api_key()
    return templates.TemplateResponse(
        request,
        "concept_categories.html",
        {
            "force": force,
            "rows": rows,
            "count": len(rows),
            "missing_key": subscription_key is None,
        },
    )


# UI endpint for displaying BC Category
@router.get("/ui/concept_categories/view", response_class=HTMLResponse)
def ui_category_detail(request: Request, name: str = "", force: bool = False):
    """Render list of biomedical concepts within a given category name.

    Query params:
      name: category name as returned by /ui/concept_categories.
    """
    category_name = name.strip()
    if not category_name:
        return HTMLResponse(
            "<p><em>Category name required.</em></p><p><a href='/ui/concept_categories'>Back</a></p>"
        )
    concepts = (
        concepts_service.fetch_biomedical_concepts_by_category(
            category_name, force=force
        )
        or []
    )
    rows = [
        {
            "code": c.get("code"),
            "title": c.get("title"),
            "href": c.get("href"),
        }
        for c in concepts
    ]
    return templates.TemplateResponse(
        request,
        "concept_category_detail.html",
        {
            "category": category_name,
            "force": force,
            "rows": rows,
            "count": len(rows),
        },
    )


# UI endpoint for displaying SDTM specializations
@router.get("/ui/sdtm/specializations", response_class=HTMLResponse)
def ui_sdtm_specializations_list(request: Request, code: Optional[str] = None):
    """Render table listing SDTM dataset specializations (title + API link).

    If `code` is provided as a query parameter, each href will include
    ?biomedicalconcept={code} (or &biomedicalconcept=... when a query string already exists).
    """
    packages = concepts_service.fetch_sdtm_specializations(force=True, code=code) or []
    rows = [
        {"title": p.get("title") or "(untitled)", "href": p.get("href")}
        for p in packages
    ]
    subscription_key = os.environ.get("CDISC_SUBSCRIPTION_KEY") or _get_cdisc_api_key()
    # Diagnostics from cache for visibility when no data appears
    last_status = _sdtm_specializations_cache.get("last_status")
    last_error = _sdtm_specializations_cache.get("last_error")
    last_url = _sdtm_specializations_cache.get("last_url")
    return templates.TemplateResponse(
        request,
        "sdtm_specializations.html",
        {
            "rows": rows,
            "count": len(rows),
            "missing_key": subscription_key is None,
            "last_status": last_status,
            "last_error": last_error,
            "last_url": last_url,
            "code": code,
        },
    )


# UI endpoint for displaying a selected SDTM specialization
@router.get("/ui/sdtm/specializations/{idx}", response_class=HTMLResponse)
def ui_sdtm_specialization_detail(
    idx: int,
    request: Request,
    code: Optional[str] = None,  # NEW: propagate code filter from query string
):
    """Detail page for a single SDTM dataset specialization.

    Lookup by index into the (optionally filtered) list.
    """
    packages = (
        concepts_service.fetch_sdtm_specializations(force=True, code=code) or []
    )  # <-- pass code
    if idx < 0 or idx >= len(packages):
        raise HTTPException(status_code=404, detail="Specialization index out of range")
    spec = packages[idx]
    title = spec.get("title") or "(untitled)"
    href = spec.get("href")

    api_key = _get_cdisc_api_key()
    subscription_key = os.environ.get("CDISC_SUBSCRIPTION_KEY")
    unified_key = subscription_key or api_key
    headers: dict[str, str] = {}
    if unified_key:
        headers["Ocp-Apim-Subscription-Key"] = unified_key
    if api_key:
        headers["Authorization"] = f"Bearer {api_key}"
        headers["api-key"] = api_key

    status = None
    error = None
    pretty_json = None
    raw_text_snippet = None
    if href:
        try:
            resp = requests.get(href, headers=headers, timeout=_HTTP_TIMEOUT)
            status = resp.status_code
            raw_text_snippet = resp.text[:500]
            if resp.status_code == 200:
                try:
                    data = resp.json()
                except ValueError:
                    error = "200 OK but response was not valid JSON"
                    data = None
                if data is not None:
                    try:
                        pretty_json = json.dumps(data, indent=2, sort_keys=True)
                    except TypeError:
                        pretty_json = json.dumps(data, indent=2)
            else:
                error = f"HTTP {resp.status_code} retrieving specialization"
        except REMOTE_PAYLOAD_ERRORS as e:
            error = f"Fetch error: {e}"[:300]
    else:
        error = "No href available for this specialization entry."

    return templates.TemplateResponse(
        request,
        "sdtm_specialization_detail.html",
        {
            "index": idx,
            "title": title,
            "href": href,
            "status": status,
            "error": error,
            "pretty_json": pretty_json,
            "raw_text_snippet": raw_text_snippet,
            "missing_key": unified_key is None,
            "total": len(packages),
            "code": code,  # optional: for breadcrumb/back link
        },
    )


@router.get("/ui/crf/specializations", response_class=HTMLResponse)
def ui_crf_specializations_list(request: Request):
    """List all available CRF specializations."""
    packages = concepts_service.fetch_crf_specializations(force=True) or []
    rows = [
        {"title": p.get("title") or "(untitled)", "href": p.get("href")}
        for p in packages
    ]
    subscription_key = os.environ.get("CDISC_SUBSCRIPTION_KEY") or _get_cdisc_api_key()
    last_status = _crf_specializations_cache.get("last_status")
    last_error = _crf_specializations_cache.get("last_error")
    last_url = _crf_specializations_cache.get("last_url")
    # Extract the /mdr/... path from the resolved specializations URL for display
    from urllib.parse import urlparse as _up

    _p = _up(last_url or "")
    spec_path = _p.path or "/mdr/specializations/crf/specializations"
    return templates.TemplateResponse(
        request,
        "crf_specializations.html",
        {
            "rows": rows,
            "count": len(rows),
            "missing_key": subscription_key is None,
            "last_status": last_status,
            "last_error": last_error,
            "last_url": last_url,
            "spec_path": spec_path,
        },
    )


@router.post("/ui/crf/specializations/refresh", response_class=HTMLResponse)
def ui_crf_specializations_refresh(request: Request):
    """Force refresh of CRF specializations cache and redirect back to list."""
    concepts_service.fetch_crf_specializations(force=True)
    if request.headers.get("HX-Request") == "true":
        return HTMLResponse("", headers={"HX-Redirect": "/ui/crf/specializations"})
    return HTMLResponse("<script>window.location='/ui/crf/specializations';</script>")


@router.get("/ui/crf/specializations/{idx}", response_class=HTMLResponse)
def ui_crf_specialization_detail(idx: int, request: Request):
    """Detail page for a single CRF specialization (tabular + raw JSON)."""
    packages = concepts_service.fetch_crf_specializations() or []
    if idx < 0 or idx >= len(packages):
        raise HTTPException(
            status_code=404, detail="CRF specialization index out of range"
        )
    spec = packages[idx]
    title = spec.get("title") or "(untitled)"
    href = spec.get("href")

    api_key = _get_cdisc_api_key()
    subscription_key = os.environ.get("CDISC_SUBSCRIPTION_KEY")
    unified_key = subscription_key or api_key
    headers: dict[str, str] = {}
    if unified_key:
        headers["Ocp-Apim-Subscription-Key"] = unified_key
    if api_key:
        headers["Authorization"] = f"Bearer {api_key}"
        headers["api-key"] = api_key

    spec_data = None
    spec_json = None
    error = None
    status = None
    if href:
        try:
            resp = requests.get(href, headers=headers, timeout=_HTTP_TIMEOUT)
            status = resp.status_code
            if resp.status_code == 200:
                try:
                    spec_data = resp.json()
                    spec_json = json.dumps(spec_data, indent=2, sort_keys=True)
                except ValueError:
                    error = "200 OK but response was not valid JSON"
            else:
                error = f"HTTP {resp.status_code} retrieving CRF specialization"
        except REMOTE_PAYLOAD_ERRORS as e:
            error = f"Fetch error: {e}"[:300]
    else:
        error = "No href available for this CRF specialization entry."

    return templates.TemplateResponse(
        request,
        "crf_specialization_detail.html",
        {
            "index": idx,
            "title": title,
            "href": href,
            "status": status,
            "error": error,
            "spec_data": spec_data,
            "spec_json": spec_json,
            "missing_key": unified_key is None,
            "total": len(packages),
        },
    )


# UI endpoint for displaying a selected BC
@router.get("/ui/concepts/{code}", response_class=HTMLResponse)
def ui_concept_detail(code: str, request: Request):
    """Detail page for a single biomedical concept. Fetches concept JSON from CDISC Library API,
    extracts title, canonical href, parentBiomedicalConcept href (if any), and parentPackage href.
    """
    # Build concept API URL
    api_href = (
        f"https://api.library.cdisc.org/api/cosmos/v2/mdr/bc/biomedicalconcepts/{code}"
    )
    headers = {}
    api_key = _get_cdisc_api_key()
    subscription_key = os.environ.get("CDISC_SUBSCRIPTION_KEY")
    # Some deployments use a single key; if only one provided, reuse it for both header styles
    unified_key = subscription_key or api_key
    if unified_key:
        headers["Ocp-Apim-Subscription-Key"] = unified_key
    if api_key:
        headers["Authorization"] = f"Bearer {api_key}"
        headers["api-key"] = api_key
    concept_json = None
    parent_bc_href = None
    parent_pkg_href = None
    parent_bc_title = None
    parent_pkg_name = None
    status = None
    try:
        resp = requests.get(api_href, headers=headers, timeout=_HTTP_TIMEOUT)
        status = resp.status_code
        if resp.status_code == 200:
            concept_json = resp.json()
            # Extract parent biomedical concept link if present
            parent_bc_href = concept_json.get(
                "parentBiomedicalConcept"
            ) or concept_json.get("parent_biomedical_concept")
            if isinstance(parent_bc_href, dict):
                parent_bc_title = parent_bc_href.get("title") or parent_bc_href.get(
                    "name"
                )
                parent_bc_href = parent_bc_href.get("href") or parent_bc_href.get("url")
            # Extract parent package link
            parent_pkg_obj = concept_json.get("parentPackage") or concept_json.get(
                "parent_package"
            )
            if isinstance(parent_pkg_obj, dict):
                parent_pkg_name = parent_pkg_obj.get("name") or parent_pkg_obj.get(
                    "title"
                )
                parent_pkg_href = parent_pkg_obj.get("href") or parent_pkg_obj.get(
                    "url"
                )
            elif isinstance(parent_pkg_obj, str):
                parent_pkg_href = parent_pkg_obj
        else:
            concept_json = {"error": f"Upstream returned {resp.status_code}"}
    except REMOTE_PAYLOAD_ERRORS as e:  # pragma: no cover
        concept_json = {"error": f"Request failed: {e}"}
    title = None
    if concept_json:
        title = (
            concept_json.get("title")
            or concept_json.get("concept_title")
            or concept_json.get("name")
            or code
        )

    # Build summary dict (scalar top-level fields only, truthy values)
    summary: dict = {}
    data_element_concepts: list = []
    if isinstance(concept_json, dict) and "error" not in concept_json:
        scalar_keys = (
            "conceptId",
            "shortName",
            "definition",
            "href",
        )
        for key in scalar_keys:
            val = concept_json.get(key)
            if val not in (None, ""):
                summary[key] = val
        list_str_keys = ("synonyms", "categories", "resultScales")
        for key in list_str_keys:
            val = concept_json.get(key)
            if isinstance(val, list) and val:
                summary[key] = ", ".join(str(item) for item in val if item is not None)
        coding = concept_json.get("coding")
        if isinstance(coding, list) and coding:
            try:
                parts = []
                for entry in coding:
                    if isinstance(entry, dict):
                        system = entry.get("system", "")
                        ccode = entry.get("code", "")
                        display = entry.get("display", "")
                        parts.append(f"{system}:{ccode} ({display})".strip())
                if parts:
                    summary["coding"] = "; ".join(parts)
                else:
                    summary["coding"] = f"{len(coding)} entries"
            except (AttributeError, TypeError):
                summary["coding"] = f"{len(coding)} entries"
        # Extract data element concepts array
        raw_decs = concept_json.get("dataElementConcepts") or []
        if isinstance(raw_decs, list):
            data_element_concepts = [d for d in raw_decs if isinstance(d, dict)]

    pretty_json = (
        json.dumps(concept_json, indent=2, sort_keys=True) if concept_json else None
    )

    return templates.TemplateResponse(
        request,
        "concept_detail.html",
        {
            "code": code,
            "title": title,
            "api_href": api_href,
            "parent_bc_href": parent_bc_href,
            "parent_bc_title": parent_bc_title,
            "parent_pkg_href": parent_pkg_href,
            "parent_pkg_name": parent_pkg_name,
            "status": status,
            "summary": summary,
            "data_element_concepts": data_element_concepts,
            "pretty_json": pretty_json,
            "missing_key": unified_key is None,
        },
    )
