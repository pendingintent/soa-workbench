"""Biomedical concept, specialization and code-chain services.

Moved from app.py; app.py re-exports the public names.
"""

import logging
from ..initialize_database import _connect
from ..utils import get_cdisc_api_key as _get_cdisc_api_key
from ..utils import get_cdisc_crf_api_key as _get_cdisc_crf_api_key
from ..utils import get_cdisc_crf_subscription_key as _get_cdisc_crf_subscription_key
from ..utils import get_concepts_override as _get_concepts_override
from ..utils import table_has_columns as _table_has_columns
from typing import Optional
import json
import os
import requests
import time
import urllib.parse

logger = logging.getLogger("soa_builder.concepts")


# CDISC Library Cosmos v2 API base URL (override via config.env)
CDISC_BC_API_BASE_URL = os.environ.get(
    "CDISC_BC_API_BASE_URL",
    "https://api.library.cdisc.org/api/cosmos/v2",
)


CDISC_CRF_API_BASE_URL = (
    os.environ.get(
        "CDISC_CRF_API_BASE_URL",
    )
    or CDISC_BC_API_BASE_URL
)


_concept_cache = {"data": None, "fetched_at": 0}


_CONCEPT_CACHE_TTL = int(os.environ.get("SOA_BUILDER_CACHE_TTL", "3600"))


# SDTM dataset specializations cache (similar TTL)
_sdtm_specializations_cache = {"data": None, "fetched_at": 0}


_SDTM_SPECIALIZATIONS_CACHE_TTL = _CONCEPT_CACHE_TTL


# Per-code SDTM specializations cache: {code: (fetched_at, [results])}
_sdtm_specializations_by_code_cache: dict[str, tuple[float, list]] = {}


# CRF specializations cache (full list)
_crf_specializations_cache = {"data": None, "fetched_at": 0}


_CRF_SPECIALIZATIONS_CACHE_TTL = _CONCEPT_CACHE_TTL


# Per-code CRF specializations cache: {code: (fetched_at, [results])}
_crf_specializations_by_code_cache: dict[str, tuple[float, list]] = {}


# Category-specific biomedical concepts cache (per category key)
_category_concepts_cache: dict[str, dict] = {}


_CATEGORY_CONCEPTS_CACHE_TTL = _CONCEPT_CACHE_TTL


# Biomedical concept categories cache (whole list)
_bc_categories_cache = {"data": None, "fetched_at": 0}


_BC_CATEGORIES_CACHE_TTL = _CONCEPT_CACHE_TTL


# Outbound HTTP timeout for CDISC Library API requests (seconds)
_HTTP_TIMEOUT = int(os.environ.get("CDISC_REQUEST_TIMEOUT", "15"))


def fetch_biomedical_concept_categories(force: bool = False) -> list[dict]:
    """Return list of Biomedical Concept Categories from CDISC Library.

    Normalized shape:
      [{'name': <category_name>, 'title': <title>, 'href': <absolute_href>}]
    """
    url = f"{CDISC_BC_API_BASE_URL}/mdr/bc/categories"
    base_prefix = CDISC_BC_API_BASE_URL
    headers = {"Accept": "application/json"}
    api_key = _get_cdisc_api_key()
    subscription_key = os.environ.get("CDISC_SUBSCRIPTION_KEY") or api_key
    # Some CDISC gateways require subscription key header, others accept bearer/api-key; send all when available.
    if subscription_key:
        headers["Ocp-Apim-Subscription-Key"] = subscription_key
    if api_key:
        headers["Authorization"] = f"Bearer {api_key}"  # bearer token style
        headers["api-key"] = api_key  # fallback header name

    def _normalize_href(h: Optional[str]) -> Optional[str]:
        if not h:
            return None
        if h.startswith("http://") or h.startswith("https://"):
            return h
        if h.startswith("/"):
            return base_prefix + h
        return base_prefix + "/" + h

    # Cache lookup
    now = time.time()
    if (
        not force
        and _bc_categories_cache.get("data")
        and now - _bc_categories_cache.get("fetched_at", 0) < _BC_CATEGORIES_CACHE_TTL
    ):
        return _bc_categories_cache.get("data") or []

    try:
        resp = requests.get(url, headers=headers, timeout=_HTTP_TIMEOUT)
        if resp.status_code != 200:
            logger.warning("BC categories fetch HTTP %s", resp.status_code)
            return []
        try:
            data = resp.json()
        except ValueError:
            logger.error("BC categories fetch 200 but non-JSON response")
            return []

        categories: list[dict] = []
        if (
            isinstance(data, dict)
            and "_links" in data
            and isinstance(data["_links"], dict)
        ):
            cat_list = data["_links"].get("categories") or []
            if isinstance(cat_list, list):
                for cat in cat_list:
                    if not isinstance(cat, dict):
                        continue
                    name = cat.get("name")
                    self_link = (cat.get("_links", {}) or {}).get("self") or {}
                    if not isinstance(self_link, dict):
                        self_link = {}
                    href = _normalize_href(self_link.get("href"))
                    title = self_link.get("title") or cat.get("label") or name or href
                    if name and href:
                        categories.append(
                            {
                                "name": str(name),
                                "title": str(title or name),
                                "href": href,
                            }
                        )
        categories.sort(key=lambda c: (c["title"] or "").lower())
        logger.info("Fetched %d BC categories from remote API", len(categories))
        _bc_categories_cache["data"] = categories
        _bc_categories_cache["fetched_at"] = now
        return categories
    except Exception as e:  # pragma: no cover
        logger.error("BC categories fetch error: %s", e)
        return []


def fetch_biomedical_concepts_by_category(name: str, force: bool = False) -> list[dict]:
    """Return biomedical concepts for a given category name.

    Uses category-specific endpoint: /mdr/bc/biomedicalconcepts?category=<name>
    Normalized list of dicts: {'code': <code>, 'title': <title>, 'href': <absolute_href>}
    Errors yield empty list; logs diagnostic info.
    """
    if not name or not name.strip():
        return []
    category = name.strip()
    base_prefix = CDISC_BC_API_BASE_URL
    # Deterministic single encoding: unquote once then re-encode
    decoded_once = urllib.parse.unquote(category)
    encoded = requests.utils.quote(decoded_once, safe="")
    url = f"{base_prefix}/mdr/bc/biomedicalconcepts?category={encoded}"
    headers = {"Accept": "application/json"}
    api_key = _get_cdisc_api_key()
    subscription_key = os.environ.get("CDISC_SUBSCRIPTION_KEY") or api_key
    if subscription_key:
        headers["Ocp-Apim-Subscription-Key"] = subscription_key
    if api_key:
        headers["Authorization"] = f"Bearer {api_key}"
        headers["api-key"] = api_key

    def _normalize_href(h: Optional[str]) -> Optional[str]:
        if not h:
            return None
        if h.startswith("http://") or h.startswith("https://"):
            return h
        if h.startswith("/"):
            return base_prefix + h
        return base_prefix + "/" + h

    # Cache lookup
    now = time.time()
    ckey = category.lower()
    if not force:
        cached = _category_concepts_cache.get(ckey)
        if cached and now - cached.get("fetched_at", 0) < _CATEGORY_CONCEPTS_CACHE_TTL:
            return cached.get("data", []) or []

    concepts: list[dict] = []
    try:
        resp = requests.get(url, headers=headers, timeout=_HTTP_TIMEOUT)
        if resp.status_code != 200:
            logger.warning(
                "BC concepts by category fetch HTTP %s",
                resp.status_code,
            )
            return []
        try:
            data = resp.json()
        except ValueError:
            logger.warning("BC concepts by category non-JSON response")
            return []

        # Strategy:
        # 1. If 'items' list present, treat as direct concept objects.
        # 2. Else if HAL '_links' present, scan all list-valued link groups for concept links.
        #    Recognize concept links by href containing '/mdr/bc/biomedicalconcepts/' or query '?concept=' style;
        #    derive code from link.get('code') or last path segment.
        # 3. Else if root is a single dict that looks like a concept, process it.
        root_items: list[dict] = []
        if isinstance(data, dict):
            # Direct items array
            if isinstance(data.get("items"), list):
                root_items = [it for it in data["items"] if isinstance(it, dict)]
            else:
                # HAL links exploration
                links = data.get("_links")
                if isinstance(links, dict):
                    # Collect potential lists under known or unknown keys
                    for key, val in links.items():
                        if key == "self":
                            continue
                        if isinstance(val, list):
                            for link in val:
                                if not isinstance(link, dict):
                                    continue
                                raw_href = link.get("href")
                                if not isinstance(raw_href, str):
                                    continue
                                href_norm = _normalize_href(raw_href)
                                # Identify concept link by path pattern
                                if "/mdr/bc/biomedicalconcepts" in raw_href:
                                    # Extract code (last path component before query) if not provided
                                    code = (
                                        link.get("code")
                                        or link.get("name")
                                        or link.get("identifier")
                                    )
                                    if not code:
                                        # Parse from path
                                        path_part = raw_href.split("?")[0].rstrip("/")
                                        code = path_part.split("/")[-1]
                                        # If code equals 'biomedicalconcepts' it is the list endpoint; skip
                                        if code == "biomedicalconcepts":
                                            code = None
                                    title = link.get("title") or code or href_norm
                                    if code and href_norm:
                                        concepts.append(
                                            {
                                                "code": str(code),
                                                "title": str(title),
                                                "href": href_norm,
                                            }
                                        )
                # Fallback single object
                if not concepts:
                    root_items = [data]
        elif isinstance(data, list):
            root_items = [it for it in data if isinstance(it, dict)]

        # Process root_items (non-HAL direct objects) if any
        for it in root_items:
            code = (
                it.get("code")
                or it.get("conceptCode")
                or it.get("identifier")
                or it.get("id")
            )
            href = _normalize_href(it.get("href") or it.get("link"))
            if not href and code:
                href = f"{base_prefix}/mdr/bc/biomedicalconcepts/{code}"
            title = it.get("title") or it.get("name") or it.get("label") or code
            if code and href:
                concepts.append({"code": str(code), "title": str(title), "href": href})

        if not concepts:
            logger.info("No biomedical concepts parsed for this category")
        concepts.sort(key=lambda c: c["title"].lower())
        logger.info("Fetched %d biomedical concepts for category", len(concepts))
        # Populate cache
        _category_concepts_cache[ckey] = {"data": concepts, "fetched_at": now}
        return concepts
    except Exception:  # pragma: no cover
        logger.exception("BC concepts by category fetch error")
        return []


def fetch_biomedical_concepts(force: bool = False):
    """Return list of biomedical concepts as [{'code':..., 'title':...}].
    Precedence: CDISC_CONCEPTS_JSON env override (for tests/offline) > cached remote fetch > empty list.
    Remote fetch uses CDISC_API_KEY header if present. Caches for TTL duration.
    """
    now = time.time()
    if (
        not force
        and _concept_cache["data"]
        and now - _concept_cache["fetched_at"] < _CONCEPT_CACHE_TTL
    ):
        return _concept_cache["data"]
    # Environment override
    override_json = _get_concepts_override()
    if override_json:
        try:
            raw = json.loads(override_json)
            items = (
                raw.get("items") if isinstance(raw, dict) and "items" in raw else raw
            )
            concepts = []
            for it in items:
                code = (
                    it.get("concept_code")
                    or it.get("code")
                    or it.get("conceptId")
                    or it.get("id")
                    or it.get("identifier")
                )
                title = it.get("title") or it.get("name") or it.get("label") or code
                if code:
                    concepts.append({"code": str(code), "title": str(title)})
            concepts.sort(key=lambda c: c["title"].lower())
            _concept_cache.update(data=concepts, fetched_at=now)
            logger.info("Loaded %d concepts from env override", len(concepts))
            return concepts
        except Exception:
            pass
    # Remote
    if os.environ.get("CDISC_SKIP_REMOTE") == "1":
        _concept_cache.update(data=[], fetched_at=now)
        logger.warning("CDISC_SKIP_REMOTE=1; concept list empty")
        return []
    url = f"{CDISC_BC_API_BASE_URL}/mdr/bc/biomedicalconcepts"
    headers = {"Accept": "application/json"}
    api_key = _get_cdisc_api_key()
    subscription_key = os.environ.get("CDISC_SUBSCRIPTION_KEY") or api_key
    # Some CDISC gateways require subscription key header, others accept bearer/api-key; send all when available.
    if subscription_key:
        headers["Ocp-Apim-Subscription-Key"] = subscription_key
    if api_key:
        headers["Authorization"] = f"Bearer {api_key}"  # bearer token style
        headers["api-key"] = api_key  # fallback header name
    try:
        resp = requests.get(url, headers=headers, timeout=_HTTP_TIMEOUT)
        _concept_cache["last_status"] = resp.status_code
        _concept_cache["last_url"] = url
        _concept_cache["last_error"] = None
        if resp.status_code == 200:
            try:
                data = resp.json()
            except ValueError:
                # Not JSON, likely HTML error despite 200
                _concept_cache["last_error"] = "200 but non-JSON response"
                logger.error("Concept fetch 200 but non-JSON body (url: %s)", url)
                return []

            # If JSON is a string, attempt second decode
            if isinstance(data, str):
                try:
                    data = json.loads(data)
                except Exception:
                    _concept_cache["last_error"] = (
                        "JSON value was a raw string; secondary parse failed"
                    )
                    logger.error(
                        "Concept fetch raw string JSON secondary parse failed (snippet: %s)",
                        str(data)[:200],
                    )
                    return []

            # Normalize possible shapes
            # Primary shapes: list of concept objects, dict with 'items', or HAL-style _links
            if (
                isinstance(data, dict)
                and "items" in data
                and isinstance(data["items"], list)
            ):
                items = data["items"]
            elif (
                isinstance(data, dict)
                and "_links" in data
                and isinstance(data["_links"], dict)
            ):
                # Extract from biomedicalConcepts links list
                links_list = data["_links"].get("biomedicalConcepts") or []
                items = []
                for link in links_list:
                    if not isinstance(link, dict):
                        continue
                    href = link.get("href")
                    title = link.get("title") or href
                    # Concept code may be last path segment
                    code = None
                    if href:
                        code = href.strip("/").split("/")[-1]
                    if code:
                        items.append({"concept_code": code, "title": title})
            elif isinstance(data, list):
                items = data
            elif isinstance(data, dict):
                # single concept object
                items = [data]
            else:
                _concept_cache["last_error"] = (
                    f"Unexpected JSON root type: {type(data).__name__}"
                )
                logger.error("Concept fetch unexpected JSON root type: %s", type(data))
                return []

            concepts = []
            for it in items:
                if not isinstance(it, dict):
                    continue  # skip non-dict entries
                code = (
                    it.get("concept_code")
                    or it.get("code")
                    or it.get("conceptId")
                    or it.get("id")
                    or it.get("identifier")
                )
                title = it.get("title") or it.get("name") or it.get("label") or code
                if code:
                    concepts.append({"code": str(code), "title": str(title)})
            concepts.sort(key=lambda c: c["title"].lower())
            _concept_cache.update(data=concepts, fetched_at=now)
            logger.info("Fetched %d concepts from remote API", len(concepts))
            return concepts
        else:
            _concept_cache["last_error"] = f"HTTP {resp.status_code}"
    except Exception as e:
        logger.error("Concept fetch error: %s", e)
        _concept_cache["last_error"] = str(e)
    _concept_cache.update(data=[], fetched_at=now)
    logger.warning("Concept list empty after fetch attempts")
    return []


def _compute_unassigned_concepts() -> list[dict]:
    """Return BC concepts not in any category, sorted by title."""
    all_concepts = {c["code"]: c for c in fetch_biomedical_concepts()}
    categories = fetch_biomedical_concept_categories()
    assigned_codes: set[str] = set()
    for cat in categories:
        for c in fetch_biomedical_concepts_by_category(cat["name"]):
            if c.get("code"):
                assigned_codes.add(c["code"])
    return sorted(
        [c for code, c in all_concepts.items() if code not in assigned_codes],
        key=lambda c: (c.get("title") or "").lower(),
    )


def fetch_sdtm_specializations(force: bool = False, code: Optional[str] = None):
    """Return list of SDTM dataset specializations as [{'title':..., 'href':...}].

    When `code` is None:
      - Fetch the full SDTM dataset specializations list.
      - Cache the normalized list in _sdtm_specializations_cache.

    When `code` is provided (e.g. 'C105585'):
      - Call the generic dataset specializations endpoint with
        ?biomedicalconcept={code}, which returns a HAL-style document.
      - Extract the SDTM subset from _links.datasetSpecializations.sdtm.
      - Do NOT use or update the main cache (code-specific result only).
    """
    now = time.time()
    base_prefix = CDISC_BC_API_BASE_URL

    def _normalize_href(h: Optional[str]) -> Optional[str]:
        if not h:
            return None
        if h.startswith("http://") or h.startswith("https://"):
            return h
        if h.startswith("/"):
            return base_prefix + h
        return base_prefix + "/" + h

    # --------- code-specific branch (use generic endpoint) ----------
    if code:
        # Return cached result if still fresh
        cached = _sdtm_specializations_by_code_cache.get(code)
        if (
            not force
            and cached is not None
            and (now - cached[0]) < _SDTM_SPECIALIZATIONS_CACHE_TTL
        ):
            return cached[1]

        url = (
            f"{base_prefix}/mdr/specializations/datasetspecializations"
            f"?biomedicalconcept={code}"
        )
        headers = {"Accept": "application/json"}
        api_key = _get_cdisc_api_key()
        subscription_key = os.environ.get("CDISC_SUBSCRIPTION_KEY") or api_key
        if subscription_key:
            headers["Ocp-Apim-Subscription-Key"] = subscription_key
        if api_key:
            headers["Authorization"] = f"Bearer {api_key}"
            headers["api-key"] = api_key

        try:
            resp = requests.get(url, headers=headers, timeout=_HTTP_TIMEOUT)
            _sdtm_specializations_cache["last_status"] = resp.status_code
            _sdtm_specializations_cache["last_url"] = url
            _sdtm_specializations_cache["last_error"] = None
            if resp.status_code != 200:
                _sdtm_specializations_cache["last_error"] = f"HTTP {resp.status_code}"
                logger.warning(
                    "SDTM specializations by BC code fetch HTTP %s for code=%s",
                    resp.status_code,
                    code,
                )
                return []
            try:
                data = resp.json()
            except ValueError:
                _sdtm_specializations_cache["last_error"] = "200 but non-JSON response"
                logger.warning(
                    "SDTM specializations by BC code non-JSON body for code=%s", code
                )
                return []

            # Expect HAL-style: _links.datasetSpecializations.sdtm is list of links
            packages: list[dict] = []
            if (
                isinstance(data, dict)
                and "_links" in data
                and isinstance(data["_links"], dict)
            ):
                ds = data["_links"].get("datasetSpecializations")
                if isinstance(ds, dict):
                    sdtm_links = ds.get("sdtm")
                    if isinstance(sdtm_links, list):
                        for link in sdtm_links:
                            if not isinstance(link, dict):
                                continue
                            href = _normalize_href(link.get("href"))
                            title = link.get("title") or href
                            packages.append({"title": title, "href": href})
            packages.sort(key=lambda p: p.get("title", "").lower())
            logger.info(
                "Fetched %d SDTM dataset specializations for biomedical concept %s",
                len(packages),
                code,
            )
            _sdtm_specializations_by_code_cache[code] = (now, packages)
            return packages
        except Exception as e:
            logger.error(
                "SDTM specializations by BC code fetch error for %s: %s", code, e
            )
            _sdtm_specializations_cache["last_error"] = str(e)
            return []

    # Cache only applies to full list
    if (
        not force
        and _sdtm_specializations_cache["data"]
        and now - _sdtm_specializations_cache["fetched_at"]
        < _SDTM_SPECIALIZATIONS_CACHE_TTL
    ):
        return _sdtm_specializations_cache["data"]

    # Env override branch
    override_json = os.environ.get("CDISC_SDTM_SPECIALIZATIONS_JSON")
    if override_json:
        try:
            raw = json.loads(override_json)
            if isinstance(raw, dict):
                if "items" in raw and isinstance(raw["items"], list):
                    items = raw["items"]
                elif "datasetSpecializations" in raw and isinstance(
                    raw["datasetSpecializations"], dict
                ):
                    items = list(raw["datasetSpecializations"].values())
                else:
                    items = [raw]
            elif isinstance(raw, list):
                items = raw
            else:
                items = []
            packages: list[dict] = []
            for it in items:
                if not isinstance(it, dict):
                    continue
                title_keys = [
                    "title",
                    "name",
                    "label",
                    "datasetLabel",
                    "datasetName",
                    "datasetSpecializationLabel",
                    "datasetSpecializationName",
                ]
                title = next((it.get(k) for k in title_keys if it.get(k)), "(untitled)")
                href = it.get("href") or it.get("link")
                if not href:
                    id_val = (
                        it.get("id")
                        or it.get("datasetSpecializationId")
                        or it.get("code")
                    )
                    if id_val:
                        href = f"{base_prefix}/mdr/specializations/sdtm/datasetspecializations/{id_val}"
                href = _normalize_href(href)
                packages.append({"title": title, "href": href})
            packages.sort(key=lambda p: p.get("title", "").lower())
            _sdtm_specializations_cache.update(data=packages, fetched_at=now)
            logger.info(
                "Loaded %d SDTM dataset specializations from override", len(packages)
            )
            return packages
        except Exception as e:
            logger.warning("SDTM override parse failed: %s", e)

    # Remote full-list branch
    if os.environ.get("CDISC_SKIP_REMOTE") == "1":
        _sdtm_specializations_cache.update(data=[], fetched_at=now)
        logger.warning("CDISC_SKIP_REMOTE=1; SDTM dataset specializations list empty")
        return []

    url = f"{base_prefix}/mdr/specializations/sdtm/datasetspecializations"  # full SDTM list
    headers = {"Accept": "application/json"}
    api_key = _get_cdisc_api_key()
    subscription_key = os.environ.get("CDISC_SUBSCRIPTION_KEY") or api_key
    if subscription_key:
        headers["Ocp-Apim-Subscription-Key"] = subscription_key
    if api_key:
        headers["Authorization"] = f"Bearer {api_key}"
        headers["api-key"] = api_key

    packages: list[dict] = []
    try:
        resp = requests.get(url, headers=headers, timeout=_HTTP_TIMEOUT)
        _sdtm_specializations_cache["last_status"] = resp.status_code
        _sdtm_specializations_cache["last_url"] = url
        _sdtm_specializations_cache["last_error"] = None
        if resp.status_code == 200:
            try:
                data = resp.json()
            except ValueError:
                _sdtm_specializations_cache["last_error"] = "200 but non-JSON response"
                data = None
            if isinstance(data, str):
                try:
                    data = json.loads(data)
                except Exception:
                    _sdtm_specializations_cache["last_error"] = (
                        "Raw string JSON secondary parse failed"
                    )
                    data = None
            items: list[dict] = []
            if isinstance(data, dict):
                if "items" in data and isinstance(data["items"], list):
                    items = data["items"]
                elif "_links" in data and isinstance(data["_links"], dict):
                    # HAL-style list via links
                    link_list = []
                    for key in (
                        "datasetSpecializations",
                        "datasetspecializations",
                        "packages",
                    ):
                        val = data["_links"].get(key)
                        if isinstance(val, list):
                            link_list = val
                            break
                    for link in link_list:
                        if not isinstance(link, dict):
                            continue
                        href = _normalize_href(link.get("href"))
                        title = link.get("title") or href
                        packages.append({"title": title, "href": href})
                elif "datasetSpecializations" in data and isinstance(
                    data["datasetSpecializations"], dict
                ):
                    items = list(data["datasetSpecializations"].values())
                else:
                    items = [data]
            elif isinstance(data, list):
                items = data

            if items:
                for it in items:
                    if not isinstance(it, dict):
                        continue
                    title_keys = [
                        "title",
                        "name",
                        "label",
                        "datasetLabel",
                        "datasetName",
                        "datasetSpecializationLabel",
                        "datasetSpecializationName",
                    ]
                    title = next(
                        (it.get(k) for k in title_keys if it.get(k)), "(untitled)"
                    )
                    href = it.get("href") or it.get("link")
                    if not href:
                        id_val = (
                            it.get("id")
                            or it.get("datasetSpecializationId")
                            or it.get("code")
                        )
                        if id_val:
                            href = f"{url}/{id_val}"
                    href = _normalize_href(href)
                    packages.append({"title": title, "href": href})
        else:
            _sdtm_specializations_cache["last_error"] = f"HTTP {resp.status_code}"
    except Exception as e:
        logger.error("SDTM dataset specializations fetch error: %s", e)
        _sdtm_specializations_cache["last_error"] = "Fetch failed; see server logs"

    packages.sort(key=lambda p: p.get("title", "").lower())
    _sdtm_specializations_cache.update(data=packages, fetched_at=now)
    logger.info(
        "Fetched %d SDTM dataset specializations from remote API (full list)",
        len(packages),
    )
    return packages


def fetch_crf_specializations(force: bool = False, code: Optional[str] = None):
    """Return list of CRF specializations as [{'title':..., 'href':...}].

    When `code` is None:
      - Fetch the full "latest version" CRF specializations list.
      - Cache the normalized list in _crf_specializations_cache.

    When `code` is provided (e.g. 'C105585'):
      - Call the generic dataset specializations endpoint with
        ?biomedicalconcept={code}, which returns a HAL-style document.
      - Extract the CRF subset from _links.datasetSpecializations.crf.
      - Do NOT use or update the main cache (code-specific result only).
    """
    now = time.time()
    base_prefix = CDISC_CRF_API_BASE_URL

    def _normalize_href(h):
        if not h:
            return None
        if h.startswith("http://") or h.startswith("https://"):
            return h
        if h.startswith("/"):
            return base_prefix + h
        return base_prefix + "/" + h

    # --------- code-specific branch (use generic endpoint) ----------
    if code:
        # Return cached result if still fresh
        cached = _crf_specializations_by_code_cache.get(code)
        if (
            not force
            and cached is not None
            and (now - cached[0]) < _CRF_SPECIALIZATIONS_CACHE_TTL
        ):
            return cached[1]

        url = (
            f"{base_prefix}/mdr/specializations/datasetspecializations"
            f"?biomedicalconcept={code}"
        )
        headers = {"Accept": "application/json"}
        api_key = _get_cdisc_crf_api_key()
        subscription_key = _get_cdisc_crf_subscription_key() or api_key
        if subscription_key:
            headers["Ocp-Apim-Subscription-Key"] = subscription_key
        if api_key:
            headers["Authorization"] = f"Bearer {api_key}"
            headers["api-key"] = api_key

        try:
            resp = requests.get(url, headers=headers, timeout=_HTTP_TIMEOUT)
            _crf_specializations_cache["last_status"] = resp.status_code
            _crf_specializations_cache["last_url"] = url
            _crf_specializations_cache["last_error"] = None
            if resp.status_code != 200:
                _crf_specializations_cache["last_error"] = f"HTTP {resp.status_code}"
                logger.warning(
                    "CRF specializations by BC code fetch HTTP %s for code=%s",
                    resp.status_code,
                    code,
                )
                return []
            try:
                data = resp.json()
            except ValueError:
                _crf_specializations_cache["last_error"] = "200 but non-JSON response"
                logger.warning(
                    "CRF specializations by BC code non-JSON body for code=%s", code
                )
                return []

            # Expect HAL-style: _links.datasetSpecializations.crf is list of links
            packages: list[dict] = []
            if (
                isinstance(data, dict)
                and "_links" in data
                and isinstance(data["_links"], dict)
            ):
                ds = data["_links"].get("datasetSpecializations")
                if isinstance(ds, dict):
                    crf_links = ds.get("crf")
                    if isinstance(crf_links, list):
                        for link in crf_links:
                            if not isinstance(link, dict):
                                continue
                            href = _normalize_href(link.get("href"))
                            title = link.get("title") or href
                            packages.append({"title": title, "href": href})
            packages.sort(key=lambda p: p.get("title", "").lower())
            logger.info(
                "Fetched %d CRF specializations for biomedical concept %s",
                len(packages),
                code,
            )
            _crf_specializations_by_code_cache[code] = (now, packages)
            return packages
        except Exception as e:
            logger.error(
                "CRF specializations by BC code fetch error for %s: %s", code, e
            )
            _crf_specializations_cache["last_error"] = str(e)
            return []

    # Cache only applies to full list
    if (
        not force
        and _crf_specializations_cache["data"] is not None
        and now - _crf_specializations_cache["fetched_at"]
        < _CRF_SPECIALIZATIONS_CACHE_TTL
    ):
        return _crf_specializations_cache["data"]

    # Env override branch
    override_json = os.environ.get("CDISC_CRF_SPECIALIZATIONS_JSON")
    if override_json:
        try:
            raw = json.loads(override_json)
            items = raw if isinstance(raw, list) else [raw]
            packages = []
            for it in items:
                if not isinstance(it, dict):
                    continue
                title = (
                    it.get("title") or it.get("name") or it.get("label") or "(untitled)"
                )
                href = _normalize_href(it.get("href") or it.get("link"))
                packages.append({"title": title, "href": href})
            packages.sort(key=lambda p: (p.get("title") or "").lower())
            _crf_specializations_cache.update(data=packages, fetched_at=now)
            logger.info("Loaded %d CRF specializations from override", len(packages))
            return packages
        except Exception as e:
            logger.warning("CRF specializations override parse failed: %s", e)

    if os.environ.get("CDISC_SKIP_REMOTE") == "1":
        _crf_specializations_cache.update(data=[], fetched_at=now)
        logger.warning("CDISC_SKIP_REMOTE=1; CRF specializations list empty")
        return []

    headers = {"Accept": "application/json"}
    api_key = _get_cdisc_crf_api_key()
    subscription_key = _get_cdisc_crf_subscription_key() or api_key
    if subscription_key:
        headers["Ocp-Apim-Subscription-Key"] = subscription_key
    if api_key:
        headers["Authorization"] = f"Bearer {api_key}"
        headers["api-key"] = api_key

    # Remote full-list branch: latest-version CRF specializations
    url = f"{base_prefix}/mdr/specializations/crf/specializations"
    packages: list[dict] = []
    try:
        resp = requests.get(url, headers=headers, timeout=_HTTP_TIMEOUT)
        _crf_specializations_cache["last_status"] = resp.status_code
        _crf_specializations_cache["last_url"] = url
        _crf_specializations_cache["last_error"] = None
        if resp.status_code == 200:
            try:
                data = resp.json()
            except ValueError:
                _crf_specializations_cache["last_error"] = "200 but non-JSON response"
                data = None
            if isinstance(data, dict) and "_links" in data:
                links = data["_links"]
                spec_links = links.get("specializations")
                if isinstance(spec_links, list):
                    for lnk in spec_links:
                        if not isinstance(lnk, dict):
                            continue
                        href = _normalize_href(lnk.get("href"))
                        title = lnk.get("title") or href
                        packages.append({"title": title, "href": href})
                elif isinstance(spec_links, dict):
                    href = _normalize_href(spec_links.get("href"))
                    title = spec_links.get("title") or href
                    packages.append({"title": title, "href": href})
            elif isinstance(data, list):
                for it in data:
                    if not isinstance(it, dict):
                        continue
                    title = (
                        it.get("title")
                        or it.get("name")
                        or it.get("label")
                        or "(untitled)"
                    )
                    href = _normalize_href(it.get("href") or it.get("link"))
                    packages.append({"title": title, "href": href})
        else:
            _crf_specializations_cache["last_error"] = (
                f"HTTP {resp.status_code}: {resp.text[:180]}"
            )
    except Exception as e:
        logger.error("CRF specializations fetch error: %s", e)
        _crf_specializations_cache["last_error"] = str(e)

    packages.sort(key=lambda p: (p.get("title") or "").lower())
    _crf_specializations_cache.update(data=packages, fetched_at=now)
    logger.info(
        "Fetched %d CRF specializations from remote API (latest version list)",
        len(packages),
    )
    return packages


# API endpoint for returning BC associated with an Activity
def _get_activity_concepts(activity_id: int):
    """Return list of concepts including concept_group_uid and group_name."""
    conn = _connect()
    cur = conn.cursor()
    has_soa = _table_has_columns(cur, "activity_concept", ("soa_id",))
    has_group = _table_has_columns(cur, "activity_concept", ("concept_group_uid",))
    if has_soa and has_group:
        cur.execute(
            "SELECT ac.concept_code, ac.concept_title, "
            "ac.concept_group_uid, cg.name AS group_name "
            "FROM activity_concept ac "
            "LEFT JOIN concept_group cg "
            "ON cg.concept_group_uid=ac.concept_group_uid "
            "WHERE ac.activity_id=? "
            "AND ac.soa_id=(SELECT soa_id FROM activity WHERE id=?) "
            "ORDER BY ac.concept_group_uid NULLS LAST, ac.id",
            (activity_id, activity_id),
        )
        rows = [
            {
                "code": r[0],
                "title": r[1],
                "concept_group_uid": r[2],
                "group_name": r[3],
            }
            for r in cur.fetchall()
        ]
    elif has_soa:
        cur.execute(
            "SELECT concept_code, concept_title "
            "FROM activity_concept WHERE activity_id=? "
            "AND soa_id=(SELECT soa_id FROM activity WHERE id=?)",
            (activity_id, activity_id),
        )
        rows = [
            {
                "code": r[0],
                "title": r[1],
                "concept_group_uid": None,
                "group_name": None,
            }
            for r in cur.fetchall()
        ]
    else:
        cur.execute(
            "SELECT concept_code, concept_title "
            "FROM activity_concept WHERE activity_id=?",
            (activity_id,),
        )
        rows = [
            {
                "code": r[0],
                "title": r[1],
                "concept_group_uid": None,
                "group_name": None,
            }
            for r in cur.fetchall()
        ]
    conn.close()
    return rows


_CDISC_SCHEME_HEADING_ORDER = [
    "COA Type",
    "Collection Method",
    "Concept Group",
    "Implementation Domain Code",
    "Age Category",
    "Therapeutic Area",
]


def _get_concept_group_sections():
    """Return concept_group rows organized into "Add group" dropdown
    sections: one per CDISC classification scheme (in a fixed display
    order), then "Custom Concept Groups" last. Only non-empty sections
    are included. Each group dict has {id, concept_group_uid, name,
    label} — unchanged shape from before CDISC groups existed.
    """
    conn = _connect()
    cur = conn.cursor()
    cur.execute(
        "SELECT id, concept_group_uid, name, label, source, cdisc_scheme_name "
        "FROM concept_group ORDER BY id"
    )
    rows = cur.fetchall()
    conn.close()

    by_heading = {}
    for r in rows:
        source, cdisc_scheme_name = r[4], r[5]
        heading = (
            cdisc_scheme_name
            if source == "cdisc" and cdisc_scheme_name
            else "Custom Concept Groups"
        )
        by_heading.setdefault(heading, []).append(
            {"id": r[0], "concept_group_uid": r[1], "name": r[2], "label": r[3]}
        )

    sections = [
        {"heading": heading, "groups": by_heading[heading]}
        for heading in _CDISC_SCHEME_HEADING_ORDER
        if heading in by_heading
    ]
    if "Custom Concept Groups" in by_heading:
        sections.append(
            {
                "heading": "Custom Concept Groups",
                "groups": by_heading["Custom Concept Groups"],
            }
        )
    return sections


def _get_concept_groups_for_cell(soa_id: int, activity_id: int):
    """Return (concept_group_sections, activity_group_uids) for
    concepts_cell rendering."""
    concept_groups = _get_concept_group_sections()
    conn = _connect()
    cur = conn.cursor()
    has_group = _table_has_columns(cur, "activity_concept", ("concept_group_uid",))
    if has_group:
        cur.execute(
            "SELECT DISTINCT concept_group_uid FROM activity_concept "
            "WHERE activity_id=? AND soa_id=? AND concept_group_uid IS NOT NULL",
            (activity_id, soa_id),
        )
        activity_group_uids = [r[0] for r in cur.fetchall()]
    else:
        activity_group_uids = []
    conn.close()
    return concept_groups, activity_group_uids


def _get_bc_categories_for_cell(soa_id: int, activity_id: int):
    """Return (bc_categories_list, activity_category_names) for concepts_cell rendering."""
    bc_categories_list = fetch_biomedical_concept_categories()
    conn = _connect()
    cur = conn.cursor()
    has_col = _table_has_columns(cur, "activity_concept", ("bc_category_name",))
    if has_col:
        cur.execute(
            "SELECT DISTINCT bc_category_name FROM activity_concept "
            "WHERE activity_id=? AND soa_id=? AND bc_category_name IS NOT NULL",
            (activity_id, soa_id),
        )
        activity_category_names = [r[0] for r in cur.fetchall()]
    else:
        activity_category_names = []
    conn.close()
    return bc_categories_list, activity_category_names


def _get_activity_surrogates(soa_id: int, activity_id: int):
    """Return (surrogates, selected_surrogate_list, selected_surrogate_uids) for concepts_cell render."""
    conn = _connect()
    cur = conn.cursor()
    cur.execute(
        "SELECT id, surrogate_uid, name, label FROM biomedical_concept_surrogate WHERE soa_id=? ORDER BY id",
        (soa_id,),
    )
    surrogates = [
        {"id": r[0], "surrogate_uid": r[1], "name": r[2], "label": r[3]}
        for r in cur.fetchall()
    ]
    cur.execute(
        "SELECT bcs.id, bcs.surrogate_uid, bcs.name, bcs.label "
        "FROM activity_surrogate asr "
        "JOIN biomedical_concept_surrogate bcs ON bcs.surrogate_uid=asr.surrogate_uid AND bcs.soa_id=asr.soa_id "
        "JOIN activity a ON a.activity_uid=asr.activity_uid AND a.soa_id=asr.soa_id "
        "WHERE asr.soa_id=? AND a.id=?",
        (soa_id, activity_id),
    )
    selected_surrogate_list = [
        {"id": r[0], "surrogate_uid": r[1], "name": r[2], "label": r[3]}
        for r in cur.fetchall()
    ]
    conn.close()
    return (
        surrogates,
        selected_surrogate_list,
        [s["surrogate_uid"] for s in selected_surrogate_list],
    )


def _upsert_code(cur, soa_id: int, concept_code: str):
    """Get-or-create a code row for this conceptId within this SoA.

    The synchronous insert records (soa_id, code_uid, code) immediately.
    A background task (_enrich_code_bg) fills in code_system, code_system_version,
    and decode from the CDISC API.

    Always returns the code_uid (pre-existing or newly inserted), or None if no code.
    """
    if not concept_code:
        return None
    cur.execute(
        "SELECT code_uid FROM code WHERE soa_id=? AND code=?", (soa_id, concept_code)
    )
    row = cur.fetchone()
    if row:
        return row[0]  # pre-existing — return uid, do not re-insert
    cur.execute(
        "SELECT code_uid FROM code WHERE soa_id=? AND code_uid LIKE 'Code_%'"
        " UNION"
        " SELECT code_uid FROM code_association WHERE soa_id=? AND code_uid LIKE 'Code_%'",
        (soa_id, soa_id),
    )
    existing = [x[0] for x in cur.fetchall() if x[0]]
    n = 1
    if existing:
        try:
            n = max(int(x.split("_")[1]) for x in existing) + 1
        except Exception:
            n = len(existing) + 1
    uid = f"Code_{n}"
    cur.execute(
        "INSERT INTO code (soa_id, code_uid, code) VALUES (?,?,?)",
        (soa_id, uid, concept_code),
    )
    return uid


def _upsert_alias_code(cur, soa_id: int, code_uid):
    """Get-or-create an alias_code row pointing to the given code_uid.

    Returns None if code_uid is None.
    Returns the existing alias_code_uid if already present for (soa_id, standard_code).
    Otherwise inserts and returns a new AliasCode_N uid.
    """
    if not code_uid:
        return None
    cur.execute(
        "SELECT alias_code_uid FROM alias_code WHERE soa_id=? AND standard_code=?",
        (soa_id, code_uid),
    )
    row = cur.fetchone()
    if row:
        return row[0]
    cur.execute(
        "SELECT alias_code_uid FROM alias_code"
        " WHERE soa_id=? AND alias_code_uid LIKE 'AliasCode_%'",
        (soa_id,),
    )
    existing = [x[0] for x in cur.fetchall() if x[0]]
    n = 1
    if existing:
        try:
            n = max(int(x.split("_")[1]) for x in existing) + 1
        except Exception:
            n = len(existing) + 1
    alias_uid = f"AliasCode_{n}"
    cur.execute(
        "INSERT INTO alias_code (soa_id, alias_code_uid, standard_code) VALUES (?,?,?)",
        (soa_id, alias_uid, code_uid),
    )
    return alias_uid


def _enrich_code_bg(concept_code: str, soa_id: int) -> None:
    """Background task: populate code_system, code_system_version, decode from CDISC API."""
    import os
    import requests as _requests

    api_key = os.environ.get("CDISC_API_KEY") or os.environ.get(
        "CDISC_SUBSCRIPTION_KEY"
    )
    subscription_key = os.environ.get("CDISC_SUBSCRIPTION_KEY") or api_key
    headers: dict = {"Accept": "application/json"}
    if subscription_key:
        headers["Ocp-Apim-Subscription-Key"] = subscription_key
    if api_key:
        headers["Authorization"] = f"Bearer {api_key}"
        headers["api-key"] = api_key
    try:
        url = (
            os.environ.get(
                "CDISC_BC_API_BASE_URL",
                "https://api.library.cdisc.org/api/cosmos/v2",
            )
            + "/mdr/bc/biomedicalconcepts/"
            + concept_code
        )
        resp = _requests.get(
            url,
            headers=headers,
            timeout=int(os.environ.get("CDISC_REQUEST_TIMEOUT", "15")),
        )
        if resp.status_code != 200:
            return
        data = resp.json()
        href = (data.get("_links") or {}).get("parentPackage") or {}
        href = href.get("href", "") if isinstance(href, dict) else ""
        try:
            code_system_version = href.split("/")[4]
        except Exception:
            code_system_version = ""
        decode = data.get("shortName")
        conn = _connect()
        cur = conn.cursor()
        cur.execute(
            "UPDATE code SET code_system=?, code_system_version=?, decode=?"
            " WHERE code=? AND soa_id=?",
            (href, code_system_version, decode, concept_code, soa_id),
        )
        conn.commit()
        conn.close()
    except Exception:
        pass


def _resolve_code_chain(cur, soa_id: int, alias_uid):
    """Return (code_uid, code_code, decode) for the given alias_code_uid in this SoA."""
    if not alias_uid:
        return None, None, None
    cur.execute(
        "SELECT standard_code FROM alias_code WHERE alias_code_uid=? AND soa_id=?",
        (alias_uid, soa_id),
    )
    ac_row = cur.fetchone()
    if not ac_row:
        return None, None, None
    code_uid_val = ac_row[0]
    cur.execute(
        "SELECT code, decode FROM code WHERE code_uid=? AND soa_id=?",
        (code_uid_val, soa_id),
    )
    c_row = cur.fetchone()
    return code_uid_val, (c_row[0] if c_row else None), (c_row[1] if c_row else None)


def _upsert_biomedical_concept(
    cur, soa_id: int, concept_uid, name: str, concept_code: str
):
    """Upsert a biomedical_concept row within an existing transaction.

    No-op when concept_uid is None (legacy schema) or already present.
    Always creates new code + alias_code rows (never reuses existing ones).
    Records a create audit entry when a new row is inserted.
    """
    if not concept_uid:
        return
    # no-op if already present
    cur.execute(
        "SELECT id FROM biomedical_concept WHERE soa_id=? AND biomedical_concept_uid=?",
        (soa_id, concept_uid),
    )
    if cur.fetchone():
        return

    # always create a new code row for this BC (never reuse)
    alias_uid = None
    if concept_code:
        cur.execute(
            "SELECT code_uid FROM code WHERE soa_id=? AND code_uid LIKE 'Code_%'"
            " UNION"
            " SELECT code_uid FROM code_association WHERE soa_id=? AND code_uid LIKE 'Code_%'",
            (soa_id, soa_id),
        )
        existing_codes = [x[0] for x in cur.fetchall() if x[0]]
        code_n = max((int(x.split("_")[1]) for x in existing_codes), default=0) + 1
        code_uid = f"Code_{code_n}"
        cur.execute(
            "INSERT INTO code (soa_id, code_uid, code) VALUES (?,?,?)",
            (soa_id, code_uid, concept_code),
        )
        # always create a new alias_code row for this BC (never reuse)
        cur.execute(
            "SELECT alias_code_uid FROM alias_code"
            " WHERE soa_id=? AND alias_code_uid LIKE 'AliasCode_%'",
            (soa_id,),
        )
        existing_aliases = [x[0] for x in cur.fetchall() if x[0]]
        alias_n = max((int(x.split("_")[1]) for x in existing_aliases), default=0) + 1
        alias_uid = f"AliasCode_{alias_n}"
        cur.execute(
            "INSERT INTO alias_code (soa_id, alias_code_uid, standard_code) VALUES (?,?,?)",
            (soa_id, alias_uid, code_uid),
        )

    cur.execute(
        "INSERT INTO biomedical_concept"
        " (soa_id, biomedical_concept_uid, name, code) VALUES (?,?,?,?)",
        (soa_id, concept_uid, name, alias_uid),
    )
    from ..audit import _record_biomedical_concept_audit

    bc_id = cur.lastrowid
    code_uid_val, code_val, decode_val = _resolve_code_chain(cur, soa_id, alias_uid)
    _record_biomedical_concept_audit(
        soa_id,
        "create",
        bc_id,
        before=None,
        after={
            "biomedical_concept_uid": concept_uid,
            "code": code_val,
            "alias_code_uid": alias_uid,
            "code_uid": code_uid_val,
            "decode": decode_val,
        },
        cur=cur,
    )


def _enrich_biomedical_concept_bg(concept_code: str, soa_id: int) -> None:
    """Background task: fetch label/description from CDISC API and persist."""
    import os
    import requests as _requests

    api_key = os.environ.get("CDISC_API_KEY") or os.environ.get(
        "CDISC_SUBSCRIPTION_KEY"
    )
    subscription_key = os.environ.get("CDISC_SUBSCRIPTION_KEY") or api_key
    headers: dict = {"Accept": "application/json"}
    if subscription_key:
        headers["Ocp-Apim-Subscription-Key"] = subscription_key
    if api_key:
        headers["Authorization"] = f"Bearer {api_key}"
        headers["api-key"] = api_key
    conn = None
    try:
        url = (
            os.environ.get(
                "CDISC_BC_API_BASE_URL",
                "https://api.library.cdisc.org/api/cosmos/v2",
            )
            + "/mdr/bc/biomedicalconcepts/"
            + concept_code
        )
        resp = _requests.get(
            url,
            headers=headers,
            timeout=int(os.environ.get("CDISC_REQUEST_TIMEOUT", "15")),
        )
        if resp.status_code != 200:
            return
        data = resp.json()
        label = data.get("shortName")
        description = data.get("definition")
        conn = _connect()
        cur = conn.cursor()
        cur.execute(
            """
            UPDATE biomedical_concept SET name=?, label=?, description=?
            WHERE soa_id=?
              AND biomedical_concept_uid IN (
                  SELECT concept_uid FROM activity_concept
                  WHERE soa_id=? AND concept_code=? AND concept_uid IS NOT NULL
              )
            """,
            (label, label, description, soa_id, soa_id, concept_code),
        )
        if label:
            cur.execute(
                """
                UPDATE activity_concept SET concept_title=?
                WHERE soa_id=? AND concept_code=?
                """,
                (label, soa_id, concept_code),
            )
        conn.commit()
    except Exception:
        pass
    finally:
        if conn:
            conn.close()


def _populate_biomedical_concept_properties_bg(
    concept_code: str, bc_uid: str, soa_id: int
) -> None:
    """Background task: populate BCP + ResponseCode rows for one BC.

    Gated by SOA_EAGER_BCP_POPULATION env var; no-op when unset.
    If ``bc_uid`` is None the uid is resolved from the DB.
    """
    import os as _os

    if _os.environ.get("SOA_EAGER_BCP_POPULATION", "1").strip().lower() in (
        "0",
        "false",
    ):
        return
    resolved_uid = bc_uid
    if not resolved_uid:
        try:
            conn = _connect()
            cur = conn.cursor()
            cur.execute(
                "SELECT bc.biomedical_concept_uid"
                " FROM biomedical_concept bc"
                " INNER JOIN activity_concept ac"
                " ON bc.biomedical_concept_uid = ac.concept_uid"
                " AND bc.soa_id = ac.soa_id"
                " WHERE bc.soa_id = ? AND ac.concept_code = ?"
                " LIMIT 1",
                (soa_id, concept_code),
            )
            row = cur.fetchone()
            conn.close()
            resolved_uid = row[0] if row else None
        except Exception:
            logger.exception(
                "_populate_biomedical_concept_properties_bg:"
                " uid lookup failed concept_code=%s soa_id=%s",
                concept_code,
                soa_id,
            )
            return
    if not resolved_uid:
        return
    from usdm.generate_biomedical_concept_properties import (
        populate_biomedical_concept_properties_for_bc,
    )

    try:
        populate_biomedical_concept_properties_for_bc(
            soa_id, resolved_uid, concept_code
        )
    except Exception:
        logger.exception(
            "_populate_biomedical_concept_properties_bg failed"
            " concept_code=%s bc_uid=%s soa_id=%s",
            concept_code,
            bc_uid,
            soa_id,
        )


def _cleanup_orphaned_concept_rows(cur, soa_id: int, removed_pairs) -> None:
    """Delete biomedical_concept/code/alias_code rows no longer referenced in this SoA.

    removed_pairs: iterable of (concept_code, concept_uid); concept_uid may be None.
    Call AFTER the activity_concept rows have been deleted (and any new ones inserted),
    so the orphan check reflects the final state of activity_concept.
    """
    from ..audit import _record_biomedical_concept_audit
    from usdm.generate_biomedical_concept_properties import delete_bc_cascade

    for concept_code, concept_uid in removed_pairs:
        if concept_uid:
            cur.execute(
                "SELECT 1 FROM activity_concept"
                " WHERE soa_id=? AND concept_uid=? LIMIT 1",
                (soa_id, concept_uid),
            )
            if cur.fetchone():
                # UID is still referenced by a live row — do not delete it.
                continue
            cur.execute(
                "SELECT id, code FROM biomedical_concept"
                " WHERE biomedical_concept_uid=? AND soa_id=?",
                (concept_uid, soa_id),
            )
            bc_row = cur.fetchone()
            if bc_row:
                bc_id, alias_uid = bc_row
                code_uid_val, code_val, decode_val = _resolve_code_chain(
                    cur, soa_id, alias_uid
                )
                _record_biomedical_concept_audit(
                    soa_id,
                    "delete",
                    bc_id,
                    before={
                        "biomedical_concept_uid": concept_uid,
                        "code": code_val,
                        "alias_code_uid": alias_uid,
                        "code_uid": code_uid_val,
                        "decode": decode_val,
                    },
                    after=None,
                    cur=cur,
                )
            # Cascade: remove the BC's properties + response codes first so
            # they never orphan when the biomedical_concept row is deleted.
            delete_bc_cascade(cur, soa_id, concept_uid)
            cur.execute(
                "DELETE FROM biomedical_concept"
                " WHERE biomedical_concept_uid=? AND soa_id=?",
                (concept_uid, soa_id),
            )
        if not concept_code:
            continue
        cur.execute(
            "SELECT 1 FROM activity_concept WHERE soa_id=? AND concept_code=? LIMIT 1",
            (soa_id, concept_code),
        )
        if cur.fetchone():
            continue  # still referenced by another activity in this SoA
        cur.execute(
            "SELECT code_uid FROM code WHERE soa_id=? AND code=?",
            (soa_id, concept_code),
        )
        code_row = cur.fetchone()
        if not code_row:
            continue
        code_uid_val = code_row[0]
        cur.execute(
            "SELECT alias_code_uid FROM alias_code WHERE soa_id=? AND standard_code=?",
            (soa_id, code_uid_val),
        )
        alias_row = cur.fetchone()
        if alias_row:
            # Audit any remaining biomedical_concept rows referencing this alias (edge case)
            cur.execute(
                "SELECT id, biomedical_concept_uid FROM biomedical_concept"
                " WHERE code=? AND soa_id=?",
                (alias_row[0], soa_id),
            )
            for edge_bc_id, edge_bc_uid in cur.fetchall():
                edge_code_uid, edge_code_val, edge_decode = _resolve_code_chain(
                    cur, soa_id, alias_row[0]
                )
                _record_biomedical_concept_audit(
                    soa_id,
                    "delete",
                    edge_bc_id,
                    before={
                        "biomedical_concept_uid": edge_bc_uid,
                        "code": edge_code_val,
                        "alias_code_uid": alias_row[0],
                        "code_uid": edge_code_uid,
                        "decode": edge_decode,
                    },
                    after=None,
                    cur=cur,
                )
                # Cascade BCP/RC cleanup for this edge-case BC.
                delete_bc_cascade(cur, soa_id, edge_bc_uid)
            cur.execute(
                "DELETE FROM biomedical_concept WHERE code=? AND soa_id=?",
                (alias_row[0], soa_id),
            )
            cur.execute(
                "DELETE FROM alias_code WHERE alias_code_uid=? AND soa_id=?",
                (alias_row[0], soa_id),
            )
        cur.execute(
            "DELETE FROM code WHERE code_uid=? AND soa_id=?",
            (code_uid_val, soa_id),
        )
