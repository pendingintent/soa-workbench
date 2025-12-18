from typing import Any, Dict, List
import os
import requests
import time
from .db import _connect

_epoch_type_cache: dict[str, Any] = {
    "data": None,
    "fetched_at": 0,
    "last_status": None,
    "last_url": None,
    "last_error": None,
    "parent_package_href": None,
}
_EPOCH_TYPE_CACHE_TTL = 60 * 60  # 1 hour


def load_epoch_type_options(force: bool = False) -> list[str]:
    """Fetch Epoch Type options from CDISC Library API codelist C99079.

    Parses _links.terms[].submissionValue and returns a sorted, deduplicated list.
    Uses env `CDISC_SUBSCRIPTION_KEY` and `_get_cdisc_api_key`-style headers when available.
    Note: This module does not import app helpers; callers should provide headers if overriding.
    """
    now = time.time()
    if (
        not force
        and _epoch_type_cache["data"]
        and now - _epoch_type_cache["fetched_at"] < _EPOCH_TYPE_CACHE_TTL
    ):
        return _epoch_type_cache["data"] or []
    # Use only the specified CDISC Library endpoint (per user requirement)
    url = "https://library.cdisc.org/api/mdr/ct/packages/sdtmct-2025-09-26/codelists/C99079"
    headers: dict[str, str] = {"Accept": "application/json"}
    subscription_key = os.environ.get("CDISC_SUBSCRIPTION_KEY")
    api_key = os.environ.get("CDISC_API_KEY") or os.environ.get(
        "CDISC_SUBSCRIPTION_KEY"
    )
    unified_key = subscription_key or api_key
    if unified_key:
        headers["Ocp-Apim-Subscription-Key"] = unified_key
    if api_key:
        headers["Authorization"] = f"Bearer {api_key}"
        headers["api-key"] = api_key
    try:
        values: list[str] = []
        last_status = None
        _epoch_type_cache.update(last_url=url, last_error=None)
        resp = requests.get(url, headers=headers, timeout=10)
        last_status = resp.status_code
        if resp.status_code != 200:
            data = {}
            top_terms = []
        else:
            data = resp.json() or {}
            # Preferred structure: top-level 'terms' list
            top_terms = []
            if isinstance(data, dict) and isinstance(data.get("terms"), list):
                top_terms = data.get("terms") or []
            elif isinstance(data, list):
                top_terms = data
            else:
                # HAL-style fallbacks
                embedded_terms = []
                if isinstance(data.get("_embedded"), dict):
                    embedded_terms = data.get("_embedded", {}).get("terms", []) or []
                link_terms = data.get("_links", {}).get("terms", []) or []
                top_terms = embedded_terms or link_terms
            # Capture parent package href if present
            try:
                if isinstance(data, dict):
                    pph = data.get("_links", {}).get("parentPackage", {}).get("href")
                    if pph:
                        _epoch_type_cache["parent_package_href"] = str(pph)
            except Exception:
                pass
            # Collect embedded submissionValue
            for t in top_terms:
                if not isinstance(t, dict):
                    continue
                sv = t.get("submissionValue") or t.get("cdisc_submission_value")
                if sv and str(sv).strip():
                    values.append(str(sv).strip())
            # If still none and we have term links, follow them
            if not values:
                for t in top_terms:
                    href = None
                    if isinstance(t, dict):
                        href = t.get("href") or t.get("_href")
                    if not href:
                        continue
                    try:
                        _epoch_type_cache.update(last_url=href)
                        term_resp = requests.get(href, headers=headers, timeout=10)
                        if term_resp.status_code == 200:
                            term_json = term_resp.json() or {}
                            sv = term_json.get("submissionValue") or term_json.get(
                                "cdisc_submission_value"
                            )
                            if sv and str(sv).strip():
                                values.append(str(sv).strip())
                    except Exception:
                        continue
        # Single endpoint only; no loop/break
        result = sorted(list(dict.fromkeys(values)))
        _epoch_type_cache.update(data=result, fetched_at=now, last_status=last_status)
        return result
    except Exception as e:
        _epoch_type_cache.update(data=[], fetched_at=now, last_error=str(e))
        return []


def load_epoch_type_map(force: bool = False) -> Dict[str, str]:
    """Fetch Epoch Type term mapping from CDISC Library API for C99079.

    Returns a dict of {term_code: submissionValue}. This enables UI preselection
    by mapping stored epoch.type code_uid -> code -> submissionValue.
    """
    now = time.time()
    # Simple TTL cache to avoid repeated remote calls
    if not force and isinstance(_epoch_type_cache.get("_map"), dict):
        cached_map = _epoch_type_cache.get("_map") or {}
        fetched = _epoch_type_cache.get("_map_fetched_at") or 0
        if cached_map and now - fetched < _EPOCH_TYPE_CACHE_TTL:
            return cached_map

    # Use only the specified CDISC Library endpoint (per user requirement)
    headers: dict[str, str] = {"Accept": "application/json"}
    subscription_key = os.environ.get("CDISC_SUBSCRIPTION_KEY")
    api_key = os.environ.get("CDISC_API_KEY") or os.environ.get(
        "CDISC_SUBSCRIPTION_KEY"
    )
    unified_key = subscription_key or api_key
    if unified_key:
        headers["Ocp-Apim-Subscription-Key"] = unified_key
    if api_key:
        headers["Authorization"] = f"Bearer {api_key}"
        headers["api-key"] = api_key

    url = "https://library.cdisc.org/api/mdr/ct/packages/sdtmct-2025-09-26/codelists/C99079"
    code_to_submission: Dict[str, str] = {}
    last_status = None
    try:
        _epoch_type_cache.update(last_url=url, last_error=None)
        resp = requests.get(url, headers=headers, timeout=10)
        last_status = resp.status_code
        if resp.status_code != 200:
            data = {}
            terms = []
        else:
            data = resp.json() or {}
            # Preferred structure: top-level 'terms' list
            terms: List[dict] = []
            if isinstance(data, dict) and isinstance(data.get("terms"), list):
                terms = data.get("terms") or []
            elif isinstance(data, list):
                terms = data
            else:
                # HAL-style fallback
                terms = data.get("_links", {}).get("terms", []) or []
            # Capture parent package href if present
            try:
                if isinstance(data, dict):
                    pph = data.get("_links", {}).get("parentPackage", {}).get("href")
                    if pph:
                        _epoch_type_cache["parent_package_href"] = str(pph)
            except Exception:
                pass
            for t in terms:
                if not isinstance(t, dict):
                    continue
                # CDISC Library returns conceptId + submissionValue in this package endpoint
                code = t.get("conceptId") or t.get("code") or t.get("termCode")
                sub = t.get("submissionValue") or t.get("cdisc_submission_value")
                if code and sub:
                    code_to_submission[str(code)] = str(sub).strip()
                    continue
                href = t.get("href") or t.get("_href")
                if not href:
                    # HAL style: try _links.self.href
                    linkself = t.get("_links", {}).get("self", {})
                    href = linkself.get("href") if isinstance(linkself, dict) else None
                if href:
                    try:
                        _epoch_type_cache.update(last_url=href)
                        term_resp = requests.get(href, headers=headers, timeout=10)
                        if term_resp.status_code == 200:
                            tj = term_resp.json() or {}
                            sub2 = tj.get("submissionValue") or tj.get(
                                "cdisc_submission_value"
                            )
                            code2 = tj.get("code") or code
                            if code2 and sub2:
                                code_to_submission[str(code2)] = str(sub2).strip()
                    except Exception:
                        pass
        # Single endpoint only; no loop/break
    except Exception as e:
        _epoch_type_cache.update(last_error=str(e))
    _epoch_type_cache.update(last_status=last_status)
    _epoch_type_cache.update(_map=code_to_submission, _map_fetched_at=now)
    return code_to_submission


def get_epoch_parent_package_href_cached() -> str | None:
    """Return cached parentPackage href from the last Epoch Type API fetch.

    This depends on a prior call to load_epoch_type_options/map to populate the cache.
    """
    val = _epoch_type_cache.get("parent_package_href")
    return str(val) if val else None


def get_next_code_uid(cur: Any, soa_id: int) -> str:
    """Compute next unique Code_N for the given SOA.

    Assumes `cur` is a sqlite cursor within an open transaction.
    """
    cur.execute(
        "SELECT code_uid FROM code WHERE soa_id=? AND code_uid LIKE 'Code_%'",
        (soa_id,),
    )
    existing = [x[0] for x in cur.fetchall() if x[0]]
    n = 1
    if existing:
        try:
            n = max(int(x.split("_")[1]) for x in existing) + 1
        except Exception:
            n = len(existing) + 1
    return f"Code_{n}"


def get_next_concept_uid(cur: Any, soa_id: int) -> str:
    """Compute next unique BiomedicalConcept_N for the given SOA.

    Assumes `cur` is a sqlite cursor within an open transaction.
    Uses activity_concept table when available; falls back safely if table missing.
    """
    try:
        cur.execute("PRAGMA table_info(activity_concept)")
        cols = {r[1] for r in cur.fetchall()}
        if "concept_uid" not in cols:
            return "BiomedicalConcept_1"
        if "soa_id" in cols:
            cur.execute(
                "SELECT concept_uid FROM activity_concept WHERE soa_id=? AND concept_uid LIKE 'BiomedicalConcept_%'",
                (soa_id,),
            )
        else:
            cur.execute(
                "SELECT concept_uid FROM activity_concept WHERE concept_uid LIKE 'BiomedicalConcept_%'"
            )
        existing = [x[0] for x in cur.fetchall() if x[0]]
        n = 1
        if existing:
            try:
                n = max(int(x.split("_")[1]) for x in existing) + 1
            except Exception:
                n = len(existing) + 1
        return f"BiomedicalConcept_{n}"
    except Exception:
        return "BiomedicalConcept_1"


def soa_exists(soa_id: int) -> bool:
    """Return True if an SOA row exists with the given id."""
    try:
        conn = _connect()
        cur = conn.cursor()
        cur.execute("SELECT 1 FROM soa WHERE id=?", (soa_id,))
        ok = cur.fetchone() is not None
        conn.close()
        return ok
    except Exception:
        return False


def table_has_columns(cur: Any, table: str, required: List[str] | tuple) -> bool:
    """Return True if all required column names exist in the given table.

    Parameters:
      cur: sqlite3 cursor
      table: table name (constant in code; not user-provided)
      required: iterable of column names to check
    """
    try:
        cur.execute(f"PRAGMA table_info({table})")
        cols = {r[1] for r in cur.fetchall()}
        return all(c in cols for c in required)
    except Exception:
        return False


def get_study_timing_type(codelist_code: str) -> Dict[str, str]:
    """Return a dictionary of {submissionValue: code} from the DDF
    Terminology (ddf_terminology) table.

    """
    conn = _connect()
    cur = conn.cursor()
    cur.execute(
        "SELECT cdisc_submission_value,code FROM ddf_terminology WHERE codelist_code=?",
        (codelist_code,),
    )
    rows = cur.fetchall()
    conn.close()

    return {str(sub): str(code) for (sub, code) in rows}


def get_encounter_id(soa_id: int) -> Dict[str, str]:
    """Return a dictionary of {name: encounter_uid} from the visit table"""
    conn = _connect()
    cur = conn.cursor()
    cur.execute(
        "SELECT name,encounter_uid FROM visit WHERE soa_id=? ORDER BY encounter_uid",
        (soa_id,),
    )
    rows = cur.fetchall()
    conn.close()
    return {str(name): str(enc_uid) for (name, enc_uid) in rows if name is not None}


def get_epoch_uid(soa_id: int) -> Dict[str, str]:
    """Return a dictionary of {name: epoch_uid} from the epoch table"""
    conn = _connect()
    cur = conn.cursor()
    cur.execute(
        "SELECT name, epoch_uid FROM epoch WHERE soa_id=? ORDER BY epoch_uid",
        (soa_id,),
    )
    rows = cur.fetchall()
    conn.close()
    return {str(name): str(epoch_uid) for (name, epoch_uid) in rows if name is not None}
