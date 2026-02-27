from typing import Any, Dict, List, Optional
import os
import re
import requests
import time
from urllib.parse import urlparse, urlunparse
from fastapi import Request
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

_env_setting_cache: dict[str, Any] = {
    "options": None,
    "fetched_at": 0,
    "last_error": None,
}
_ENV_SETTING_CACHE_TTL = 60 * 60  # 1 hour

_contact_mode_cache: dict[str, Any] = {
    "options": None,
    "fetched_at": 0,
    "last_error": None,
}
_CONTACT_MODE_CACHE_TTL = 60 * 60  # 1 hour


# Constants for the helper function
_ISO_DURATION_RE = re.compile(
    r"^P"  # starts with 'P'
    r"(?:(?P<years>\d+)Y)?"  # years
    r"(?:(?P<months>\d+)M)?"  # months (date part)
    r"(?:(?P<weeks>\d+)W)?"  # weeks
    r"(?:(?P<days>\d+)D)?"  # days
    r"(?:T"  # time part
    r"(?:(?P<hours>\d+)H)?"
    r"(?:(?P<minutes>\d+)M)?"
    r"(?:(?P<seconds>\d+)S)?"
    r")?$"
)


# USDM JSON generator helper
def _get_biomedical_concept_ids(soa_id: int, activity_uid: int) -> List[str]:
    conn = _connect()
    cur = conn.cursor()
    cur.execute(
        "SELECT concept_uid from activity_concept where soa_id=? and activity_uid=?",
        (
            soa_id,
            activity_uid,
        ),
    )
    rows = cur.fetchall()
    conn.close()
    bc_uids = [r[0] for r in rows] or []
    return bc_uids


def redirect_url_from_referer(request: Request, fallback: str) -> str:
    """Return the Referer URL if it's a same-origin /ui/ path, else fallback."""
    referer = request.headers.get("referer", "")
    if referer:
        parsed = urlparse(referer)
        base = urlparse(str(request.base_url))
        if parsed.netloc == base.netloc and parsed.path.startswith("/ui/"):
            return urlunparse(("", "", parsed.path, "", parsed.query, parsed.fragment))
    return fallback


# Helper function to convert ISO-8601 duration/period strings
# to days, using these common approximations for years and months
"""
    1 year = 365 days
    1 month = 30 days
    1 week = 7 days
    1 hour = 1/24 day
    1 minute = 1/(24*60) day
    1 second = 1/(24*3600) day
"""


def iso_duration_to_days(iso_duration: str) -> float:
    """
    Convert an ISO-8601 duration (e.g. 'P1D', 'P2W', 'P1Y2M3D', 'P1DT12H')
    into a number of days (float).

    Uses approximations: 1Y=365d, 1M=30d.
    Returns:
        float: Number of days represented by the duration.
        None: If the input is empty or not a valid ISO-8601 duration string.
    """
    if not iso_duration:
        return None

    m = _ISO_DURATION_RE.match(iso_duration)
    if not m:
        return None

    parts = {k: int(v) if v is not None else 0 for k, v in m.groupdict().items()}

    days = 0.0
    days += parts["years"] * 365
    days += parts["months"] * 30
    days += parts["weeks"] * 7
    days += parts["days"]
    days += parts["hours"] / 24.0
    days += parts["minutes"] / (24.0 * 60.0)
    days += parts["seconds"] / (24.0 * 3600.0)

    return days


def _nz(s: Optional[str]) -> Optional[str]:
    s = (s or "").strip()
    return s or None


def get_cdisc_api_key():
    return os.environ.get("CDISC_API_KEY")


def get_concepts_override():
    return os.environ.get("CDISC_CONCEPTS_JSON")


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


# Function for creating {code: submission_value} for Arm type selector
def load_arm_type_map() -> Dict[str, str]:
    """Fetch Arm Type term mapping from the protocol_terminology database table"""
    conn = _connect()
    cur = conn.cursor()
    cur.execute(
        """
        SELECT code,cdisc_submission_value FROM protocol_terminology
        WHERE codelist_code='C174222'
        ORDER BY cdisc_submission_value
        """
    )
    rows = cur.fetchall()
    conn.close()
    return {
        str(code): str(sv) for (code, sv) in rows if code is not None and sv is not None
    }


# Function for creating {code: submission_value} for Arm dataOriginType selector
def load_arm_data_origin_type_map() -> Dict[str, str]:
    """Fetch arm data origin type from the ddf_terminology database table"""
    conn = _connect()
    cur = conn.cursor()
    cur.execute(
        """
        SELECT code,cdisc_submission_value FROM ddf_terminology
        WHERE codelist_code='C188727'
        ORDER BY cdisc_submission_value
        """
    )
    rows = cur.fetchall()
    conn.close()
    return {
        str(code): str(sv) for (code, sv) in rows if code is not None and sv is not None
    }


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


# Helper function to generate new alias_code_uid value
def get_next_alias_code_uid(cur: Any, soa_id: int) -> str:
    """Compute next unique AliasCode_ for the given SOA.
    Assumes `cur` is a sqlite cursor within an open transaction.
    """
    cur.execute(
        "SELECT alias_code_uid from alias_code WHERE soa_id=? AND alias_code_uid LIKE 'AliasCode_%",
        (soa_id,),
    )
    existing = [x[0] for x in cur.fetchall() if x[0]]
    n = 1
    if existing:
        try:
            n = max(int(x.split("_")[1]) for x in existing) + 1
        except Exception:
            n = len(existing) + 1
    return f"AliasCode_{n}"


# Helper function to generate new code_uid value
def get_next_code_uid(cur: Any, soa_id: int) -> str:
    """Compute next unique Code_N for the given SOA.

    Assumes `cur` is a sqlite cursor within an open transaction.
    """
    cur.execute(
        "SELECT code_uid FROM code_association WHERE soa_id=? AND code_uid LIKE 'Code_%'",
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


def get_conditions(soa_id: int) -> Dict[str, str]:
    """
    Return a dictionary of {name: condition_assignment_uid} from condition_assignment table

    :param soa_id: soa identifier
    :type soa_id: int
    :return {name: condition_assignment_uid}
    :rtype: Dict[str, str]
    """
    conn = _connect()
    cur = conn.cursor()
    cur.execute(
        "SELECT name,condition_assignment_uid FROM condition_assignment WHERE soa_id=? ORDER BY name",
        (soa_id,),
    )
    rows = cur.fetchall()
    conn.close()
    return {
        str(name): str(condition_assignment_uid)
        for (name, condition_assignment_uid) in rows
    }


def get_scheduled_activity_instance(soa_id: int) -> Dict[str, str]:
    """
    Return Dictionary of {name: instance_uid} from instances table

    :param soa_id: soa identifier
    :type soa_id: int
    :return: {name: instance_uid}
    :rtype: Dict[str, str]
    """
    conn = _connect()
    cur = conn.cursor()
    cur.execute(
        """
        SELECT name,instance_uid FROM instances WHERE soa_id=?
        UNION
        SELECT name,instance_uid FROM decision_instances WHERE soa_id=?
        ORDER BY name
        """,
        (soa_id, soa_id),
    )
    rows = cur.fetchall()
    conn.close()
    return {str(name): str(instance_uid) for (name, instance_uid) in rows}


def get_schedule_timeline(soa_id: int) -> Dict[str, str]:
    """
    Return list of {name: schedule_timeline_uid} from schedule_timelines

    :param soa_id: soa identifier
    :type soa_id: int
    :return: {name: schedule_timeline_uid}
    :rtype: Dict[str, str]
    """
    conn = _connect()
    cur = conn.cursor()
    cur.execute(
        "SELECT name,schedule_timeline_uid FROM schedule_timelines WHERE soa_id=? ORDER BY schedule_timeline_uid",
        (soa_id,),
    )
    rows = cur.fetchall()
    conn.close()
    return {
        str(name): str(schedule_timeline_uid) for (name, schedule_timeline_uid) in rows
    }


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


def get_epoch_id(soa_id: int) -> Dict[str, str]:
    """Return dictionary of {id: name} from epoch table"""
    conn = _connect()
    cur = conn.cursor()
    cur.execute(
        "SELECT id,name FROM epoch WHERE soa_id=? ORDER BY id,name",
        (soa_id,),
    )
    rows = cur.fetchall()
    conn.close()
    return {int(id): str(name) for (id, name) in rows if id is not None}


def get_sdtm_submission_values(url: str, codelist_code: str) -> Dict[str, str]:
    """Return a mapping of {conceptId: submissionValue} from the CDISC Library
    for the given codelist_code. `url` should be the codelists base endpoint.
    """
    full_url = f"{url.rstrip('/')}/{codelist_code}"
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

    mapping: Dict[str, str] = {}
    try:
        resp = requests.get(full_url, headers=headers, timeout=10)
        if resp.status_code != 200:
            return {}
        data = resp.json() or {}

        # Prefer top-level 'terms'; fall back to HAL-style links
        terms: List[dict] = []
        if isinstance(data, dict) and isinstance(data.get("terms"), list):
            terms = data.get("terms") or []
        elif isinstance(data, list):
            terms = data
        else:
            terms = data.get("_links", {}).get("terms", []) or []

        for t in terms:
            if not isinstance(t, dict):
                continue
            code = t.get("conceptId") or t.get("code") or t.get("termCode")
            sv = t.get("submissionValue") or t.get("cdisc_submission_value")
            if code and sv:
                mapping[str(code)] = str(sv).strip()
                continue

            # If only a link is provided, follow it to resolve fields
            href = t.get("href") or t.get("_href")
            if not href:
                linkself = t.get("_links", {}).get("self", {})
                href = linkself.get("href") if isinstance(linkself, dict) else None
            if href:
                try:
                    tr = requests.get(href, headers=headers, timeout=10)
                    if tr.status_code == 200:
                        tj = tr.json() or {}
                        code2 = tj.get("conceptId") or tj.get("code") or code
                        sv2 = tj.get("submissionValue") or tj.get(
                            "cdisc_submission_value"
                        )
                        if code2 and sv2:
                            mapping[str(code2)] = str(sv2).strip()
                except Exception:
                    pass

        return mapping
    except Exception:
        return {}


def get_study_timings(soa_id: int) -> Dict[str, str]:
    """Return a Dict of {name: timing_uid} from the database
    `timing` table for the SOA

    """
    conn = _connect()
    cur = conn.cursor()
    cur.execute(
        "SELECT name,timing_uid from timing WHERE soa_id=? ORDER BY name",
        (soa_id,),
    )
    rows = cur.fetchall()
    conn.close()
    return {
        str(name): str(timing_uid) for (name, timing_uid) in rows if name is not None
    }


def get_study_transition_rules(soa_id: int) -> Dict[str, str]:
    """Return a Dict of {name: transition_rule_uid} from the database
    `transition_rule` table for the SOA

    """
    conn = _connect()
    cur = conn.cursor()
    cur.execute(
        "SELECT name,transition_rule_uid from transition_rule WHERE soa_id=? ORDER BY name",
        (soa_id,),
    )
    rows = cur.fetchall()
    conn.close()
    return {
        str(name): str(transition_rule_uid)
        for (name, transition_rule_uid) in rows
        if name is not None
    }


def get_timing_id(soa_id: int) -> Dict[str, str]:
    """Return a dictionary of {id: name} from timing table"""
    conn = _connect()
    cur = conn.cursor()
    cur.execute(
        "SELECT id,name from timing WHERE soa_id=? ORDER BY id,name",
        (soa_id,),
    )
    rows = cur.fetchall()
    conn.close()
    return {int(id): str(name) for (id, name) in rows if id is not None}


def get_encounter_type_sv(soa_id: int, code_uid: str):
    """Return the submission value for the encounter type using Code_{n} value"""
    conn = _connect()
    cur = conn.cursor()
    cur.execute(
        """
        SELECT ddf.cdisc_submission_value FROM visit v
        INNER JOIN code_association c ON v.type=c.code_uid AND v.soa_id=c.soa_id
        INNER JOIN ddf_terminology ddf ON c.codelist_code=ddf.codelist_code AND c.code=ddf.code
        WHERE v.soa_id =? AND v.type=?
        """,
        (soa_id, code_uid),
    )
    row = cur.fetchone()
    conn.close()
    return row


def get_latest_sdtm_ct_href(timeout: int = 10) -> str | None:
    """Return the href for the latest SDTM Controlled Terminology package."""
    url = "https://library.cdisc.org/api/mdr/ct/packages"
    headers: dict[str, str] = {"Accept": "application/json"}
    subscription_key = os.environ.get("CDISC_SUBSCRIPTION_KEY")
    api_key = os.environ.get("CDISC_API_KEY") or subscription_key
    if subscription_key:
        headers["Ocp-Apim-Subscription-Key"] = subscription_key
    if api_key:
        headers["Authorization"] = f"Bearer {api_key}"
        headers["api-key"] = api_key

    def _extract_date(name: str) -> tuple:
        parts = name.split("-")
        if len(parts) >= 4 and parts[0].lower() == "sdtmct":
            try:
                return int(parts[1]), int(parts[2]), int(parts[3])
            except ValueError:
                pass
        return (0, 0, 0)

    try:
        resp = requests.get(url, headers=headers, timeout=timeout)
        if resp.status_code != 200:
            return None
        payload = resp.json() or {}
    except Exception:
        return None

    packages = []
    if isinstance(payload, list):
        packages = payload
    elif isinstance(payload, dict):
        packages = (
            payload.get("packages")
            or payload.get("_embedded", {}).get("packages")
            or payload.get("items")
            or payload.get("_links", {}).get("packages")
            or []
        )

    latest = None
    latest_date = (0, 0, 0)
    for pkg in packages:
        if not isinstance(pkg, dict):
            continue
        raw_href = (
            pkg.get("href")
            or pkg.get("url")
            or pkg.get("_links", {}).get("self", {}).get("href")
        )
        title = pkg.get("name") or pkg.get("packageName") or pkg.get("title") or ""
        segment = (raw_href or "").rstrip("/").split("/")[-1]
        name = (segment or title).lower()
        if not name.startswith("sdtmct-"):
            continue
        date_tuple = _extract_date(name)
        if date_tuple <= latest_date:
            continue
        latest = name
        latest_date = date_tuple

    return latest


def get_encounter_environment_sv(soa_id: int, code_uid: str):
    """Resolve the environmental setting submission value via CDISC Library."""
    if not code_uid:
        return None

    conn = _connect()
    cur = conn.cursor()
    cur.execute(
        "SELECT code FROM code_association WHERE soa_id=? AND code_uid=?",
        (soa_id, code_uid),
    )
    row = cur.fetchone()
    conn.close()
    if not row:
        return None
    target_code = str(row[0]).strip()

    package_slug = get_latest_sdtm_ct_href()
    if not package_slug:
        return None

    url = (
        f"https://library.cdisc.org/api/mdr/ct/packages/"
        f"{package_slug}/codelists/C127262"
    )

    headers: dict[str, str] = {"Accept": "application/json"}
    subscription_key = os.environ.get("CDISC_SUBSCRIPTION_KEY")
    api_key = os.environ.get("CDISC_API_KEY") or subscription_key
    if subscription_key:
        headers["Ocp-Apim-Subscription-Key"] = subscription_key
    if api_key:
        headers["Authorization"] = f"Bearer {api_key}"
        headers["api-key"] = api_key

    def _match_term(term: dict[str, Any]) -> str | None:
        term_id = next(
            (
                term.get(field)
                for field in (
                    "conceptId",
                    "concept_id",
                    "code",
                    "termCode",
                    "term_code",
                )
                if term.get(field)
            ),
            None,
        )
        if term_id and str(term_id).lower() == target_code.lower():
            submission = term.get("submissionValue") or term.get(
                "cdisc_submission_value"
            )
            if submission:
                return str(submission).strip()
        return None

    def _extract_terms(data: Any) -> List[dict]:
        if isinstance(data, list):
            return [t for t in data if isinstance(t, dict)]
        if isinstance(data, dict):
            if isinstance(data.get("terms"), list):
                return [t for t in data["terms"] if isinstance(t, dict)]
            embedded = data.get("_embedded", {})
            if isinstance(embedded, dict) and isinstance(embedded.get("terms"), list):
                return [t for t in embedded["terms"] if isinstance(t, dict)]
        return []

    try:
        resp = requests.get(url, headers=headers, timeout=10)
        if resp.status_code != 200:
            return None
        payload = resp.json() or {}
    except Exception:
        return None

    for term in _extract_terms(payload):
        submission = _match_term(term)
        if submission:
            return submission

    term_links = payload.get("_links", {}).get("terms") or []
    if isinstance(term_links, dict):
        term_links = [term_links]

    for link in term_links:
        href = link.get("href")
        if not href:
            continue
        if href.startswith("/"):
            href = f"https://library.cdisc.org{href}"
        try:
            term_resp = requests.get(href, headers=headers, timeout=10)
            if term_resp.status_code != 200:
                continue
            term_data = term_resp.json() or {}
        except Exception:
            continue
        submission = _match_term(term_data if isinstance(term_data, dict) else {})
        if submission:
            return submission

    return None


# Generic function to return submission value for provided codelist_code and code
def get_submission_value_for_code(soa_id: int, codelist_code: str, code_uid: str):
    """Resolve the environmental setting submission value via CDISC Library."""
    if not code_uid:
        return None

    conn = _connect()
    cur = conn.cursor()
    cur.execute(
        "SELECT code FROM code_association WHERE soa_id=? AND code_uid=?",
        (soa_id, code_uid),
    )
    row = cur.fetchone()
    conn.close()
    if not row:
        return None
    target_code = str(row[0]).strip()

    package_slug = get_latest_sdtm_ct_href()
    if not package_slug:
        return None

    url = (
        f"https://library.cdisc.org/api/mdr/ct/packages/"
        f"{package_slug}/codelists/{codelist_code}"
    )

    headers: dict[str, str] = {"Accept": "application/json"}
    subscription_key = os.environ.get("CDISC_SUBSCRIPTION_KEY")
    api_key = os.environ.get("CDISC_API_KEY") or subscription_key
    if subscription_key:
        headers["Ocp-Apim-Subscription-Key"] = subscription_key
    if api_key:
        headers["Authorization"] = f"Bearer {api_key}"
        headers["api-key"] = api_key

    def _match_term(term: dict[str, Any]) -> str | None:
        term_id = next(
            (
                term.get(field)
                for field in (
                    "conceptId",
                    "concept_id",
                    "code",
                    "termCode",
                    "term_code",
                )
                if term.get(field)
            ),
            None,
        )
        if term_id and str(term_id).lower() == target_code.lower():
            submission = term.get("submissionValue") or term.get(
                "cdisc_submission_value"
            )
            if submission:
                return str(submission).strip()
        return None

    def _extract_terms(data: Any) -> List[dict]:
        if isinstance(data, list):
            return [t for t in data if isinstance(t, dict)]
        if isinstance(data, dict):
            if isinstance(data.get("terms"), list):
                return [t for t in data["terms"] if isinstance(t, dict)]
            embedded = data.get("_embedded", {})
            if isinstance(embedded, dict) and isinstance(embedded.get("terms"), list):
                return [t for t in embedded["terms"] if isinstance(t, dict)]
        return []

    try:
        resp = requests.get(url, headers=headers, timeout=10)
        if resp.status_code != 200:
            return None
        payload = resp.json() or {}
    except Exception:
        return None

    for term in _extract_terms(payload):
        submission = _match_term(term)
        if submission:
            return submission

    term_links = payload.get("_links", {}).get("terms") or []
    if isinstance(term_links, dict):
        term_links = [term_links]

    for link in term_links:
        href = link.get("href")
        if not href:
            continue
        if href.startswith("/"):
            href = f"https://library.cdisc.org{href}"
        try:
            term_resp = requests.get(href, headers=headers, timeout=10)
            if term_resp.status_code != 200:
                continue
            term_data = term_resp.json() or {}
        except Exception:
            continue
        submission = _match_term(term_data if isinstance(term_data, dict) else {})
        if submission:
            return submission

    return None


# Return environmentalSettings options from CDISC Library API
def load_environmental_setting_options(force: bool = False) -> List[dict[str, str]]:
    """Return [{'submissionValue': ..., 'conceptId': ...}, ...] for env settings."""
    now = time.time()
    if (
        not force
        and _env_setting_cache["options"]
        and now - _env_setting_cache["fetched_at"] < _ENV_SETTING_CACHE_TTL
    ):
        return _env_setting_cache["options"]

    slug = get_latest_sdtm_ct_href()
    if not slug:
        _env_setting_cache.update(options=[], fetched_at=now, last_error="missing slug")
        return []

    url = f"https://library.cdisc.org/api/mdr/ct/packages/" f"{slug}/codelists/C127262"
    headers: dict[str, str] = {"Accept": "application/json"}
    subscription_key = os.environ.get("CDISC_SUBSCRIPTION_KEY")
    api_key = os.environ.get("CDISC_API_KEY") or subscription_key
    if subscription_key:
        headers["Ocp-Apim-Subscription-Key"] = subscription_key
    if api_key:
        headers["Authorization"] = f"Bearer {api_key}"
        headers["api-key"] = api_key

    def _collect_terms(payload: Any) -> List[dict]:
        if isinstance(payload, list):
            return [t for t in payload if isinstance(t, dict)]
        if isinstance(payload, dict):
            if isinstance(payload.get("terms"), list):
                return [t for t in payload["terms"] if isinstance(t, dict)]
            embedded = payload.get("_embedded", {})
            if isinstance(embedded, dict) and isinstance(embedded.get("terms"), list):
                return [t for t in embedded["terms"] if isinstance(t, dict)]
        return []

    options: list[dict[str, str]] = []
    try:
        resp = requests.get(url, headers=headers, timeout=10)
        if resp.status_code != 200:
            raise RuntimeError(f"HTTP {resp.status_code}")
        data = resp.json() or {}
        terms = _collect_terms(data)

        def _ensure_option(term: dict) -> None:
            concept = term.get("conceptId") or term.get("code") or term.get("termCode")
            submission = term.get("submissionValue") or term.get(
                "cdisc_submission_value"
            )
            if concept and submission:
                options.append(
                    {
                        "conceptId": str(concept).strip(),
                        "submissionValue": str(submission).strip(),
                        "package": slug,
                    }
                )

        for term in terms:
            _ensure_option(term)

        if not options:
            for term in terms:
                href = term.get("href") or term.get("_href")
                if not href:
                    link_self = term.get("_links", {}).get("self", {})
                    href = (
                        link_self.get("href") if isinstance(link_self, dict) else None
                    )
                if not href:
                    continue
                if href.startswith("/"):
                    href = f"https://library.cdisc.org{href}"
                try:
                    t_resp = requests.get(href, headers=headers, timeout=10)
                    if t_resp.status_code == 200:
                        _ensure_option(t_resp.json() or {})
                except Exception:
                    continue

        options.sort(key=lambda item: item["submissionValue"])
        _env_setting_cache.update(options=options, fetched_at=now, last_error=None)
    except Exception as exc:
        _env_setting_cache.update(options=[], fetched_at=now, last_error=str(exc))
        options = []

    return options


# Return contact mode options from CDISC Library API
def load_contact_mode_options(force: bool = False) -> List[dict[str, str]]:
    """Return [{'submissionValue': ..., 'conceptId': ...}] for contact modes"""
    now = time.time()
    if (
        not force
        and _contact_mode_cache["options"]
        and now - _contact_mode_cache["fetched_at"] < _ENV_SETTING_CACHE_TTL
    ):
        return _contact_mode_cache["options"]

    slug = get_latest_sdtm_ct_href()
    if not slug:
        _contact_mode_cache.update(
            optoins=[], fetched_at=now, last_error="missing_slug"
        )
        return []

    url = f"https://library.cdisc.org/api/mdr/ct/packages/" f"{slug}/codelists/C171445"
    headers: dict[str, str] = {"Accept": "application/json"}
    subscription_key = os.environ.get("CDISC_SUBSCRIPTION_KEY")
    api_key = os.environ.get("CDISC_API_KEY") or subscription_key
    if subscription_key:
        headers["Ocp-Apim-Subscription-Key"] = subscription_key
    if api_key:
        headers["Authorization"] = f"Bearer {api_key}"
        headers["api-key"] = api_key

    def _collect_terms(payload: Any) -> List[dict]:
        if isinstance(payload, list):
            return [t for t in payload if isinstance(t, dict)]
        if isinstance(payload, dict):
            if isinstance(payload.get("terms"), list):
                return [t for t in payload["terms"] if isinstance(t, dict)]
            embedded = payload.get("_embedded", {})
            if isinstance(embedded, dict) and isinstance(embedded.get("terms"), list):
                return [t for t in embedded["terms"] if isinstance(t, dict)]
        return []

    options: list[dict[str, str]] = []
    try:
        resp = requests.get(url, headers=headers, timeout=10)
        if resp.status_code != 200:
            raise RuntimeError(f"HTTP {resp.status_code}")
        data = resp.json() or {}
        terms = _collect_terms(data)

        def _ensure_options(term: dict) -> None:
            concept = term.get("conceptId") or term.get("code") or term.get("termCode")
            submission = term.get("submissionValue") or term.get(
                "cdisc_submission_value"
            )
            if concept and submission:
                options.append(
                    {
                        "conceptId": str(concept).strip(),
                        "submissionValue": str(submission).strip(),
                        "package": slug,
                    }
                )

        for term in terms:
            _ensure_options(term)

        if not options:
            for term in terms:
                href = term.get("href") or term.get("_href")
                if not href:
                    link_self = term.gbet("_links", {}).get("self", {})
                    href = (
                        link_self.get("href") if isinstance(link_self, dict) else None
                    )
                if not href:
                    continue
                if href.startswith("/"):
                    href = f"https://library.cdisc.org{href}"
                try:
                    t_resp = requests.get(href, headers=headers, timeout=10)
                    if t_resp.status_code == 200:
                        _ensure_options(t_resp.json() or {})
                except Exception:
                    continue

            options.sort(key=lambda item: item["submissionValue"])
            _contact_mode_cache.update(options=options, fetched_at=now, last_error=None)
    except Exception as exc:
        _contact_mode_cache.update(options=[], fetched_at=now, last_error=str(exc))
        options = []

    return options
