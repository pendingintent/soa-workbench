# utils for usdm generators
import os
import functools
import requests
import logging
from urllib.parse import urlparse
from typing import List, Dict, Optional, Any, Tuple

from soa_builder.web.db import _connect
from soa_builder.web.utils import get_latest_sdtm_ct_href as _get_latest_sdtm_ct_href

logger = logging.getLogger("usdm.usdm_utils")
URL_PREFIX = (
    os.environ.get(
        "CDISC_BC_API_BASE_URL",
        "https://api.library.cdisc.org/api/cosmos/v2",
    )
    + "/"
)


# Generic helper functions for USDM generator scripts
def _build_api_headers() -> dict:
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
    return headers


# Generic function to return submission value for provided codelist_code and code
@functools.lru_cache(maxsize=256)
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

    package_slug = _get_latest_sdtm_ct_href()
    if not package_slug:
        return None

    url = (
        f"https://library.cdisc.org/api/mdr/ct/packages/"
        f"{package_slug}/codelists/{codelist_code}"
    )

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
        resp = requests.get(url, headers=_build_api_headers(), timeout=10)
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
            term_resp = requests.get(href, headers=_build_api_headers(), timeout=10)
            if term_resp.status_code != 200:
                continue
            term_data = term_resp.json() or {}
        except Exception:
            continue
        submission = _match_term(term_data if isinstance(term_data, dict) else {})
        if submission:
            return submission

    return None


# Helper functions for populating biomedical concepts
@functools.lru_cache(maxsize=256)
def _fetch_dss_spec(dss_href: str) -> Dict[str, Any]:
    """Fetch the full SDTM dataset specialization from a stored DSS href.

    Returns the raw API response dict (which contains ``variables[]``,
    ``_links.parentPackage``, etc.), or ``{}`` on any error.
    Cached per href for the process lifetime.
    """
    if not dss_href:
        return {}
    try:
        resp = requests.get(dss_href, headers=_build_api_headers(), timeout=15)
        if resp.status_code != 200:
            logger.warning(
                "_fetch_dss_spec: %s returned %s", dss_href, resp.status_code
            )
            return {}
        return resp.json()
    except (requests.RequestException, ValueError) as e:
        logger.warning("_fetch_dss_spec failed for %s: %s", dss_href, e)
        return {}


@functools.lru_cache(maxsize=128)
def _fetch_dss_variable_map(dss_href: str) -> Dict[str, List[str]]:
    """Fetch a DSS href once and return {variable_name: valueList}. Cached per href."""
    data = _fetch_dss_spec(dss_href)
    return {v["name"]: v.get("valueList", []) for v in data.get("variables", [])}


@functools.lru_cache(maxsize=256)
def _get_dss_response_codes(
    biomedical_concept_uid: str, variable_name: str, soa_id: int
) -> List[str]:
    """Return responseCodes for a single DSS variable. Cached per (bc_uid, name, soa_id)."""
    conn = _connect()
    cur = conn.cursor()
    cur.execute(
        "SELECT acd.dss_href FROM activity_concept_dss acd"
        " JOIN activity_concept ac"
        " ON ac.soa_id=acd.soa_id"
        " AND ac.activity_id=acd.activity_id"
        " AND ac.concept_code=acd.concept_code"
        " WHERE ac.concept_uid=? AND acd.soa_id=?"
        " LIMIT 1",
        (biomedical_concept_uid, soa_id),
    )
    row = cur.fetchone()
    conn.close()
    if not row or not row[0]:
        return []
    return _fetch_dss_variable_map(row[0]).get(variable_name, [])


@functools.lru_cache(maxsize=1)
def _get_latest_bc_package_version() -> str:
    """Return the latest CDISC BC package date string (e.g. '2024-09-27').

    Used as ``codeSystemVersion`` on BiomedicalConceptProperty codes.
    Falls back to '' on any error.
    """
    url = URL_PREFIX + "mdr/bc/packages"
    try:
        resp = requests.get(url, headers=_build_api_headers(), timeout=15)
        if resp.status_code != 200:
            return ""
        payload = resp.json() or {}
    except (requests.RequestException, ValueError):
        return ""

    packages: list = []
    if isinstance(payload, list):
        packages = payload
    elif isinstance(payload, dict):
        packages = (
            payload.get("_links", {}).get("packages")
            or payload.get("packages")
            or payload.get("_embedded", {}).get("packages")
            or payload.get("items")
            or []
        )

    latest_date = (0, 0, 0)
    latest_version = ""
    for pkg in packages:
        if not isinstance(pkg, dict):
            continue
        href = (
            pkg.get("href")
            or (pkg.get("_links") or {}).get("self", {}).get("href", "")
            or ""
        )
        # href like /mdr/bc/packages/bc2024-09-27
        slug = href.rstrip("/").split("/")[-1]
        # slug like bc2024-09-27
        parts = slug.lstrip("bc").split("-")
        if len(parts) == 3:
            try:
                date_tuple = (int(parts[0]), int(parts[1]), int(parts[2]))
                version = "-".join(parts)
                if date_tuple > latest_date:
                    latest_date = date_tuple
                    latest_version = version
            except ValueError:
                continue
    return latest_version


_biomedical_concept_cache: Dict[str, Dict[str, Any]] = {}


def _get_biomedical_concept_data(concept_code: str) -> Dict[str, Any]:
    """Fetch the full CDISC Biomedical Concept API response.

    Cached per code, but only successful (200) responses are cached —
    a transient failure (rate limit, timeout, auth hiccup) must not
    permanently poison the cache for the life of the process.
    """
    if concept_code in _biomedical_concept_cache:
        return _biomedical_concept_cache[concept_code]
    url = URL_PREFIX + "mdr/bc/biomedicalconcepts/" + concept_code
    try:
        resp = requests.get(url, headers=_build_api_headers(), timeout=15)
        if resp.status_code != 200:
            logger.warning(
                "_get_biomedical_concept_data: %s returned %s",
                url,
                resp.status_code,
            )
            return {}
        data = resp.json()
    except (requests.RequestException, ValueError) as e:
        logger.warning(
            "_get_biomedical_concept_data failed for %s: %s", concept_code, e
        )
        return {}
    _biomedical_concept_cache[concept_code] = data
    return data


def _absolute_url(href: str) -> str:
    if href.startswith("http"):
        return href
    return (
        os.environ.get(
            "CDISC_BC_API_BASE_URL",
            "https://api.library.cdisc.org/api/cosmos/v2",
        )
        + href
    )


@functools.lru_cache(maxsize=1)
def _get_sdtm_package_specialization_index() -> Dict[str, str]:
    """Return {generic_bc_concept_code: sdtm_spec_full_url}.

    Iterates ALL SDTM packages (not just the latest) so every dataset
    specialization is indexed, regardless of which package version it
    first appeared in.  Matches the approach in cdisc_bc_library.py.

    Cached for the process lifetime; built lazily on first use.
    """
    try:
        resp = requests.get(
            URL_PREFIX + "mdr/specializations/sdtm/packages",
            headers=_build_api_headers(),
            timeout=15,
        )
        if resp.status_code != 200:
            logger.warning(
                "_get_sdtm_package_specialization_index: packages request returned %s",
                resp.status_code,
            )
            return {}
        packages = resp.json().get("_links", {}).get("packages") or []
        if not packages:
            logger.warning("_get_sdtm_package_specialization_index: no packages found")
            return {}

        logger.info(
            "_get_sdtm_package_specialization_index: building index"
            " from %d SDTM packages",
            len(packages),
        )
        index: Dict[str, str] = {}
        for pkg in packages:
            pkg_href = pkg.get("href", "")
            if not pkg_href:
                continue
            pkg_resp = requests.get(
                _absolute_url(pkg_href),
                headers=_build_api_headers(),
                timeout=15,
            )
            if pkg_resp.status_code != 200:
                logger.warning(
                    "_get_sdtm_package_specialization_index: package %s returned %s",
                    pkg_href,
                    pkg_resp.status_code,
                )
                continue
            specs = (
                pkg_resp.json().get("_links", {}).get("datasetSpecializations") or []
            )
            for spec in specs:
                spec_href = spec.get("href", "")
                if not spec_href:
                    continue
                full_url = _absolute_url(spec_href)
                sr = requests.get(full_url, headers=_build_api_headers(), timeout=15)
                if sr.status_code != 200:
                    continue
                parent_href = (
                    sr.json()
                    .get("_links", {})
                    .get("parentBiomedicalConcept", {})
                    .get("href", "")
                )
                if parent_href:
                    concept_code = parent_href.rstrip("/").split("/")[-1]
                    index[concept_code] = full_url

        logger.info(
            "_get_sdtm_package_specialization_index: indexed %d BCs",
            len(index),
        )
        return index
    except (requests.RequestException, ValueError) as e:
        logger.exception("_get_sdtm_package_specialization_index failed: %s", e)
        return {}


@functools.lru_cache(maxsize=256)
def _get_sdtm_specialization_data(concept_code: str) -> Dict[str, Any]:
    """Return the SDTM dataset specialization for a BC, or {} if none.

    Uses the package-navigation index to find the spec URL, then fetches
    the full specialization dict (which contains ``variables[]``).
    """
    spec_url = _get_sdtm_package_specialization_index().get(concept_code)
    if not spec_url:
        return {}
    try:
        resp = requests.get(spec_url, headers=_build_api_headers(), timeout=15)
        if resp.status_code == 200:
            return resp.json()
    except (requests.RequestException, ValueError):
        pass
    return {}


def _get_biomedical_concept_synonyms(concept_code: str) -> List[str]:
    """Return synonyms from the BC API response."""
    return _get_biomedical_concept_data(concept_code).get("synonyms", []) or []


def _get_biomedical_concept_reference(concept_code: str) -> str:
    """Return the CDISC Library self-link for a BC (e.g.
    '/mdr/bc/biomedicalconcepts/C105585'), or '' if unavailable."""
    data = _get_biomedical_concept_data(concept_code)
    return (data.get("_links") or {}).get("self", {}).get("href", "") or ""


# Helper for Activities
@functools.lru_cache(maxsize=256)
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


# Helper for Arms
def _get_type_code_tuple(soa_id: int, code_uid: str) -> Tuple[str, str, str, str]:
    """Fetch ARM type codes enriched from CDISC Library Protocol CT."""
    from soa_builder.web.utils import get_protocol_ct_rows, get_protocol_ct_term

    conn = _connect()
    cur = conn.cursor()
    cur.execute(
        "SELECT DISTINCT codelist_table, code, codelist_code "
        "FROM code_association WHERE soa_id=? AND code_uid=?",
        (soa_id, code_uid),
    )
    rows = cur.fetchall()
    conn.close()

    payload = get_protocol_ct_rows()
    slug = payload.get("slug") or ""
    version = ""
    if slug:
        parts = slug.split("-")
        if len(parts) >= 4:
            version = f"{parts[-3]}-{parts[-2]}-{parts[-1]}"

    code_system: list = []
    code_code: list = []
    code_decode: list = []
    code_system_version: list = []
    for codelist_table, code, codelist_code in rows:
        term = get_protocol_ct_term(codelist_code, code)
        if not term:
            continue
        code_system.append(codelist_table)
        code_code.append(code)
        code_decode.append(term.get("submission_value") or "")
        code_system_version.append(version)
    return code_code, code_decode, code_system, code_system_version


def _get_data_origin_type_tuple(
    soa_id: int, code_uid: str
) -> Tuple[str, str, str, str]:
    """Enrich ARM data-origin-type code_association rows via CDISC Library DDF CT."""
    from soa_builder.web.utils import get_ddf_ct_rows, get_ddf_ct_term

    conn = _connect()
    cur = conn.cursor()
    cur.execute(
        "SELECT DISTINCT codelist_table, code, codelist_code "
        "FROM code_association WHERE soa_id=? AND code_uid=?",
        (soa_id, code_uid),
    )
    rows = cur.fetchall()
    conn.close()

    payload = get_ddf_ct_rows()
    slug = payload.get("slug") or ""
    version = ""
    if slug:
        parts = slug.split("-")
        if len(parts) >= 4:
            version = f"{parts[-3]}-{parts[-2]}-{parts[-1]}"

    code_system: list = []
    code_code: list = []
    code_decode: list = []
    code_system_version: list = []
    for codelist_table, code, codelist_code in rows:
        term = get_ddf_ct_term(codelist_code, code)
        if not term:
            continue
        code_system.append(codelist_table)
        code_code.append(code)
        code_decode.append(term.get("submission_value") or "")
        code_system_version.append(version)
    return code_code, code_decode, code_system, code_system_version


# Helpders for Elements
def _get_transition_end_rule(
    soa_id: int, transition_rule_uid: Optional[str]
) -> Optional[Dict[str, Any]]:
    if not transition_rule_uid:
        return None
    conn = _connect()
    cur = conn.cursor()
    cur.execute(
        "SELECT tr.name, tr.label, tr.description, tr.text FROM transition_rule tr WHERE soa_id=? AND transition_rule_uid=?",
        (soa_id, transition_rule_uid),
    )
    row = cur.fetchone()
    conn.close()
    if not row:
        return None
    return {
        "id": transition_rule_uid,
        "extensionAttributes": [],
        "name": row[0] or None,
        "label": row[1] or None,
        "description": row[2] or None,
        "text": row[3] or None,
        "instanceType": "TransitionRule",
    }


def _get_transition_start_rule(
    soa_id: int, transition_rule_uid: Optional[str]
) -> Optional[Dict[str, Any]]:
    if not transition_rule_uid:
        return None
    conn = _connect()
    cur = conn.cursor()
    cur.execute(
        "SELECT tr.name, tr.label, tr.description, tr.text FROM transition_rule tr WHERE soa_id=? AND transition_rule_uid=?",
        (soa_id, transition_rule_uid),
    )
    row = cur.fetchone()
    conn.close()
    if not row:
        return None
    return {
        "id": transition_rule_uid,
        "extensionAttributes": [],
        "name": row[0] or None,
        "label": row[1] or None,
        "description": row[2] or None,
        "text": row[3] or None,
        "instanceType": "TransitionRule",
    }


def _get_timing_name(soa_id: int, timing_id: Optional[int]) -> Optional[str]:
    """
    Get timing UID for a given timing ID.

    Args:
        soa_id: The SOA ID
        timing_id: The timing table ID (can be None)

    Returns:
        Timing UID string or None if not found or timing_id is None
    """
    if timing_id is None:
        return None

    conn = _connect()
    cur = conn.cursor()
    cur.execute(
        "SELECT timing_uid FROM timing WHERE id=? AND soa_id=?",
        (timing_id, soa_id),
    )
    row = cur.fetchone()
    conn.close()

    return row[0] if (row and row[0] is not None) else None


def _get_code_tuple(soa_id: int, code_uid: str) -> Tuple[List[str], List[str]]:
    """
    Get code and code system lists for a given code UID.

    Args:
        soa_id: The SOA ID
        code_uid: The Code UID (e.g., 'Code_1')

    Returns:
        Tuple of ([codes], [code_systems]) - both are lists that may be empty

    Note:
        Returns lists because a single code_uid can map to multiple codes
        via code_association table (e.g., same concept in multiple codelists).
        Callers should handle list unpacking appropriately.
    """
    conn = _connect()
    cur = conn.cursor()
    cur.execute(
        "SELECT DISTINCT c.codelist_table, c.code "
        "FROM code_association c WHERE c.soa_id=? AND c.code_uid=?",
        (soa_id, code_uid),
    )
    rows = cur.fetchall()
    conn.close()

    code_system = [r[0] for r in rows]
    code = [r[1] for r in rows]

    return code, code_system


def _build_level_code(
    soa_id: int, code_uid: Optional[str], kind: str
) -> Dict[str, Any]:
    """
    Return a USDM Code-Output dict for an Objective/Endpoint level.

    Falls back to an empty Code shell when the association is missing.
    The submission value stored in code_association.code is reused for
    both code and decode (the level codelists C188725/C188726 store the
    submission value, not a C-code).
    """
    if not code_uid:
        return {
            "id": f"Code_{kind}Level_unknown",
            "extensionAttributes": [],
            "code": "",
            "codeSystem": "",
            "codeSystemVersion": "",
            "decode": "",
            "instanceType": "Code",
        }
    conn = _connect()
    cur = conn.cursor()
    cur.execute(
        "SELECT codelist_table, codelist_code, code "
        "FROM code_association WHERE soa_id=? AND code_uid=? LIMIT 1",
        (soa_id, code_uid),
    )
    row = cur.fetchone()
    conn.close()
    if not row:
        return {
            "id": code_uid,
            "extensionAttributes": [],
            "code": "",
            "codeSystem": "",
            "codeSystemVersion": "",
            "decode": "",
            "instanceType": "Code",
        }
    codelist_table, _codelist_code, code = row
    version = ""
    slug = (codelist_table or "").rstrip("/").split("/")[-1]
    if slug:
        parts = slug.split("-")
        if len(parts) >= 4:
            version = f"{parts[-3]}-{parts[-2]}-{parts[-1]}"
    return {
        "id": code_uid,
        "extensionAttributes": [],
        "code": code or "",
        "codeSystem": "http://www.cdisc.org",
        "codeSystemVersion": version,
        "decode": code or "",
        "instanceType": "Code",
    }


# Helper functions for study timing
def _load_generate_study_timings():
    """Return the timing builder from usdm.generate_study_timings (tries several names)."""
    try:
        import usdm.generate_study_timings as gst
    except Exception:
        import sys
        from pathlib import Path

        here = Path(__file__).resolve()
        src_dir = here.parents[2] / "src"
        if src_dir.exists() and str(src_dir) not in sys.path:
            sys.path.insert(0, str(src_dir))
        import usdm.generate_study_timings as gst
    for name in (
        "build_usdm_study_timings",
        "build_usdm_timings",
        "generate_study_timings",
    ):
        fn = getattr(gst, name, None)
        if callable(fn):
            return fn
    raise ImportError("usdm.generate_study_timings missing expected builder function")


def _load_generate_study_instances():
    """Return the instances builder from usdm.generate_scheduled_activity_instances."""
    try:
        import usdm.generate_scheduled_activity_instances as gsai
    except Exception:
        import sys
        from pathlib import Path

        here = Path(__file__).resolve()
        src_dir = here.parents[2] / "src"
        if src_dir.exists() and str(src_dir) not in sys.path:
            sys.path.insert(0, str(src_dir))
        import usdm.generate_scheduled_activity_instances as gsai
    for name in (
        "build_usdm_instances",
        "generate_scheduled_activity_instances",
    ):
        fn = getattr(gsai, name, None)
        if callable(fn):
            return fn
    raise ImportError(
        "usdm.generate_scheduled_activity_instances missing expected builder function"
    )


def _load_generate_decision_instances():
    """Return the decision instances builder from usdm.generate_scheduled_decision_instances."""
    try:
        import usdm.generate_scheduled_decision_instances as gsdi
    except Exception:
        import sys
        from pathlib import Path

        here = Path(__file__).resolve()
        src_dir = here.parents[2] / "src"
        if src_dir.exists() and str(src_dir) not in sys.path:
            sys.path.insert(0, str(src_dir))
        import usdm.generate_scheduled_decision_instances as gsdi
    fn = getattr(gsdi, "build_usdm_decision_instances", None)
    if callable(fn):
        return fn
    raise ImportError(
        "usdm.generate_scheduled_decision_instances missing build_usdm_decision_instances"
    )


# Helpers for Scheduled Activity Instances
def _get_activity_ids(soa_id: int, encounter_uid: str) -> List[str]:
    conn = _connect()
    cur = conn.cursor()
    cur.execute(
        "SELECT a.activity_uid from activity a "
        "INNER JOIN matrix_cells m ON a.id = m.activity_id AND a.soa_id = m.soa_id "
        "INNER JOIN visit v ON m.visit_id = v.id AND m.soa_id = v.soa_id "
        "INNER JOIN instances i ON v.encounter_uid = i.encounter_uid AND v.soa_id = i.soa_id "
        "WHERE i.soa_id=? and i.encounter_uid=?",
        (
            soa_id,
            encounter_uid,
        ),
    )
    rows = cur.fetchall()
    conn.close()
    activity_uids = [r[0] for r in rows] or []
    return activity_uids


# Helpers for Scheduled Decision Instances
def _get_condition_assignments(
    soa_id: int, decision_instance_uid: str
) -> List[Dict[str, Any]]:
    conn = _connect()
    cur = conn.cursor()
    cur.execute(
        "SELECT condition_assignment_uid, condition, condition_target_uid "
        "FROM condition_assignment "
        "WHERE soa_id=? AND decision_instance_uid=? "
        "ORDER BY order_index, id",
        (soa_id, decision_instance_uid),
    )
    rows = cur.fetchall()
    conn.close()
    return [
        {
            "id": r[0],
            "extensionAttributes": [],
            "condition": r[1],
            "conditionTargetId": r[2],
            "instanceType": "Condition",
        }
        for r in rows
    ]


# Helpers for Study Cells
def _get_element_ids(soa_id: int, study_cell_uid: str) -> List[str]:
    conn = _connect()
    cur = conn.cursor()
    cur.execute(
        "SELECT element_uid from study_cell WHERE soa_id=? AND study_cell_uid=? ORDER BY element_uid",
        (
            soa_id,
            study_cell_uid,
        ),
    )
    rows = cur.fetchall()
    conn.close()
    # Deduplicate and preserve stable order
    element_uids = [r[0] for r in rows] or []
    seen = set()
    ordered_unique: List[str] = []
    for uid in element_uids:
        if uid and uid not in seen:
            seen.add(uid)
            ordered_unique.append(uid)
    return ordered_unique


# Helpers for Study Epochs
@functools.lru_cache(maxsize=256)
def _get_epoch_code_values(
    soa_id: int, epoch_type: str, code: str
) -> Tuple[str, str, str]:
    """
    Fetch epoch code values from CDISC Library API.

    Args:
        soa_id: SOA ID (for logging/context)
        epoch_type: Epoch type identifier
        code: CDISC concept ID to look up (e.g., 'C99079')

    Returns:
        Tuple of (code_system, code_system_version, decode)
        Returns empty strings if API fails or term not found
    """
    logger = logging.getLogger("usdm.generate_epochs")

    # Initialize safe defaults (prevents UnboundLocalError)
    code_system = ""
    code_system_version = ""
    decode = ""

    url = "https://library.cdisc.org/api/mdr/ct/packages/sdtmct-2025-09-26/codelists/C99079"

    try:
        resp = requests.get(url, headers=_build_api_headers(), timeout=10)
        if resp.status_code != 200:
            logger.warning(
                "Failed to fetch epoch codes from %s (status %d) for code %s",
                url,
                resp.status_code,
                code,
            )
            return code_system, code_system_version, decode

        content = resp.json()
        parsed_url = urlparse(url)
        code_system = parsed_url.scheme + "://" + parsed_url.netloc
        code_system_version = parsed_url.path.split("/", 7)[5]

        # Guard against missing 'terms' key
        top_terms = content.get("terms") or []
        for term in top_terms:
            if term.get("conceptId") == code:
                decode = term.get("submissionValue") or ""
                break  # Found matching term, exit loop

        if not decode:
            logger.debug(
                "No matching term found for conceptId=%s in %s",
                code,
                url,
            )

    except requests.RequestException as e:
        logger.warning(
            "Request error fetching epoch codes from %s: %s",
            url,
            e,
        )
    except (ValueError, KeyError) as e:
        logger.warning(
            "Error parsing epoch code response from %s: %s",
            url,
            e,
        )

    return code_system, code_system_version, decode


# Helpers for Study Timings
def _get_timing_code_values(soa_id: int, code_uid: str) -> Tuple[str, str, str, str]:
    """Enrich timing code_association rows via CDISC Library DDF CT."""
    from soa_builder.web.utils import get_ddf_ct_rows, get_ddf_ct_term

    conn = _connect()
    cur = conn.cursor()
    cur.execute(
        "SELECT DISTINCT codelist_table, code, codelist_code "
        "FROM code_association WHERE soa_id=? AND code_uid=?",
        (soa_id, code_uid),
    )
    rows = cur.fetchall()
    conn.close()

    payload = get_ddf_ct_rows()
    slug = payload.get("slug") or ""
    version = ""
    if slug:
        parts = slug.split("-")
        if len(parts) >= 4:
            version = f"{parts[-3]}-{parts[-2]}-{parts[-1]}"

    code_system: list = []
    code_code: list = []
    code_decode: list = []
    code_system_version: list = []
    for codelist_table, code, codelist_code in rows:
        term = get_ddf_ct_term(codelist_code, code)
        if not term:
            continue
        code_system.append(codelist_table)
        code_code.append(code)
        code_decode.append(term.get("submission_value") or "")
        code_system_version.append(version)
    return code_code, code_decode, code_system, code_system_version


# Helper to return study metadata
def _get_soa_metadata(soa_id: int) -> Dict[str, Optional[str]]:
    conn = _connect()
    cur = conn.cursor()
    cur.execute(
        "SELECT name, study_id, study_label, study_description FROM soa WHERE id=?",
        (soa_id,),
    )
    row = cur.fetchone()
    conn.close()
    if row is None:
        raise ValueError(f"No SOA found with id={soa_id}")
    return {
        "name": row[0],
        "study_id": row[1],
        "study_label": row[2],
        "study_description": row[3],
    }
