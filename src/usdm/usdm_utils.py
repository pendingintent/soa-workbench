# utils for usdm generators
import os
import functools
import requests
import logging
from urllib.parse import urlparse
from typing import List, Dict, Optional, Any, Tuple
from soa_builder.web.db import _connect
from soa_builder.web.utils import get_latest_sdtm_ct_href as _get_latest_sdtm_ct_href

URL_PREFIX = "https://api.library.cdisc.org/api/cosmos/v2/"


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


# Helper functions for populating biomedical concepts
@functools.lru_cache(maxsize=128)
def _fetch_dss_variable_map(dss_href: str) -> Dict[str, List[str]]:
    """Fetch a DSS href once and return {variable_name: valueList}. Cached per href."""
    try:
        resp = requests.get(dss_href, headers=_build_api_headers(), timeout=15)
        if resp.status_code != 200:
            return {}
        data = resp.json()
        return {v["name"]: v.get("valueList", []) for v in data.get("variables", [])}
    except (requests.RequestException, ValueError) as e:
        print(f"Error fetching DSS variable map: {e}")
        return {}


@functools.lru_cache(maxsize=256)
def _get_dss_response_codes(
    biomedical_concept_uid: str, variable_name: str, soa_id: int
) -> List[str]:
    """Return responseCodes for a single DSS variable. Cached per (bc_uid, name, soa_id)."""
    conn = _connect()
    cur = conn.cursor()
    cur.execute(
        "SELECT dss_href FROM activity_concept WHERE concept_uid=? AND soa_id=?",
        (biomedical_concept_uid, soa_id),
    )
    row = cur.fetchone()
    conn.close()
    if not row or not row[0]:
        return []
    return _fetch_dss_variable_map(row[0]).get(variable_name, [])


@functools.lru_cache(maxsize=256)
def _get_biomedical_concept_synonyms(concept_code: str) -> List[str]:
    """Fetch the synonyms of a biomedical concept using the CDISC API. Cached per code."""
    url = URL_PREFIX + "mdr/bc/biomedicalconcepts/" + concept_code
    try:
        resp = requests.get(url, headers=_build_api_headers(), timeout=15)
        if resp.status_code != 200:
            return []
        data = resp.json()
        return data.get("synonyms", [])
    except (requests.RequestException, ValueError) as e:
        print(f"Error fetching biomedical concept synonyms: {e}")
        return []


@functools.lru_cache(maxsize=256)
def _get_biomedical_concept_properties(
    soa_id: int, biomedical_concept_uid: str
) -> Optional[Dict[str, any]]:
    """Fetch biomedical concept properties from the database using the BiomedicalConcept_{}."""

    conn = _connect()
    cur = conn.cursor()
    cur.execute(
        """
        SELECT
            p.biomedical_concept_property_uid id,
            p.name name,
            p.label label,
            p.isRequired isRequired,
            p.datatype datatype,
            p.biomedical_concept_uid biomedical_concept_uid,
            a.alias_code_uid alias_code_uid,
            a.standard_code standard_code,
            c.code code,
            c.code_system code_system,
            c.code_system_version code_system_version,
            c.decode decode
        FROM biomedical_concept_property p
        INNER JOIN alias_code a ON p.code = a.alias_code_uid AND p.soa_id = a.soa_id
        INNER JOIN code c ON a.standard_code = c.code_uid AND a.soa_id = c.soa_id
        WHERE p.soa_id = ? AND p.biomedical_concept_uid = ?
        ORDER BY p.id;
        """,
        (soa_id, biomedical_concept_uid),
    )
    rows = cur.fetchall()
    cur.close()
    conn.close()
    out: List[Dict[str, Any]] = []

    for r in rows:
        id = r[0]
        name = r[1]
        label = r[2]
        isRequired = bool(r[3])
        datatype = r[4]
        bc_uid = r[5]
        alias_code_uid = r[6]
        standard_code = r[7]
        code = r[8]
        code_system = r[9]
        code_system_version = r[10]
        decode = r[11]

        isEnabled = None
        response_codes = _get_dss_response_codes(bc_uid, name, soa_id)

        property = {
            "id": id,
            "name": name,
            "label": label,
            "isRequired": isRequired,
            "isEnabled": isEnabled,
            "datatype": datatype,
            "responseCodes": response_codes,
            "code": {
                "id": alias_code_uid,
                "extensionAttributes": [],
                "standardCode": {
                    "id": standard_code,
                    "extensionAttributes": [],
                    "code": code,
                    "codeSystem": code_system,
                    "codeSystemVersion": code_system_version,
                    "decode": decode,
                    "instanceType": "Code",
                },
            },
            "notes": [],
            "instanceType": "BiomedicalConceptProperty",
        }
        out.append(property)

    return out


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
    """Fetch type codes for ARMS only.  These values are stored in the protocol_terminology table."""
    conn = _connect()
    cur = conn.cursor()
    cur.execute(
        "SELECT DISTINCT c.codelist_table, p.code,p.cdisc_submission_value,p.dataset_date "
        "FROM code_association c INNER JOIN protocol_terminology p ON c.codelist_code = p.codelist_code "
        "AND c.code = p.code WHERE c.soa_id=? AND c.code_uid=?",
        (
            soa_id,
            code_uid,
        ),
    )
    rows = cur.fetchall()
    conn.close()
    code_system = [r[0] for r in rows]
    code_code = [r[1] for r in rows]
    code_decode = [r[2] for r in rows]
    code_system_version = [r[3] for r in rows]

    return code_code, code_decode, code_system, code_system_version


def _get_data_origin_type_tuple(
    soa_id: int, code_uid: str
) -> Tuple[str, str, str, str]:
    conn = _connect()
    cur = conn.cursor()
    cur.execute(
        "SELECT DISTINCT c.codelist_table,d.code,d.cdisc_submission_value,d.dataset_date "
        "FROM code_association c INNER JOIN ddf_terminology d ON c.codelist_code = d.codelist_code "
        "AND c.code = d.code WHERE c.soa_id=? AND c.code_uid=?",
        (
            soa_id,
            code_uid,
        ),
    )
    rows = cur.fetchall()
    conn.close()
    code_system = [r[0] for r in rows]
    code_code = [r[1] for r in rows]
    code_decode = [r[2] for r in rows]
    code_system_version = [r[3] for r in rows]

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


def _get_timing_name(soa_id: int, timing_id: Optional[int]) -> str:
    conn = _connect()
    cur = conn.cursor()
    cur.execute(
        "SELECT timing_uid FROM timing WHERE id=? AND soa_id=?",
        (
            timing_id,
            soa_id,
        ),
    )
    row = cur.fetchone()
    conn.close()
    timing_uid = row[0] if (row and row[0] is not None) else None

    return timing_uid


def _get_code_tuple(soa_id: int, code_uid: str) -> Tuple[str, str]:
    conn = _connect()
    cur = conn.cursor()
    cur.execute(
        "SELECT DISTINCT c.codelist_table,c.code "
        "FROM code_association c WHERE c.soa_id=? AND c.code_uid=?",
        (
            soa_id,
            code_uid,
        ),
    )
    rows = cur.fetchall()
    conn.close()
    code_system = [r[0] for r in rows]
    code = [r[1] for r in rows]

    return code, code_system


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
def _get_epoch_code_values(soa_id: int, epoch_type: str, code: str) -> Tuple[
    str,
    str,
    str,
]:
    logger = logging.getLogger("usdm.generate_epochs")
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

    resp = requests.get(url, headers=headers, timeout=10)
    if resp.status_code != 200:
        logger.exception("No response from {} for code {}".format(url, epoch_type))
    else:
        content = resp.json()
        parsed_url = urlparse(url)
        code_system = parsed_url.scheme + "://" + parsed_url.netloc
        code_system_version = parsed_url.path.split("/", 7)[5]

        top_terms = content.get("terms")
        for term in top_terms:
            if term.get("conceptId") == code:
                decode = term.get("submissionValue")

    return code_system, code_system_version, decode


# Helpers for Study Timings
def _get_timing_code_values(soa_id: int, code_uid: str) -> Tuple[str, str, str, str]:
    conn = _connect()
    cur = conn.cursor()
    cur.execute(
        "SELECT DISTINCT c.codelist_table,d.code,d.cdisc_submission_value,d.dataset_date "
        "FROM code_association c INNER JOIN ddf_terminology d ON c.codelist_code = d.codelist_code "
        "AND c.code = d.code WHERE c.soa_id=? AND c.code_uid=?",
        (
            soa_id,
            code_uid,
        ),
    )
    rows = cur.fetchall()
    conn.close()
    code_system = [r[0] for r in rows]
    code_code = [r[1] for r in rows]
    code_decode = [r[2] for r in rows]
    code_system_version = [r[3] for r in rows]

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
