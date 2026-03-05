#!/usr/bin/env python3
from concurrent.futures import ThreadPoolExecutor
from typing import Optional, List, Dict, Any
import functools
import os
import requests

from soa_builder.web.utils import _connect

# GLobal API URL prefix
URL_PREFIX = "https://api.library.cdisc.org/api/cosmos/v2/"


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


def build_usdm_biomedical_concepts(soa_id: int) -> List[Dict[str, Any]]:
    """
    Build USDM BiomedicalConcept-Output objects for the given SOA.

    USDM BiomedicalConcept-Output (subset):
        - id: string
        - name?: string
        - label?: string
        - extensionAttributes?: ExtensionAttribute-Output[] (empty)
        - synonyms?: []
        - reference?: string
        - code?: string
        - notes?: CommentAnnotation-Output[]
        - instanceType: "BiomedicalConcept"
    """
    conn = _connect()
    cur = conn.cursor()
    cur.execute(
        """
        SELECT
            bc.biomedical_concept_uid id,
            bc.name name,
            bc.label label,
            bc.code alias_code,
            ac.concept_code concept_code,
            ac.dss_href reference,
            c.code_uid code_uid,
            c.code_system code_system,
            c.code_system_version code_system_version,
            c.decode decode
        FROM biomedical_concept bc
        INNER JOIN activity_concept ac ON bc.biomedical_concept_uid = ac.concept_uid AND bc.soa_id = ac.soa_id
        INNER JOIN alias_code a ON bc.code = a.alias_code_uid AND bc.soa_id = a.soa_id
        INNER JOIN code c ON a.standard_code = c.code_uid AND a.soa_id = c.soa_id
        WHERE bc.soa_id = ?
        ORDER BY bc.id;
        """,
        (soa_id,),
    )
    rows = cur.fetchall()
    conn.close()

    # Prefetch all synonyms in parallel — one API call per concept, all concurrent
    concept_codes = [r[4] for r in rows]
    with ThreadPoolExecutor(max_workers=8) as pool:
        synonyms_list = list(pool.map(_get_biomedical_concept_synonyms, concept_codes))
    synonyms_map = dict(zip(concept_codes, synonyms_list))

    out: List[Dict[str, Any]] = []

    for r in rows:
        id = r[0]
        name = r[1]
        label = r[2]
        alias_code = r[3]
        concept_code = r[4]
        reference = r[5]
        code_uid = r[6]
        code_system = r[7]
        code_system_version = r[8]
        decode = r[9]

        synonyms = synonyms_map[concept_code]

        biomedical_concept = {
            "id": id,
            "extensionAttributes": [],
            "name": name,
            "label": label,
            "synonyms": synonyms,
            "reference": reference,
            "properties": _get_biomedical_concept_properties(soa_id, id),
            "code": {
                "id": alias_code,
                "extensionAttributes": [],
                "standardCode": {
                    "id": code_uid,
                    "extensionAttributes": [],
                    "code": concept_code,
                    "codeSystem": code_system,
                    "codeSystemVersion": code_system_version,
                    "decode": decode,
                    "instanceType": "Code",
                },
                "standardCodeAliases": [],
                "instanceType": "AliasCode",
            },
            "notes": [],
            "instanceType": "BiomedicalConcept",
        }
        out.append(biomedical_concept)
    return out


if __name__ == "__main__":
    import argparse
    import json
    import logging
    import sys

    logger = logging.getLogger("usdm.generate_biomedical_concepts")

    parser = argparse.ArgumentParser(
        description="Export USDM Biomedical Concepts for a SOA."
    )
    parser.add_argument(
        "soa_id", type=int, help="SOA id to export Biomedical Concepts for"
    )
    parser.add_argument(
        "-o", "--output", default="-", help="Output file path or '-' for stdout"
    )
    parser.add_argument("--indent", type=int, default=2, help="JSON indent")
    args = parser.parse_args()

    try:
        biomedical_concepts = build_usdm_biomedical_concepts(args.soa_id)
    except Exception:
        logger.exception(
            "Failed to build Biomedical Concepts for soa_id=%s", args.soa_id
        )
        sys.exit(1)

    payload = json.dumps(biomedical_concepts, indent=args.indent)
    if args.output in ("-", "/dev/stdout"):
        sys.stdout.write(
            "Output suppressed: this document may contain sensitive data. "
            "Use an explicit -o <file> path to export.\n"
        )
    else:
        with open(args.output, "w", encoding="utf-8") as f:
            f.write(payload + "\n")
