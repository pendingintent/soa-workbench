#!/usr/bin/env python3
# Prefer absolute import; fallback to adding src/ to sys.path when run directly
from typing import Optional, List, Dict, Any
import functools
import os
import requests

try:
    from soa_builder.web.app import _connect  # reuse existing DB connector
except ImportError:
    import sys
    from pathlib import Path

    here = Path(__file__).resolve()
    src_dir = here.parents[2] / "src"
    if src_dir.exists() and str(src_dir) not in sys.path:
        sys.path.insert(0, str(src_dir))
    from soa_builder.web.app import _connect  # type: ignore

try:
    from soa_builder.web.utils import (
        _get_biomedical_concept_ids as _get_biomedical_concept_ids,
    )
except ImportError:
    import sys
    from pathlib import Path

    sys.path.insert(0, str(Path(__file__).parent.parent))
    from soa_builder.web.utils import (
        _get_biomedical_concept_ids as _get_biomedical_concept_ids,
    )

try:
    from soa_builder.web.utils import _nz as _nz
except ImportError:
    import sys
    from pathlib import Path

    sys.path.insert(0, str(Path(__file__).parent.parent))
    from soa_builder.web.utils import _nz


# GLobal API URL prefix
URL_PREFIX = "https://api.library.cdisc.org/api/cosmos/v2/"


@functools.lru_cache(maxsize=256)
def _get_concept_by_code(concept_code: str) -> Optional[Dict[str, Any]]:
    """Fetch full concept data for concept_code from the CDISC Library API."""

    url = URL_PREFIX + "mdr/bc/biomedicalconcepts/" + concept_code
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
        resp = requests.get(url, headers=headers, timeout=15)
        if resp.status_code != 200:
            return None
        data = resp.json()
        if not isinstance(data, dict):
            return None
        code = data.get("conceptId")
        title = data.get("title") or data.get("name") or data.get("label") or code
        return {"code": str(code), "title": str(title), "href": url, "raw": data}
    except Exception:
        return None


def _get_dss_url_from_concept(concept_code: str) -> str:
    """Helper to fetch url for dataset specialization using biomedical concept code."""

    url = (
        URL_PREFIX
        + "mdr/specializations/datasetspecializations?biomedicalconcept="
        + concept_code
    )
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
        resp = requests.get(url, headers=headers, timeout=15)
        if resp.status_code != 200:
            return None
        data = resp.json()
        if not isinstance(data, dict):
            return None
        sdtm_links = data["_links"]["datasetSpecializations"]["sdtm"] or None
        href = sdtm_links[0]["href"]
        if href.startswith("/"):
            href = "https://api.library.cdisc.org/api/cosmos/v2" + href
        return href
    except Exception:
        return None


@functools.lru_cache(maxsize=256)
def _get_dss_by_url(url: str) -> Optional[Dict[str, Any]]:
    """Helper to return the raw response from request to the DSS Library API."""
    url = url
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
        resp = requests.get(url, headers=headers, timeout=15)
        if resp.status_code != 200:
            return None
        data = resp.json()
        if not isinstance(data, dict):
            return None
        return {"raw": data}
    except Exception:
        return None


def _get_bc_properties(
    bc_raw_data: Dict[str, Any], dss_raw_data: Dict[str, Any]
) -> Optional[Dict[str, Any]]:
    """Helper to construct the USDM JSON properties attribute for biomedical concept

    - id: "BiomedicalConceptProperty_{}",
    - extensionAttributes": [],
    - name: "",
    - label: "",
    - isRequired: None,
    - isEnabled": None,
    - datatype": "",
    - responseCodes": {},
    - code: {
        - id
        - extensionAttributes
        - code
        - codeSystem
        - codeSystemVersion
        - decode
        - instanceType: "Code"
    },
    - notes": [],
    "instanceType": "BiomedicalConceptProperty"
    """
    try:
        concept_ids = [
            dec["conceptId"]
            for dec in bc_raw_data["raw"]["dataElementConcepts"]
            if "conceptId" in dec
        ]
    except:
        concept_ids = []

    out: List[Dict[str, Any]] = []

    try:
        dss_vars = dss_raw_data["raw"]["variables"]
    except Exception:
        dss_vars = []

    try:
        dss_code_system = dss_raw_data["raw"]["_links"]["parentPackage"]["href"]
        dss_code_system_version = dss_code_system.split("/")[5]
    except:
        dss_code_system = ""
        dss_code_system_version = ""

    # dss_url = _get_dss_url_from_concept(bc_raw_data["code"])
    for idx, concept in enumerate(concept_ids):
        # print("idx: " + str(idx) + ", " + concept)
        id = "BiomedicalConceptProperty_" + str(idx)
        dss_var = dss_vars[idx] if idx < len(dss_vars) else {}
        if "shortName" in bc_raw_data["raw"]["dataElementConcepts"][idx]:
            name = bc_raw_data["raw"]["dataElementConcepts"][idx]["shortName"]
        else:
            name = ""
        if "shortName" in bc_raw_data["raw"]["dataElementConcepts"][idx]:
            label = bc_raw_data["raw"]["dataElementConcepts"][idx]["shortName"]
        else:
            label = ""
        isRequired = dss_var.get("mandatoryVariable", "")
        isEnabled = ""
        if "dataType" in bc_raw_data["raw"]["dataElementConcepts"][idx]:
            datatype = bc_raw_data["raw"]["dataElementConcepts"][idx]["dataType"]
        else:
            datatype = ""
        responseCodes = dss_var.get("valueList", [])
        decode = dss_var.get("name", "")
        code = bc_raw_data["raw"]["dataElementConcepts"][idx]["conceptId"]
        notes = []
        instanceType = "BiomedicalConceptProperty"

        property = {
            "id": id,
            "name": name,
            "label": label,
            "isRequired": isRequired,
            "isEnabled": isEnabled,
            "datatype": datatype,
            "responseCodes": responseCodes,
            "code": {
                "id": "AliasCode_" + str(idx),
                "extensionAttributes": [],
                "standardCode": {
                    "id": "Code_{}",
                    "extensionAttributes": [],
                    "code": code,
                    "codeSystem": dss_code_system,  # parentPackage/href of the DSS
                    "codeSystemVersion": dss_code_system_version,
                    "decode": decode,
                    "instanceType": "Code",
                },
            },
            "notes": notes,
            "instanceType": instanceType,
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
        SELECT concept_uid,concept_code,concept_title,dss_href FROM activity_concept WHERE soa_id=?
        ORDER BY COALESCE(concept_uid, 'zzz')
        """,
        (soa_id,),
    )
    rows = cur.fetchall()
    conn.close()

    code_system = (
        "https://api.library.cdisc.org/api/cosmos/v2/mdr/bc/biomedicalconcepts/"
    )

    out: List[Dict[str, Any]] = []
    for idx, r in enumerate(rows):
        concept_uid = r[0]
        concept_code = r[1]
        concept_title = r[2]
        dss_href = r[3]

        bc_raw_data = _get_concept_by_code(concept_code)
        if bc_raw_data is None:
            bc_raw_data = {"code": concept_code, "raw": {}}
        bc = bc_raw_data

        # Use stored dss_href when available; fall back to live lookup
        if dss_href:
            dss_raw_data = _get_dss_by_url(dss_href)
        else:
            dss_url = _get_dss_url_from_concept(concept_code)
            dss_raw_data = _get_dss_by_url(dss_url)
        if dss_raw_data is None:
            dss_raw_data = {"raw": {}}
        try:
            concept_ids = [
                dec["conceptId"]
                for dec in bc["raw"]["dataElementConcepts"]
                if "conceptId" in dec
            ]
        except:
            concept_ids = []

        # print(concept_ids)

        try:
            synonyms = bc["raw"]["synonyms"]
        except:
            synonyms = []

        try:
            reference = bc["raw"]["_links"]["parentPackage"]["href"]
            version = reference.split("/")[4]
        except:
            reference = ""
            version = ""
        try:
            label = bc["raw"]["_links"]["self"]["title"]
        except:
            label = ""
        try:
            name = bc["raw"]["shortName"]
        except:
            name = ""

        biomedical_concept = {
            "id": concept_uid,
            "extensionAttributes": [],
            "name": name,
            "label": label,
            "synonyms": synonyms,
            "reference": reference + "/" + bc["code"],
            "properties": _get_bc_properties(bc_raw_data, dss_raw_data),
            "code": {
                "id": "AliasCode_{}",
                "extensionAttributes": [],
                "standardCode": {
                    "id": "Code_{}",
                    "extensionAttributes": [],
                    "code": bc["code"],
                    "codeSystem": "https://api.library.cdisc.org/api/cosmos/v2",
                    "codeSystemVersion": version,
                    "decode": _nz(concept_title),
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
