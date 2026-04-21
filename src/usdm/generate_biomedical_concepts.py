#!/usr/bin/env python3
from concurrent.futures import ThreadPoolExecutor
from typing import List, Dict, Any

from soa_builder.web.db import _connect
from .usdm_utils import (
    _get_biomedical_concept_synonyms as _get_biomedical_concept_synonyms,
    _get_biomedical_concept_reference as _get_biomedical_concept_reference,
)


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
            c.code_uid code_uid,
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

    # Prefetch synonyms and reference in parallel — one BC API call per concept,
    # shared by both helpers via the cached _get_biomedical_concept_data()
    concept_codes = [r[4] for r in rows]
    with ThreadPoolExecutor(max_workers=8) as pool:
        synonyms_list = list(pool.map(_get_biomedical_concept_synonyms, concept_codes))
        reference_list = list(
            pool.map(_get_biomedical_concept_reference, concept_codes)
        )
    synonyms_map = dict(zip(concept_codes, synonyms_list))
    reference_map = dict(zip(concept_codes, reference_list))

    out: List[Dict[str, Any]] = []

    for r in rows:
        id = r[0]
        name = r[1]
        label = r[2]
        alias_code = r[3]
        concept_code = r[4]
        code_uid = r[5]
        code_system_version = r[6]
        decode = r[7]
        synonyms = synonyms_map[concept_code]
        reference = reference_map[concept_code]

        biomedical_concept = {
            "id": id,
            "extensionAttributes": [],
            "name": name,
            "label": label,
            "synonyms": synonyms,
            "reference": reference,
            "properties": [],
            "code": {
                "id": alias_code,
                "extensionAttributes": [],
                "standardCode": {
                    "id": code_uid,
                    "extensionAttributes": [],
                    "code": concept_code,
                    "codeSystem": "http://www.cdisc.org",
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
