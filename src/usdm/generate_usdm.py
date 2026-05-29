#!/usr/bin/env python3
"""
Full USDM document generator.

Produces a Study-Output → StudyVersion-Output → InterventionalStudyDesign-Output
hierarchy, populating sub-entities from the existing per-entity generators.
"""

from typing import List, Dict, Any
import logging
from .usdm_utils import _get_soa_metadata
from soa_builder.web.utils import _nz

from usdm.generate_activities import build_usdm_activities
from usdm.generate_arms import build_usdm_arms
from usdm.generate_elements import build_usdm_elements
from usdm.generate_encounters import build_usdm_encounters
from usdm.generate_schedule_timelines import build_usdm_schedule_timelines
from usdm.generate_study_cells import build_usdm_study_cells
from usdm.generate_study_epochs import build_usdm_epochs
from usdm.generate_biomedical_concepts import build_usdm_biomedical_concepts
from usdm.generate_bc_surrogates import build_usdm_bc_surrogates
from usdm.generate_objectives import build_usdm_objectives
from usdm.generate_amendments import build_usdm_amendments
from usdm.generate_study_titles import build_usdm_titles

logger = logging.getLogger("usdm.generate_usdm")


def build_usdm(soa_id: int) -> Dict[str, Any]:
    """
    Build a complete USDM Study-Output document for the given SOA.

    Returns the full hierarchy:
      Study -> versions[0] -> studyDesigns[0] (InterventionalStudyDesign)
    """
    meta = _get_soa_metadata(soa_id)

    def _safe(label: str, fn, *args) -> List[Dict[str, Any]]:
        try:
            return fn(*args)
        except Exception:
            logger.warning(
                "Failed to build %s for soa_id=%s, using empty list",
                label,
                soa_id,
                exc_info=True,
            )
            return []

    study_design = {
        "id": "InterventionalStudyDesign_1",
        "extensionAttributes": [],
        "name": meta["name"] or "",
        "label": _nz(meta["study_label"]),
        "description": _nz(meta["study_description"]),
        "studyType": None,
        "studyPhase": None,
        "therapeuticAreas": [],
        "characteristics": [],
        "encounters": _safe("encounters", build_usdm_encounters, soa_id),
        "activities": _safe("activities", build_usdm_activities, soa_id),
        "arms": _safe("arms", build_usdm_arms, soa_id),
        "studyCells": _safe("studyCells", build_usdm_study_cells, soa_id),
        "rationale": "",
        "epochs": _safe("epochs", build_usdm_epochs, soa_id),
        "elements": _safe("elements", build_usdm_elements, soa_id),
        "estimands": [],
        "indications": [],
        "studyInterventionIds": [],
        "objectives": _safe("objectives", build_usdm_objectives, soa_id),
        "population": {
            "id": "StudyDesignPopulation_1",
            "extensionAttributes": [],
            "name": "Population_1",
            "label": None,
            "description": None,
            "includesHealthySubjects": False,
            "plannedEnrollmentNumber": None,
            "plannedCompletionNumber": None,
            "plannedSex": [],
            "criterionIds": [],
            "plannedAge": None,
            "notes": [],
            "cohorts": [],
            "instanceType": "StudyDesignPopulation",
        },
        "scheduleTimelines": _safe(
            "scheduleTimelines", build_usdm_schedule_timelines, soa_id
        ),
        "biospecimenRetentions": [],
        "documentVersionIds": [],
        "eligibilityCriteria": [],
        "analysisPopulations": [],
        "notes": [],
        "subTypes": [],
        "model": {
            "id": "Code_StudyDesignModel",
            "extensionAttributes": [],
            "code": "",
            "codeSystem": "",
            "codeSystemVersion": "",
            "decode": "",
            "instanceType": "Code",
        },
        "intentTypes": [],
        "blindingSchema": None,
        "instanceType": "InterventionalStudyDesign",
    }

    study_version = {
        "id": "StudyVersion_1",
        "extensionAttributes": [],
        "versionIdentifier": "1",
        "rationale": "",
        "studyIdentifiers": [
            {
                "id": "StudyIdentifier_1",
                "extensionAttributes": [],
                "text": meta["study_id"] or "",
                "scopeId": "",
                "instanceType": "StudyIdentifier",
            }
        ],
        "referenceIdentifiers": [],
        "studyDesigns": [study_design],
        "titles": build_usdm_titles(soa_id),
        "documentVersionIds": [],
        "dateValues": [],
        "amendments": _safe("amendments", build_usdm_amendments, soa_id),
        "businessTherapeuticAreas": [],
        "biomedicalConcepts": build_usdm_biomedical_concepts(soa_id),
        "bcSurrogates": _safe("bcSurrogates", build_usdm_bc_surrogates, soa_id),
        "notes": [],
        "instanceType": "StudyVersion",
    }

    study = {
        "id": None,
        "extensionAttributes": [],
        "name": meta["study_id"] or meta["name"] or "",
        "description": _nz(meta["study_description"]),
        "label": _nz(meta["study_label"]),
        "versions": [study_version],
        "documentedBy": [],
        "instanceType": "Study",
    }

    return {
        "study": study,
        "usdmVersion": "4.0",
        "systemName": "SOA Workbench",
        "systemVersion": "1.0.0",
    }


if __name__ == "__main__":
    import argparse
    import json
    import logging
    import sys

    logger = logging.getLogger("usdm.generate_usdm")

    parser = argparse.ArgumentParser(
        description="Export a full USDM Study document for a SOA."
    )
    parser.add_argument("soa_id", type=int, help="SOA id to export")
    parser.add_argument(
        "-o", "--output", default="-", help="Output file path or '-' for stdout"
    )
    parser.add_argument("--indent", type=int, default=2, help="JSON indent")
    args = parser.parse_args()

    try:
        document = build_usdm(args.soa_id)
    except Exception:
        logger.exception("Failed to build USDM document for soa_id=%s", args.soa_id)
        sys.exit(1)

    payload = json.dumps(document, indent=args.indent)
    if args.output in ("-", "/dev/stdout"):
        sys.stdout.write(
            "Output suppressed: this document may contain sensitive data. "
            "Use an explicit -o <file> path to export.\n"
        )
    else:
        with open(args.output, "w", encoding="utf-8") as f:
            f.write(payload + "\n")
