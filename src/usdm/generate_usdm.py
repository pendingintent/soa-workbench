#!/usr/bin/env python3
"""
Full USDM document generator.

Produces a Study-Output → StudyVersion-Output → InterventionalStudyDesign-Output
hierarchy, populating sub-entities from the existing per-entity generators.
"""
from typing import Optional, List, Dict, Any
import logging

logger = logging.getLogger("usdm.generate_usdm")

try:
    from soa_builder.web.app import _connect
except ImportError:
    import sys
    from pathlib import Path

    here = Path(__file__).resolve()
    src_dir = here.parents[2] / "src"
    if src_dir.exists() and str(src_dir) not in sys.path:
        sys.path.insert(0, str(src_dir))
    from soa_builder.web.app import _connect  # type: ignore

from usdm.generate_activities import build_usdm_activities
from usdm.generate_arms import build_usdm_arms
from usdm.generate_elements import build_usdm_elements
from usdm.generate_encounters import build_usdm_encounters
from usdm.generate_schedule_timelines import build_usdm_schedule_timelines
from usdm.generate_study_cells import build_usdm_study_cells
from usdm.generate_study_epochs import build_usdm_epochs


def _nz(s: Optional[str]) -> Optional[str]:
    s = (s or "").strip()
    return s or None


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
                "Failed to build %s for soa_id=%s, using empty list", label, soa_id
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
        "objectives": [],
        "population": {
            "id": "StudyDesignPopulation_1",
            "extensionAttributes": [],
            "name": "",
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
        "titles": [
            {
                "id": "StudyTitle_1",
                "extensionAttributes": [],
                "text": meta["study_label"] or meta["name"] or "",
                "type": {
                    "id": "Code_StudyTitleType",
                    "extensionAttributes": [],
                    "code": "C99905x2",
                    "codeSystem": "http://www.cdisc.org",
                    "codeSystemVersion": "",
                    "decode": "Official Study Title",
                    "instanceType": "Code",
                },
                "instanceType": "StudyTitle",
            }
        ],
        "documentVersionIds": [],
        "dateValues": [],
        "amendments": [],
        "businessTherapeuticAreas": [],
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
        sys.stdout.write(payload + "\n")
    else:
        with open(args.output, "w", encoding="utf-8") as f:
            f.write(payload + "\n")
