#!/usr/bin/env python3
"""
Full USDM document generator.

Produces a Study-Output → StudyVersion-Output → InterventionalStudyDesign-Output
hierarchy, populating sub-entities from the existing per-entity generators.
"""

import subprocess
from datetime import datetime
from typing import Any, Dict, List, Optional
import logging
from .usdm_utils import _get_soa_metadata
from soa_builder.web.db import _connect
from soa_builder.web.utils import (
    _nz,
    get_next_extension_attribute_uid,
    get_next_extension_class_uid,
)

from usdm.generate_activities import build_usdm_activities
from usdm.generate_arms import build_usdm_arms
from usdm.generate_elements import build_usdm_elements
from usdm.generate_encounters import build_usdm_encounters
from usdm.generate_schedule_timelines import build_usdm_schedule_timelines
from usdm.generate_study_cells import build_usdm_study_cells
from usdm.generate_study_epochs import build_usdm_epochs
from usdm.generate_biomedical_concepts import build_usdm_biomedical_concepts
from usdm.generate_bc_surrogates import build_usdm_bc_surrogates
from usdm.generate_bc_categories import build_usdm_bc_categories
from usdm.generate_objectives import build_usdm_objectives
from usdm.generate_amendments import build_usdm_amendments
from usdm.generate_study_titles import build_usdm_titles
from usdm.generate_organizations import build_usdm_organizations
from usdm.generate_roles import build_usdm_roles
from usdm.generate_study_interventions import build_usdm_study_interventions
from usdm.generate_estimands import build_usdm_estimands
from usdm.generate_indications import build_usdm_indications
from usdm.generate_study_identifiers import build_usdm_study_identifiers

logger = logging.getLogger("usdm.generate_usdm")


def _git_branch() -> str:
    try:
        branch = (
            subprocess.check_output(
                ["git", "rev-parse", "--abbrev-ref", "HEAD"],
                stderr=subprocess.DEVNULL,
            )
            .decode()
            .strip()
        )
    except Exception:
        branch = "unknown"
    if branch.startswith("release-v-"):
        version = branch[len("release-v-") :]
        parts = version.split(".")
        while len(parts) < 3:
            parts.append("0")
        return ".".join(parts[:3])
    return branch


def _get_or_create_tool_uids(soa_id: int) -> Dict[str, str]:
    """Return the 5 stable tool extension UIDs for the SOA.

    On first call the UIDs are generated (monotonic max+1) and persisted
    to soa_tool_extension so subsequent USDM generations reuse identical
    UIDs.
    """
    conn = _connect()
    cur = conn.cursor()
    try:
        cur.execute(
            "SELECT ea_outer_uid, ec_uid,"
            " ea_name_uid, ea_version_uid, ea_date_uid"
            " FROM soa_tool_extension WHERE soa_id=?",
            (soa_id,),
        )
        row = cur.fetchone()
        if row and all(row):
            return {
                "ea_outer": row[0],
                "ec": row[1],
                "ea_name": row[2],
                "ea_version": row[3],
                "ea_date": row[4],
            }
        ea_start_uid = get_next_extension_attribute_uid(cur, soa_id)
        ea_n = int(ea_start_uid.split("_")[1])
        ec_uid = get_next_extension_class_uid(cur, soa_id)
        uids = {
            "ea_outer": f"ExtensionAttribute_{ea_n}",
            "ec": ec_uid,
            "ea_name": f"ExtensionAttribute_{ea_n + 1}",
            "ea_version": f"ExtensionAttribute_{ea_n + 2}",
            "ea_date": f"ExtensionAttribute_{ea_n + 3}",
        }
        cur.execute(
            "INSERT INTO soa_tool_extension"
            " (soa_id, ea_outer_uid, ec_uid,"
            "  ea_name_uid, ea_version_uid, ea_date_uid)"
            " VALUES (?, ?, ?, ?, ?, ?)",
            (
                soa_id,
                uids["ea_outer"],
                uids["ec"],
                uids["ea_name"],
                uids["ea_version"],
                uids["ea_date"],
            ),
        )
        conn.commit()
        return uids
    finally:
        conn.close()


def _tool_extension_attribute(uids: Dict[str, str], timestamp: str) -> Dict[str, Any]:
    """Build the outer ExtensionAttribute that wraps the tool ExtensionClass."""
    return {
        "id": uids["ea_outer"],
        "url": ("http://www.cdisc.org/usdm/extensions/studyDesignSolution"),
        "valueExtensionClass": {
            "id": uids["ec"],
            "url": ("http://www.cdisc.org/usdm/extensions/StudyDesignSolution"),
            "extensionAttributes": [
                {
                    "id": uids["ea_name"],
                    "url": "tool-name",
                    "valueString": "SoA Workbench",
                    "instanceType": "ExtensionAttribute",
                },
                {
                    "id": uids["ea_version"],
                    "url": "tool-version",
                    "valueString": _git_branch(),
                    "instanceType": "ExtensionAttribute",
                },
                {
                    "id": uids["ea_date"],
                    "url": "usdm-creation-date",
                    "valueString": timestamp,
                    "instanceType": "ExtensionAttribute",
                },
            ],
            "instanceType": "ExtensionClass",
        },
        "instanceType": "ExtensionAttribute",
    }


def build_usdm(soa_id: int, timestamp: Optional[str] = None) -> Dict[str, Any]:
    """
    Build a complete USDM Study-Output document for the given SOA.

    Returns the full hierarchy:
      Study -> versions[0] -> studyDesigns[0] (InterventionalStudyDesign)
    """
    if timestamp is None:
        timestamp = datetime.now().strftime("%Y%m%dT%H:%M")
    tool_uids = _get_or_create_tool_uids(soa_id)
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

    study_interventions = _safe(
        "studyInterventions", build_usdm_study_interventions, soa_id
    )
    estimands = _safe("estimands", build_usdm_estimands, soa_id)
    indications = _safe("indications", build_usdm_indications, soa_id)

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
        "estimands": estimands,
        "indications": indications,
        "studyInterventionIds": [i["id"] for i in study_interventions],
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
        "extensionAttributes": [_tool_extension_attribute(tool_uids, timestamp)],
        "versionIdentifier": "1",
        "rationale": "",
        "studyIdentifiers": _safe(
            "studyIdentifiers", build_usdm_study_identifiers, soa_id
        ),
        "referenceIdentifiers": [],
        "studyDesigns": [study_design],
        "titles": build_usdm_titles(soa_id),
        "documentVersionIds": [],
        "dateValues": [],
        "amendments": _safe("amendments", build_usdm_amendments, soa_id),
        "organizations": _safe("organizations", build_usdm_organizations, soa_id),
        "roles": _safe("roles", build_usdm_roles, soa_id),
        "studyInterventions": study_interventions,
        "businessTherapeuticAreas": [],
        "biomedicalConcepts": build_usdm_biomedical_concepts(soa_id),
        "bcSurrogates": _safe("bcSurrogates", build_usdm_bc_surrogates, soa_id),
        "bcCategories": build_usdm_bc_categories(soa_id),
        "eligibilityCriterionItems": [],
        "narrativeContentItems": [],
        "abbreviations": [],
        "administrableProducts": [],
        "medicalDevices": [],
        "productOrganizationRoles": [],
        "dictionaries": [],
        "conditions": [],
        "notes": [],
        "instanceType": "StudyVersion",
    }

    study = {
        "id": f"Study_{soa_id}",
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
        "systemVersion": _git_branch(),
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
