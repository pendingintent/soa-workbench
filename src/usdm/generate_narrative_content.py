#!/usr/bin/env python3
"""Build USDM NarrativeContent / NarrativeContentItem objects for NCT01797120.

NarrativeContent objects represent the hierarchical section structure of
the protocol document (StudyDefinitionDocumentVersion.contents[]).

NarrativeContentItem objects hold the XHTML text body for each section
(StudyVersion.narrativeContentItems[]).

Both are derived from ``files/NCT01797120_sections.json``, which was
produced by extracting section text from the protocol PDF. That
extraction only exists for study NCT01797120, so every builder here
takes a ``soa_id`` and only returns data when the requested SOA's
``study_id`` matches ``_NARRATIVE_CONTENT_STUDY_ID`` -- otherwise it
returns empty results without touching the data file, so this
study-specific content never leaks into another SOA's export.

Reference: USDM-IG v4.0 §4.20 Unstructured Content.
"""

from __future__ import annotations

import json
import logging
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

from usdm.usdm_utils import _get_soa_metadata

logger = logging.getLogger("usdm.generate_narrative_content")

# The only study for which section text has been extracted.
_NARRATIVE_CONTENT_STUDY_ID = "NCT01797120"

# Path to the pre-extracted section data relative to this file.
_DATA_FILE = (
    Path(__file__).resolve().parent.parent.parent
    / "files"
    / "NCT01797120_sections.json"
)


def _soa_has_narrative_content(soa_id: int) -> bool:
    """Return True if *soa_id* is the study the section data covers."""
    meta = _get_soa_metadata(soa_id)
    return meta.get("study_id") == _NARRATIVE_CONTENT_STUDY_ID


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------


def _load_sections() -> List[Dict[str, Any]]:
    """Load section records from the pre-extracted data file."""
    if not _DATA_FILE.exists():
        raise FileNotFoundError(
            f"Section data file not found: {_DATA_FILE}. "
            "Re-run the extraction script to regenerate it."
        )
    with _DATA_FILE.open(encoding="utf-8") as fh:
        return json.load(fh)


def _build_ids(
    sections: List[Dict[str, Any]],
) -> Tuple[
    Dict[str, str],  # sec_num -> NarrativeContent_N id
    Dict[str, str],  # sec_num -> NarrativeContentItem_N id
]:
    """Return two look-up dicts mapping section number to USDM id."""
    nc_ids: Dict[str, str] = {}
    nci_ids: Dict[str, str] = {}
    for i, sec in enumerate(sections, start=1):
        n = sec["sectionNumber"]
        nc_ids[n] = f"NarrativeContent_{i}"
        nci_ids[n] = f"NarrativeContentItem_{i}"
    return nc_ids, nci_ids


def _siblings_of(
    parent: Optional[str],
    sections: List[Dict[str, Any]],
) -> List[str]:
    """Return section numbers whose parent equals *parent*, in order."""
    return [s["sectionNumber"] for s in sections if s.get("parent") == parent]


# ---------------------------------------------------------------------------
# Public builders
# ---------------------------------------------------------------------------


def build_usdm_narrative_content_items(soa_id: int) -> List[Dict[str, Any]]:
    """Return NarrativeContentItem-Output objects for StudyVersion.

    Each item carries the XHTML text for a protocol section and is
    referenced by the corresponding NarrativeContent via contentItemId.
    Returns an empty list for any SOA other than NCT01797120, since
    section data has only been extracted for that study.
    """
    if not _soa_has_narrative_content(soa_id):
        return []
    sections = _load_sections()
    _, nci_ids = _build_ids(sections)

    items: List[Dict[str, Any]] = []
    for sec in sections:
        n = sec["sectionNumber"]
        items.append(
            {
                "id": nci_ids[n],
                "extensionAttributes": [],
                "name": f"NCI_{n.replace('.', '_')}",
                "text": sec.get("text", ""),
                "instanceType": "NarrativeContentItem",
            }
        )
    return items


def build_usdm_narrative_contents(soa_id: int) -> List[Dict[str, Any]]:
    """Return NarrativeContent-Output objects.

    These are intended for StudyDefinitionDocumentVersion.contents[].
    Returns an empty list for any SOA other than NCT01797120, since
    section data has only been extracted for that study.
    """
    if not _soa_has_narrative_content(soa_id):
        return []
    sections = _load_sections()
    nc_ids, nci_ids = _build_ids(sections)

    # Pre-compute children lists keyed by parent sec_num (None = root)
    children_map: Dict[Optional[str], List[str]] = {}
    for sec in sections:
        p = sec.get("parent")
        children_map.setdefault(p, []).append(sec["sectionNumber"])

    contents: List[Dict[str, Any]] = []
    for sec in sections:
        n = sec["sectionNumber"]
        parent = sec.get("parent")

        # Siblings for navigation
        siblings = children_map.get(parent, [])
        idx = siblings.index(n)
        prev_id: Optional[str] = nc_ids[siblings[idx - 1]] if idx > 0 else None
        next_id: Optional[str] = (
            nc_ids[siblings[idx + 1]] if idx < len(siblings) - 1 else None
        )

        # Children of this section
        child_sec_nums = children_map.get(n, [])
        child_nc_ids = [nc_ids[c] for c in child_sec_nums]

        contents.append(
            {
                "id": nc_ids[n],
                "extensionAttributes": [],
                "name": f"NC_{n.replace('.', '_')}",
                "sectionNumber": n,
                "sectionTitle": sec["sectionTitle"],
                "displaySectionNumber": True,
                "displaySectionTitle": True,
                "childIds": child_nc_ids,
                "previousId": prev_id,
                "nextId": next_id,
                "contentItemId": nci_ids[n],
                "instanceType": "NarrativeContent",
            }
        )
    return contents


def build_usdm_study_definition_document(
    soa_id: int,
    document_version_id: str = "StudyDefinitionDocumentVersion_1",
    document_id: str = "StudyDefinitionDocument_1",
) -> Optional[Dict[str, Any]]:
    """Return a StudyDefinitionDocument-Output object for Study.documentedBy[].

    The document wraps a single StudyDefinitionDocumentVersion whose
    ``contents`` array holds all NarrativeContent objects. Returns
    None for any SOA other than NCT01797120, since section data has
    only been extracted for that study.

    Args:
        soa_id: The SOA to build the document for.
        document_version_id: Stable id for the document version entity.
        document_id: Stable id for the document entity.
    """
    if not _soa_has_narrative_content(soa_id):
        return None
    contents = build_usdm_narrative_contents(soa_id)

    version: Dict[str, Any] = {
        "id": document_version_id,
        "extensionAttributes": [],
        "version": "2",
        "status": {
            "id": "Code_DocStatus_1",
            "extensionAttributes": [],
            "code": "C25508",
            "codeSystem": "http://www.cdisc.org",
            "codeSystemVersion": "2025-09-26",
            "decode": "Final",
            "instanceType": "Code",
        },
        "dateValues": [
            {
                "id": "GovernanceDate_DocVersion_1",
                "extensionAttributes": [],
                "name": "PROTOCOL_VERSION_DATE",
                "label": "Protocol Version Date",
                "description": "Date of the final protocol version",
                "type": {
                    "id": "Code_DocDate_1",
                    "extensionAttributes": [],
                    "code": "C215663",
                    "codeSystem": "http://www.cdisc.org",
                    "codeSystemVersion": "2025-09-26",
                    "decode": "Effective Date",
                    "instanceType": "Code",
                },
                "dateValue": "2014-01-22",
                "geographicScopes": [],
                "instanceType": "GovernanceDate",
            }
        ],
        "contents": contents,
        "notes": [],
        "instanceType": "StudyDefinitionDocumentVersion",
    }

    document: Dict[str, Any] = {
        "id": document_id,
        "extensionAttributes": [],
        "name": "NCT01797120_PROTOCOL",
        "label": (
            "PrE0102: Randomized, Double-Blind, Placebo-Controlled "
            "Phase II Trial of Fulvestrant plus Everolimus"
        ),
        "description": (
            "Version 2.0, January 22 2014 protocol document for "
            "PrECOG study PrE0102 (NCT01797120)."
        ),
        "language": {
            "id": "Code_DocLang_1",
            "extensionAttributes": [],
            "code": "en",
            "codeSystem": "ISO",
            "codeSystemVersion": "1",
            "decode": "English",
            "instanceType": "Code",
        },
        "type": {
            "id": "Code_DocType_1",
            "extensionAttributes": [],
            "code": "C70817",
            "codeSystem": "http://www.cdisc.org",
            "codeSystemVersion": "2025-09-26",
            "decode": "Protocol",
            "instanceType": "Code",
        },
        "templateName": "PrECOG Protocol Template",
        "versions": [version],
        "childIds": [],
        "notes": [],
        "instanceType": "StudyDefinitionDocument",
    }
    return document


# ---------------------------------------------------------------------------
# CLI entry point
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    import argparse
    import sys

    parser = argparse.ArgumentParser(
        description=("Export USDM NarrativeContent entities for NCT01797120.")
    )
    parser.add_argument(
        "--mode",
        choices=["items", "contents", "document", "all"],
        default="all",
        help=(
            "items = NarrativeContentItem list; "
            "contents = NarrativeContent list; "
            "document = full StudyDefinitionDocument; "
            "all = {'items':..., 'contents':..., 'document':...}"
        ),
    )
    parser.add_argument(
        "--soa-id",
        type=int,
        required=True,
        help="SOA id to build narrative content for (must be NCT01797120)",
    )
    parser.add_argument(
        "-o",
        "--output",
        default="-",
        help="Output file path or '-' for stdout",
    )
    parser.add_argument("--indent", type=int, default=2)
    args = parser.parse_args()

    logging.basicConfig(level=logging.INFO)

    if args.mode == "items":
        payload = build_usdm_narrative_content_items(args.soa_id)
    elif args.mode == "contents":
        payload = build_usdm_narrative_contents(args.soa_id)
    elif args.mode == "document":
        payload = build_usdm_study_definition_document(args.soa_id)
    else:
        payload = {
            "narrativeContentItems": build_usdm_narrative_content_items(args.soa_id),
            "narrativeContents": build_usdm_narrative_contents(args.soa_id),
            "studyDefinitionDocument": build_usdm_study_definition_document(
                args.soa_id
            ),
        }

    text = json.dumps(payload, indent=args.indent, ensure_ascii=False)
    if args.output in ("-", "/dev/stdout"):
        sys.stdout.write(text + "\n")
    else:
        with open(args.output, "w", encoding="utf-8") as fh:
            fh.write(text + "\n")
        logger.info("Written to %s", args.output)
