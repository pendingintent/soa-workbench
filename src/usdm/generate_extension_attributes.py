#!/usr/bin/env python3
"""USDM generator for BiomedicalConcept.extensionAttributes.

Sources data from ``activity_concept_dss`` (SDTM dataset specialization
assignments). Each DSS row carries a stable ``ExtensionAttribute_N``
identifier in ``activity_concept_dss.extension_attribute_uid``, which
is backfilled idempotently by :func:`populate_extension_attributes`.

A BiomedicalConcept emits one ExtensionAttribute per associated DSS
row (ordered by ``id``); if a BC has no DSS row, it emits an empty
list.
"""

import logging
from typing import List, Dict, Any

from soa_builder.web.db import _connect

logger = logging.getLogger("usdm.generate_extension_attributes")

EXTENSION_URL = "http://www.cdisc.org/usdm/extensions/specializations/sdtm"
EXTENSION_URL_CRF = "http://www.cdisc.org/usdm/extensions/specializations/crf"
MDR_PREFIX = "/mdr"


def _trim_href_to_mdr(href: str) -> str:
    """Return the substring of ``href`` starting at the first ``/mdr``.

    Falls back to the full ``href`` if ``/mdr`` is not present.
    """
    if not href:
        return ""
    idx = href.find(MDR_PREFIX)
    if idx < 0:
        return href
    return href[idx:]


def populate_extension_attributes(soa_id: int) -> None:
    """Backfill ExtensionAttribute_N UIDs onto activity_concept_dss rows
    that don't yet have one.

    Idempotent: rows with a non-NULL, non-empty
    ``extension_attribute_uid`` are skipped.
    """
    conn = _connect()
    cur = conn.cursor()
    try:
        cur.execute(
            "SELECT id FROM activity_concept_dss"
            " WHERE soa_id=?"
            " AND (extension_attribute_uid IS NULL"
            " OR extension_attribute_uid = '')"
            " ORDER BY id",
            (soa_id,),
        )
        ids = [r[0] for r in cur.fetchall()]
        if not ids:
            return
        cur.execute(
            "SELECT extension_attribute_uid FROM activity_concept_dss"
            " WHERE soa_id=?"
            " AND extension_attribute_uid LIKE 'ExtensionAttribute_%'"
            " UNION ALL"
            " SELECT extension_attribute_uid FROM activity_concept_crf"
            " WHERE soa_id=?"
            " AND extension_attribute_uid LIKE 'ExtensionAttribute_%'",
            (soa_id, soa_id),
        )
        existing = [r[0] for r in cur.fetchall() if r[0]]
        try:
            next_n = max(int(x.split("_")[1]) for x in existing) + 1
        except (ValueError, IndexError):
            next_n = len(existing) + 1
        for row_id in ids:
            ea_uid = f"ExtensionAttribute_{next_n}"
            next_n += 1
            cur.execute(
                "UPDATE activity_concept_dss"
                " SET extension_attribute_uid=?"
                " WHERE id=? AND soa_id=?",
                (ea_uid, row_id, soa_id),
            )
        conn.commit()
    except Exception:
        conn.rollback()
        logger.exception("populate_extension_attributes failed soa_id=%s", soa_id)
        raise
    finally:
        conn.close()


def populate_crf_extension_attributes(soa_id: int) -> None:
    """Backfill ExtensionAttribute_N UIDs onto activity_concept_crf rows
    that don't yet have one.

    Idempotent: rows with a non-NULL, non-empty
    ``extension_attribute_uid`` are skipped. The counter spans both
    ``activity_concept_dss`` and ``activity_concept_crf`` to keep UIDs
    globally unique within a study.
    """
    conn = _connect()
    cur = conn.cursor()
    try:
        cur.execute(
            "SELECT id FROM activity_concept_crf"
            " WHERE soa_id=?"
            " AND (extension_attribute_uid IS NULL"
            " OR extension_attribute_uid = '')"
            " ORDER BY id",
            (soa_id,),
        )
        ids = [r[0] for r in cur.fetchall()]
        if not ids:
            return
        cur.execute(
            "SELECT extension_attribute_uid FROM activity_concept_dss"
            " WHERE soa_id=?"
            " AND extension_attribute_uid LIKE 'ExtensionAttribute_%'"
            " UNION ALL"
            " SELECT extension_attribute_uid FROM activity_concept_crf"
            " WHERE soa_id=?"
            " AND extension_attribute_uid LIKE 'ExtensionAttribute_%'",
            (soa_id, soa_id),
        )
        existing = [r[0] for r in cur.fetchall() if r[0]]
        try:
            next_n = max(int(x.split("_")[1]) for x in existing) + 1
        except (ValueError, IndexError):
            next_n = len(existing) + 1
        for row_id in ids:
            ea_uid = f"ExtensionAttribute_{next_n}"
            next_n += 1
            cur.execute(
                "UPDATE activity_concept_crf"
                " SET extension_attribute_uid=?"
                " WHERE id=? AND soa_id=?",
                (ea_uid, row_id, soa_id),
            )
        conn.commit()
    except Exception:
        conn.rollback()
        logger.exception("populate_crf_extension_attributes failed soa_id=%s", soa_id)
        raise
    finally:
        conn.close()


def build_usdm_dss_extension_attributes(
    soa_id: int, biomedical_concept_uid: str
) -> List[Dict[str, Any]]:
    """Return one ExtensionAttribute dict per DSS row for the given BC.

    Joins ``activity_concept_dss`` to ``activity_concept`` by
    ``(soa_id, activity_id, concept_code)`` filtering on
    ``ac.concept_uid = biomedical_concept_uid``.
    """
    conn = _connect()
    cur = conn.cursor()
    try:
        cur.execute(
            "SELECT acd.extension_attribute_uid, acd.dss_href"
            " FROM activity_concept_dss acd"
            " INNER JOIN activity_concept ac"
            " ON ac.soa_id = acd.soa_id"
            " AND ac.activity_id = acd.activity_id"
            " AND ac.concept_code = acd.concept_code"
            " WHERE acd.soa_id = ?"
            " AND ac.concept_uid = ?"
            " ORDER BY acd.id",
            (soa_id, biomedical_concept_uid),
        )
        rows = cur.fetchall()
    finally:
        conn.close()

    out: List[Dict[str, Any]] = []
    for ea_uid, href in rows:
        if not ea_uid:
            continue
        out.append(
            {
                "id": ea_uid,
                "url": EXTENSION_URL,
                "valueString": _trim_href_to_mdr(href or ""),
                "instanceType": "ExtensionAttribute",
            }
        )
    return out


def build_usdm_crf_extension_attributes(
    soa_id: int, biomedical_concept_uid: str
) -> List[Dict[str, Any]]:
    """Return one ExtensionAttribute dict per CRF row for the given BC.

    Joins ``activity_concept_crf`` to ``activity_concept`` by
    ``(soa_id, activity_id, concept_code)`` filtering on
    ``ac.concept_uid = biomedical_concept_uid``.
    """
    conn = _connect()
    cur = conn.cursor()
    try:
        cur.execute(
            "SELECT acc.extension_attribute_uid, acc.crf_href"
            " FROM activity_concept_crf acc"
            " INNER JOIN activity_concept ac"
            " ON ac.soa_id = acc.soa_id"
            " AND ac.activity_id = acc.activity_id"
            " AND ac.concept_code = acc.concept_code"
            " WHERE acc.soa_id = ?"
            " AND ac.concept_uid = ?"
            " ORDER BY acc.id",
            (soa_id, biomedical_concept_uid),
        )
        rows = cur.fetchall()
    finally:
        conn.close()

    out: List[Dict[str, Any]] = []
    for ea_uid, href in rows:
        if not ea_uid:
            continue
        out.append(
            {
                "id": ea_uid,
                "url": EXTENSION_URL_CRF,
                "valueString": _trim_href_to_mdr(href or ""),
                "instanceType": "ExtensionAttribute",
            }
        )
    return out
