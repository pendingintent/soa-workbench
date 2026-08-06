#!/usr/bin/env python3
"""USDM generator for Activity.extensionAttributes: BC groupings.

Sources data from Activities that have been assigned a CDISC
Biomedical Concept Grouping (a classification scheme + value, from
the ``cdisc-biomedical-concept-groupings`` service) via
``activity_concept.concept_group_uid`` / ``activity_surrogate
.concept_group_uid`` joined back to ``concept_group`` where
``source='cdisc'``.

Each (activity, concept_group) assignment carries a stable set of
UIDs persisted in ``activity_grouping_extension``, backfilled
idempotently by :func:`populate_activity_grouping_extensions`.

One assigned group produces one outer ``ExtensionAttribute`` whose
``valueExtensionClass`` wraps two flat sibling ``ExtensionAttribute``
children (classification scheme id, classification value id).
"""

import logging
from typing import Any, Dict, List

from soa_builder.web.db import _connect
from soa_builder.web.utils import (
    get_next_extension_attribute_uid,
    get_next_extension_class_uid,
)

logger = logging.getLogger("usdm.generate_activity_grouping_extensions")

EXTENSION_URL = "http://www.cdisc.org/usdm/extensions/biomedicalConceptGrouping"
EXTENSION_CLASS_URL = "http://www.cdisc.org/usdm/extensions/BiomedicalConceptGrouping"


def _assigned_cdisc_groups(cur: Any, soa_id: int) -> List[tuple]:
    """Return distinct (activity_uid, concept_group_uid) pairs for
    CDISC-sourced groups assigned to activities in this SOA.
    """
    cur.execute(
        "SELECT DISTINCT ac.activity_uid, ac.concept_group_uid"
        " FROM activity_concept ac"
        " INNER JOIN concept_group cg"
        " ON cg.concept_group_uid = ac.concept_group_uid"
        " WHERE ac.soa_id=? AND cg.source='cdisc'"
        " AND ac.concept_group_uid IS NOT NULL"
        " UNION"
        " SELECT DISTINCT asr.activity_uid, asr.concept_group_uid"
        " FROM activity_surrogate asr"
        " INNER JOIN concept_group cg"
        " ON cg.concept_group_uid = asr.concept_group_uid"
        " WHERE asr.soa_id=? AND cg.source='cdisc'"
        " AND asr.concept_group_uid IS NOT NULL",
        (soa_id, soa_id),
    )
    return cur.fetchall()


def populate_activity_grouping_extensions(soa_id: int) -> None:
    """Backfill activity_grouping_extension rows for newly-discovered
    (activity_uid, concept_group_uid) assignments.

    Idempotent: existing rows for a given (soa_id, activity_uid,
    concept_group_uid) are left untouched.
    """
    conn = _connect()
    cur = conn.cursor()
    try:
        pairs = _assigned_cdisc_groups(cur, soa_id)
        if not pairs:
            return
        cur.execute(
            "SELECT activity_uid, concept_group_uid"
            " FROM activity_grouping_extension WHERE soa_id=?",
            (soa_id,),
        )
        existing_pairs = set(cur.fetchall())
        new_pairs = [p for p in pairs if p not in existing_pairs]
        if not new_pairs:
            return
        ea_n = int(get_next_extension_attribute_uid(cur, soa_id).split("_")[1])
        ec_n = int(get_next_extension_class_uid(cur, soa_id).split("_")[1])
        for activity_uid, concept_group_uid in new_pairs:
            cur.execute(
                "INSERT INTO activity_grouping_extension"
                " (soa_id, activity_uid, concept_group_uid,"
                "  ea_outer_uid, ec_uid, ea_scheme_uid, ea_value_uid)"
                " VALUES (?, ?, ?, ?, ?, ?, ?)",
                (
                    soa_id,
                    activity_uid,
                    concept_group_uid,
                    f"ExtensionAttribute_{ea_n}",
                    f"ExtensionClass_{ec_n}",
                    f"ExtensionAttribute_{ea_n + 1}",
                    f"ExtensionAttribute_{ea_n + 2}",
                ),
            )
            ea_n += 3
            ec_n += 1
        conn.commit()
    except Exception:
        conn.rollback()
        logger.exception(
            "populate_activity_grouping_extensions failed soa_id=%s", soa_id
        )
        raise
    finally:
        conn.close()


def build_usdm_activity_grouping_extensions_bulk(
    soa_id: int,
) -> Dict[str, List[Dict[str, Any]]]:
    """Return one nested ExtensionAttribute dict per assigned CDISC
    group, grouped by activity_uid.
    """
    conn = _connect()
    cur = conn.cursor()
    try:
        cur.execute(
            "SELECT age.activity_uid, age.ea_outer_uid, age.ec_uid,"
            " age.ea_scheme_uid, age.ea_value_uid,"
            " cg.cdisc_scheme_id, cg.cdisc_value_id"
            " FROM activity_grouping_extension age"
            " INNER JOIN concept_group cg"
            " ON cg.concept_group_uid = age.concept_group_uid"
            " WHERE age.soa_id=?"
            " ORDER BY age.id",
            (soa_id,),
        )
        rows = cur.fetchall()
    finally:
        conn.close()

    out: Dict[str, List[Dict[str, Any]]] = {}
    for (
        activity_uid,
        ea_outer_uid,
        ec_uid,
        ea_scheme_uid,
        ea_value_uid,
        scheme_id,
        value_id,
    ) in rows:
        out.setdefault(activity_uid, []).append(
            {
                "id": ea_outer_uid,
                "url": EXTENSION_URL,
                "valueExtensionClass": {
                    "id": ec_uid,
                    "url": EXTENSION_CLASS_URL,
                    "extensionAttributes": [
                        {
                            "id": ea_scheme_uid,
                            "url": "classification-scheme-id",
                            "valueString": scheme_id or "",
                            "instanceType": "ExtensionAttribute",
                        },
                        {
                            "id": ea_value_uid,
                            "url": "classification-value-id",
                            "valueString": value_id or "",
                            "instanceType": "ExtensionAttribute",
                        },
                    ],
                    "instanceType": "ExtensionClass",
                },
                "instanceType": "ExtensionAttribute",
            }
        )
    return out
