#!/usr/bin/env python3
"""Build USDM Estimand-Output objects for a SOA.

Reference: USDM_API_v4.0.0.json
  Estimand.interventionIds: list[str]
  Estimand.variableOfInterestId: str
  Estimand.analysisPopulationId: str  (deferred — emits "")
  Estimand.intercurrentEvents: list[IntercurrentEvent]
"""

from typing import Any, Dict, List, Optional

from soa_builder.web.db import _connect


def build_usdm_estimands(soa_id: int) -> List[Dict[str, Any]]:
    conn = _connect()
    cur = conn.cursor()
    cur.execute(
        "SELECT id, estimand_uid, name, label, description, population_summary"
        " FROM estimand WHERE soa_id=? ORDER BY order_index, id",
        (soa_id,),
    )
    rows = cur.fetchall()

    result = []
    for row in rows:
        eid, estimand_uid, name, label, description, population_summary = row

        # Variables of interest (endpoint UIDs)
        cur.execute(
            "SELECT endpoint_uid FROM estimand_variable"
            " WHERE soa_id=? AND estimand_id=? ORDER BY order_index, id",
            (soa_id, eid),
        )
        variable_uids = [r[0] for r in cur.fetchall()]

        # Linked intervention UIDs
        cur.execute(
            "SELECT intervention_uid FROM estimand_intervention"
            " WHERE soa_id=? AND estimand_id=? ORDER BY order_index, id",
            (soa_id, eid),
        )
        intervention_uids = [r[0] for r in cur.fetchall()]

        # Intercurrent events
        cur.execute(
            "SELECT event_uid, name, label, description, text, strategy"
            " FROM intercurrent_event"
            " WHERE soa_id=? AND estimand_id=? ORDER BY order_index, id",
            (soa_id, eid),
        )
        ice_rows = cur.fetchall()

        result.append(
            _build_estimand(
                estimand_uid,
                name,
                label,
                description,
                population_summary,
                variable_uids,
                intervention_uids,
                ice_rows,
            )
        )
    conn.close()
    return result


def _build_estimand(
    estimand_uid: str,
    name: str,
    label: Optional[str],
    description: Optional[str],
    population_summary: Optional[str],
    variable_uids: List[str],
    intervention_uids: List[str],
    ice_rows: list,
) -> Dict[str, Any]:
    intercurrent_events = [_build_ice(r) for r in ice_rows]
    # USDM schema: variableOfInterestId is a single required string;
    # use the first linked endpoint uid, or "" if none linked yet.
    variable_of_interest_id = variable_uids[0] if variable_uids else ""
    return {
        "id": estimand_uid,
        "extensionAttributes": [],
        "name": name or "",
        "label": label or None,
        "description": description or None,
        "populationSummary": population_summary or "",
        "analysisPopulationId": "",
        "variableOfInterestId": variable_of_interest_id,
        "interventionIds": intervention_uids,
        "intercurrentEvents": intercurrent_events,
        "notes": [],
        "instanceType": "Estimand",
    }


def _build_ice(row: tuple) -> Dict[str, Any]:
    event_uid, name, label, description, text, strategy = row
    return {
        "id": event_uid,
        "extensionAttributes": [],
        "name": name or "",
        "label": label or None,
        "description": description or None,
        "text": text or "",
        "strategy": strategy or "",
        "notes": [],
        "instanceType": "IntercurrentEvent",
    }
