"""Tests for the estimands router (API + USDM output)."""

import re
from unittest.mock import patch

from fastapi.testclient import TestClient

from soa_builder.web.app import app

client = TestClient(app)

_PATCH_OBJ_SLUG = patch(
    "soa_builder.web.routers.objectives.get_latest_ddf_ct_href",
    return_value=None,
)
_PATCH_EP_SLUG = patch(
    "soa_builder.web.routers.endpoints.get_latest_ddf_ct_href",
    return_value="ddfct-2024-01-01",
)


def _new_soa(name: str) -> int:
    r = client.post("/soa", json={"name": name})
    assert r.status_code in (200, 201), r.text
    return r.json()["id"]


def _new_intervention(soa_id: int, name: str = "Drug A") -> dict:
    r = client.post(
        f"/soa/{soa_id}/study-interventions",
        json={"name": name},
    )
    assert r.status_code == 201, r.text
    return r.json()


def _new_endpoint(soa_id: int, name: str = "Endpoint A") -> dict:
    with _PATCH_OBJ_SLUG:
        obj = client.post(
            f"/soa/{soa_id}/objectives",
            json={"name": "Objective for " + name, "level": "Primary Objective"},
        ).json()
    with _PATCH_EP_SLUG:
        r = client.post(
            f"/soa/{soa_id}/endpoints",
            json={
                "name": name,
                "level": "Primary Endpoint",
                "objective_uid": obj["objective_uid"],
            },
        )
    assert r.status_code == 201, r.text
    return r.json()


def _create_estimand(soa_id: int, **kwargs) -> dict:
    body = {"name": "Estimand Alpha"}
    body.update(kwargs)
    r = client.post(f"/soa/{soa_id}/estimands", json=body)
    assert r.status_code == 201, r.text
    return r.json()


# ---------------------------------------------------------------------------
# UID and CRUD
# ---------------------------------------------------------------------------


def test_create_estimand_returns_uid():
    soa_id = _new_soa("Est Create UID Test")
    body = _create_estimand(soa_id, name="Est 1")
    assert re.match(r"^Estimand_\d+$", body["estimand_uid"])
    assert body["name"] == "Est 1"


def test_estimand_uid_monotonic():
    soa_id = _new_soa("Est Monotonic Test")
    r1 = _create_estimand(soa_id, name="Est A")
    r2 = _create_estimand(soa_id, name="Est B")
    assert r1["estimand_uid"] == "Estimand_1"
    assert r2["estimand_uid"] == "Estimand_2"

    r = client.delete(f"/soa/{soa_id}/estimands/{r1['id']}")
    assert r.status_code == 200

    r3 = _create_estimand(soa_id, name="Est C")
    assert r3["estimand_uid"] == "Estimand_3"


def test_list_estimands():
    soa_id = _new_soa("Est List Test")
    _create_estimand(soa_id, name="Alpha")
    _create_estimand(soa_id, name="Beta")
    resp = client.get(f"/soa/{soa_id}/estimands")
    assert resp.status_code == 200
    data = resp.json()
    assert len(data) == 2
    assert data[0]["name"] == "Alpha"
    assert data[1]["name"] == "Beta"


def test_list_estimands_empty():
    soa_id = _new_soa("Est Empty Test")
    resp = client.get(f"/soa/{soa_id}/estimands")
    assert resp.status_code == 200
    assert resp.json() == []


def test_delete_estimand():
    soa_id = _new_soa("Est Delete Test")
    body = _create_estimand(soa_id, name="To Delete")
    eid = body["id"]
    r = client.delete(f"/soa/{soa_id}/estimands/{eid}")
    assert r.status_code == 200
    assert client.get(f"/soa/{soa_id}/estimands").json() == []


def test_unknown_soa_returns_404():
    r = client.post("/soa/999999/estimands", json={"name": "X"})
    assert r.status_code == 404

    r = client.get("/soa/999999/estimands")
    assert r.status_code == 404

    r = client.delete("/soa/999999/estimands/1")
    assert r.status_code == 404


# ---------------------------------------------------------------------------
# Variables of Interest (Endpoints)
# ---------------------------------------------------------------------------


def test_variable_of_interest_stored_and_listed():
    soa_id = _new_soa("Est VOI Test")
    ep = _new_endpoint(soa_id, "Primary Endpoint")
    endpoint_uid = ep["endpoint_uid"]

    _create_estimand(
        soa_id,
        name="Est VOI",
        variable_uids=[endpoint_uid],
    )
    data = client.get(f"/soa/{soa_id}/estimands").json()
    assert endpoint_uid in data[0]["variable_uids"]


def test_link_and_unlink_variable():
    soa_id = _new_soa("Est Var Link Test")
    ep = _new_endpoint(soa_id, "Endpoint Link")
    endpoint_uid = ep["endpoint_uid"]

    est = _create_estimand(soa_id, name="Est Var")
    eid = est["id"]

    r = client.post(
        f"/soa/{soa_id}/estimands/{eid}/variables",
        json={"endpoint_uid": endpoint_uid},
    )
    assert r.status_code == 201

    data = client.get(f"/soa/{soa_id}/estimands").json()
    assert endpoint_uid in data[0]["variable_uids"]

    r = client.delete(f"/soa/{soa_id}/estimands/{eid}/variables/{endpoint_uid}")
    assert r.status_code == 200

    data = client.get(f"/soa/{soa_id}/estimands").json()
    assert endpoint_uid not in data[0]["variable_uids"]


# ---------------------------------------------------------------------------
# Intervention link / unlink
# ---------------------------------------------------------------------------


def test_link_and_unlink_intervention():
    soa_id = _new_soa("Est Link Test")
    si = _new_intervention(soa_id, "Drug Link")
    intervention_uid = si["intervention_uid"]

    est = _create_estimand(soa_id, name="Est Link")
    eid = est["id"]

    # Link via API
    r = client.post(
        f"/soa/{soa_id}/estimands/{eid}/interventions",
        json={"intervention_uid": intervention_uid},
    )
    assert r.status_code == 201

    data = client.get(f"/soa/{soa_id}/estimands").json()
    assert intervention_uid in data[0]["intervention_uids"]

    # Unlink via API
    r = client.delete(f"/soa/{soa_id}/estimands/{eid}/interventions/{intervention_uid}")
    assert r.status_code == 200

    data = client.get(f"/soa/{soa_id}/estimands").json()
    assert intervention_uid not in data[0]["intervention_uids"]


def test_create_estimand_with_intervention_uids():
    soa_id = _new_soa("Est Create With Interventions Test")
    si = _new_intervention(soa_id, "Drug Inline")
    uid = si["intervention_uid"]

    _create_estimand(soa_id, name="Est Inline", intervention_uids=[uid])
    data = client.get(f"/soa/{soa_id}/estimands").json()
    assert uid in data[0]["intervention_uids"]


# ---------------------------------------------------------------------------
# IntercurrentEvent sub-resource
# ---------------------------------------------------------------------------


def test_add_and_delete_intercurrent_event():
    soa_id = _new_soa("Est ICE Test")
    est = _create_estimand(soa_id, name="Est ICE")
    eid = est["id"]

    r = client.post(
        f"/soa/{soa_id}/estimands/{eid}/intercurrent-events",
        json={
            "name": "ICE Alpha",
            "text": "Patient discontinued due to AE",
            "strategy": "hypothetical",
        },
    )
    assert r.status_code == 201, r.text
    ice = r.json()
    assert re.match(r"^IntercurrentEvent_\d+$", ice["event_uid"])

    data = client.get(f"/soa/{soa_id}/estimands").json()
    assert len(data[0]["intercurrent_events"]) == 1
    assert data[0]["intercurrent_events"][0]["name"] == "ICE Alpha"

    # Delete
    r = client.delete(f"/soa/{soa_id}/estimands/{eid}/intercurrent-events/{ice['id']}")
    assert r.status_code == 200

    data = client.get(f"/soa/{soa_id}/estimands").json()
    assert data[0]["intercurrent_events"] == []


def test_ice_uid_monotonic():
    soa_id = _new_soa("Est ICE Monotonic Test")
    est = _create_estimand(soa_id, name="Est ICE Mono")
    eid = est["id"]

    def _add_ice(name):
        r = client.post(
            f"/soa/{soa_id}/estimands/{eid}/intercurrent-events",
            json={"name": name, "text": "t", "strategy": "s"},
        )
        assert r.status_code == 201
        return r.json()

    ice1 = _add_ice("ICE 1")
    ice2 = _add_ice("ICE 2")
    assert ice1["event_uid"] == "IntercurrentEvent_1"
    assert ice2["event_uid"] == "IntercurrentEvent_2"

    r = client.delete(f"/soa/{soa_id}/estimands/{eid}/intercurrent-events/{ice1['id']}")
    assert r.status_code == 200

    ice3 = _add_ice("ICE 3")
    assert ice3["event_uid"] == "IntercurrentEvent_3"


# ---------------------------------------------------------------------------
# Cascade delete
# ---------------------------------------------------------------------------


def test_delete_estimand_cascades():
    soa_id = _new_soa("Est Cascade Delete Test")
    si = _new_intervention(soa_id, "Drug Cascade")
    ep = _new_endpoint(soa_id, "Endpoint Cascade")
    est = _create_estimand(
        soa_id,
        name="Est Cascade",
        intervention_uids=[si["intervention_uid"]],
        variable_uids=[ep["endpoint_uid"]],
    )
    eid = est["id"]

    client.post(
        f"/soa/{soa_id}/estimands/{eid}/intercurrent-events",
        json={"name": "ICE", "text": "x", "strategy": "y"},
    )

    r = client.delete(f"/soa/{soa_id}/estimands/{eid}")
    assert r.status_code == 200

    assert client.get(f"/soa/{soa_id}/estimands").json() == []


# ---------------------------------------------------------------------------
# USDM output
# ---------------------------------------------------------------------------


def test_usdm_estimand_in_output():
    soa_id = _new_soa("Est USDM Test")
    si = _new_intervention(soa_id, "Drug USDM")
    intervention_uid = si["intervention_uid"]
    ep = _new_endpoint(soa_id, "Primary Endpoint USDM")
    endpoint_uid = ep["endpoint_uid"]

    _create_estimand(
        soa_id,
        name="Est USDM Alpha",
        population_summary="All randomised subjects",
        variable_uids=[endpoint_uid],
        intervention_uids=[intervention_uid],
    )

    resp = client.get(f"/soa/{soa_id}/usdm_json/full")
    assert resp.status_code == 200
    doc = resp.json()
    design = doc["study"]["versions"][0]["studyDesigns"][0]

    estimands = design["estimands"]
    assert len(estimands) == 1
    est = estimands[0]
    assert est["instanceType"] == "Estimand"
    assert est["name"] == "Est USDM Alpha"
    assert est["id"].startswith("Estimand_")
    assert est["populationSummary"] == "All randomised subjects"
    assert est["variableOfInterestId"] == endpoint_uid
    assert intervention_uid in est["interventionIds"]
    assert est["analysisPopulationId"] == ""
    assert est["intercurrentEvents"] == []
    assert est["notes"] == []


def test_usdm_estimand_with_ice():
    soa_id = _new_soa("Est USDM ICE Test")
    est = _create_estimand(soa_id, name="Est ICE USDM")
    eid = est["id"]

    r = client.post(
        f"/soa/{soa_id}/estimands/{eid}/intercurrent-events",
        json={
            "name": "Discontinuation",
            "label": "Disc",
            "text": "Patient withdrew consent",
            "strategy": "principal stratum",
        },
    )
    assert r.status_code == 201

    resp = client.get(f"/soa/{soa_id}/usdm_json/full")
    assert resp.status_code == 200
    design = resp.json()["study"]["versions"][0]["studyDesigns"][0]
    est_out = design["estimands"][0]

    assert len(est_out["intercurrentEvents"]) == 1
    ice = est_out["intercurrentEvents"][0]
    assert ice["instanceType"] == "IntercurrentEvent"
    assert ice["id"].startswith("IntercurrentEvent_")
    assert ice["name"] == "Discontinuation"
    assert ice["label"] == "Disc"
    assert ice["text"] == "Patient withdrew consent"
    assert ice["strategy"] == "principal stratum"
    assert ice["notes"] == []
