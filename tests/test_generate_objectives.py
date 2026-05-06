"""Tests for usdm.generate_objectives."""

from unittest.mock import patch

from fastapi.testclient import TestClient

from soa_builder.web.app import app
from usdm.generate_objectives import build_usdm_objectives

client = TestClient(app)


def _new_soa(name: str) -> int:
    return client.post("/soa", json={"name": name}).json()["id"]


def test_build_usdm_objectives_empty():
    soa_id = _new_soa("Objectives Generator Empty")
    assert build_usdm_objectives(soa_id) == []


@patch(
    "soa_builder.web.routers.objectives.get_latest_ddf_ct_href",
    return_value="ddfct-2024-01-01",
)
@patch(
    "soa_builder.web.routers.endpoints.get_latest_ddf_ct_href",
    return_value="ddfct-2024-01-01",
)
def test_build_usdm_objectives_with_nested_endpoints(_e_slug, _o_slug):
    soa_id = _new_soa("Objectives Generator Full")

    client.post(
        f"/soa/{soa_id}/objectives",
        json={
            "name": "Assess safety",
            "level": "Primary Objective",
            "text": "To assess safety...",
        },
    )
    client.post(
        f"/soa/{soa_id}/objectives",
        json={
            "name": "Assess PK",
            "level": "Secondary Objective",
            "text": "Pharmacokinetics",
        },
    )

    client.post(
        f"/soa/{soa_id}/endpoints",
        json={
            "name": "AE rate",
            "level": "Primary Endpoint",
            "purpose": "Safety endpoint",
            "text": "Incidence of AEs",
            "objective_uid": "Objective_1",
        },
    )
    client.post(
        f"/soa/{soa_id}/endpoints",
        json={
            "name": "Cmax",
            "level": "Secondary Endpoint",
            "purpose": "PK endpoint",
            "text": "Maximum concentration",
            "objective_uid": "Objective_2",
        },
    )

    out = build_usdm_objectives(soa_id)
    assert len(out) == 2
    obj1, obj2 = out

    assert obj1["instanceType"] == "Objective"
    assert obj1["id"] == "Objective_1"
    assert obj1["name"] == "Assess safety"
    assert obj1["text"] == "To assess safety..."
    assert obj1["level"]["instanceType"] == "Code"
    assert obj1["level"]["code"] == "Primary Objective"
    assert len(obj1["endpoints"]) == 1
    assert obj1["endpoints"][0]["id"] == "Endpoint_1"
    assert obj1["endpoints"][0]["instanceType"] == "Endpoint"
    assert obj1["endpoints"][0]["purpose"] == "Safety endpoint"

    assert len(obj2["endpoints"]) == 1
    assert obj2["endpoints"][0]["id"] == "Endpoint_2"


def test_build_usdm_objectives_required_fields():
    """Each objective in the output must have all USDM-required fields."""
    soa_id = _new_soa("Objectives Required Fields")

    with patch(
        "soa_builder.web.routers.objectives.get_latest_ddf_ct_href",
        return_value="ddfct-2024-01-01",
    ):
        client.post(
            f"/soa/{soa_id}/objectives",
            json={
                "name": "Obj A",
                "level": "Primary Objective",
                "text": "Primary text",
            },
        )

    out = build_usdm_objectives(soa_id)
    required = {"id", "name", "text", "level", "instanceType"}
    for obj in out:
        assert required.issubset(obj.keys())
        assert obj["instanceType"] == "Objective"
        assert isinstance(obj["endpoints"], list)
