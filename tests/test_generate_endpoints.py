"""Tests for usdm.generate_endpoints."""

from unittest.mock import patch

from fastapi.testclient import TestClient

from soa_builder.web.app import app
from usdm.generate_endpoints import build_usdm_endpoints

client = TestClient(app)


def _new_soa(name: str) -> int:
    return client.post("/soa", json={"name": name}).json()["id"]


def test_build_usdm_endpoints_empty():
    soa_id = _new_soa("Endpoints Generator Empty")
    assert build_usdm_endpoints(soa_id) == []


@patch(
    "soa_builder.web.routers.objectives.get_latest_ddf_ct_href",
    return_value="ddfct-2024-01-01",
)
@patch(
    "soa_builder.web.routers.endpoints.get_latest_ddf_ct_href",
    return_value="ddfct-2024-01-01",
)
def test_build_usdm_endpoints_required_fields(_e_slug, _o_slug):
    soa_id = _new_soa("Endpoints Generator Full")

    client.post(
        f"/soa/{soa_id}/objectives",
        json={"name": "Obj A", "level": "Primary Objective"},
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
            "objective_uid": "Objective_1",
        },
    )

    out = build_usdm_endpoints(soa_id)
    assert len(out) == 2
    required = {"id", "name", "text", "purpose", "level", "instanceType"}
    for ep in out:
        assert required.issubset(ep.keys())
        assert ep["instanceType"] == "Endpoint"
        assert ep["level"]["instanceType"] == "Code"

    assert out[0]["id"] == "Endpoint_1"
    assert out[0]["purpose"] == "Safety endpoint"
    assert out[0]["level"]["code"] == "Primary Endpoint"
    assert out[1]["id"] == "Endpoint_2"
    assert out[1]["purpose"] == "PK endpoint"
