"""Timing.valueLabel is a required string in USDM v4.0 (DDF00082).

A timing saved without a value label used to export ``null``. The
generator now falls back to the timing's ISO 8601 value.
"""

from fastapi.testclient import TestClient

from soa_builder.web.app import app
from usdm.generate_study_timings import build_usdm_timings

client = TestClient(app)


def _timing(soa_id: int, **fields) -> str:
    resp = client.post(f"/soa/{soa_id}/timings", json={"name": "T", **fields})
    assert resp.status_code in (200, 201), resp.text
    return resp.json()["timing_uid"]


def _exported(soa_id: int, timing_uid: str) -> dict:
    return next(t for t in build_usdm_timings(soa_id) if t["id"] == timing_uid)


def test_missing_value_label_falls_back_to_value():
    soa_id = client.post("/soa", json={"name": "Timing label fallback"}).json()["id"]
    uid = _timing(soa_id, value="P0D")

    assert _exported(soa_id, uid)["valueLabel"] == "P0D"


def test_value_label_is_kept_when_set():
    soa_id = client.post("/soa", json={"name": "Timing label kept"}).json()["id"]
    uid = _timing(soa_id, value="P5W", value_label="5 weeks after last dose")

    assert _exported(soa_id, uid)["valueLabel"] == "5 weeks after last dose"


def test_value_label_is_always_a_string():
    soa_id = client.post("/soa", json={"name": "Timing label string"}).json()["id"]
    uid = _timing(soa_id)

    assert isinstance(_exported(soa_id, uid)["valueLabel"], str)
