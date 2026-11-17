"""A timing's Value and window can be cleared.

The Value, Window Lower and Window Upper fields are ISO 8601
durations. Sending one empty must clear it, the same as the other
text fields on the row. Leaving it out keeps the stored value.
"""

from fastapi.testclient import TestClient

from soa_builder.web.app import app

client = TestClient(app)

_DURATIONS = {
    "value": "P7D",
    "value_label": "Day 7",
    "window_lower": "-P2D",
    "window_upper": "P3D",
    "window_label": "-2..+3 days",
}


def _timing_with_durations(name):
    soa_id = client.post("/soa", json={"name": name}).json()["id"]
    resp = client.post(f"/soa/{soa_id}/timings", json={"name": "T1", **_DURATIONS})
    assert resp.status_code == 201, resp.text
    return soa_id, resp.json()["id"]


def _get(soa_id, timing_id):
    rows = client.get(f"/soa/{soa_id}/timings").json()
    return next(r for r in rows if r["id"] == timing_id)


def test_ui_clears_value_and_window():
    soa_id, t_id = _timing_with_durations("Clear timing value")
    resp = client.post(
        f"/ui/soa/{soa_id}/timings/{t_id}/update",
        data={
            "name": "T1",
            "value": "",
            "value_label": "",
            "window_lower": "",
            "window_upper": "",
            "window_label": "",
        },
        follow_redirects=False,
    )
    assert resp.status_code == 303, resp.text

    after = _get(soa_id, t_id)
    for field in _DURATIONS:
        assert after[field] is None, field


def test_ui_clears_value_only_and_keeps_window():
    soa_id, t_id = _timing_with_durations("Clear value keep window")
    form = {"name": "T1", **_DURATIONS, "value": "", "value_label": ""}
    resp = client.post(
        f"/ui/soa/{soa_id}/timings/{t_id}/update",
        data=form,
        follow_redirects=False,
    )
    assert resp.status_code == 303, resp.text

    after = _get(soa_id, t_id)
    assert after["value"] is None
    assert after["value_label"] is None
    assert after["window_lower"] == "-P2D"
    assert after["window_upper"] == "P3D"
    assert after["window_label"] == "-2..+3 days"


def test_ui_clearing_part_of_window_is_rejected():
    soa_id, t_id = _timing_with_durations("Partial window clear")
    form = {"name": "T1", **_DURATIONS, "window_upper": ""}
    resp = client.post(
        f"/ui/soa/{soa_id}/timings/{t_id}/update",
        data=form,
        follow_redirects=False,
    )
    assert resp.status_code == 400
    assert "window_upper" in resp.text
    assert _get(soa_id, t_id)["window_upper"] == "P3D"


def test_api_patch_empty_string_clears_value():
    soa_id, t_id = _timing_with_durations("API clear value")
    resp = client.patch(
        f"/soa/{soa_id}/timings/{t_id}", json={"name": "T1", "value": ""}
    )
    assert resp.status_code == 200, resp.text
    assert resp.json()["value"] is None


def test_api_patch_clearing_one_window_bound_is_rejected():
    # Only window_upper is sent; clearing it would leave a stored
    # window with a lower bound and label but no upper bound.
    soa_id, t_id = _timing_with_durations("API partial window clear")
    resp = client.patch(
        f"/soa/{soa_id}/timings/{t_id}", json={"name": "T1", "window_upper": ""}
    )
    assert resp.status_code == 400
    assert "window_upper" in resp.text
    assert _get(soa_id, t_id)["window_upper"] == "P3D"


def test_api_patch_omitted_durations_are_kept():
    soa_id, t_id = _timing_with_durations("API keep durations")
    resp = client.patch(
        f"/soa/{soa_id}/timings/{t_id}", json={"name": "T1", "label": "L"}
    )
    assert resp.status_code == 200, resp.text
    after = resp.json()
    assert after["value"] == "P7D"
    assert after["window_lower"] == "-P2D"
    assert after["window_upper"] == "P3D"


def test_api_patch_invalid_duration_still_rejected():
    soa_id, t_id = _timing_with_durations("API bad duration")
    resp = client.patch(
        f"/soa/{soa_id}/timings/{t_id}", json={"name": "T1", "value": "7 days"}
    )
    assert resp.status_code == 422
