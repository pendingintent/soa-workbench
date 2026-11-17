"""iso_duration_to_days feeds the matrix's Study Day row.

A negative duration (e.g. -P14D, a screening visit before Day 1) must
keep its sign rather than disappear from the matrix.
"""

import pytest
from fastapi.testclient import TestClient

from soa_builder.web.app import app
from soa_builder.web.utils import iso_duration_to_days

client = TestClient(app)


@pytest.mark.parametrize(
    "value, expected",
    [
        ("P14D", 14.0),
        ("-P14D", -14.0),
        ("-P2W", -14.0),
        ("-P1DT12H", -1.5),
        ("P0D", 0.0),
    ],
)
def test_converts_signed_durations(value, expected):
    assert iso_duration_to_days(value) == expected


@pytest.mark.parametrize("value", ["", None, "14 days", "--P1D", "P-1D"])
def test_rejects_invalid(value):
    assert iso_duration_to_days(value) is None


def test_matrix_study_day_row_shows_negative_day():
    soa_id = client.post("/soa", json={"name": "Negative study day"}).json()["id"]
    tl = client.post(
        f"/soa/{soa_id}/schedule_timelines",
        json={"name": "Main", "main_timeline": True},
    ).json()
    timing = client.post(
        f"/soa/{soa_id}/timings",
        json={"name": "Screening timing", "value": "-P14D"},
    ).json()
    # The matrix reads the timing through the encounter's scheduledAtId.
    visit = client.post(
        f"/soa/{soa_id}/visits",
        json={"name": "Screening", "scheduledAtId": str(timing["id"])},
    )
    assert visit.status_code in (200, 201), visit.text
    resp = client.post(
        f"/soa/{soa_id}/instances",
        json={
            "name": "Screening",
            "member_of_timeline": tl["schedule_timeline_uid"],
            # First encounter in a new study; the create response
            # does not return its uid.
            "encounter_uid": "Encounter_1",
        },
    )
    assert resp.status_code == 201, resp.text

    html = client.get(f"/ui/soa/{soa_id}/edit").text
    row = html.split('class="matrix-study-day-row"', 1)[1].split("</tr>", 1)[0]
    assert "-14" in row
