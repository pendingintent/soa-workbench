"""The study timing page's dropdowns can be set back to none.

Each row on /ui/soa/{id}/study_timing posts its whole form. Choosing
the empty "-- Select ... --" option sends the field empty, which must
clear it. A field the form does not send is left unchanged.
"""

from fastapi.testclient import TestClient

from soa_builder.web.app import app
from soa_builder.web.routers.schedule_timelines import list_schedule_timelines

client = TestClient(app)


def _new_soa(name):
    return client.post("/soa", json={"name": name}).json()["id"]


def _get(soa_id, kind, entity_id):
    if kind == "schedule_timelines":
        # No JSON list endpoint for timelines; read through the router.
        rows = list_schedule_timelines(soa_id)
    else:
        rows = client.get(f"/soa/{soa_id}/{kind}").json()
    return next(r for r in rows if r["id"] == entity_id)


# --- Decision instances -------------------------------------------------


def _decision_instance(soa_id):
    di = client.post(
        f"/soa/{soa_id}/decision_instances",
        json={
            "name": "D1",
            "default_condition_uid": "ScheduledActivityInstance_1",
            "epoch_uid": "StudyEpoch_1",
            "member_of_timeline": "ScheduleTimeline_1",
        },
    ).json()
    return di["id"]


def _decision_form(**overrides):
    form = {
        "name": "D1",
        "label": "",
        "description": "",
        "default_condition_uid": "ScheduledActivityInstance_1",
        "epoch_uid": "StudyEpoch_1",
        "member_of_timeline": "ScheduleTimeline_1",
    }
    form.update(overrides)
    return form


def test_decision_instance_dropdowns_can_be_cleared():
    soa_id = _new_soa("Clear decision dropdowns")
    di_id = _decision_instance(soa_id)

    resp = client.post(
        f"/ui/soa/{soa_id}/decision_instances/{di_id}/update",
        data=_decision_form(
            default_condition_uid="", epoch_uid="", member_of_timeline=""
        ),
        follow_redirects=False,
    )
    assert resp.status_code == 303

    after = _get(soa_id, "decision_instances", di_id)
    assert after["default_condition_uid"] is None
    assert after["epoch_uid"] is None
    assert after["member_of_timeline"] is None
    assert after["name"] == "D1"


def test_decision_instance_partial_form_keeps_other_fields():
    soa_id = _new_soa("Partial decision form")
    di_id = _decision_instance(soa_id)

    client.post(
        f"/ui/soa/{soa_id}/decision_instances/{di_id}/update",
        data={"label": "Renamed", "name": ""},
    )
    after = _get(soa_id, "decision_instances", di_id)
    assert after["label"] == "Renamed"
    assert after["name"] == "D1"
    assert after["epoch_uid"] == "StudyEpoch_1"


# --- Schedule timelines -------------------------------------------------


def _timeline(soa_id):
    st = client.post(
        f"/soa/{soa_id}/schedule_timelines",
        json={
            "name": "Main",
            "main_timeline": True,
            "entry_condition": "Enrolled",
            "entry_id": "ScheduledActivityInstance_1",
            "exit_id": "ScheduleTimelineExit_1",
        },
    ).json()
    return st["id"]


def _timeline_form(**overrides):
    form = {
        "name": "Main",
        "label": "",
        "description": "",
        "main_timeline": "true",
        "entry_condition": "Enrolled",
        "entry_id": "ScheduledActivityInstance_1",
        "exit_id": "ScheduleTimelineExit_1",
    }
    form.update(overrides)
    return form


def test_timeline_entry_and_exit_can_be_cleared():
    soa_id = _new_soa("Clear timeline dropdowns")
    st_id = _timeline(soa_id)

    resp = client.post(
        f"/ui/soa/{soa_id}/schedule_timelines/{st_id}/update",
        data=_timeline_form(entry_id="", exit_id="", entry_condition=""),
        follow_redirects=False,
    )
    assert resp.status_code == 303

    after = _get(soa_id, "schedule_timelines", st_id)
    assert after["entry_id"] is None
    assert after["exit_id"] is None
    assert after["entry_condition"] is None
    assert after["main_timeline"] is True


def test_timeline_blank_name_is_rejected_and_kept():
    soa_id = _new_soa("Blank timeline name")
    st_id = _timeline(soa_id)

    resp = client.post(
        f"/ui/soa/{soa_id}/schedule_timelines/{st_id}/update",
        data=_timeline_form(name="  "),
        follow_redirects=False,
    )
    assert resp.status_code == 400
    assert _get(soa_id, "schedule_timelines", st_id)["name"] == "Main"


# --- Timings ------------------------------------------------------------


def _timing(soa_id):
    t_id = client.post(f"/soa/{soa_id}/timings", json={"name": "T1"}).json()["id"]
    # Set the coded and linked fields directly through the JSON API.
    client.patch(
        f"/soa/{soa_id}/timings/{t_id}",
        json={
            "name": "T1",
            "type": "Code_901",
            "relative_to_from": "Code_902",
            "relative_from_schedule_instance": "ScheduledActivityInstance_1",
            "relative_to_schedule_instance": "ScheduledActivityInstance_2",
            "member_of_timeline": "ScheduleTimeline_1",
        },
    )
    return t_id


def _timing_form(**overrides):
    form = {
        "name": "T1",
        "label": "",
        "description": "",
        "value": "",
        "value_label": "",
        "window_label": "",
        "window_lower": "",
        "window_upper": "",
    }
    form.update(overrides)
    return form


def test_timing_dropdowns_can_be_cleared():
    soa_id = _new_soa("Clear timing dropdowns")
    t_id = _timing(soa_id)
    before = _get(soa_id, "timings", t_id)
    assert before["relative_from_schedule_instance"] == "ScheduledActivityInstance_1"

    resp = client.post(
        f"/ui/soa/{soa_id}/timings/{t_id}/update",
        data=_timing_form(
            type_submission_value="",
            relative_to_from_submission_value="",
            relative_from_schedule_instance="",
            relative_to_schedule_instance="",
            member_of_timeline="",
        ),
        follow_redirects=False,
    )
    assert resp.status_code == 303

    after = _get(soa_id, "timings", t_id)
    assert after["type"] is None
    assert after["relative_to_from"] is None
    assert after["relative_from_schedule_instance"] is None
    assert after["relative_to_schedule_instance"] is None
    assert after["member_of_timeline"] is None
    assert after["name"] == "T1"


def test_timing_partial_form_keeps_other_fields():
    soa_id = _new_soa("Partial timing form")
    t_id = _timing(soa_id)

    client.post(
        f"/ui/soa/{soa_id}/timings/{t_id}/update",
        data={"name": "T1", "label": "Renamed"},
    )
    after = _get(soa_id, "timings", t_id)
    assert after["label"] == "Renamed"
    assert after["type"] == "Code_901"
    assert after["relative_from_schedule_instance"] == "ScheduledActivityInstance_1"
    assert after["member_of_timeline"] == "ScheduleTimeline_1"
