"""USDM ScheduleTimeline.exits generation.

DDF00037 and DDF00108 need each timeline to define an exit, and a
ScheduledActivityInstance.timelineExitId must resolve to one of the
exits of its timeline. The exits come from the timeline's own exit_id
and from the timeline_exit_id of its instances.
"""

from fastapi.testclient import TestClient

from soa_builder.web.app import app
from usdm.generate_schedule_timelines import build_usdm_schedule_timelines

client = TestClient(app)


def _new_soa(name: str) -> int:
    return client.post("/soa", json={"name": name}).json()["id"]


def _timeline(soa_id: int, name: str, main: bool, exit_id: str = "") -> str:
    resp = client.post(
        f"/soa/{soa_id}/schedule_timelines",
        json={"name": name, "main_timeline": main, "exit_id": exit_id},
    )
    assert resp.status_code in (200, 201), resp.text
    return resp.json()["schedule_timeline_uid"]


def _instance(soa_id: int, name: str, timeline_uid: str, exit_id: str = "") -> None:
    resp = client.post(
        f"/soa/{soa_id}/instances",
        json={
            "name": name,
            "member_of_timeline": timeline_uid,
            "timeline_exit_id": exit_id,
        },
    )
    assert resp.status_code in (200, 201), resp.text


def _exits(soa_id: int) -> dict[str, list[dict]]:
    return {tl["id"]: tl["exits"] for tl in build_usdm_schedule_timelines(soa_id)}


def _exit(exit_id: str) -> dict:
    return {
        "id": exit_id,
        "extensionAttributes": [],
        "instanceType": "ScheduleTimelineExit",
    }


def test_timeline_exit_id_becomes_a_schedule_timeline_exit():
    soa_id = _new_soa("Exit from timeline exit_id")
    tl = _timeline(soa_id, "Main", True, exit_id="ScheduleTimelineExit_1")

    assert _exits(soa_id)[tl] == [_exit("ScheduleTimelineExit_1")]


def test_instance_timeline_exit_ids_are_added_once_per_timeline():
    soa_id = _new_soa("Exit from instances")
    main = _timeline(soa_id, "Main", True, exit_id="ScheduleTimelineExit_1")
    sub = _timeline(soa_id, "Discontinuation", False)
    _instance(soa_id, "V17", main, exit_id="ScheduleTimelineExit_1")
    _instance(soa_id, "Early exit", main, exit_id="ScheduleTimelineExit_3")
    _instance(soa_id, "V17A", sub, exit_id="ScheduleTimelineExit_2")

    exits = _exits(soa_id)

    assert exits[main] == [
        _exit("ScheduleTimelineExit_1"),
        _exit("ScheduleTimelineExit_3"),
    ]
    assert exits[sub] == [_exit("ScheduleTimelineExit_2")]


def test_timeline_without_exits_has_empty_list():
    soa_id = _new_soa("No exits")
    tl = _timeline(soa_id, "Main", True)
    _instance(soa_id, "V1", tl)

    assert _exits(soa_id)[tl] == []


def test_every_instance_exit_resolves_to_a_timeline_exit():
    soa_id = _new_soa("Exit references resolve")
    tl = _timeline(soa_id, "Main", True)
    _instance(soa_id, "V17", tl, exit_id="ScheduleTimelineExit_7")

    timeline = next(t for t in build_usdm_schedule_timelines(soa_id) if t["id"] == tl)
    exit_ids = {e["id"] for e in timeline["exits"]}
    referenced = {
        i["timelineExitId"] for i in timeline["instances"] if i.get("timelineExitId")
    }
    assert referenced == {"ScheduleTimelineExit_7"}
    assert referenced <= exit_ids
