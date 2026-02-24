"""Tests for ScheduledDecisionInstance and ConditionAssignment routes."""

from fastapi.testclient import TestClient

from soa_builder.web.app import app

client = TestClient(app)


def test_list_decision_instances_empty():
    """GET /soa/{soa_id}/decision_instances returns empty list for a new SOA."""
    r = client.post("/soa", json={"name": "DI Empty Test"})
    soa_id = r.json()["id"]

    resp = client.get(f"/soa/{soa_id}/decision_instances")
    assert resp.status_code == 200
    assert resp.json() == []


def test_create_decision_instance():
    """POST /soa/{soa_id}/decision_instances creates a decision instance with UID."""
    r = client.post("/soa", json={"name": "DI Create Test"})
    soa_id = r.json()["id"]

    resp = client.post(
        f"/soa/{soa_id}/decision_instances",
        json={"name": "End of Cycle?"},
    )
    assert resp.status_code == 201
    data = resp.json()
    assert data["name"] == "End of Cycle?"
    assert data["instance_uid"] == "ScheduledDecisionInstance_1"
    assert data["id"] is not None


def test_create_decision_instance_uid_monotonic():
    """Second decision instance in same SOA gets ScheduledDecisionInstance_2."""
    r = client.post("/soa", json={"name": "DI Monotonic Test"})
    soa_id = r.json()["id"]

    r1 = client.post(f"/soa/{soa_id}/decision_instances", json={"name": "D1"})
    r2 = client.post(f"/soa/{soa_id}/decision_instances", json={"name": "D2"})
    assert r1.json()["instance_uid"] == "ScheduledDecisionInstance_1"
    assert r2.json()["instance_uid"] == "ScheduledDecisionInstance_2"


def test_create_decision_instance_404_unknown_soa():
    """POST to nonexistent SOA returns 404."""
    resp = client.post("/soa/999999/decision_instances", json={"name": "X"})
    assert resp.status_code == 404


def test_update_decision_instance():
    """PATCH updates name and description."""
    r = client.post("/soa", json={"name": "DI Update Test"})
    soa_id = r.json()["id"]

    cr = client.post(
        f"/soa/{soa_id}/decision_instances",
        json={"name": "Original", "description": "Old desc"},
    )
    di_id = cr.json()["id"]

    resp = client.patch(
        f"/soa/{soa_id}/decision_instances/{di_id}",
        json={"name": "Updated", "description": "New desc"},
    )
    assert resp.status_code == 200
    data = resp.json()
    assert data["name"] == "Updated"
    assert data["description"] == "New desc"
    assert "updated_fields" in data


def test_delete_decision_instance():
    """DELETE removes the decision instance."""
    r = client.post("/soa", json={"name": "DI Delete Test"})
    soa_id = r.json()["id"]

    cr = client.post(f"/soa/{soa_id}/decision_instances", json={"name": "ToDelete"})
    di_id = cr.json()["id"]

    resp = client.delete(f"/soa/{soa_id}/decision_instances/{di_id}")
    assert resp.status_code == 200
    assert resp.json()["deleted"] is True

    # Confirm it's gone
    list_resp = client.get(f"/soa/{soa_id}/decision_instances")
    assert all(d["id"] != di_id for d in list_resp.json())


def test_delete_decision_instance_404():
    """DELETE nonexistent decision instance returns 404."""
    r = client.post("/soa", json={"name": "DI Delete 404 Test"})
    soa_id = r.json()["id"]

    resp = client.delete(f"/soa/{soa_id}/decision_instances/999999")
    assert resp.status_code == 404


def test_ui_decision_instances_200():
    """GET /ui/soa/{soa_id}/decision_instances returns 200 HTML."""
    r = client.post("/soa", json={"name": "DI UI Test"})
    soa_id = r.json()["id"]

    resp = client.get(f"/ui/soa/{soa_id}/decision_instances")
    assert resp.status_code == 200
    assert "text/html" in resp.headers.get("content-type", "")


def test_create_condition_assignment():
    """POST /soa/{soa_id}/condition_assignments creates a ConditionAssignment."""
    r = client.post("/soa", json={"name": "CA Create Test"})
    soa_id = r.json()["id"]

    # Create the decision instance
    di_r = client.post(
        f"/soa/{soa_id}/decision_instances",
        json={"name": "Branch Point"},
    )
    di_uid = di_r.json()["instance_uid"]

    resp = client.post(
        f"/soa/{soa_id}/condition_assignments",
        json={
            "name": "Eligibility Assignment",
            "decision_instance_uid": di_uid,
            "condition": "Eligibility met",
            "condition_target_uid": "ScheduledActivityInstance_1",
        },
    )
    assert resp.status_code == 201
    data = resp.json()
    assert data["condition_assignment_uid"] == "ConditionAssignment_1"
    assert data["decision_instance_uid"] == di_uid
    assert data["condition"] == "Eligibility met"
    assert data["condition_target_uid"] == "ScheduledActivityInstance_1"


def test_condition_assignment_uid_monotonic():
    """Second condition assignment gets ConditionAssignment_2."""
    r = client.post("/soa", json={"name": "CA Monotonic Test"})
    soa_id = r.json()["id"]

    di_r = client.post(f"/soa/{soa_id}/decision_instances", json={"name": "D"})
    di_uid = di_r.json()["instance_uid"]

    r1 = client.post(
        f"/soa/{soa_id}/condition_assignments",
        json={
            "name": "CA1",
            "decision_instance_uid": di_uid,
            "condition": "C1",
            "condition_target_uid": "ScheduledActivityInstance_1",
        },
    )
    r2 = client.post(
        f"/soa/{soa_id}/condition_assignments",
        json={
            "name": "CA2",
            "decision_instance_uid": di_uid,
            "condition": "C2",
            "condition_target_uid": "ScheduledActivityInstance_2",
        },
    )
    assert r1.json()["condition_assignment_uid"] == "ConditionAssignment_1"
    assert r2.json()["condition_assignment_uid"] == "ConditionAssignment_2"


def test_list_condition_assignments_by_decision_instance():
    """GET ?decision_instance_uid= filters correctly."""
    r = client.post("/soa", json={"name": "CA Filter Test"})
    soa_id = r.json()["id"]

    di1 = client.post(f"/soa/{soa_id}/decision_instances", json={"name": "D1"}).json()[
        "instance_uid"
    ]
    di2 = client.post(f"/soa/{soa_id}/decision_instances", json={"name": "D2"}).json()[
        "instance_uid"
    ]

    client.post(
        f"/soa/{soa_id}/condition_assignments",
        json={
            "name": "CA-A",
            "decision_instance_uid": di1,
            "condition": "A",
            "condition_target_uid": "T1",
        },
    )
    client.post(
        f"/soa/{soa_id}/condition_assignments",
        json={
            "name": "CA-B",
            "decision_instance_uid": di2,
            "condition": "B",
            "condition_target_uid": "T2",
        },
    )

    resp = client.get(
        f"/soa/{soa_id}/condition_assignments?decision_instance_uid={di1}"
    )
    assert resp.status_code == 200
    data = resp.json()
    assert len(data) == 1
    assert data[0]["condition"] == "A"


def test_delete_condition_assignment():
    """DELETE removes the condition assignment."""
    r = client.post("/soa", json={"name": "CA Delete Test"})
    soa_id = r.json()["id"]

    di_uid = client.post(
        f"/soa/{soa_id}/decision_instances", json={"name": "D"}
    ).json()["instance_uid"]
    ca_r = client.post(
        f"/soa/{soa_id}/condition_assignments",
        json={
            "name": "CA-X",
            "decision_instance_uid": di_uid,
            "condition": "X",
            "condition_target_uid": "T",
        },
    )
    ca_id = ca_r.json()["id"]

    resp = client.delete(f"/soa/{soa_id}/condition_assignments/{ca_id}")
    assert resp.status_code == 200
    assert resp.json()["deleted"] is True


def test_usdm_includes_decision_instances():
    """Full USDM JSON includes ScheduledDecisionInstance in timeline instances."""
    r = client.post("/soa", json={"name": "USDM DI Test"})
    soa_id = r.json()["id"]

    # Create a schedule timeline
    tl_r = client.post(
        f"/soa/{soa_id}/schedule_timelines",
        json={"name": "Main", "main_timeline": True},
    )
    tl_uid = tl_r.json()["schedule_timeline_uid"]

    # Create a decision instance linked to the timeline
    client.post(
        f"/soa/{soa_id}/decision_instances",
        json={"name": "Cycle Decision", "member_of_timeline": tl_uid},
    )

    resp = client.get(f"/soa/{soa_id}/usdm_json/full")
    assert resp.status_code == 200
    usdm = resp.json()

    # Find the timeline in the USDM output
    timelines = (
        usdm.get("study", {})
        .get("versions", [{}])[0]
        .get("studyDesigns", [{}])[0]
        .get("scheduleTimelines", [])
    )
    assert len(timelines) >= 1
    instances = timelines[0]["instances"]
    decision_instances = [
        i for i in instances if i.get("instanceType") == "ScheduledDecisionInstance"
    ]
    assert len(decision_instances) == 1
    assert decision_instances[0]["id"] == "ScheduledDecisionInstance_1"
    assert decision_instances[0]["name"] == "Cycle Decision"


def test_usdm_decision_instance_condition_assignments():
    """USDM JSON nests conditionAssignments inside ScheduledDecisionInstance."""
    r = client.post("/soa", json={"name": "USDM CA Test"})
    soa_id = r.json()["id"]

    tl_r = client.post(
        f"/soa/{soa_id}/schedule_timelines",
        json={"name": "Timeline", "main_timeline": True},
    )
    tl_uid = tl_r.json()["schedule_timeline_uid"]

    di_r = client.post(
        f"/soa/{soa_id}/decision_instances",
        json={"name": "Branch", "member_of_timeline": tl_uid},
    )
    di_uid = di_r.json()["instance_uid"]

    client.post(
        f"/soa/{soa_id}/condition_assignments",
        json={
            "name": "Cycle Assignment",
            "decision_instance_uid": di_uid,
            "condition": "not reached cycle 12",
            "condition_target_uid": "ScheduledActivityInstance_1",
        },
    )

    resp = client.get(f"/soa/{soa_id}/usdm_json/full")
    assert resp.status_code == 200
    usdm = resp.json()

    timelines = (
        usdm.get("study", {})
        .get("versions", [{}])[0]
        .get("studyDesigns", [{}])[0]
        .get("scheduleTimelines", [])
    )
    instances = timelines[0]["instances"]
    di = next(
        i for i in instances if i.get("instanceType") == "ScheduledDecisionInstance"
    )
    assert len(di["conditionAssignments"]) == 1
    ca = di["conditionAssignments"][0]
    assert ca["condition"] == "not reached cycle 12"
    assert ca["conditionTargetId"] == "ScheduledActivityInstance_1"
    assert ca["instanceType"] == "ConditionAssignment"
    assert ca["id"] == "ConditionAssignment_1"
