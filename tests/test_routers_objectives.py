"""Tests for the objectives router (API + UI + level/code_association)."""

from unittest.mock import patch

from fastapi.testclient import TestClient

from soa_builder.web.app import app
from soa_builder.web.db import _connect

client = TestClient(app)


def _new_soa(name: str) -> int:
    r = client.post("/soa", json={"name": name})
    return r.json()["id"]


def test_list_objectives_empty():
    soa_id = _new_soa("Objectives Empty Test")
    resp = client.get(f"/soa/{soa_id}/objectives")
    assert resp.status_code == 200
    assert resp.json() == []


def test_list_objectives_nonexistent_soa():
    resp = client.get("/soa/999999/objectives")
    assert resp.status_code == 404


def test_create_objective_requires_level():
    soa_id = _new_soa("Objective Level Required Test")
    # Pydantic rejects missing required field
    resp = client.post(f"/soa/{soa_id}/objectives", json={"name": "No level"})
    assert resp.status_code == 422

    # Empty-string level → 400 from router validation
    resp = client.post(
        f"/soa/{soa_id}/objectives",
        json={"name": "Blank level", "level": "   "},
    )
    assert resp.status_code == 400


def test_create_objective_requires_name():
    soa_id = _new_soa("Objective Name Required Test")
    resp = client.post(
        f"/soa/{soa_id}/objectives",
        json={"name": "   ", "level": "Primary Objective"},
    )
    assert resp.status_code == 400


@patch(
    "soa_builder.web.routers.objectives.get_latest_ddf_ct_href",
    return_value="ddfct-2024-01-01",
)
def test_create_objective_inserts_level_code(_mock_slug):
    soa_id = _new_soa("Objective Create Test")
    resp = client.post(
        f"/soa/{soa_id}/objectives",
        json={
            "name": "Assess safety",
            "level": "Primary Objective",
            "label": "SAFETY",
            "description": "Evaluate AEs",
            "text": "To assess safety...",
        },
    )
    assert resp.status_code == 201
    body = resp.json()
    assert body["objective_uid"] == "Objective_1"
    assert body["name"] == "Assess safety"
    assert body["level"] == "Primary Objective"
    assert body["level_code_uid"] == "Code_1"

    # Confirm the code_association row was persisted
    conn = _connect()
    cur = conn.cursor()
    cur.execute(
        "SELECT codelist_code, code FROM code_association "
        "WHERE soa_id=? AND code_uid=?",
        (soa_id, body["level_code_uid"]),
    )
    row = cur.fetchone()
    conn.close()
    assert row is not None
    assert row[0] == "C188725"
    assert row[1] == "Primary Objective"


@patch("soa_builder.web.routers.objectives.get_latest_ddf_ct_href", return_value=None)
def test_list_and_order(_mock_slug):
    soa_id = _new_soa("Objective List Test")
    for i in range(3):
        r = client.post(
            f"/soa/{soa_id}/objectives",
            json={"name": f"Obj {i}", "level": "Primary Objective"},
        )
        assert r.status_code == 201

    resp = client.get(f"/soa/{soa_id}/objectives")
    assert resp.status_code == 200
    rows = resp.json()
    assert len(rows) == 3
    assert [r["objective_uid"] for r in rows] == [
        "Objective_1",
        "Objective_2",
        "Objective_3",
    ]
    assert [r["order_index"] for r in rows] == [1, 2, 3]


@patch("soa_builder.web.routers.objectives.get_latest_ddf_ct_href", return_value=None)
def test_update_objective_swaps_level_code(_mock_slug):
    soa_id = _new_soa("Objective Update Test")
    create = client.post(
        f"/soa/{soa_id}/objectives",
        json={"name": "Orig", "level": "Primary Objective"},
    )
    obj_id = create.json()["id"]
    first_level_uid = create.json()["level_code_uid"]

    resp = client.patch(
        f"/soa/{soa_id}/objectives/{obj_id}",
        json={"name": "Updated", "level": "Secondary Objective"},
    )
    assert resp.status_code == 200
    body = resp.json()
    assert body["name"] == "Updated"
    # code_uid is preserved across level changes; the submission value
    # on the existing code_association row is updated in place.
    assert body["level_code_uid"] == first_level_uid

    conn = _connect()
    cur = conn.cursor()
    cur.execute(
        "SELECT code FROM code_association WHERE soa_id=? AND code_uid=?",
        (soa_id, first_level_uid),
    )
    assert cur.fetchone()[0] == "Secondary Objective"
    conn.close()


@patch("soa_builder.web.routers.objectives.get_latest_ddf_ct_href", return_value=None)
def test_delete_objective_drops_level_and_reindexes(_mock_slug):
    soa_id = _new_soa("Objective Delete Reindex Test")
    ids = []
    for i in range(3):
        r = client.post(
            f"/soa/{soa_id}/objectives",
            json={"name": f"Obj {i}", "level": "Primary Objective"},
        )
        ids.append(r.json()["id"])

    # Delete the middle one
    resp = client.delete(f"/soa/{soa_id}/objectives/{ids[1]}")
    assert resp.status_code == 200

    rows = client.get(f"/soa/{soa_id}/objectives").json()
    assert [r["order_index"] for r in rows] == [1, 2]
    # UIDs never recycled
    uids = {r["objective_uid"] for r in rows}
    assert uids == {"Objective_1", "Objective_3"}


@patch("soa_builder.web.routers.objectives.get_latest_ddf_ct_href", return_value=None)
def test_objective_uid_never_recycled_after_delete(_mock_slug):
    soa_id = _new_soa("Objective UID Monotonic Test")
    r1 = client.post(
        f"/soa/{soa_id}/objectives",
        json={"name": "A", "level": "Primary Objective"},
    )
    r2 = client.post(
        f"/soa/{soa_id}/objectives",
        json={"name": "B", "level": "Primary Objective"},
    )
    assert r1.json()["objective_uid"] == "Objective_1"
    assert r2.json()["objective_uid"] == "Objective_2"

    client.delete(f"/soa/{soa_id}/objectives/{r2.json()['id']}")

    r3 = client.post(
        f"/soa/{soa_id}/objectives",
        json={"name": "C", "level": "Primary Objective"},
    )
    assert r3.json()["objective_uid"] == "Objective_3"


@patch("soa_builder.web.routers.endpoints.get_latest_ddf_ct_href", return_value=None)
@patch("soa_builder.web.routers.objectives.get_latest_ddf_ct_href", return_value=None)
def test_delete_objective_orphans_child_endpoints(_mock_obj_slug, _mock_ep_slug):
    soa_id = _new_soa("Objective Orphan Test")
    obj = client.post(
        f"/soa/{soa_id}/objectives",
        json={"name": "Parent", "level": "Primary Objective"},
    ).json()
    ep = client.post(
        f"/soa/{soa_id}/endpoints",
        json={
            "name": "Child",
            "level": "Primary Endpoint",
            "objective_uid": obj["objective_uid"],
        },
    ).json()

    resp = client.delete(f"/soa/{soa_id}/objectives/{obj['id']}")
    assert resp.status_code == 200
    assert resp.json()["orphaned_endpoints"] == 1

    # Endpoint still exists, but objective_uid is now NULL
    endpoints = client.get(f"/soa/{soa_id}/endpoints").json()
    assert len(endpoints) == 1
    assert endpoints[0]["id"] == ep["id"]
    assert endpoints[0]["objective_uid"] is None


@patch("soa_builder.web.routers.objectives.get_latest_ddf_ct_href", return_value=None)
def test_ui_create_objective_redirects(_mock_slug):
    soa_id = _new_soa("Objective UI Create Test")
    resp = client.post(
        f"/ui/soa/{soa_id}/objectives/create",
        data={"name": "From UI", "level": "Primary Objective"},
        follow_redirects=False,
    )
    assert resp.status_code in (200, 303)
    rows = client.get(f"/soa/{soa_id}/objectives").json()
    assert len(rows) == 1
    assert rows[0]["name"] == "From UI"
