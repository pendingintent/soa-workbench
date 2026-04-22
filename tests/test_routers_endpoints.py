"""Tests for the endpoints router (API + UI + level/parent FK)."""

from unittest.mock import patch

from fastapi.testclient import TestClient

from soa_builder.web.app import app
from soa_builder.web.db import _connect

client = TestClient(app)


def _new_soa(name: str) -> int:
    r = client.post("/soa", json={"name": name})
    return r.json()["id"]


def test_list_endpoints_empty():
    soa_id = _new_soa("Endpoints Empty Test")
    resp = client.get(f"/soa/{soa_id}/endpoints")
    assert resp.status_code == 200
    assert resp.json() == []


def test_list_endpoints_nonexistent_soa():
    resp = client.get("/soa/999999/endpoints")
    assert resp.status_code == 404


@patch("soa_builder.web.routers.objectives.get_latest_ddf_ct_href", return_value=None)
def test_create_endpoint_requires_level(_obj_slug):
    soa_id = _new_soa("Endpoint Level Required Test")
    obj = client.post(
        f"/soa/{soa_id}/objectives",
        json={"name": "Parent", "level": "Primary Objective"},
    ).json()

    # Missing level → 422
    resp = client.post(
        f"/soa/{soa_id}/endpoints",
        json={"name": "X", "objective_uid": obj["objective_uid"]},
    )
    assert resp.status_code == 422

    # Whitespace-only level → 400
    resp = client.post(
        f"/soa/{soa_id}/endpoints",
        json={
            "name": "X",
            "level": "   ",
            "objective_uid": obj["objective_uid"],
        },
    )
    assert resp.status_code == 400


@patch("soa_builder.web.routers.objectives.get_latest_ddf_ct_href", return_value=None)
def test_create_endpoint_requires_valid_objective(_obj_slug):
    soa_id = _new_soa("Endpoint FK Required Test")
    resp = client.post(
        f"/soa/{soa_id}/endpoints",
        json={
            "name": "Orphan",
            "level": "Primary Endpoint",
            "objective_uid": "Objective_999",
        },
    )
    assert resp.status_code == 400


@patch(
    "soa_builder.web.routers.endpoints.get_latest_ddf_ct_href",
    return_value="ddfct-2024-01-01",
)
@patch("soa_builder.web.routers.objectives.get_latest_ddf_ct_href", return_value=None)
def test_create_endpoint_inserts_level_code(_obj_slug, _ep_slug):
    soa_id = _new_soa("Endpoint Create Test")
    obj = client.post(
        f"/soa/{soa_id}/objectives",
        json={"name": "Parent", "level": "Primary Objective"},
    ).json()

    resp = client.post(
        f"/soa/{soa_id}/endpoints",
        json={
            "name": "Primary safety",
            "level": "Primary Endpoint",
            "objective_uid": obj["objective_uid"],
            "purpose": "Measure SAE rate",
            "text": "Rate of SAEs at week 12",
        },
    )
    assert resp.status_code == 201
    body = resp.json()
    assert body["endpoint_uid"] == "Endpoint_1"
    assert body["objective_uid"] == obj["objective_uid"]
    assert body["level"] == "Primary Endpoint"
    assert body["purpose"] == "Measure SAE rate"

    conn = _connect()
    cur = conn.cursor()
    cur.execute(
        "SELECT codelist_code, code FROM code_association "
        "WHERE soa_id=? AND code_uid=?",
        (soa_id, body["level_code_uid"]),
    )
    row = cur.fetchone()
    conn.close()
    assert row[0] == "C188726"
    assert row[1] == "Primary Endpoint"


@patch("soa_builder.web.routers.endpoints.get_latest_ddf_ct_href", return_value=None)
@patch("soa_builder.web.routers.objectives.get_latest_ddf_ct_href", return_value=None)
def test_update_endpoint(_obj_slug, _ep_slug):
    soa_id = _new_soa("Endpoint Update Test")
    obj = client.post(
        f"/soa/{soa_id}/objectives",
        json={"name": "Parent", "level": "Primary Objective"},
    ).json()
    ep = client.post(
        f"/soa/{soa_id}/endpoints",
        json={
            "name": "Orig",
            "level": "Primary Endpoint",
            "objective_uid": obj["objective_uid"],
        },
    ).json()

    resp = client.patch(
        f"/soa/{soa_id}/endpoints/{ep['id']}",
        json={"name": "Updated", "purpose": "Refined purpose"},
    )
    assert resp.status_code == 200
    body = resp.json()
    assert body["name"] == "Updated"
    assert body["purpose"] == "Refined purpose"
    # level_code_uid unchanged when level not passed
    assert body["level_code_uid"] == ep["level_code_uid"]


@patch("soa_builder.web.routers.endpoints.get_latest_ddf_ct_href", return_value=None)
@patch("soa_builder.web.routers.objectives.get_latest_ddf_ct_href", return_value=None)
def test_update_endpoint_reparent(_obj_slug, _ep_slug):
    soa_id = _new_soa("Endpoint Reparent Test")
    obj1 = client.post(
        f"/soa/{soa_id}/objectives",
        json={"name": "A", "level": "Primary Objective"},
    ).json()
    obj2 = client.post(
        f"/soa/{soa_id}/objectives",
        json={"name": "B", "level": "Primary Objective"},
    ).json()
    ep = client.post(
        f"/soa/{soa_id}/endpoints",
        json={
            "name": "Moves",
            "level": "Primary Endpoint",
            "objective_uid": obj1["objective_uid"],
        },
    ).json()

    # Reparent to obj2
    resp = client.patch(
        f"/soa/{soa_id}/endpoints/{ep['id']}",
        json={"objective_uid": obj2["objective_uid"]},
    )
    assert resp.status_code == 200
    assert resp.json()["objective_uid"] == obj2["objective_uid"]

    # Reparent to unknown → 400
    resp = client.patch(
        f"/soa/{soa_id}/endpoints/{ep['id']}",
        json={"objective_uid": "Objective_999"},
    )
    assert resp.status_code == 400


@patch("soa_builder.web.routers.endpoints.get_latest_ddf_ct_href", return_value=None)
@patch("soa_builder.web.routers.objectives.get_latest_ddf_ct_href", return_value=None)
def test_delete_endpoint_drops_level_code(_obj_slug, _ep_slug):
    soa_id = _new_soa("Endpoint Delete Test")
    obj = client.post(
        f"/soa/{soa_id}/objectives",
        json={"name": "Parent", "level": "Primary Objective"},
    ).json()
    ep = client.post(
        f"/soa/{soa_id}/endpoints",
        json={
            "name": "ToDelete",
            "level": "Primary Endpoint",
            "objective_uid": obj["objective_uid"],
        },
    ).json()

    level_uid = ep["level_code_uid"]
    resp = client.delete(f"/soa/{soa_id}/endpoints/{ep['id']}")
    assert resp.status_code == 200

    conn = _connect()
    cur = conn.cursor()
    cur.execute(
        "SELECT COUNT(*) FROM code_association WHERE soa_id=? AND code_uid=?",
        (soa_id, level_uid),
    )
    assert cur.fetchone()[0] == 0
    conn.close()


@patch("soa_builder.web.routers.endpoints.get_latest_ddf_ct_href", return_value=None)
@patch("soa_builder.web.routers.objectives.get_latest_ddf_ct_href", return_value=None)
def test_endpoint_uid_never_recycled(_obj_slug, _ep_slug):
    soa_id = _new_soa("Endpoint UID Monotonic Test")
    obj = client.post(
        f"/soa/{soa_id}/objectives",
        json={"name": "Parent", "level": "Primary Objective"},
    ).json()
    r1 = client.post(
        f"/soa/{soa_id}/endpoints",
        json={
            "name": "A",
            "level": "Primary Endpoint",
            "objective_uid": obj["objective_uid"],
        },
    ).json()
    r2 = client.post(
        f"/soa/{soa_id}/endpoints",
        json={
            "name": "B",
            "level": "Primary Endpoint",
            "objective_uid": obj["objective_uid"],
        },
    ).json()
    assert r1["endpoint_uid"] == "Endpoint_1"
    assert r2["endpoint_uid"] == "Endpoint_2"

    client.delete(f"/soa/{soa_id}/endpoints/{r2['id']}")
    r3 = client.post(
        f"/soa/{soa_id}/endpoints",
        json={
            "name": "C",
            "level": "Primary Endpoint",
            "objective_uid": obj["objective_uid"],
        },
    ).json()
    assert r3["endpoint_uid"] == "Endpoint_3"


@patch("soa_builder.web.routers.endpoints.get_latest_ddf_ct_href", return_value=None)
@patch("soa_builder.web.routers.objectives.get_latest_ddf_ct_href", return_value=None)
def test_ui_create_endpoint_redirects(_obj_slug, _ep_slug):
    soa_id = _new_soa("Endpoint UI Create Test")
    obj = client.post(
        f"/soa/{soa_id}/objectives",
        json={"name": "Parent", "level": "Primary Objective"},
    ).json()
    resp = client.post(
        f"/ui/soa/{soa_id}/endpoints/create",
        data={
            "name": "From UI",
            "level": "Primary Endpoint",
            "objective_uid": obj["objective_uid"],
        },
        follow_redirects=False,
    )
    assert resp.status_code in (200, 303)
    rows = client.get(f"/soa/{soa_id}/endpoints").json()
    assert len(rows) == 1
    assert rows[0]["name"] == "From UI"
