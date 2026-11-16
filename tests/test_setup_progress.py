"""Getting-started checklist on the edit page (issue #382).

``get_setup_progress`` derives each setup step's state from data the
study already holds, so the checklist needs no stored state.
"""

import pytest

from fastapi.testclient import TestClient

from soa_builder.web.app import app
from soa_builder.web.db import _connect
from soa_builder.web.services.setup_progress import get_setup_progress

client = TestClient(app)

STEP_KEYS = [
    "study",
    "titles",
    "epochs",
    "arms",
    "encounters",
    "activities",
    "instances",
    "matrix",
    "concepts",
]


def _exec(sql, params=()):
    conn = _connect()
    try:
        cur = conn.cursor()
        cur.execute(sql, params)
        conn.commit()
        return cur.lastrowid
    finally:
        conn.close()


def _by_key(soa_id):
    return {item["key"]: item for item in get_setup_progress(soa_id)}


@pytest.fixture
def soa_id():
    resp = client.post("/soa", json={"name": "Checklist Study"})
    return resp.json()["id"]


def test_new_study_has_every_step_and_nothing_done(soa_id):
    items = get_setup_progress(soa_id)
    assert [i["key"] for i in items] == STEP_KEYS
    assert not any(i["done"] for i in items)
    for item in items:
        assert item["label"]
        assert item["href"].startswith(f"/ui/soa/{soa_id}")


def test_study_details_done_when_study_id_set(soa_id):
    client.post(
        f"/ui/soa/{soa_id}/update_meta",
        data={"name": "Checklist Study", "study_id": f"CHK-{soa_id}"},
    )
    assert _by_key(soa_id)["study"]["done"]


def test_counts_follow_created_entities(soa_id):
    client.post(f"/soa/{soa_id}/titles", params={"text": "Official"})
    client.post(f"/soa/{soa_id}/epochs", json={"name": "Screening"})
    client.post(f"/soa/{soa_id}/epochs", json={"name": "Treatment"})
    client.post(f"/soa/{soa_id}/arms", json={"name": "Drug A"})
    client.post(f"/soa/{soa_id}/visits", json={"name": "Visit 1"})
    client.post(f"/soa/{soa_id}/activities", json={"name": "Vitals"})

    progress = _by_key(soa_id)
    assert progress["titles"]["count"] == 1
    assert progress["epochs"]["count"] == 2
    assert progress["arms"]["count"] == 1
    assert progress["encounters"]["count"] == 1
    assert progress["activities"]["count"] == 1
    for key in ("titles", "epochs", "arms", "encounters", "activities"):
        assert progress[key]["done"], key
    for key in ("instances", "matrix", "concepts"):
        assert not progress[key]["done"], key


def test_matrix_and_concept_steps(soa_id):
    activity_id = client.post(
        f"/soa/{soa_id}/activities", json={"name": "Vitals"}
    ).json()["activity_id"]
    instance_id = client.post(
        f"/soa/{soa_id}/instances", json={"name": "Day 1"}
    ).json()["id"]
    _exec(
        "INSERT INTO matrix_cells (soa_id, instance_id, activity_id, status)"
        " VALUES (?, ?, ?, 'X')",
        (soa_id, instance_id, activity_id),
    )
    _exec(
        "INSERT INTO activity_concept (soa_id, activity_id, concept_code,"
        " concept_title) VALUES (?, ?, 'C49676', 'Pulse Rate')",
        (soa_id, activity_id),
    )

    progress = _by_key(soa_id)
    assert progress["instances"]["count"] == 1
    assert progress["matrix"]["done"]
    assert progress["concepts"]["count"] == 1
    assert progress["concepts"]["done"]


def test_progress_is_scoped_to_one_study(soa_id):
    other = client.post("/soa", json={"name": "Other"}).json()["id"]
    client.post(f"/soa/{other}/epochs", json={"name": "Elsewhere"})
    assert _by_key(soa_id)["epochs"]["count"] == 0


def test_edit_page_renders_checklist(soa_id):
    resp = client.get(f"/ui/soa/{soa_id}/edit")
    assert resp.status_code == 200
    assert 'id="setup-checklist"' in resp.text
    assert f"0 of {len(STEP_KEYS)} complete" in resp.text
    # Open while anything is incomplete
    assert 'id="setup-checklist" class="collapsible am-checklist" open' in (resp.text)


def test_edit_page_checklist_flips_when_epoch_added(soa_id):
    client.post(f"/soa/{soa_id}/epochs", json={"name": "Screening"})
    resp = client.get(f"/ui/soa/{soa_id}/edit")
    assert f"1 of {len(STEP_KEYS)} complete" in resp.text
    assert 'data-step="epochs" data-done="1"' in resp.text
    assert 'data-step="arms" data-done="0"' in resp.text
