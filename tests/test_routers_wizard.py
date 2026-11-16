"""Guided study setup wizard (issue #382).

Study Details -> Titles -> Epochs -> Arms -> Encounters -> Activities
-> Biomedical Concepts -> Review. Each step reuses the existing create
helpers, so UIDs and audit rows match the rest of the workbench.
"""

import json
import uuid

import pytest

from fastapi.testclient import TestClient

from soa_builder.web.app import app
from soa_builder.web.db import _connect
from soa_builder.web.migrations.m0115_wizard_state import m0115_wizard_state
from soa_builder.web.services import concepts as concepts_service

client = TestClient(app)

CONCEPTS = [
    {"code": "C49676", "title": "Pulse Rate"},
    {"code": "C25298", "title": "Systolic Blood Pressure"},
]


def _query(sql, params=()):
    conn = _connect()
    try:
        return conn.execute(sql, params).fetchall()
    finally:
        conn.close()


def _state(soa_id):
    rows = _query(
        "SELECT current_step, completed_steps, status FROM wizard_state WHERE soa_id=?",
        (soa_id,),
    )
    if not rows:
        return None
    step, completed, status = rows[0]
    return {"step": step, "completed": json.loads(completed), "status": status}


def _post(soa_id, step, **data):
    return client.post(f"/ui/wizard/{soa_id}/{step}", data=data, follow_redirects=False)


def _new_study(**overrides):
    data = {
        "name": "Wizard Study",
        "study_id": f"WIZ-{uuid.uuid4().hex[:8]}",
        "study_label": "WIZ",
        "study_description": "Built by the wizard",
    }
    data.update(overrides)
    resp = client.post("/ui/wizard/new", data=data, follow_redirects=False)
    assert resp.status_code == 303, resp.text
    soa_id = int(resp.headers["location"].split("/")[3])
    return soa_id, resp


@pytest.fixture
def soa_id():
    return _new_study()[0]


@pytest.fixture
def no_concepts(monkeypatch):
    monkeypatch.setattr(concepts_service, "fetch_biomedical_concepts", lambda: [])


@pytest.fixture
def fake_concepts(monkeypatch):
    monkeypatch.setattr(concepts_service, "fetch_biomedical_concepts", lambda: CONCEPTS)


# --- Step 1: study details -------------------------------------------


def test_new_wizard_page_renders_study_step():
    resp = client.get("/ui/wizard/new")
    assert resp.status_code == 200
    assert 'data-help-key="study"' in resp.text
    assert 'action="/ui/wizard/new"' in resp.text


def test_creating_study_starts_a_draft_and_moves_to_titles():
    soa_id, resp = _new_study()
    assert resp.headers["location"] == f"/ui/wizard/{soa_id}/titles"
    assert _state(soa_id) == {
        "step": "titles",
        "completed": ["study"],
        "status": "draft",
    }
    row = _query("SELECT name, study_label FROM soa WHERE id=?", (soa_id,))
    assert row == [("Wizard Study", "WIZ")]


def test_duplicate_study_id_rerenders_with_error():
    _, _ = _new_study(study_id="WIZ-DUPLICATE-1")
    resp = client.post(
        "/ui/wizard/new",
        data={"name": "Again", "study_id": "WIZ-DUPLICATE-1"},
        follow_redirects=False,
    )
    assert resp.status_code == 400
    assert "study_id already exists" in resp.text


def test_study_id_is_required():
    resp = client.post(
        "/ui/wizard/new",
        data={"name": "No id", "study_id": " "},
        follow_redirects=False,
    )
    assert resp.status_code == 400
    assert "required" in resp.text


def test_study_step_updates_existing_study(soa_id):
    resp = _post(
        soa_id,
        "study",
        action="continue",
        name="Renamed",
        study_id="",
        study_label="NEW",
        study_description="",
    )
    assert resp.status_code == 303
    assert _query("SELECT name, study_label FROM soa WHERE id=?", (soa_id,)) == [
        ("Renamed", "NEW")
    ]


# --- Entity steps reuse the existing helpers --------------------------


def test_titles_step_creates_title(soa_id):
    resp = _post(soa_id, "titles", action="add", text="A Phase 2 Study")
    assert resp.status_code == 303
    assert resp.headers["location"] == f"/ui/wizard/{soa_id}/titles"
    rows = _query(
        "SELECT study_title_uid, text FROM study_title WHERE soa_id=?", (soa_id,)
    )
    assert rows == [("StudyTitle_1", "A Phase 2 Study")]
    page = client.get(f"/ui/wizard/{soa_id}/titles")
    assert "A Phase 2 Study" in page.text


def test_epochs_step_creates_epoch_with_uid_and_audit(soa_id):
    _post(soa_id, "epochs", action="add", name="Screening")
    rows = _query("SELECT id, epoch_uid FROM epoch WHERE soa_id=?", (soa_id,))
    assert [r[1] for r in rows] == ["StudyEpoch_1"]
    audit = _query(
        "SELECT action FROM epoch_audit WHERE soa_id=? AND epoch_id=?",
        (soa_id, rows[0][0]),
    )
    assert ("create",) in audit


def test_add_without_name_rerenders_with_error(soa_id):
    resp = _post(soa_id, "epochs", action="add", name=" ")
    assert resp.status_code == 400
    assert "Epoch name required" in resp.text


def test_arms_step_creates_arm(soa_id):
    _post(soa_id, "arms", action="add", name="Drug A")
    rows = _query("SELECT arm_uid FROM arm WHERE soa_id=?", (soa_id,))
    assert rows == [("StudyArm_1",)]


def test_encounters_step_creates_timeline_and_instances(soa_id):
    # Epochs exist, but an encounter is created without choosing one:
    # in USDM an Encounter has no epoch, and the instance's epochId is
    # optional (set later on the edit page if wanted).
    _post(soa_id, "epochs", action="add", name="Screening")
    _post(soa_id, "epochs", action="add", name="Treatment")
    resp = _post(soa_id, "encounters", action="add", name="Visit 1")
    assert resp.status_code == 303
    _post(soa_id, "encounters", action="add", name="Visit 2")

    timelines = _query(
        "SELECT schedule_timeline_uid, main_timeline FROM schedule_timelines"
        " WHERE soa_id=?",
        (soa_id,),
    )
    assert len(timelines) == 1
    timeline_uid, main = timelines[0]
    assert main == 1

    visits = _query(
        "SELECT encounter_uid FROM visit WHERE soa_id=? ORDER BY id", (soa_id,)
    )
    assert [v[0] for v in visits] == ["Encounter_1", "Encounter_2"]

    instances = _query(
        "SELECT encounter_uid, epoch_uid, member_of_timeline FROM instances"
        " WHERE soa_id=? ORDER BY order_index",
        (soa_id,),
    )
    assert instances == [
        ("Encounter_1", None, timeline_uid),
        ("Encounter_2", None, timeline_uid),
    ]


def test_encounters_step_has_no_epoch_select(soa_id):
    _post(soa_id, "epochs", action="add", name="Screening")
    page = client.get(f"/ui/wizard/{soa_id}/encounters")
    assert page.status_code == 200
    assert 'name="epoch_uid"' not in page.text
    assert 'id="wz-epoch"' not in page.text


def test_encounters_step_ignores_a_posted_epoch(soa_id):
    _post(soa_id, "epochs", action="add", name="Screening")
    _post(soa_id, "encounters", action="add", name="V1", epoch_uid="StudyEpoch_1")
    rows = _query("SELECT epoch_uid FROM instances WHERE soa_id=?", (soa_id,))
    assert rows == [(None,)]


def _build_schedule(soa_id):
    _post(soa_id, "epochs", action="add", name="Treatment")
    _post(soa_id, "encounters", action="add", name="Day 1")
    _post(soa_id, "encounters", action="add", name="Day 8")
    _post(soa_id, "activities", action="add", name="Vital Signs")
    _post(soa_id, "activities", action="add", name="ECG")
    instances = [
        r[0]
        for r in _query(
            "SELECT id FROM instances WHERE soa_id=? ORDER BY order_index",
            (soa_id,),
        )
    ]
    activities = [
        r[0]
        for r in _query(
            "SELECT id FROM activity WHERE soa_id=? ORDER BY order_index",
            (soa_id,),
        )
    ]
    return instances, activities


def _cells(soa_id):
    return set(
        _query(
            "SELECT instance_id, activity_id FROM matrix_cells WHERE soa_id=?",
            (soa_id,),
        )
    )


def test_activities_grid_writes_matrix_cells(soa_id):
    (day1, day8), (vitals, ecg) = _build_schedule(soa_id)
    resp = client.post(
        f"/ui/wizard/{soa_id}/activities",
        data={
            "action": "save",
            "grid": "1",
            "cell": [f"{day1}:{vitals}", f"{day8}:{vitals}", f"{day1}:{ecg}"],
        },
        follow_redirects=False,
    )
    assert resp.status_code == 303
    assert _cells(soa_id) == {(day1, vitals), (day8, vitals), (day1, ecg)}

    # Unticking removes the cell; the grid is the full desired state.
    client.post(
        f"/ui/wizard/{soa_id}/activities",
        data={"action": "save", "grid": "1", "cell": [f"{day1}:{ecg}"]},
        follow_redirects=False,
    )
    assert _cells(soa_id) == {(day1, ecg)}


def test_activities_grid_ignores_ids_from_other_studies(soa_id):
    (day1, _), (vitals, _) = _build_schedule(soa_id)
    client.post(
        f"/ui/wizard/{soa_id}/activities",
        data={"action": "save", "grid": "1", "cell": ["999999:999999", "x:y"]},
        follow_redirects=False,
    )
    assert _cells(soa_id) == set()


def test_activities_page_shows_grid(soa_id):
    (day1, _), (vitals, _) = _build_schedule(soa_id)
    page = client.get(f"/ui/wizard/{soa_id}/activities")
    assert f'value="{day1}:{vitals}"' in page.text
    assert "Day 8" in page.text


def _encounter_with_epoch_on_instance(soa_id):
    """An encounter with a label whose instance also has an epoch.

    Instances made by the wizard before the epoch prompt was removed,
    or edited on the edit page, can carry an epoch; the wizard's
    column headings must still show the encounter label, not that
    epoch.
    """
    _post(soa_id, "epochs", action="add", name="EpochShouldNotShow")
    _post(soa_id, "encounters", action="add", name="Day 1", label="D1-LABEL")
    _post(soa_id, "activities", action="add", name="Vital Signs")
    conn = _connect()
    try:
        conn.execute(
            "UPDATE instances SET epoch_uid='StudyEpoch_1' WHERE soa_id=?",
            (soa_id,),
        )
        conn.commit()
    finally:
        conn.close()


@pytest.mark.parametrize("step", ["activities", "review"])
def test_matrix_columns_show_encounter_label_not_epoch(soa_id, step):
    _encounter_with_epoch_on_instance(soa_id)
    page = client.get(f"/ui/wizard/{soa_id}/{step}")
    assert page.status_code == 200
    assert "D1-LABEL" in page.text
    assert "EpochShouldNotShow" not in page.text


def test_concepts_step_assigns_concepts(soa_id, fake_concepts):
    _, (vitals, ecg) = _build_schedule(soa_id)
    page = client.get(f"/ui/wizard/{soa_id}/concepts")
    assert "Pulse Rate" in page.text
    resp = client.post(
        f"/ui/wizard/{soa_id}/concepts",
        data={
            "action": "save",
            "concepts_form": "1",
            "concept": [f"{vitals}:C49676", f"{vitals}:C25298"],
        },
        follow_redirects=False,
    )
    assert resp.status_code == 303
    rows = _query(
        "SELECT activity_id, concept_code FROM activity_concept"
        " WHERE soa_id=? ORDER BY concept_code",
        (soa_id,),
    )
    assert rows == [(vitals, "C25298"), (vitals, "C49676")]


def test_concepts_step_offline_shows_notice(soa_id, no_concepts):
    _post(soa_id, "activities", action="add", name="Vital Signs")
    page = client.get(f"/ui/wizard/{soa_id}/concepts")
    assert page.status_code == 200
    assert "No biomedical concepts are available" in page.text


# --- Navigation and state ---------------------------------------------


def test_continue_requires_an_entity_on_required_steps(soa_id):
    resp = _post(soa_id, "epochs", action="continue")
    assert resp.status_code == 400
    assert "Add at least one" in resp.text
    assert _state(soa_id)["completed"] == ["study"]


def test_continue_marks_step_complete_and_advances(soa_id):
    _post(soa_id, "epochs", action="add", name="Screening")
    resp = _post(soa_id, "epochs", action="continue")
    assert resp.headers["location"] == f"/ui/wizard/{soa_id}/arms"
    state = _state(soa_id)
    assert state["step"] == "arms"
    assert "epochs" in state["completed"]


def test_titles_step_is_optional(soa_id):
    resp = _post(soa_id, "titles", action="continue")
    assert resp.status_code == 303
    assert resp.headers["location"] == f"/ui/wizard/{soa_id}/epochs"


def test_back_and_skip(soa_id):
    resp = _post(soa_id, "arms", action="back")
    assert resp.headers["location"] == f"/ui/wizard/{soa_id}/epochs"
    resp = _post(soa_id, "arms", action="skip")
    assert resp.headers["location"] == f"/ui/wizard/{soa_id}/encounters"
    state = _state(soa_id)
    assert state["step"] == "encounters"
    assert "arms" not in state["completed"]


def test_resume_redirects_to_current_step(soa_id):
    _post(soa_id, "titles", action="skip")
    resp = client.get(f"/ui/wizard/{soa_id}", follow_redirects=False)
    assert resp.status_code == 303
    assert resp.headers["location"] == f"/ui/wizard/{soa_id}/epochs"


def test_resume_without_state_starts_at_study_step():
    plain = client.post("/soa", json={"name": "Pre-wizard"}).json()["id"]
    resp = client.get(f"/ui/wizard/{plain}", follow_redirects=False)
    assert resp.headers["location"] == f"/ui/wizard/{plain}/study"


def test_unknown_step_is_404(soa_id):
    assert client.get(f"/ui/wizard/{soa_id}/nope").status_code == 404
    assert _post(soa_id, "nope", action="continue").status_code == 404


def test_missing_study_is_404():
    assert client.get("/ui/wizard/999999/epochs").status_code == 404


def test_every_step_renders_with_help(soa_id, no_concepts):
    for step, key in [
        ("study", "study"),
        ("titles", "title"),
        ("epochs", "epoch"),
        ("arms", "arm"),
        ("encounters", "encounter"),
        ("activities", "activity"),
        ("concepts", "biomedical_concept"),
        ("review", "matrix"),
    ]:
        resp = client.get(f"/ui/wizard/{soa_id}/{step}")
        assert resp.status_code == 200, step
        assert f'data-help-key="{key}"' in resp.text, step
        assert 'class="am-wizard-progress"' in resp.text, step


# --- Finish -----------------------------------------------------------


def test_finish_refuses_until_required_steps_have_data(soa_id):
    resp = client.post(f"/ui/wizard/{soa_id}/finish", follow_redirects=False)
    assert resp.status_code == 400
    assert "Epochs" in resp.text
    assert _state(soa_id)["status"] == "draft"


def test_full_walkthrough_builds_a_matrix(soa_id, no_concepts):
    _post(soa_id, "titles", action="add", text="Official Title")
    _post(soa_id, "arms", action="add", name="Placebo")
    (day1, day8), (vitals, ecg) = _build_schedule(soa_id)
    client.post(
        f"/ui/wizard/{soa_id}/activities",
        data={"action": "save", "grid": "1", "cell": [f"{day1}:{vitals}"]},
        follow_redirects=False,
    )

    review = client.get(f"/ui/wizard/{soa_id}/review")
    assert 'class="fz-table am-wizard-matrix"' in review.text
    assert "Vital Signs" in review.text

    resp = client.post(f"/ui/wizard/{soa_id}/finish", follow_redirects=False)
    assert resp.status_code == 303
    assert resp.headers["location"] == f"/ui/soa/{soa_id}/edit"
    assert _state(soa_id)["status"] == "complete"

    edit = client.get(f"/ui/soa/{soa_id}/edit")
    assert "Day 1" in edit.text
    assert 'data-step="matrix" data-done="1"' in edit.text


# --- Entry points and persistence -------------------------------------


def test_index_links_to_wizard():
    assert 'href="/ui/wizard/new"' in client.get("/").text


def test_edit_checklist_offers_resume_for_drafts(soa_id):
    edit = client.get(f"/ui/soa/{soa_id}/edit")
    assert f'href="/ui/wizard/{soa_id}"' in edit.text


def test_migration_is_idempotent():
    m0115_wizard_state()
    m0115_wizard_state()


def test_deleting_study_removes_wizard_state(soa_id):
    assert _state(soa_id) is not None
    study_id = _query("SELECT study_id FROM soa WHERE id=?", (soa_id,))[0][0]
    resp = client.post(f"/ui/soa/{soa_id}/delete", data={"confirm_study_id": study_id})
    assert resp.status_code == 200
    assert _state(soa_id) is None
