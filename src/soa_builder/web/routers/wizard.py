"""Guided study setup wizard (issue #382).

Walks a new user through Study Details -> Titles -> Epochs -> Arms ->
Encounters -> Activities -> Biomedical Concepts -> Review, explaining
each USDM concept on the way, and ends with a basic Schedule of
Activities matrix.

Every step calls the existing create helpers (``add_epoch``,
``create_arm``, ``add_visit`` ...) rather than writing SQL itself, so
UIDs and audit rows match the rest of the workbench. Progress lives
in ``wizard_state`` (see ``services.wizard_state``).

Routes:
    GET  /ui/wizard/new               step 1 before a study exists
    POST /ui/wizard/new               create the study, go to step 2
    GET  /ui/wizard/{soa_id}          resume at the current step
    GET  /ui/wizard/{soa_id}/{step}   show a step
    POST /ui/wizard/{soa_id}/{step}   action=add|save|continue|back|skip
    POST /ui/wizard/{soa_id}/finish   validate and mark complete
"""

import logging

from dataclasses import dataclass
from typing import Callable
from typing import List
from typing import Optional

from fastapi import APIRouter
from fastapi import BackgroundTasks
from fastapi import Form
from fastapi import HTTPException
from fastapi import Request
from fastapi.responses import HTMLResponse
from fastapi.responses import RedirectResponse

from ..db import _connect
from ..schemas import ActivityCreate
from ..schemas import ArmCreate
from ..schemas import EpochCreate
from ..schemas import InstanceCreate
from ..schemas import ScheduleTimelineCreate
from ..schemas import VisitCreate
from ..services import concepts as concepts_service
from ..services.matrix import set_instance_cell
from ..services.setup_progress import get_setup_progress
from ..services.studies import DuplicateStudyIdError
from ..services.studies import MissingStudyIdError
from ..services.studies import create_soa_record
from ..services.studies import update_soa_record
from ..services.wizard_state import STATUS_COMPLETE
from ..services.wizard_state import STATUS_DRAFT
from ..services.wizard_state import get_wizard_state
from ..services.wizard_state import save_wizard_state
from ..templating import templates
from ..utils import get_ddf_ct_rows
from ..utils import load_arm_data_origin_type_map
from ..utils import load_arm_type_map
from ..utils import load_epoch_type_map
from ..utils import soa_exists
from . import activities as activities_router
from . import arms as arms_router
from . import epochs as epochs_router
from . import instances as instances_router
from . import schedule_timelines as timelines_router
from . import study_titles as titles_router
from . import visits as visits_router

logger = logging.getLogger(__name__)
router = APIRouter()

MAIN_TIMELINE_NAME = "Main Timeline"


@dataclass(frozen=True)
class WizardStep:
    """One wizard page.

    ``required`` steps need at least one entity (``progress_key`` in
    get_setup_progress) before Continue or Finish succeed. ``add``
    creates one entity from the form; ``save`` applies a whole-page
    form (study details, schedule grid, concepts); ``load`` returns
    the extra template context.
    """

    key: str
    title: str
    template: str
    help_key: str
    required: bool = False
    progress_key: Optional[str] = None
    load: Optional[Callable[[int], dict]] = None
    add: Optional[Callable[[int, "StepForm"], None]] = None
    save: Optional[Callable[[int, "StepForm", BackgroundTasks], None]] = None


@dataclass
class StepForm:
    """The union of every step's form fields."""

    name: str = ""
    label: str = ""
    description: str = ""
    study_id: str = ""
    text: str = ""
    type_code: str = ""
    data_origin_type: str = ""
    grid: str = ""
    cell: tuple = ()
    concepts_form: str = ""
    concept: tuple = ()


class StepError(ValueError):
    """A user-correctable problem; the step is re-rendered with it."""


# --- small queries ----------------------------------------------------


def _rows(sql: str, params: tuple) -> list:
    conn = _connect()
    try:
        return conn.execute(sql, params).fetchall()
    finally:
        conn.close()


def _study(soa_id: int) -> dict:
    rows = _rows(
        "SELECT name, study_id, study_label, study_description FROM soa WHERE id=?",
        (soa_id,),
    )
    name, study_id, label, description = rows[0]
    return {
        "name": name or "",
        "study_id": study_id or "",
        "study_label": label or "",
        "study_description": description or "",
    }


def _activities(soa_id: int) -> List[dict]:
    return [
        {"id": r[0], "name": r[1], "uid": r[2]}
        for r in _rows(
            "SELECT id, name, activity_uid FROM activity WHERE soa_id=?"
            " ORDER BY order_index, id",
            (soa_id,),
        )
    ]


def _instances(soa_id: int) -> List[dict]:
    """Matrix columns: each instance with its encounter's label."""
    return [
        {"id": r[0], "name": r[1], "encounter_label": r[2] or ""}
        for r in _rows(
            "SELECT i.id, i.name, v.label FROM instances i"
            " LEFT JOIN visit v ON v.encounter_uid=i.encounter_uid"
            " AND v.soa_id=i.soa_id"
            " WHERE i.soa_id=? ORDER BY i.order_index, i.id",
            (soa_id,),
        )
    ]


def _cells(soa_id: int) -> set:
    return {
        (r[0], r[1])
        for r in _rows(
            "SELECT instance_id, activity_id FROM matrix_cells"
            " WHERE soa_id=? AND instance_id IS NOT NULL",
            (soa_id,),
        )
    }


def _concept_codes(soa_id: int) -> dict:
    codes: dict = {}
    for activity_id, code in _rows(
        "SELECT activity_id, concept_code FROM activity_concept WHERE soa_id=?",
        (soa_id,),
    ):
        codes.setdefault(activity_id, set()).add(code)
    return codes


def _pairs(values, valid_left: set, valid_right: set) -> set:
    """Parse "left:right" id pairs, keeping only ids of this study."""
    pairs = set()
    for value in values:
        left, _, right = str(value).partition(":")
        try:
            pair = (int(left), right if valid_right is None else int(right))
        except ValueError:
            continue
        if pair[0] in valid_left and (valid_right is None or pair[1] in valid_right):
            pairs.add(pair)
    return pairs


# --- step loaders -----------------------------------------------------


def _load_study(soa_id: int) -> dict:
    return {"study": _study(soa_id)}


def _load_titles(soa_id: int) -> dict:
    return {
        "titles": titles_router._list_titles(soa_id),
        "title_type_options": titles_router._get_title_type_options(),
    }


def _load_epochs(soa_id: int) -> dict:
    epochs = _rows(
        "SELECT name, epoch_uid FROM epoch WHERE soa_id=? ORDER BY order_index, id",
        (soa_id,),
    )
    return {
        "items": [{"name": n, "uid": u} for n, u in epochs],
        "epoch_type_options": load_epoch_type_map(),
    }


def _load_arms(soa_id: int) -> dict:
    arms = _rows(
        "SELECT name, arm_uid FROM arm WHERE soa_id=? ORDER BY order_index, id",
        (soa_id,),
    )
    return {
        "items": [{"name": n, "uid": u} for n, u in arms],
        "arm_type_options": load_arm_type_map(),
        "data_origin_type_options": load_arm_data_origin_type_map(),
    }


def _load_encounters(soa_id: int) -> dict:
    rows = _rows(
        "SELECT name, encounter_uid FROM visit WHERE soa_id=? ORDER BY order_index, id",
        (soa_id,),
    )
    return {"items": [{"name": n, "uid": u} for n, u in rows]}


def _load_activities(soa_id: int) -> dict:
    return {
        "activities": _activities(soa_id),
        "instances": _instances(soa_id),
        "cells": _cells(soa_id),
    }


def _load_concepts(soa_id: int) -> dict:
    current = _concept_codes(soa_id)
    activities = _activities(soa_id)
    for activity in activities:
        activity["codes"] = current.get(activity["id"], set())
    return {
        "activities": activities,
        "concept_options": concepts_service.fetch_biomedical_concepts(),
    }


def _load_review(soa_id: int) -> dict:
    context = _load_activities(soa_id)
    context["progress"] = get_setup_progress(soa_id)
    return context


# --- step actions -----------------------------------------------------


def _save_study(soa_id: int, form: StepForm, _bg: BackgroundTasks) -> None:
    try:
        update_soa_record(
            soa_id, form.name, form.study_id, form.label, form.description
        )
    except (DuplicateStudyIdError, MissingStudyIdError) as exc:
        raise StepError(str(exc)) from exc


def _ct_version(slug: Optional[str]) -> str:
    """Return the CT package date from a slug like ddfct-2025-09-26."""
    parts = (slug or "").split("-")
    return "-".join(parts[-3:]) if len(parts) >= 4 else ""


def _add_title(soa_id: int, form: StepForm) -> None:
    label = ""
    if form.type_code:
        options = titles_router._get_title_type_options()
        label = next((o["label"] for o in options if o["code"] == form.type_code), "")
    titles_router.create_title(
        soa_id,
        form.text,
        type_concept_id=form.type_code,
        type_preferred_term=label,
        type_version=_ct_version(get_ddf_ct_rows().get("slug")),
    )


def _add_epoch(soa_id: int, form: StepForm) -> None:
    epochs_router.add_epoch(
        soa_id,
        EpochCreate(
            name=form.name,
            epoch_label=form.label or None,
            epoch_description=form.description or None,
            type=form.type_code or None,
        ),
    )


def _add_arm(soa_id: int, form: StepForm) -> None:
    arms_router.create_arm(
        soa_id,
        ArmCreate(
            name=form.name,
            label=form.label or None,
            description=form.description or None,
            type=form.type_code or None,
            data_origin_type=form.data_origin_type or None,
        ),
    )


def _main_timeline_uid(soa_id: int) -> str:
    """Return the main ScheduleTimeline's UID, creating it if needed."""
    rows = _rows(
        "SELECT schedule_timeline_uid FROM schedule_timelines"
        " WHERE soa_id=? AND main_timeline=1 ORDER BY order_index LIMIT 1",
        (soa_id,),
    )
    if rows:
        return rows[0][0]
    created = timelines_router.create_schedule_timeline(
        soa_id,
        ScheduleTimelineCreate(name=MAIN_TIMELINE_NAME, main_timeline=True),
    )
    return created["schedule_timeline_uid"]


def _add_encounter(soa_id: int, form: StepForm) -> None:
    visit = visits_router.add_visit(
        soa_id,
        VisitCreate(
            name=form.name,
            label=form.label or None,
            description=form.description or None,
        ),
    )
    encounter_uid = _rows("SELECT encounter_uid FROM visit WHERE id=?", (visit["id"],))[
        0
    ][0]
    # Each encounter becomes one column of the matrix: a
    # ScheduledActivityInstance on the main timeline. In USDM an
    # Encounter has no epoch and the instance's epochId is optional,
    # so the wizard does not ask for one; it can be set later in the
    # Scheduled Instances section of the edit page.
    instances_router.create_instance(
        soa_id,
        InstanceCreate(
            name=form.name.strip(),
            label=form.label or None,
            encounter_uid=encounter_uid,
            member_of_timeline=_main_timeline_uid(soa_id),
        ),
    )


def _add_activity(soa_id: int, form: StepForm) -> None:
    if not form.name.strip():
        raise StepError("Activity name required")
    activities_router.add_activity(
        soa_id,
        ActivityCreate(
            name=form.name.strip(),
            label=form.label or None,
            description=form.description or None,
        ),
    )


def _save_schedule(soa_id: int, form: StepForm, _bg: BackgroundTasks) -> None:
    if not form.grid:
        return
    instance_ids = {i["id"] for i in _instances(soa_id)}
    activity_ids = {a["id"] for a in _activities(soa_id)}
    wanted = _pairs(form.cell, instance_ids, activity_ids)
    current = {
        pair
        for pair in _cells(soa_id)
        if pair[0] in instance_ids and pair[1] in activity_ids
    }
    for instance_id, activity_id in wanted - current:
        set_instance_cell(soa_id, instance_id, activity_id, True)
    for instance_id, activity_id in current - wanted:
        set_instance_cell(soa_id, instance_id, activity_id, False)


def _save_concepts(soa_id: int, form: StepForm, bg: BackgroundTasks) -> None:
    if not form.concepts_form:
        return
    activity_ids = {a["id"] for a in _activities(soa_id)}
    wanted: dict = {aid: set() for aid in activity_ids}
    for activity_id, code in _pairs(form.concept, activity_ids, None):
        if code.strip():
            wanted[activity_id].add(code.strip())
    current = _concept_codes(soa_id)
    for activity_id, codes in wanted.items():
        if codes != current.get(activity_id, set()):
            activities_router.set_activity_concepts(
                soa_id, activity_id, sorted(codes), bg
            )


WIZARD_STEPS = [
    WizardStep(
        "study",
        "Study details",
        "wizard_study.html",
        "study",
        required=True,
        progress_key="study",
        load=_load_study,
        save=_save_study,
    ),
    WizardStep(
        "titles",
        "Titles",
        "wizard_titles.html",
        "title",
        load=_load_titles,
        add=_add_title,
    ),
    WizardStep(
        "epochs",
        "Epochs",
        "wizard_entities.html",
        "epoch",
        required=True,
        progress_key="epochs",
        load=_load_epochs,
        add=_add_epoch,
    ),
    WizardStep(
        "arms",
        "Arms",
        "wizard_entities.html",
        "arm",
        required=True,
        progress_key="arms",
        load=_load_arms,
        add=_add_arm,
    ),
    WizardStep(
        "encounters",
        "Encounters",
        "wizard_entities.html",
        "encounter",
        required=True,
        progress_key="encounters",
        load=_load_encounters,
        add=_add_encounter,
    ),
    WizardStep(
        "activities",
        "Activities",
        "wizard_activities.html",
        "activity",
        required=True,
        progress_key="activities",
        load=_load_activities,
        add=_add_activity,
        save=_save_schedule,
    ),
    WizardStep(
        "concepts",
        "Biomedical concepts",
        "wizard_concepts.html",
        "biomedical_concept",
        load=_load_concepts,
        save=_save_concepts,
    ),
    WizardStep(
        "review",
        "Review",
        "wizard_review.html",
        "matrix",
        load=_load_review,
    ),
]
STEPS_BY_KEY = {step.key: step for step in WIZARD_STEPS}
FIRST_STEP = WIZARD_STEPS[0].key


def _neighbour(step_key: str, offset: int) -> str:
    keys = [s.key for s in WIZARD_STEPS]
    index = keys.index(step_key) + offset
    return keys[max(0, min(index, len(keys) - 1))]


def _missing_required(soa_id: int) -> List[str]:
    """Titles of required steps that still have no data."""
    done = {p["key"]: p["done"] for p in get_setup_progress(soa_id)}
    return [
        s.title for s in WIZARD_STEPS if s.required and not done.get(s.progress_key)
    ]


def _step_is_satisfied(soa_id: int, step: WizardStep) -> bool:
    if not step.required:
        return True
    done = {p["key"]: p["done"] for p in get_setup_progress(soa_id)}
    return bool(done.get(step.progress_key))


def _state_or_new(soa_id: int, step_key: str) -> dict:
    state = get_wizard_state(soa_id)
    if state is None:
        save_wizard_state(soa_id, step_key, [])
        state = get_wizard_state(soa_id)
    return state


def _get_step(soa_id: Optional[int], step_key: str) -> WizardStep:
    if soa_id is not None and not soa_exists(soa_id):
        raise HTTPException(404, "SOA not found")
    step = STEPS_BY_KEY.get(step_key)
    if step is None:
        raise HTTPException(404, "Unknown wizard step")
    return step


def _render(
    request: Request,
    soa_id: Optional[int],
    step: WizardStep,
    error: Optional[str] = None,
    status_code: int = 200,
    form: Optional[StepForm] = None,
) -> HTMLResponse:
    state = get_wizard_state(soa_id) if soa_id is not None else None
    completed = set(state["completed_steps"]) if state else set()
    context = {
        "soa_id": soa_id,
        "step": step,
        "steps": WIZARD_STEPS,
        "completed": completed,
        "is_first": step.key == FIRST_STEP,
        "is_last": step.key == WIZARD_STEPS[-1].key,
        "error": error,
        "form": form or StepForm(),
    }
    if soa_id is not None:
        study = _study(soa_id)
        context["study_name"] = study["name"]
        context["study_label"] = study["study_label"]
        if step.load:
            context.update(step.load(soa_id))
    return templates.TemplateResponse(
        request, step.template, context, status_code=status_code
    )


def _redirect(url: str) -> RedirectResponse:
    return RedirectResponse(url, status_code=303)


# --- routes -----------------------------------------------------------


@router.get("/ui/wizard/new", response_class=HTMLResponse)
def ui_wizard_new(request: Request):
    """Step 1 before the study exists."""
    return _render(request, None, STEPS_BY_KEY[FIRST_STEP])


@router.post("/ui/wizard/new", response_class=HTMLResponse)
def ui_wizard_create(
    request: Request,
    name: str = Form(""),
    study_id: str = Form(""),
    study_label: str = Form(""),
    study_description: str = Form(""),
):
    """Create the study as a wizard draft and move on to Titles."""
    form = StepForm(
        name=name, study_id=study_id, label=study_label, description=study_description
    )
    step = STEPS_BY_KEY[FIRST_STEP]
    if not name.strip() or not study_id.strip():
        return _render(
            request,
            None,
            step,
            "Study name and study ID are required",
            400,
            form,
        )
    try:
        soa_id = create_soa_record(
            name.strip(), study_id, study_label, study_description
        )
    except DuplicateStudyIdError as exc:
        return _render(request, None, step, str(exc), 400, form)
    next_key = _neighbour(FIRST_STEP, 1)
    save_wizard_state(soa_id, next_key, [FIRST_STEP])
    return _redirect(f"/ui/wizard/{soa_id}/{next_key}")


@router.get("/ui/wizard/{soa_id}", response_class=HTMLResponse)
def ui_wizard_resume(soa_id: int):
    """Resume a draft at its current step."""
    if not soa_exists(soa_id):
        raise HTTPException(404, "SOA not found")
    state = _state_or_new(soa_id, FIRST_STEP)
    step_key = state["current_step"]
    if step_key not in STEPS_BY_KEY:
        step_key = FIRST_STEP
    return _redirect(f"/ui/wizard/{soa_id}/{step_key}")


@router.post("/ui/wizard/{soa_id}/finish", response_class=HTMLResponse)
def ui_wizard_finish(request: Request, soa_id: int):
    """Mark the wizard complete once every required step has data."""
    review = _get_step(soa_id, "review")
    state = _state_or_new(soa_id, review.key)
    missing = _missing_required(soa_id)
    if missing:
        return _render(
            request,
            soa_id,
            review,
            "Before finishing, add: " + ", ".join(missing),
            400,
        )
    save_wizard_state(
        soa_id,
        review.key,
        state["completed_steps"] + [review.key],
        STATUS_COMPLETE,
    )
    return _redirect(f"/ui/soa/{soa_id}/edit")


@router.get("/ui/wizard/{soa_id}/{step_key}", response_class=HTMLResponse)
def ui_wizard_step(request: Request, soa_id: int, step_key: str):
    """Show one step."""
    step = _get_step(soa_id, step_key)
    _state_or_new(soa_id, step.key)
    return _render(request, soa_id, step)


@router.post("/ui/wizard/{soa_id}/{step_key}", response_class=HTMLResponse)
def ui_wizard_step_post(
    request: Request,
    soa_id: int,
    step_key: str,
    background_tasks: BackgroundTasks,
    action: str = Form("continue"),
    name: str = Form(""),
    label: str = Form(""),
    description: str = Form(""),
    study_id: str = Form(""),
    study_label: str = Form(""),
    study_description: str = Form(""),
    text: str = Form(""),
    type_code: str = Form(""),
    data_origin_type: str = Form(""),
    grid: str = Form(""),
    cell: List[str] = Form([]),
    concepts_form: str = Form(""),
    concept: List[str] = Form([]),
):
    """Apply one step's form, then stay, go back, skip or continue."""
    step = _get_step(soa_id, step_key)
    state = _state_or_new(soa_id, step.key)
    form = StepForm(
        name=name,
        label=label or study_label,
        description=description or study_description,
        study_id=study_id,
        text=text,
        type_code=type_code,
        data_origin_type=data_origin_type,
        grid=grid,
        cell=tuple(cell),
        concepts_form=concepts_form,
        concept=tuple(concept),
    )
    completed = list(state["completed_steps"])
    status = state["status"] or STATUS_DRAFT

    if action == "back":
        prev_key = _neighbour(step.key, -1)
        save_wizard_state(soa_id, prev_key, completed, status)
        return _redirect(f"/ui/wizard/{soa_id}/{prev_key}")
    if action == "skip":
        next_key = _neighbour(step.key, 1)
        save_wizard_state(soa_id, next_key, completed, status)
        return _redirect(f"/ui/wizard/{soa_id}/{next_key}")

    try:
        if action == "add":
            if step.add is None:
                raise HTTPException(400, "This step has nothing to add")
            step.add(soa_id, form)
        elif step.save is not None:
            step.save(soa_id, form, background_tasks)
    except StepError as exc:
        return _render(request, soa_id, step, str(exc), 400, form)
    except HTTPException as exc:
        if exc.status_code == 404:
            raise
        return _render(request, soa_id, step, str(exc.detail), 400, form)

    if action in ("add", "save"):
        return _redirect(f"/ui/wizard/{soa_id}/{step.key}")

    # continue
    if not _step_is_satisfied(soa_id, step):
        return _render(
            request,
            soa_id,
            step,
            f"Add at least one {step.title.lower().rstrip('s')} before continuing,"
            " or choose Skip.",
            400,
            form,
        )
    next_key = _neighbour(step.key, 1)
    save_wizard_state(soa_id, next_key, completed + [step.key], status)
    return _redirect(f"/ui/wizard/{soa_id}/{next_key}")
