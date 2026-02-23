import io
import json
import logging
import os

from fastapi import APIRouter, HTTPException, Request
from fastapi.responses import HTMLResponse, StreamingResponse
from fastapi.templating import Jinja2Templates

from ..db import _connect
from ..utils import soa_exists

router = APIRouter()
logger = logging.getLogger("soa_builder.web.routers.usdm_json")
templates = Jinja2Templates(
    directory=os.path.join(os.path.dirname(__file__), "..", "templates")
)

_COMPONENTS = [
    ("full", "Full USDM Document", "usdm_full.json"),
    ("arms", "Arms", "usdm_arms.json"),
    ("activities", "Activities", "usdm_activities.json"),
    ("elements", "Study Elements", "usdm_elements.json"),
    ("encounters", "Encounters", "usdm_encounters.json"),
    ("epochs", "Study Epochs", "usdm_epochs.json"),
    ("schedule_timelines", "Schedule Timelines", "usdm_schedule_timelines.json"),
    ("timings", "Timings", "usdm_timings.json"),
    ("instances", "Scheduled Activity Instances", "usdm_instances.json"),
    ("study_cells", "Study Cells", "usdm_study_cells.json"),
]


def _build(component: str, soa_id: int):
    """Delegate to the appropriate usdm generator."""
    if component == "full":
        from usdm.generate_usdm import build_usdm

        return build_usdm(soa_id)
    if component == "arms":
        from usdm.generate_arms import build_usdm_arms

        return build_usdm_arms(soa_id)
    if component == "activities":
        from usdm.generate_activities import build_usdm_activities

        return build_usdm_activities(soa_id)
    if component == "elements":
        from usdm.generate_elements import build_usdm_elements

        return build_usdm_elements(soa_id)
    if component == "encounters":
        from usdm.generate_encounters import build_usdm_encounters

        return build_usdm_encounters(soa_id)
    if component == "epochs":
        from usdm.generate_study_epochs import build_usdm_epochs

        return build_usdm_epochs(soa_id)
    if component == "schedule_timelines":
        from usdm.generate_schedule_timelines import build_usdm_schedule_timelines

        return build_usdm_schedule_timelines(soa_id)
    if component == "timings":
        from usdm.generate_study_timings import build_usdm_timings

        return build_usdm_timings(soa_id, None)
    if component == "instances":
        from usdm.generate_scheduled_activity_instances import build_usdm_instances

        return build_usdm_instances(soa_id, None)
    if component == "study_cells":
        from usdm.generate_study_cells import build_usdm_study_cells

        return build_usdm_study_cells(soa_id)
    raise ValueError(f"Unknown component: {component}")


@router.get("/ui/soa/{soa_id}/usdm_json", response_class=HTMLResponse)
def ui_usdm_json(request: Request, soa_id: int):
    if not soa_exists(soa_id):
        raise HTTPException(404, "SOA not found")
    conn = _connect()
    cur = conn.cursor()
    cur.execute("SELECT name, study_id, study_label FROM soa WHERE id=?", (soa_id,))
    row = cur.fetchone()
    conn.close()
    return templates.TemplateResponse(
        request,
        "usdm_json.html",
        {
            "soa_id": soa_id,
            "study_name": row[0],
            "study_id_value": row[1],
            "study_label": row[2],
            "components": _COMPONENTS,
        },
    )


@router.get("/soa/{soa_id}/usdm_json/{component}")
def download_usdm_component(soa_id: int, component: str):
    if not soa_exists(soa_id):
        raise HTTPException(404, "SOA not found")
    valid_keys = {c[0] for c in _COMPONENTS}
    if component not in valid_keys:
        raise HTTPException(400, f"Unknown component '{component}'")
    try:
        data = _build(component, soa_id)
    except Exception as exc:
        logger.exception(
            "Failed to build USDM component %s for soa_id=%s", component, soa_id
        )
        raise HTTPException(500, f"Failed to generate {component}: {exc}") from exc
    filename = next(c[2] for c in _COMPONENTS if c[0] == component)
    payload = json.dumps(data, indent=2) + "\n"
    buf = io.BytesIO(payload.encode("utf-8"))
    return StreamingResponse(
        buf,
        media_type="application/json",
        headers={"Content-Disposition": f'attachment; filename="{filename}"'},
    )
