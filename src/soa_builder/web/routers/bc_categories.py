import logging
import os

from fastapi import APIRouter, BackgroundTasks, Form, HTTPException, Request
from fastapi.responses import HTMLResponse
from fastapi.templating import Jinja2Templates

from ..db import _connect
from ..utils import (
    get_next_concept_uid as _get_next_concept_uid,
    soa_exists,
)

router = APIRouter()
ui_router = APIRouter()
logger = logging.getLogger("soa_builder.web.routers.bc_categories")
templates = Jinja2Templates(
    directory=os.path.join(os.path.dirname(__file__), "..", "templates")
)


def _expand_category_to_activity(
    cur, soa_id: int, activity_id: int, activity_uid: str, category_name: str
) -> list:
    """Bulk-insert all CDISC BC category concepts into activity_concept.

    Skips concepts already present (any source). Returns list of newly
    inserted concept codes for background enrichment.
    """
    from ..app import (
        fetch_biomedical_concepts_by_category,
        _upsert_biomedical_concept,
    )

    concepts = fetch_biomedical_concepts_by_category(category_name)
    added_codes = []
    for c in concepts:
        code = c.get("code", "")
        title = c.get("title", "")
        if not code:
            continue
        cur.execute(
            "SELECT 1 FROM activity_concept "
            "WHERE activity_id=? AND soa_id=? AND concept_code=?",
            (activity_id, soa_id, code),
        )
        if cur.fetchone():
            continue
        concept_uid = _get_next_concept_uid(cur, soa_id)
        cur.execute(
            "INSERT INTO activity_concept "
            "(soa_id, activity_id, activity_uid, concept_uid, "
            "concept_code, concept_title, bc_category_name) "
            "VALUES (?,?,?,?,?,?,?)",
            (
                soa_id,
                activity_id,
                activity_uid,
                concept_uid,
                code,
                title,
                category_name,
            ),
        )
        _upsert_biomedical_concept(cur, soa_id, concept_uid, title, code)
        added_codes.append(code)
    return added_codes


@ui_router.post(
    "/ui/soa/{soa_id}/activity/{activity_id}/bc-categories/add",
    response_class=HTMLResponse,
)
def ui_add_category_to_activity(
    request: Request,
    background_tasks: BackgroundTasks,
    soa_id: int,
    activity_id: int,
    category_name: str = Form(...),
):
    if not soa_exists(soa_id):
        raise HTTPException(404, "SOA not found")
    conn = _connect()
    cur = conn.cursor()
    cur.execute(
        "SELECT activity_uid FROM activity WHERE id=? AND soa_id=?",
        (activity_id, soa_id),
    )
    act_row = cur.fetchone()
    if not act_row:
        conn.close()
        raise HTTPException(404, "Activity not found")
    activity_uid = act_row[0]

    added_codes = _expand_category_to_activity(
        cur, soa_id, activity_id, activity_uid, category_name
    )
    conn.commit()
    conn.close()

    from ..app import (
        _enrich_biomedical_concept_bg,
        _enrich_code_bg,
        _populate_biomedical_concept_properties_bg,
    )

    for code in added_codes:
        background_tasks.add_task(_enrich_biomedical_concept_bg, code, soa_id)
        background_tasks.add_task(_enrich_code_bg, code, soa_id)
        background_tasks.add_task(
            _populate_biomedical_concept_properties_bg,
            code,
            None,
            soa_id,
        )

    from .bc_surrogates import _render_concepts_cell

    return _render_concepts_cell(request, soa_id, activity_id)


@ui_router.post(
    "/ui/soa/{soa_id}/activity/{activity_id}/bc-categories/remove",
    response_class=HTMLResponse,
)
def ui_remove_category_from_activity(
    request: Request,
    soa_id: int,
    activity_id: int,
    category_name: str = Form(...),
):
    if not soa_exists(soa_id):
        raise HTTPException(404, "SOA not found")
    conn = _connect()
    cur = conn.cursor()
    cur.execute(
        "SELECT 1 FROM activity WHERE id=? AND soa_id=?",
        (activity_id, soa_id),
    )
    if not cur.fetchone():
        conn.close()
        raise HTTPException(404, "Activity not found")

    cur.execute(
        "DELETE FROM activity_concept "
        "WHERE activity_id=? AND soa_id=? AND bc_category_name=?",
        (activity_id, soa_id, category_name),
    )
    conn.commit()
    conn.close()

    from .bc_surrogates import _render_concepts_cell

    return _render_concepts_cell(request, soa_id, activity_id)
