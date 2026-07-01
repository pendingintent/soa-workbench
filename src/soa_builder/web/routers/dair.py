"""DAIR router: UI page and DOCX download for Digital Amendment
Impact Reports."""

import io
import logging
import os
import re
from datetime import datetime, timezone

from fastapi import APIRouter, HTTPException, Request
from fastapi.responses import HTMLResponse, StreamingResponse
from fastapi.templating import Jinja2Templates

from ..db import _connect
from ..routers._freeze_helpers import _list_freezes
from ..utils import soa_exists

router = APIRouter()
logger = logging.getLogger("soa_builder.web.routers.dair")
templates = Jinja2Templates(
    directory=os.path.join(os.path.dirname(__file__), "..", "templates")
)

_DOCX_MEDIA_TYPE = (
    "application/vnd.openxmlformats-officedocument.wordprocessingml.document"
)


def _safe_filename_component(value: str) -> str:
    """Strip characters unsafe for filenames/HTTP headers from ``value``."""
    return re.sub(r"[^A-Za-z0-9._-]", "_", value)


@router.get(
    "/ui/soa/{soa_id}/dair",
    response_class=HTMLResponse,
    name="ui_dair",
)
def ui_dair(request: Request, soa_id: int):
    if not soa_exists(soa_id):
        raise HTTPException(404, "SOA not found")
    conn = _connect()
    cur = conn.cursor()
    cur.execute(
        "SELECT name, study_id, study_label FROM soa WHERE id=?",
        (soa_id,),
    )
    row = cur.fetchone()
    conn.close()
    freezes = _list_freezes(soa_id)
    return templates.TemplateResponse(
        request,
        "dair.html",
        {
            "soa_id": soa_id,
            "study_name": row[0] if row else f"SoA {soa_id}",
            "study_id_value": row[1] if row else None,
            "study_label": row[2] if row else None,
            "freezes": freezes,
        },
    )


@router.get("/soa/{soa_id}/dair/download")
def download_dair(
    soa_id: int,
    base_freeze_id: int,
    revised_freeze_id: int,
):
    if not soa_exists(soa_id):
        raise HTTPException(404, "SOA not found")
    if base_freeze_id == revised_freeze_id:
        raise HTTPException(400, "Base and revised freeze IDs must be different")

    from usdm.generate_dair import build_dair

    try:
        docx_bytes = build_dair(soa_id, base_freeze_id, revised_freeze_id)
    except ValueError as exc:
        raise HTTPException(404, str(exc)) from exc

    conn = _connect()
    cur = conn.cursor()
    cur.execute("SELECT study_id FROM soa WHERE id=?", (soa_id,))
    row = cur.fetchone()
    conn.close()
    study_id = (row[0] if row else None) or f"SoA{soa_id}"
    date_str = datetime.now(timezone.utc).strftime("%Y%m%d")
    filename = f"{_safe_filename_component(study_id)}-DAIR-{date_str}.docx"
    return StreamingResponse(
        io.BytesIO(docx_bytes),
        media_type=_DOCX_MEDIA_TYPE,
        headers={"Content-Disposition": f'attachment; filename="{filename}"'},
    )
