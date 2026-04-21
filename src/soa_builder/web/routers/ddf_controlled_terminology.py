import logging
import os
from typing import Optional

from fastapi import APIRouter, HTTPException, Request
from fastapi.responses import HTMLResponse, JSONResponse
from fastapi.templating import Jinja2Templates

from ..utils import get_ddf_ct_rows

router = APIRouter()
logger = logging.getLogger("soa_builder.web.routers.ddf_controlled_terminology")
templates = Jinja2Templates(
    directory=os.path.join(os.path.dirname(__file__), "..", "templates")
)

_COLUMNS = [
    "code",
    "codelist_code",
    "codelist_name",
    "submission_value",
    "definition",
    "synonyms",
    "preferred_term",
]
_SEARCHABLE = _COLUMNS


def _apply_filters(
    rows: list[dict],
    search: Optional[str],
    code: Optional[str],
    codelist_name: Optional[str],
    codelist_code: Optional[str],
) -> list[dict]:
    out = rows
    if code:
        out = [r for r in out if r.get("code") == code]
    if codelist_name:
        out = [r for r in out if r.get("codelist_name") == codelist_name]
    if codelist_code:
        out = [r for r in out if r.get("codelist_code") == codelist_code]
    if search and not code:
        needle = search.lower()
        out = [
            r
            for r in out
            if any(needle in str(r.get(col) or "").lower() for col in _SEARCHABLE)
        ]
    return out


@router.get("/ddf-ct/terminology", response_class=JSONResponse)
def get_ddf_ct_terminology(
    search: Optional[str] = None,
    code: Optional[str] = None,
    codelist_name: Optional[str] = None,
    codelist_code: Optional[str] = None,
    limit: int = 50,
    offset: int = 0,
    refresh: bool = False,
):
    """Query the cached DDF Controlled Terminology package."""
    limit = max(1, min(limit, 200))
    offset = max(0, offset)

    payload = get_ddf_ct_rows(force=refresh)
    all_rows = payload.get("rows") or []
    matched = _apply_filters(all_rows, search, code, codelist_name, codelist_code)
    page = matched[offset : offset + limit]

    return {
        "slug": payload.get("slug"),
        "error": payload.get("error"),
        "total_count": len(all_rows),
        "matched_count": len(matched),
        "limit": limit,
        "offset": offset,
        "filters": {
            "search": search,
            "code": code,
            "codelist_name": codelist_name,
            "codelist_code": codelist_code,
        },
        "columns": _COLUMNS,
        "rows": page,
    }


@router.get(
    "/ui/ddf-ct/terminology",
    response_class=HTMLResponse,
    name="ui_ddf_controlled_terminology",
)
def ui_ddf_controlled_terminology(
    request: Request,
    search: Optional[str] = None,
    code: Optional[str] = None,
    codelist_name: Optional[str] = None,
    codelist_code: Optional[str] = None,
    limit: int = 50,
    offset: int = 0,
    refresh: bool = False,
):
    """Detail page for DDF Controlled Terminology sourced from CDISC Library."""
    try:
        data = get_ddf_ct_terminology(
            search=search,
            code=code,
            codelist_name=codelist_name,
            codelist_code=codelist_code,
            limit=limit,
            offset=offset,
            refresh=refresh,
        )
    except Exception as exc:
        logger.exception("Failed to render DDF CT page")
        raise HTTPException(500, f"Failed to render DDF CT: {exc}")

    return templates.TemplateResponse(
        request,
        "ddf_controlled_terminology.html",
        {
            **data,
            "search": search or "",
            "code": code or "",
            "codelist_name": codelist_name or "",
            "codelist_code": codelist_code or "",
        },
    )
