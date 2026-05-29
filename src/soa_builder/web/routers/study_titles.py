"""Routes for managing Study Titles (StudyTitle USDM entity)."""

import json
import logging
import os

from fastapi import APIRouter, Form, HTTPException, Request
from fastapi.responses import HTMLResponse
from fastapi.templating import Jinja2Templates

from ..audit import _record_study_title_audit
from ..db import _connect
from ..utils import (
    get_ddf_ct_rows,
    get_latest_ddf_ct_href,
    get_next_code_uid,
    soa_exists,
)

router = APIRouter()
logger = logging.getLogger("soa_builder.web.routers.study_titles")
templates = Jinja2Templates(
    directory=os.path.join(os.path.dirname(__file__), "..", "templates")
)

_TITLE_TYPE_CODELIST = "C207419"


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _next_study_title_uid(cur, soa_id: int) -> str:
    """Return next StudyTitle_N UID, never reusing deleted UIDs."""
    max_n = 0
    cur.execute(
        "SELECT study_title_uid FROM study_title "
        "WHERE soa_id=? AND study_title_uid LIKE 'StudyTitle_%'",
        (soa_id,),
    )
    for (uid,) in cur.fetchall():
        if isinstance(uid, str) and uid.startswith("StudyTitle_"):
            try:
                n = int(uid.split("_")[-1])
                if n > max_n:
                    max_n = n
            except (ValueError, IndexError):
                pass
    cur.execute(
        "SELECT before_json, after_json FROM study_title_audit WHERE soa_id=?",
        (soa_id,),
    )
    for before_raw, after_raw in cur.fetchall():
        for raw in (before_raw, after_raw):
            if not raw:
                continue
            try:
                uid = json.loads(raw).get("study_title_uid", "")
                if isinstance(uid, str) and uid.startswith("StudyTitle_"):
                    n = int(uid.split("_")[-1])
                    if n > max_n:
                        max_n = n
            except Exception:
                pass
    return f"StudyTitle_{max_n + 1}"


def _get_title_type_options() -> list:
    """Return [{code: conceptId, label: preferredTerm}] for C207419."""
    payload = get_ddf_ct_rows()
    rows = payload.get("rows") or []
    return [
        {
            "code": r["code"],
            "label": (
                r.get("preferred_term") or r.get("submission_value") or r["code"]
            ),
        }
        for r in rows
        if r.get("codelist_code") == _TITLE_TYPE_CODELIST and r.get("code")
    ]


def _list_titles(soa_id: int) -> list:
    """Return all study titles for an SOA as a list of dicts."""
    conn = _connect()
    cur = conn.cursor()
    cur.execute(
        "SELECT st.id, st.study_title_uid, st.text, st.type_code_uid, "
        "c.decode "
        "FROM study_title st "
        "LEFT JOIN code c ON c.code_uid = st.type_code_uid AND c.soa_id = st.soa_id "
        "WHERE st.soa_id=? ORDER BY st.order_index, st.id",
        (soa_id,),
    )
    rows = [
        {
            "id": r[0],
            "study_title_uid": r[1],
            "text": r[2],
            "type_code_uid": r[3],
            "type_decode": r[4] or "",
        }
        for r in cur.fetchall()
    ]
    conn.close()
    return rows


def _render_partial(request: Request, soa_id: int) -> HTMLResponse:
    """Return the study-titles partial with refreshed data."""
    titles = _list_titles(soa_id)
    options = _get_title_type_options()
    return templates.TemplateResponse(
        request,
        "study_titles_partial.html",
        {
            "soa_id": soa_id,
            "study_titles": titles,
            "title_type_options": options,
        },
    )


# ---------------------------------------------------------------------------
# JSON API routes
# ---------------------------------------------------------------------------


@router.get("/soa/{soa_id}/titles")
def list_titles(soa_id: int):
    """List all study titles for an SOA."""
    if not soa_exists(soa_id):
        raise HTTPException(404, "SOA not found")
    return _list_titles(soa_id)


@router.post("/soa/{soa_id}/titles", status_code=201)
def create_title(
    soa_id: int,
    text: str,
    type_concept_id: str = "",
    type_preferred_term: str = "",
    type_version: str = "",
):
    """Create a study title. Returns the new title row."""
    if not soa_exists(soa_id):
        raise HTTPException(404, "SOA not found")
    text = (text or "").strip()
    if not text:
        raise HTTPException(400, "text is required")

    conn = _connect()
    cur = conn.cursor()

    # Insert type code into code table (stores conceptId + preferredTerm)
    type_code_uid = None
    if type_concept_id:
        type_code_uid = get_next_code_uid(cur, soa_id)
        cur.execute(
            "INSERT INTO code "
            "(soa_id, code_uid, code, code_system, code_system_version, decode) "
            "VALUES (?,?,?,?,?,?)",
            (
                soa_id,
                type_code_uid,
                type_concept_id,
                "http://www.cdisc.org",
                type_version or "",
                type_preferred_term or "",
            ),
        )

    study_title_uid = _next_study_title_uid(cur, soa_id)
    cur.execute(
        "SELECT COALESCE(MAX(order_index), 0) + 1 FROM study_title WHERE soa_id=?",
        (soa_id,),
    )
    order_index = cur.fetchone()[0]

    cur.execute(
        "INSERT INTO study_title "
        "(soa_id, study_title_uid, text, type_code_uid, order_index) "
        "VALUES (?,?,?,?,?)",
        (soa_id, study_title_uid, text, type_code_uid, order_index),
    )
    title_id = cur.lastrowid
    conn.commit()
    conn.close()

    after = {
        "study_title_uid": study_title_uid,
        "text": text,
        "type_code_uid": type_code_uid,
        "order_index": order_index,
    }
    _record_study_title_audit(soa_id, "create", title_id, before=None, after=after)

    return {
        "id": title_id,
        "study_title_uid": study_title_uid,
        "text": text,
        "type_code_uid": type_code_uid,
        "order_index": order_index,
    }


@router.delete("/soa/{soa_id}/titles/{title_id}")
def delete_title(soa_id: int, title_id: int):
    """Delete a study title and its type code."""
    if not soa_exists(soa_id):
        raise HTTPException(404, "SOA not found")

    conn = _connect()
    cur = conn.cursor()
    cur.execute(
        "SELECT study_title_uid, text, type_code_uid FROM study_title "
        "WHERE id=? AND soa_id=?",
        (title_id, soa_id),
    )
    row = cur.fetchone()
    if not row:
        conn.close()
        raise HTTPException(404, "Title not found")

    before = {
        "study_title_uid": row[0],
        "text": row[1],
        "type_code_uid": row[2],
    }
    if row[2]:
        cur.execute(
            "DELETE FROM code WHERE soa_id=? AND code_uid=?",
            (soa_id, row[2]),
        )
    cur.execute(
        "DELETE FROM study_title WHERE id=? AND soa_id=?",
        (title_id, soa_id),
    )
    # Reindex remaining titles
    cur.execute(
        "SELECT id FROM study_title WHERE soa_id=? ORDER BY order_index, id",
        (soa_id,),
    )
    for idx, (tid,) in enumerate(cur.fetchall(), start=1):
        cur.execute("UPDATE study_title SET order_index=? WHERE id=?", (idx, tid))
    conn.commit()
    conn.close()

    _record_study_title_audit(soa_id, "delete", title_id, before=before, after=None)
    return {"deleted_title_id": title_id}


# ---------------------------------------------------------------------------
# HTMX UI routes
# ---------------------------------------------------------------------------


@router.get("/ui/soa/{soa_id}/titles-partial", response_class=HTMLResponse)
def ui_titles_partial(request: Request, soa_id: int):
    """Return the study-titles HTMX partial."""
    if not soa_exists(soa_id):
        raise HTTPException(404, "SOA not found")
    return _render_partial(request, soa_id)


@router.post("/ui/soa/{soa_id}/titles-add", response_class=HTMLResponse)
def ui_titles_add(
    request: Request,
    soa_id: int,
    text: str = Form(""),
    type_code: str = Form(""),
    type_decode: str = Form(""),
):
    """HTMX: add a study title and return the refreshed partial."""
    if not soa_exists(soa_id):
        raise HTTPException(404, "SOA not found")
    text = text.strip()
    if not text:
        return _render_partial(request, soa_id)

    slug = get_latest_ddf_ct_href() or ""
    version = ""
    if slug:
        parts = slug.split("-")
        if len(parts) >= 4:
            version = f"{parts[-3]}-{parts[-2]}-{parts[-1]}"

    conn = _connect()
    cur = conn.cursor()

    type_code_uid = None
    if type_code:
        type_code_uid = get_next_code_uid(cur, soa_id)
        cur.execute(
            "INSERT INTO code "
            "(soa_id, code_uid, code, code_system, code_system_version, decode) "
            "VALUES (?,?,?,?,?,?)",
            (
                soa_id,
                type_code_uid,
                type_code,
                "http://www.cdisc.org",
                version,
                type_decode or "",
            ),
        )

    study_title_uid = _next_study_title_uid(cur, soa_id)
    cur.execute(
        "SELECT COALESCE(MAX(order_index), 0) + 1 FROM study_title WHERE soa_id=?",
        (soa_id,),
    )
    order_index = cur.fetchone()[0]

    cur.execute(
        "INSERT INTO study_title "
        "(soa_id, study_title_uid, text, type_code_uid, order_index) "
        "VALUES (?,?,?,?,?)",
        (soa_id, study_title_uid, text, type_code_uid, order_index),
    )
    title_id = cur.lastrowid
    conn.commit()
    conn.close()

    after = {
        "study_title_uid": study_title_uid,
        "text": text,
        "type_code_uid": type_code_uid,
        "order_index": order_index,
    }
    _record_study_title_audit(soa_id, "create", title_id, before=None, after=after)
    return _render_partial(request, soa_id)


@router.post(
    "/ui/soa/{soa_id}/titles/{title_id}/delete",
    response_class=HTMLResponse,
)
def ui_titles_delete(request: Request, soa_id: int, title_id: int):
    """HTMX: delete a study title and return the refreshed partial."""
    if not soa_exists(soa_id):
        raise HTTPException(404, "SOA not found")
    delete_title(soa_id, title_id)
    return _render_partial(request, soa_id)
