"""Routes for managing Study Identifiers (StudyIdentifier USDM entity)."""

import logging
import os

from fastapi import APIRouter, Form, HTTPException, Request
from fastapi.responses import HTMLResponse
from fastapi.templating import Jinja2Templates

from ..audit import _record_study_identifier_audit
from ..db import _connect
from ..utils import get_next_study_identifier_uid, soa_exists

router = APIRouter()
ui_router = APIRouter()
logger = logging.getLogger("soa_builder.web.routers.study_identifiers")
templates = Jinja2Templates(
    directory=os.path.join(os.path.dirname(__file__), "..", "templates")
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _list_identifiers(soa_id: int) -> list:
    """Return all study identifiers for an SOA as a list of dicts."""
    conn = _connect()
    cur = conn.cursor()
    cur.execute(
        "SELECT si.id, si.study_identifier_uid, si.text,"
        " si.scope_org_uid, o.name"
        " FROM study_identifier si"
        " LEFT JOIN organization o"
        "   ON o.organization_uid = si.scope_org_uid"
        "   AND o.soa_id = si.soa_id"
        " WHERE si.soa_id=? ORDER BY si.order_index, si.id",
        (soa_id,),
    )
    rows = [
        {
            "id": r[0],
            "study_identifier_uid": r[1],
            "text": r[2],
            "scope_org_uid": r[3] or "",
            "org_name": r[4] or "",
        }
        for r in cur.fetchall()
    ]
    conn.close()
    return rows


_SPONSOR_CODE = "C70793"
_SPONSOR_CODE_SYSTEM = "http://www.cdisc.org"


def _list_orgs(soa_id: int) -> list:
    """Return only Clinical Study Sponsor orgs (C70793) for dropdown."""
    conn = _connect()
    cur = conn.cursor()
    cur.execute(
        "SELECT o.organization_uid, o.name"
        " FROM organization o"
        " LEFT JOIN code c"
        "   ON c.code_uid = o.type_code_uid AND c.soa_id = o.soa_id"
        " WHERE o.soa_id=? AND c.code=? AND c.code_system=?"
        " ORDER BY o.order_index, o.id",
        (soa_id, _SPONSOR_CODE, _SPONSOR_CODE_SYSTEM),
    )
    rows = [{"organization_uid": r[0], "name": r[1]} for r in cur.fetchall()]
    conn.close()
    return rows


def _render_partial(request: Request, soa_id: int) -> HTMLResponse:
    """Return the study-identifiers partial with refreshed data."""
    identifiers = _list_identifiers(soa_id)
    orgs = _list_orgs(soa_id)
    return templates.TemplateResponse(
        request,
        "study_identifiers_partial.html",
        {
            "soa_id": soa_id,
            "study_identifiers": identifiers,
            "orgs": orgs,
        },
    )


# ---------------------------------------------------------------------------
# JSON API routes
# ---------------------------------------------------------------------------


@router.get("/soa/{soa_id}/study-identifiers")
def list_study_identifiers(soa_id: int):
    """List all study identifiers for an SOA."""
    if not soa_exists(soa_id):
        raise HTTPException(404, "SOA not found")
    return _list_identifiers(soa_id)


@router.post("/soa/{soa_id}/study-identifiers", status_code=201)
def create_study_identifier(
    soa_id: int,
    text: str,
    scope_org_uid: str = "",
):
    """Create a study identifier. Returns the new row.

    Only one study identifier is permitted per SOA.
    """
    if not soa_exists(soa_id):
        raise HTTPException(404, "SOA not found")
    text = (text or "").strip()
    if not text:
        raise HTTPException(400, "text is required")

    conn = _connect()
    cur = conn.cursor()

    cur.execute("SELECT COUNT(*) FROM study_identifier WHERE soa_id=?", (soa_id,))
    if cur.fetchone()[0] > 0:
        conn.close()
        raise HTTPException(409, "Only one study identifier is permitted per SOA")

    study_identifier_uid = get_next_study_identifier_uid(cur, soa_id)
    cur.execute(
        "SELECT COALESCE(MAX(order_index), 0) + 1 FROM study_identifier WHERE soa_id=?",
        (soa_id,),
    )
    order_index = cur.fetchone()[0]

    cur.execute(
        "INSERT INTO study_identifier"
        " (soa_id, study_identifier_uid, text, scope_org_uid, order_index)"
        " VALUES (?,?,?,?,?)",
        (soa_id, study_identifier_uid, text, scope_org_uid or "", order_index),
    )
    si_id = cur.lastrowid
    conn.commit()
    conn.close()

    after = {
        "study_identifier_uid": study_identifier_uid,
        "text": text,
        "scope_org_uid": scope_org_uid or "",
        "order_index": order_index,
    }
    _record_study_identifier_audit(soa_id, "create", si_id, before=None, after=after)
    return {
        "id": si_id,
        "study_identifier_uid": study_identifier_uid,
        "text": text,
        "scope_org_uid": scope_org_uid or "",
        "order_index": order_index,
    }


@router.delete("/soa/{soa_id}/study-identifiers/{si_id}", status_code=200)
def delete_study_identifier(soa_id: int, si_id: int):
    """Delete a study identifier."""
    if not soa_exists(soa_id):
        raise HTTPException(404, "SOA not found")

    conn = _connect()
    cur = conn.cursor()
    cur.execute(
        "SELECT study_identifier_uid, text, scope_org_uid, order_index"
        " FROM study_identifier WHERE id=? AND soa_id=?",
        (si_id, soa_id),
    )
    row = cur.fetchone()
    if not row:
        conn.close()
        raise HTTPException(404, "Study identifier not found")

    before = {
        "study_identifier_uid": row[0],
        "text": row[1],
        "scope_org_uid": row[2] or "",
        "order_index": row[3],
    }
    cur.execute(
        "DELETE FROM study_identifier WHERE id=? AND soa_id=?",
        (si_id, soa_id),
    )
    conn.commit()
    conn.close()

    _record_study_identifier_audit(soa_id, "delete", si_id, before=before, after=None)
    return {"deleted": si_id}


# ---------------------------------------------------------------------------
# UI (HTMX) routes
# ---------------------------------------------------------------------------


@ui_router.post(
    "/ui/soa/{soa_id}/study-identifiers-add",
    response_class=HTMLResponse,
)
def ui_add_study_identifier(
    request: Request,
    soa_id: int,
    text: str = Form(""),
    scope_org_uid: str = Form(""),
):
    """Add a study identifier via HTMX form."""
    if not soa_exists(soa_id):
        raise HTTPException(404, "SOA not found")
    text = (text or "").strip()
    if not text:
        return _render_partial(request, soa_id)

    conn = _connect()
    cur = conn.cursor()

    cur.execute("SELECT COUNT(*) FROM study_identifier WHERE soa_id=?", (soa_id,))
    if cur.fetchone()[0] > 0:
        conn.close()
        return _render_partial(request, soa_id)

    study_identifier_uid = get_next_study_identifier_uid(cur, soa_id)
    cur.execute(
        "SELECT COALESCE(MAX(order_index), 0) + 1 FROM study_identifier WHERE soa_id=?",
        (soa_id,),
    )
    order_index = cur.fetchone()[0]

    cur.execute(
        "INSERT INTO study_identifier"
        " (soa_id, study_identifier_uid, text, scope_org_uid, order_index)"
        " VALUES (?,?,?,?,?)",
        (soa_id, study_identifier_uid, text, scope_org_uid or "", order_index),
    )
    si_id = cur.lastrowid
    conn.commit()
    conn.close()

    after = {
        "study_identifier_uid": study_identifier_uid,
        "text": text,
        "scope_org_uid": scope_org_uid or "",
        "order_index": order_index,
    }
    _record_study_identifier_audit(soa_id, "create", si_id, before=None, after=after)
    return _render_partial(request, soa_id)


@ui_router.post(
    "/ui/soa/{soa_id}/study-identifiers/{si_id}/delete",
    response_class=HTMLResponse,
)
def ui_delete_study_identifier(
    request: Request,
    soa_id: int,
    si_id: int,
):
    """Delete a study identifier via HTMX."""
    if not soa_exists(soa_id):
        raise HTTPException(404, "SOA not found")

    conn = _connect()
    cur = conn.cursor()
    cur.execute(
        "SELECT study_identifier_uid, text, scope_org_uid, order_index"
        " FROM study_identifier WHERE id=? AND soa_id=?",
        (si_id, soa_id),
    )
    row = cur.fetchone()
    if row:
        before = {
            "study_identifier_uid": row[0],
            "text": row[1],
            "scope_org_uid": row[2] or "",
            "order_index": row[3],
        }
        cur.execute(
            "DELETE FROM study_identifier WHERE id=? AND soa_id=?",
            (si_id, soa_id),
        )
        conn.commit()
        _record_study_identifier_audit(
            soa_id, "delete", si_id, before=before, after=None
        )
    conn.close()
    return _render_partial(request, soa_id)
