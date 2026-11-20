"""Routes for managing Study Identifiers (StudyIdentifier USDM entity).

A study version may carry several identifiers (USDM
StudyVersion.studyIdentifiers is 1..*): the sponsor's protocol
number plus registry numbers such as NCT or EU CT numbers, each
scoped to the organization that issued it. Entry enforces the USDM
v4 CORE rules:

- DDF00172 (ERROR): exactly one sponsor-scoped identifier, so a
  second one is rejected.
- DDF00173 (ERROR): identifier text is unique within an
  organization's scope.
- DDF00174 (WARNING): an organization is not expected to issue more
  than one identifier for the study. Allowed (EudraCT and CTIS
  numbers both sit under CTIS), but reported.
"""

import logging
import os
from typing import List

from fastapi import APIRouter, Form, HTTPException, Request
from fastapi.responses import HTMLResponse
from fastapi.templating import Jinja2Templates
from pydantic import BaseModel

from ..audit import _record_study_identifier_audit
from ..db import _connect
from ..utils import get_next_study_identifier_uid, soa_exists

router = APIRouter()
ui_router = APIRouter()
logger = logging.getLogger("soa_builder.web.routers.study_identifiers")
templates = Jinja2Templates(
    directory=os.path.join(os.path.dirname(__file__), "..", "templates")
)

# NCIt "Clinical Study Sponsor" organization type
_SPONSOR_CODE = "C70793"
_SPONSOR_CODE_SYSTEM = "http://www.cdisc.org"

_DDF00174_WARNING = (
    "DDF00174: {org} has more than one identifier for this study"
    " (allowed, but not expected)."
)


class StudyIdentifierOrderIn(BaseModel):
    study_identifier_uids: List[str]


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _list_identifiers(soa_id: int) -> list:
    """Return all study identifiers for an SOA as a list of dicts.

    ``shares_org`` is True when the row's organization scopes more
    than one identifier (DDF00174).
    """
    conn = _connect()
    try:
        cur = conn.cursor()
        cur.execute(
            "SELECT si.id, si.study_identifier_uid, si.text,"
            " si.scope_org_uid, o.name, c.decode"
            " FROM study_identifier si"
            " LEFT JOIN organization o"
            "   ON o.organization_uid = si.scope_org_uid"
            "   AND o.soa_id = si.soa_id"
            " LEFT JOIN code c"
            "   ON c.code_uid = o.type_code_uid AND c.soa_id = o.soa_id"
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
                "org_type": r[5] or "",
            }
            for r in cur.fetchall()
        ]
    finally:
        conn.close()
    org_counts: dict = {}
    for row in rows:
        if row["scope_org_uid"]:
            org_counts[row["scope_org_uid"]] = (
                org_counts.get(row["scope_org_uid"], 0) + 1
            )
    for row in rows:
        row["shares_org"] = org_counts.get(row["scope_org_uid"], 0) > 1
    return rows


def _list_scope_orgs(soa_id: int) -> list:
    """Return every organization that can scope an identifier.

    Sponsor organizations come first, then the rest in their own
    order. Each dict carries the organization type decode and an
    ``is_sponsor`` flag.
    """
    conn = _connect()
    try:
        cur = conn.cursor()
        cur.execute(
            "SELECT o.organization_uid, o.name, c.decode,"
            " (c.code=? AND c.code_system=?) AS is_sponsor"
            " FROM organization o"
            " LEFT JOIN code c"
            "   ON c.code_uid = o.type_code_uid AND c.soa_id = o.soa_id"
            " WHERE o.soa_id=?"
            " ORDER BY is_sponsor DESC, o.order_index, o.id",
            (_SPONSOR_CODE, _SPONSOR_CODE_SYSTEM, soa_id),
        )
        return [
            {
                "organization_uid": r[0],
                "name": r[1],
                "type": r[2] or "",
                "is_sponsor": bool(r[3]),
            }
            for r in cur.fetchall()
        ]
    finally:
        conn.close()


def _create_identifier(soa_id: int, text: str, scope_org_uid: str) -> dict:
    """Validate and insert a study identifier; return the new row.

    Raises HTTPException 400 for missing or unknown input and 409 for
    a DDF00172/DDF00173 conflict. The returned dict has a
    ``warnings`` list (DDF00174).
    """
    text = (text or "").strip()
    scope_org_uid = (scope_org_uid or "").strip()
    if not text:
        raise HTTPException(400, "text is required")
    if not scope_org_uid:
        raise HTTPException(400, "An organization (scope_org_uid) is required")

    orgs = {o["organization_uid"]: o for o in _list_scope_orgs(soa_id)}
    org = orgs.get(scope_org_uid)
    if org is None:
        raise HTTPException(400, f"Unknown organization {scope_org_uid!r}")

    conn = _connect()
    try:
        cur = conn.cursor()
        cur.execute(
            "SELECT scope_org_uid, text FROM study_identifier WHERE soa_id=?",
            (soa_id,),
        )
        existing = [(r[0], r[1]) for r in cur.fetchall()]
        if org["is_sponsor"] and any(
            orgs.get(uid, {}).get("is_sponsor") for uid, _ in existing
        ):
            raise HTTPException(
                409,
                "DDF00172: the study already has a sponsor identifier;"
                " only one is allowed",
            )
        if (scope_org_uid, text) in existing:
            raise HTTPException(
                409,
                f"DDF00173: {org['name']} already has identifier {text!r}",
            )
        warnings = (
            [_DDF00174_WARNING.format(org=org["name"])]
            if any(uid == scope_org_uid for uid, _ in existing)
            else []
        )

        study_identifier_uid = get_next_study_identifier_uid(cur, soa_id)
        cur.execute(
            "SELECT COALESCE(MAX(order_index), 0) + 1"
            " FROM study_identifier WHERE soa_id=?",
            (soa_id,),
        )
        order_index = cur.fetchone()[0]
        cur.execute(
            "INSERT INTO study_identifier"
            " (soa_id, study_identifier_uid, text, scope_org_uid, order_index)"
            " VALUES (?,?,?,?,?)",
            (soa_id, study_identifier_uid, text, scope_org_uid, order_index),
        )
        si_id = cur.lastrowid
        conn.commit()
    finally:
        conn.close()

    after = {
        "study_identifier_uid": study_identifier_uid,
        "text": text,
        "scope_org_uid": scope_org_uid,
        "order_index": order_index,
    }
    _record_study_identifier_audit(soa_id, "create", si_id, before=None, after=after)
    return {"id": si_id, **after, "warnings": warnings}


def _delete_identifier(soa_id: int, si_id: int) -> bool:
    """Delete a study identifier; return False if it does not exist."""
    conn = _connect()
    try:
        cur = conn.cursor()
        cur.execute(
            "SELECT study_identifier_uid, text, scope_org_uid, order_index"
            " FROM study_identifier WHERE id=? AND soa_id=?",
            (si_id, soa_id),
        )
        row = cur.fetchone()
        if not row:
            return False
        cur.execute(
            "DELETE FROM study_identifier WHERE id=? AND soa_id=?",
            (si_id, soa_id),
        )
        conn.commit()
    finally:
        conn.close()
    before = {
        "study_identifier_uid": row[0],
        "text": row[1],
        "scope_org_uid": row[2] or "",
        "order_index": row[3],
    }
    _record_study_identifier_audit(soa_id, "delete", si_id, before=before, after=None)
    return True


def reorder_identifiers(soa_id: int, uids: List[str]) -> list:
    """Set order_index to follow ``uids``; return the reordered list."""
    current = [r["study_identifier_uid"] for r in _list_identifiers(soa_id)]
    if sorted(uids) != sorted(current):
        raise HTTPException(400, "Give every study identifier UID exactly once")
    conn = _connect()
    try:
        conn.executemany(
            "UPDATE study_identifier SET order_index=?"
            " WHERE soa_id=? AND study_identifier_uid=?",
            [(n, soa_id, uid) for n, uid in enumerate(uids, start=1)],
        )
        conn.commit()
    finally:
        conn.close()
    _record_study_identifier_audit(
        soa_id,
        "reorder",
        None,
        before={"order": current},
        after={"order": list(uids)},
    )
    return _list_identifiers(soa_id)


def _move(soa_id: int, si_id: int, step: int) -> None:
    """Swap an identifier with its neighbour; no-op past either end."""
    rows = _list_identifiers(soa_id)
    ids = [r["id"] for r in rows]
    if si_id not in ids:
        return
    i = ids.index(si_id)
    j = i + step
    if 0 <= j < len(rows):
        uids = [r["study_identifier_uid"] for r in rows]
        uids[i], uids[j] = uids[j], uids[i]
        reorder_identifiers(soa_id, uids)


def _render_partial(
    request: Request,
    soa_id: int,
    error: str = "",
    warnings: list = None,
    form: dict = None,
) -> HTMLResponse:
    """Return the study-identifiers partial with refreshed data."""
    return templates.TemplateResponse(
        request,
        "study_identifiers_partial.html",
        {
            "soa_id": soa_id,
            "study_identifiers": _list_identifiers(soa_id),
            "orgs": _list_scope_orgs(soa_id),
            "si_error": error,
            "si_warnings": warnings or [],
            "si_form": form or {},
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

    ``scope_org_uid`` is required. 409 on a second sponsor identifier
    (DDF00172) or duplicate text within an organization (DDF00173).
    ``warnings`` lists DDF00174 when the organization already scopes
    another identifier.
    """
    if not soa_exists(soa_id):
        raise HTTPException(404, "SOA not found")
    return _create_identifier(soa_id, text, scope_org_uid)


@router.put("/soa/{soa_id}/study-identifiers/order")
def reorder_study_identifiers(soa_id: int, payload: StudyIdentifierOrderIn):
    """Reorder study identifiers; the body lists every UID once."""
    if not soa_exists(soa_id):
        raise HTTPException(404, "SOA not found")
    return reorder_identifiers(soa_id, payload.study_identifier_uids)


@router.delete("/soa/{soa_id}/study-identifiers/{si_id}", status_code=200)
def delete_study_identifier(soa_id: int, si_id: int):
    """Delete a study identifier."""
    if not soa_exists(soa_id):
        raise HTTPException(404, "SOA not found")
    if not _delete_identifier(soa_id, si_id):
        raise HTTPException(404, "Study identifier not found")
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
    """Add a study identifier via HTMX form.

    Validation errors are shown in the form, which keeps the user's
    input so it can be corrected.
    """
    if not soa_exists(soa_id):
        raise HTTPException(404, "SOA not found")
    try:
        created = _create_identifier(soa_id, text, scope_org_uid)
    except HTTPException as exc:
        return _render_partial(
            request,
            soa_id,
            error=exc.detail,
            form={"text": text, "scope_org_uid": scope_org_uid},
        )
    return _render_partial(request, soa_id, warnings=created["warnings"])


@ui_router.post(
    "/ui/soa/{soa_id}/study-identifiers/{si_id}/move_up",
    response_class=HTMLResponse,
)
def ui_move_study_identifier_up(request: Request, soa_id: int, si_id: int):
    """Move a study identifier one place up via HTMX."""
    if not soa_exists(soa_id):
        raise HTTPException(404, "SOA not found")
    _move(soa_id, si_id, -1)
    return _render_partial(request, soa_id)


@ui_router.post(
    "/ui/soa/{soa_id}/study-identifiers/{si_id}/move_down",
    response_class=HTMLResponse,
)
def ui_move_study_identifier_down(request: Request, soa_id: int, si_id: int):
    """Move a study identifier one place down via HTMX."""
    if not soa_exists(soa_id):
        raise HTTPException(404, "SOA not found")
    _move(soa_id, si_id, 1)
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
    _delete_identifier(soa_id, si_id)
    return _render_partial(request, soa_id)
