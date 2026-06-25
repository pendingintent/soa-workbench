import json
import logging
import os
from typing import Optional
from fastapi import APIRouter, Form, HTTPException, Request
from fastapi.responses import HTMLResponse, JSONResponse
from fastapi.templating import Jinja2Templates

from ..audit import _record_person_audit
from ..db import _connect
from ..utils import soa_exists

router = APIRouter(prefix="/soa/{soa_id}")
ui_router = APIRouter()
logger = logging.getLogger("soa_builder.web.routers.persons")
templates = Jinja2Templates(
    directory=os.path.join(os.path.dirname(__file__), "..", "templates")
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _next_person_uid(cur, soa_id: int) -> str:
    """Return next Person_N UID, never reusing deleted UIDs."""
    max_n = 0
    cur.execute(
        "SELECT person_uid FROM person WHERE soa_id=? AND person_uid LIKE 'Person_%'",
        (soa_id,),
    )
    for (uid,) in cur.fetchall():
        if isinstance(uid, str) and uid.startswith("Person_"):
            try:
                n = int(uid.split("_")[-1])
                if n > max_n:
                    max_n = n
            except (ValueError, IndexError):
                pass
    cur.execute(
        "SELECT before_json, after_json FROM person_audit WHERE soa_id=?",
        (soa_id,),
    )
    for before_raw, after_raw in cur.fetchall():
        for raw in (before_raw, after_raw):
            if not raw:
                continue
            try:
                uid = json.loads(raw).get("person_uid", "")
                if isinstance(uid, str) and uid.startswith("Person_"):
                    n = int(uid.split("_")[-1])
                    if n > max_n:
                        max_n = n
            except Exception:
                pass
    return f"Person_{max_n + 1}"


def _parse_lines(raw: Optional[str]) -> list:
    """Split newline/comma-delimited text into a stripped, non-empty list."""
    if not raw:
        return []
    parts = []
    for line in raw.replace(",", "\n").splitlines():
        v = line.strip()
        if v:
            parts.append(v)
    return parts


def _list_persons(soa_id: int) -> list:
    """Return all PersonName entities for an SOA."""
    conn = _connect()
    cur = conn.cursor()
    cur.execute(
        "SELECT p.id, p.person_uid, p.person_name_uid, p.name,"
        " p.text, p.family_name, p.given_names, p.prefixes,"
        " p.suffixes, p.organization_uid, p.job_title"
        " FROM person p"
        " WHERE p.soa_id=? ORDER BY p.order_index, p.id",
        (soa_id,),
    )
    person_rows = cur.fetchall()

    cur.execute(
        "SELECT organization_uid, name FROM organization WHERE soa_id=?",
        (soa_id,),
    )
    org_name_map = {r[0]: r[1] for r in cur.fetchall()}
    conn.close()

    result = []
    for row in person_rows:
        (
            pid,
            person_uid,
            person_name_uid,
            name,
            text,
            family_name,
            given_names_raw,
            prefixes_raw,
            suffixes_raw,
            org_uid,
            job_title,
        ) = row
        result.append(
            {
                "id": pid,
                "person_uid": person_uid,
                "person_name_uid": person_name_uid,
                "name": name,
                "job_title": job_title,
                "text": text,
                "family_name": family_name,
                "given_names": json.loads(given_names_raw) if given_names_raw else [],
                "prefixes": json.loads(prefixes_raw) if prefixes_raw else [],
                "suffixes": json.loads(suffixes_raw) if suffixes_raw else [],
                "organization_uid": org_uid,
                "org_name": org_name_map.get(org_uid, org_uid) if org_uid else None,
            }
        )
    return result


def _delete_role_assignments(cur, soa_id: int, person_id: int) -> None:
    cur.execute(
        "DELETE FROM role_person WHERE soa_id=? AND person_id=?",
        (soa_id, person_id),
    )


def _assert_no_person_org_role_org_conflict(
    cur, soa_id: int, person_id: int, org_uid: Optional[str]
) -> None:
    """Raise 422 if person org ref conflicts with any assigned role org."""
    if not org_uid:
        return
    cur.execute(
        "SELECT r.role_uid, r.name FROM role r"
        " JOIN role_person rp ON rp.role_id = r.id AND rp.soa_id = r.soa_id"
        " WHERE r.soa_id=? AND rp.person_id=?"
        " AND r.organization_ids IS NOT NULL",
        (soa_id, person_id),
    )
    conflicts = cur.fetchall()
    if conflicts:
        names = ", ".join(r[1] or r[0] for r in conflicts)
        raise HTTPException(
            422,
            f"Person has an organizationId; the following assigned roles "
            f"also have organizationIds and cannot be combined: {names}",
        )


# ---------------------------------------------------------------------------
# JSON API endpoints
# ---------------------------------------------------------------------------


@router.get("/persons", response_class=JSONResponse)
def list_persons(soa_id: int):
    if not soa_exists(soa_id):
        raise HTTPException(404, "SOA not found")
    return _list_persons(soa_id)


@router.post("/persons", status_code=201, response_class=JSONResponse)
def create_person(soa_id: int, body: dict):
    if not soa_exists(soa_id):
        raise HTTPException(404, "SOA not found")
    name = (body.get("name") or "").strip()
    if not name:
        raise HTTPException(400, "name is required")

    org_uid = (body.get("organization_uid") or "").strip() or None

    conn = _connect()
    cur = conn.cursor()
    try:
        cur.execute(
            "SELECT COALESCE(MAX(order_index), 0) FROM person WHERE soa_id=?",
            (soa_id,),
        )
        order_index = (cur.fetchone()[0] or 0) + 1
        person_uid = _next_person_uid(cur, soa_id)
        suffix = person_uid.split("_")[-1]
        person_name_uid = f"PersonName_{suffix}"

        given = body.get("given_names") or []
        prefixes = body.get("prefixes") or []
        suffixes = body.get("suffixes") or []

        cur.execute(
            "INSERT INTO person"
            " (soa_id, person_uid, person_name_uid, name, job_title,"
            " text, family_name, given_names, prefixes, suffixes,"
            " organization_uid, order_index)"
            " VALUES (?,?,?,?,?,?,?,?,?,?,?,?)",
            (
                soa_id,
                person_uid,
                person_name_uid,
                name,
                body.get("job_title") or None,
                body.get("text") or None,
                body.get("family_name") or None,
                json.dumps(given) if given else None,
                json.dumps(prefixes) if prefixes else None,
                json.dumps(suffixes) if suffixes else None,
                org_uid,
                order_index,
            ),
        )
        person_id = cur.lastrowid
        conn.commit()
    except Exception:
        conn.rollback()
        conn.close()
        raise
    conn.close()

    _record_person_audit(
        soa_id,
        "create",
        person_id,
        after={"person_uid": person_uid, "name": name},
    )
    return {"id": person_id, "person_uid": person_uid, "name": name}


@router.delete(
    "/persons/{person_id}",
    status_code=200,
    response_class=JSONResponse,
)
def delete_person(soa_id: int, person_id: int):
    if not soa_exists(soa_id):
        raise HTTPException(404, "SOA not found")
    conn = _connect()
    cur = conn.cursor()
    cur.execute(
        "SELECT id, person_uid, name FROM person WHERE id=? AND soa_id=?",
        (person_id, soa_id),
    )
    row = cur.fetchone()
    if not row:
        conn.close()
        raise HTTPException(404, "Person not found")
    pid, person_uid, name = row
    before = {"person_uid": person_uid, "name": name}

    _delete_role_assignments(cur, soa_id, pid)
    cur.execute(
        "DELETE FROM person WHERE id=? AND soa_id=?",
        (person_id, soa_id),
    )
    cur.execute(
        "SELECT id FROM person WHERE soa_id=? ORDER BY order_index, id",
        (soa_id,),
    )
    for idx, (r,) in enumerate(cur.fetchall(), start=1):
        cur.execute("UPDATE person SET order_index=? WHERE id=?", (idx, r))
    conn.commit()
    conn.close()
    _record_person_audit(soa_id, "delete", pid, before=before)
    return {"deleted": person_uid}


# ---------------------------------------------------------------------------
# HTMX UI endpoints
# ---------------------------------------------------------------------------


def _partial_response(request: Request, soa_id: int) -> HTMLResponse:
    from .organizations import _list_organizations
    from .roles import _get_role_type_options, _list_roles

    persons = _list_persons(soa_id)
    roles = _list_roles(soa_id)
    organizations = _list_organizations(soa_id)
    return templates.TemplateResponse(
        request,
        "persons_oob_response.html",
        {
            "soa_id": soa_id,
            "persons": persons,
            "roles": roles,
            "role_type_options": _get_role_type_options(),
            "organizations": organizations,
        },
    )


@ui_router.get(
    "/ui/soa/{soa_id}/orgs-roles-persons",
    response_class=HTMLResponse,
    name="ui_orgs_roles_persons",
)
def ui_orgs_roles_persons(request: Request, soa_id: int):
    if not soa_exists(soa_id):
        raise HTTPException(404, "SOA not found")
    from .organizations import (
        _get_countries_options,
        _get_org_type_options,
        _list_organizations,
    )
    from .roles import _get_role_type_options, _list_roles

    conn = _connect()
    cur = conn.cursor()
    cur.execute("SELECT name, study_label FROM soa WHERE id=?", (soa_id,))
    row = cur.fetchone()
    conn.close()
    soa_name = row[0] if row else ""
    study_label = row[1] if row else None

    organizations = _list_organizations(soa_id)
    roles = _list_roles(soa_id)
    persons = _list_persons(soa_id)

    return templates.TemplateResponse(
        request,
        "orgs_roles_persons.html",
        {
            "soa_id": soa_id,
            "soa_name": soa_name,
            "study_label": study_label,
            "organizations": organizations,
            "org_type_options": _get_org_type_options(),
            "countries_options": _get_countries_options(),
            "roles": roles,
            "role_type_options": _get_role_type_options(),
            "persons": persons,
        },
    )


@ui_router.post(
    "/ui/soa/{soa_id}/persons-add",
    response_class=HTMLResponse,
)
def ui_persons_add(
    request: Request,
    soa_id: int,
    name: str = Form(""),
    job_title: str = Form(""),
    text: str = Form(""),
    family_name: str = Form(""),
    given_names: str = Form(""),
    prefixes: str = Form(""),
    suffixes: str = Form(""),
    organization_uid: str = Form(""),
):
    if not soa_exists(soa_id):
        raise HTTPException(404, "SOA not found")
    name = name.strip()
    if not name:
        raise HTTPException(400, "name is required")

    org_uid = organization_uid.strip() or None
    given_list = _parse_lines(given_names)
    prefix_list = _parse_lines(prefixes)
    suffix_list = _parse_lines(suffixes)

    conn = _connect()
    cur = conn.cursor()
    try:
        cur.execute(
            "SELECT COALESCE(MAX(order_index), 0) FROM person WHERE soa_id=?",
            (soa_id,),
        )
        order_index = (cur.fetchone()[0] or 0) + 1
        person_uid = _next_person_uid(cur, soa_id)
        suffix_n = person_uid.split("_")[-1]
        person_name_uid = f"PersonName_{suffix_n}"

        cur.execute(
            "INSERT INTO person"
            " (soa_id, person_uid, person_name_uid, name, job_title,"
            " text, family_name, given_names, prefixes, suffixes,"
            " organization_uid, order_index)"
            " VALUES (?,?,?,?,?,?,?,?,?,?,?,?)",
            (
                soa_id,
                person_uid,
                person_name_uid,
                name,
                job_title.strip() or None,
                text.strip() or None,
                family_name.strip() or None,
                json.dumps(given_list) if given_list else None,
                json.dumps(prefix_list) if prefix_list else None,
                json.dumps(suffix_list) if suffix_list else None,
                org_uid,
                order_index,
            ),
        )
        person_id = cur.lastrowid
        conn.commit()
    except Exception:
        conn.rollback()
        conn.close()
        raise
    conn.close()

    _record_person_audit(
        soa_id,
        "create",
        person_id,
        after={"person_uid": person_uid, "name": name},
    )
    return _partial_response(request, soa_id)


@ui_router.post(
    "/ui/soa/{soa_id}/persons/{person_id}/delete",
    response_class=HTMLResponse,
)
def ui_persons_delete(
    request: Request,
    soa_id: int,
    person_id: int,
):
    if not soa_exists(soa_id):
        raise HTTPException(404, "SOA not found")
    conn = _connect()
    cur = conn.cursor()
    cur.execute(
        "SELECT id, person_uid, name FROM person WHERE id=? AND soa_id=?",
        (person_id, soa_id),
    )
    row = cur.fetchone()
    if not row:
        conn.close()
        raise HTTPException(404, "Person not found")
    pid, person_uid, name = row
    before = {"person_uid": person_uid, "name": name}

    _delete_role_assignments(cur, soa_id, pid)
    cur.execute(
        "DELETE FROM person WHERE id=? AND soa_id=?",
        (person_id, soa_id),
    )
    cur.execute(
        "SELECT id FROM person WHERE soa_id=? ORDER BY order_index, id",
        (soa_id,),
    )
    for idx, (r,) in enumerate(cur.fetchall(), start=1):
        cur.execute("UPDATE person SET order_index=? WHERE id=?", (idx, r))
    conn.commit()
    conn.close()
    _record_person_audit(soa_id, "delete", pid, before=before)
    return _partial_response(request, soa_id)


@ui_router.post(
    "/ui/soa/{soa_id}/persons/{person_id}/update",
    response_class=HTMLResponse,
)
def ui_persons_update(
    request: Request,
    soa_id: int,
    person_id: int,
    name: str = Form(""),
    job_title: str = Form(""),
    text: str = Form(""),
    family_name: str = Form(""),
    given_names: str = Form(""),
    prefixes: str = Form(""),
    suffixes: str = Form(""),
    organization_uid: str = Form(""),
):
    if not soa_exists(soa_id):
        raise HTTPException(404, "SOA not found")
    name = name.strip()
    if not name:
        raise HTTPException(400, "name is required")
    conn = _connect()
    cur = conn.cursor()
    cur.execute(
        "SELECT id, person_uid, name FROM person WHERE id=? AND soa_id=?",
        (person_id, soa_id),
    )
    row = cur.fetchone()
    if not row:
        conn.close()
        raise HTTPException(404, "Person not found")
    pid, person_uid, old_name = row
    before = {"person_uid": person_uid, "name": old_name}

    org_uid_clean = organization_uid.strip() or None
    try:
        _assert_no_person_org_role_org_conflict(cur, soa_id, pid, org_uid_clean)

        given_list = _parse_lines(given_names)
        prefix_list = _parse_lines(prefixes)
        suffix_list = _parse_lines(suffixes)

        cur.execute(
            "UPDATE person SET name=?, job_title=?, text=?, family_name=?,"
            " given_names=?, prefixes=?, suffixes=?, organization_uid=?"
            " WHERE id=? AND soa_id=?",
            (
                name,
                job_title.strip() or None,
                text.strip() or None,
                family_name.strip() or None,
                json.dumps(given_list) if given_list else None,
                json.dumps(prefix_list) if prefix_list else None,
                json.dumps(suffix_list) if suffix_list else None,
                org_uid_clean,
                person_id,
                soa_id,
            ),
        )
        conn.commit()
    except Exception:
        conn.rollback()
        conn.close()
        raise
    conn.close()
    _record_person_audit(
        soa_id,
        "update",
        pid,
        before=before,
        after={"person_uid": person_uid, "name": name},
    )
    return _partial_response(request, soa_id)
