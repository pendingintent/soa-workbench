import json
import logging
import os
from typing import List, Optional

from fastapi import APIRouter, Form, HTTPException, Request
from fastapi.responses import HTMLResponse, JSONResponse
from fastapi.templating import Jinja2Templates

from ..audit import _record_role_audit
from ..db import _connect
from ..utils import (
    get_ddf_ct_rows,
    get_latest_ddf_ct_href,
    get_next_code_uid,
    soa_exists,
)

router = APIRouter(prefix="/soa/{soa_id}")
ui_router = APIRouter()
logger = logging.getLogger("soa_builder.web.routers.roles")
templates = Jinja2Templates(
    directory=os.path.join(os.path.dirname(__file__), "..", "templates")
)

_ROLE_TYPE_CODELIST = "C215480"


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _next_role_uid(cur, soa_id: int) -> str:
    """Return next Role_N UID, never reusing deleted UIDs."""
    max_n = 0
    cur.execute(
        "SELECT role_uid FROM role WHERE soa_id=? AND role_uid LIKE 'StudyRole_%'",
        (soa_id,),
    )
    for (uid,) in cur.fetchall():
        if isinstance(uid, str) and uid.startswith("StudyRole_"):
            try:
                n = int(uid.split("_")[-1])
                if n > max_n:
                    max_n = n
            except (ValueError, IndexError):
                pass
    cur.execute(
        "SELECT before_json, after_json FROM role_audit WHERE soa_id=?",
        (soa_id,),
    )
    for before_raw, after_raw in cur.fetchall():
        for raw in (before_raw, after_raw):
            if not raw:
                continue
            try:
                uid = json.loads(raw).get("role_uid", "")
                if isinstance(uid, str) and uid.startswith("StudyRole_"):
                    n = int(uid.split("_")[-1])
                    if n > max_n:
                        max_n = n
            except Exception:
                pass
    return f"StudyRole_{max_n + 1}"


def _get_role_type_options() -> list:
    """Return [{code, label}] for DDF CT codelist C215480, alpha sorted."""
    payload = get_ddf_ct_rows()
    rows = payload.get("rows") or []
    options = [
        {
            "code": r["code"],
            "label": (
                r.get("preferred_term") or r.get("submission_value") or r["code"]
            ),
        }
        for r in rows
        if r.get("codelist_code") == _ROLE_TYPE_CODELIST and r.get("code")
    ]
    return sorted(options, key=lambda o: o["label"].lower())


def _ddf_ct_version() -> str:
    slug = get_latest_ddf_ct_href() or ""
    parts = slug.split("-")
    return "-".join(parts[-3:]) if len(parts) >= 3 else ""


def _insert_role_code(
    cur, soa_id: int, concept_id: str, preferred_term: str, version: str
) -> str:
    """Insert a code-table row for the role type and return Code_N UID."""
    code_uid = get_next_code_uid(cur, soa_id)
    cur.execute(
        "INSERT INTO code (soa_id, code_uid, code, decode, code_system, "
        "code_system_version) VALUES (?,?,?,?,?,?)",
        (
            soa_id,
            code_uid,
            concept_id,
            preferred_term,
            "http://www.cdisc.org",
            version,
        ),
    )
    return code_uid


def _delete_code(cur, soa_id: int, code_uid: Optional[str]) -> None:
    if not code_uid:
        return
    cur.execute(
        "DELETE FROM code WHERE soa_id=? AND code_uid=?",
        (soa_id, code_uid),
    )


def _list_roles(soa_id: int) -> list:
    """Return all roles for an SOA as a list of dicts."""
    conn = _connect()
    cur = conn.cursor()
    cur.execute(
        "SELECT r.id, r.role_uid, r.name, r.label, r.description, "
        "r.code_uid, c.decode AS type_decode, c.code AS type_code_value, "
        "r.organization_ids, r.masking "
        "FROM role r "
        "LEFT JOIN code c "
        "ON c.code_uid = r.code_uid AND c.soa_id = r.soa_id "
        "WHERE r.soa_id=? ORDER BY r.order_index, r.id",
        (soa_id,),
    )
    rows = cur.fetchall()

    # Fetch org names for display
    cur.execute(
        "SELECT organization_uid, name FROM organization WHERE soa_id=?",
        (soa_id,),
    )
    org_map = {r[0]: r[1] for r in cur.fetchall()}

    # Fetch assigned person UIDs per role
    cur.execute(
        "SELECT rp.role_id, p.person_uid"
        " FROM role_person rp"
        " JOIN person p ON p.id = rp.person_id AND p.soa_id = rp.soa_id"
        " WHERE rp.soa_id=?",
        (soa_id,),
    )
    person_uid_map: dict = {}
    for role_id_val, p_uid in cur.fetchall():
        person_uid_map.setdefault(role_id_val, []).append(p_uid)

    conn.close()

    result = []
    for r in rows:
        org_ids = json.loads(r[8]) if r[8] else []
        org_names = (
            ", ".join(org_map.get(uid, uid) for uid in org_ids) if org_ids else ""
        )
        result.append(
            {
                "id": r[0],
                "role_uid": r[1],
                "name": r[2],
                "label": r[3],
                "description": r[4],
                "code_uid": r[5],
                "type_decode": r[6] or "",
                "type_code_value": r[7] or "",
                "organization_ids": org_ids,
                "org_names": org_names,
                "masking": bool(r[9]),
                "assigned_person_uids": person_uid_map.get(r[0], []),
            }
        )
    return result


# ---------------------------------------------------------------------------
# JSON API endpoints
# ---------------------------------------------------------------------------


@router.get("/roles", response_class=JSONResponse)
def list_roles(soa_id: int):
    if not soa_exists(soa_id):
        raise HTTPException(404, "SOA not found")
    return _list_roles(soa_id)


@router.post("/roles", status_code=201, response_class=JSONResponse)
def create_role(soa_id: int, body: dict):
    if not soa_exists(soa_id):
        raise HTTPException(404, "SOA not found")
    name = (body.get("name") or "").strip()
    if not name:
        raise HTTPException(400, "name is required")

    type_concept_id = (body.get("type_concept_id") or "").strip()
    type_preferred_term = (body.get("type_preferred_term") or "").strip()
    type_version = (body.get("type_version") or _ddf_ct_version()).strip()
    org_ids = body.get("organization_ids") or []
    person_uids = body.get("person_uids") or []
    masking = bool(body.get("masking", False))

    conn = _connect()
    cur = conn.cursor()
    try:
        _assert_no_role_org_person_org_conflict(cur, soa_id, org_ids, person_uids)
        code_uid = None
        if type_concept_id:
            code_uid = _insert_role_code(
                cur, soa_id, type_concept_id, type_preferred_term, type_version
            )

        cur.execute(
            "SELECT COALESCE(MAX(order_index), 0) FROM role WHERE soa_id=?",
            (soa_id,),
        )
        order_index = (cur.fetchone()[0] or 0) + 1

        role_uid = _next_role_uid(cur, soa_id)
        cur.execute(
            "INSERT INTO role "
            "(soa_id, role_uid, name, label, description, code_uid, "
            "organization_ids, masking, order_index) "
            "VALUES (?,?,?,?,?,?,?,?,?)",
            (
                soa_id,
                role_uid,
                name,
                body.get("label") or None,
                body.get("description") or None,
                code_uid,
                json.dumps(org_ids) if org_ids else None,
                1 if masking else 0,
                order_index,
            ),
        )
        role_id = cur.lastrowid
        conn.commit()
    except Exception:
        conn.rollback()
        conn.close()
        raise
    conn.close()

    _record_role_audit(
        soa_id,
        "create",
        role_id,
        after={"role_uid": role_uid, "name": name},
    )
    return {"id": role_id, "role_uid": role_uid, "name": name}


@router.delete(
    "/roles/{role_id}",
    status_code=200,
    response_class=JSONResponse,
)
def delete_role(soa_id: int, role_id: int):
    if not soa_exists(soa_id):
        raise HTTPException(404, "SOA not found")
    conn = _connect()
    cur = conn.cursor()
    cur.execute(
        "SELECT id, role_uid, name, code_uid FROM role WHERE id=? AND soa_id=?",
        (role_id, soa_id),
    )
    row = cur.fetchone()
    if not row:
        conn.close()
        raise HTTPException(404, "Role not found")
    (rid, role_uid, name, code_uid) = row
    before = {"role_uid": role_uid, "name": name}

    _delete_person_assignments(cur, soa_id, rid)
    _delete_code(cur, soa_id, code_uid)
    cur.execute(
        "DELETE FROM role WHERE id=? AND soa_id=?",
        (role_id, soa_id),
    )
    cur.execute(
        "SELECT id FROM role WHERE soa_id=? ORDER BY order_index, id",
        (soa_id,),
    )
    for idx, (r,) in enumerate(cur.fetchall(), start=1):
        cur.execute("UPDATE role SET order_index=? WHERE id=?", (idx, r))
    conn.commit()
    conn.close()
    _record_role_audit(soa_id, "delete", rid, before=before)
    return {"deleted": role_uid}


# ---------------------------------------------------------------------------
# HTMX UI endpoints
# ---------------------------------------------------------------------------


def _partial_response(request: Request, soa_id: int) -> HTMLResponse:
    from .organizations import _list_organizations
    from .persons import _list_persons

    roles = _list_roles(soa_id)
    role_type_options = _get_role_type_options()
    organizations = _list_organizations(soa_id)
    persons = _list_persons(soa_id)
    return templates.TemplateResponse(
        request,
        "roles_partial.html",
        {
            "soa_id": soa_id,
            "roles": roles,
            "role_type_options": role_type_options,
            "organizations": organizations,
            "persons": persons,
        },
    )


def _assign_persons_to_role(cur, soa_id: int, role_id: int, person_uids: list) -> None:
    """Insert role_person rows for each matching person UID."""
    for p_uid in person_uids:
        if not p_uid.strip():
            continue
        cur.execute(
            "SELECT id FROM person WHERE soa_id=? AND person_uid=?",
            (soa_id, p_uid.strip()),
        )
        row = cur.fetchone()
        if row:
            cur.execute(
                "INSERT OR IGNORE INTO role_person"
                " (soa_id, role_id, person_id) VALUES (?,?,?)",
                (soa_id, role_id, row[0]),
            )


def _delete_person_assignments(cur, soa_id: int, role_id: int) -> None:
    cur.execute(
        "DELETE FROM role_person WHERE soa_id=? AND role_id=?",
        (soa_id, role_id),
    )


def _assert_no_role_org_person_org_conflict(
    cur, soa_id: int, org_ids: list, person_uids: list
) -> None:
    """Raise 422 if role org refs and any person org ref would coexist."""
    if not org_ids or not person_uids:
        return
    placeholders = ",".join("?" * len(person_uids))
    cur.execute(
        f"SELECT person_uid, name FROM person"
        f" WHERE soa_id=? AND person_uid IN ({placeholders})"
        f" AND organization_uid IS NOT NULL",
        [soa_id, *person_uids],
    )
    conflicts = cur.fetchall()
    if conflicts:
        names = ", ".join(r[1] or r[0] for r in conflicts)
        raise HTTPException(
            422,
            f"Role has organizationIds; the following assigned persons "
            f"also have an organizationId and cannot be combined: {names}",
        )


@ui_router.post(
    "/ui/soa/{soa_id}/roles-add",
    response_class=HTMLResponse,
)
def ui_roles_add(
    request: Request,
    soa_id: int,
    name: str = Form(""),
    label: str = Form(""),
    description: str = Form(""),
    role_type_code: str = Form(""),
    role_type_decode: str = Form(""),
    organization_ids: List[str] = Form(default=[]),
    person_uids: List[str] = Form(default=[]),
    masking: str = Form(""),
):
    if not soa_exists(soa_id):
        raise HTTPException(404, "SOA not found")
    name = name.strip()
    if not name:
        raise HTTPException(400, "name is required")

    type_version = _ddf_ct_version()
    org_ids = [o for o in organization_ids if o.strip()]
    clean_person_uids = [p for p in person_uids if p.strip()]
    is_masked = masking.strip().lower() == "on"

    conn = _connect()
    cur = conn.cursor()
    try:
        _assert_no_role_org_person_org_conflict(cur, soa_id, org_ids, clean_person_uids)
        code_uid = None
        if role_type_code.strip():
            code_uid = _insert_role_code(
                cur,
                soa_id,
                role_type_code.strip(),
                role_type_decode.strip(),
                type_version,
            )

        cur.execute(
            "SELECT COALESCE(MAX(order_index), 0) FROM role WHERE soa_id=?",
            (soa_id,),
        )
        order_index = (cur.fetchone()[0] or 0) + 1
        role_uid = _next_role_uid(cur, soa_id)
        cur.execute(
            "INSERT INTO role "
            "(soa_id, role_uid, name, label, description, code_uid, "
            "organization_ids, masking, order_index) "
            "VALUES (?,?,?,?,?,?,?,?,?)",
            (
                soa_id,
                role_uid,
                name,
                label.strip() or None,
                description.strip() or None,
                code_uid,
                json.dumps(org_ids) if org_ids else None,
                1 if is_masked else 0,
                order_index,
            ),
        )
        role_id = cur.lastrowid
        _assign_persons_to_role(cur, soa_id, role_id, person_uids)
        conn.commit()
    except Exception:
        conn.rollback()
        conn.close()
        raise
    conn.close()

    _record_role_audit(
        soa_id,
        "create",
        role_id,
        after={"role_uid": role_uid, "name": name},
    )
    return _partial_response(request, soa_id)


@ui_router.post(
    "/ui/soa/{soa_id}/roles/{role_id}/delete",
    response_class=HTMLResponse,
)
def ui_roles_delete(
    request: Request,
    soa_id: int,
    role_id: int,
):
    if not soa_exists(soa_id):
        raise HTTPException(404, "SOA not found")
    conn = _connect()
    cur = conn.cursor()
    cur.execute(
        "SELECT id, role_uid, name, code_uid FROM role WHERE id=? AND soa_id=?",
        (role_id, soa_id),
    )
    row = cur.fetchone()
    if not row:
        conn.close()
        raise HTTPException(404, "Role not found")
    (rid, role_uid, name, code_uid) = row
    before = {"role_uid": role_uid, "name": name}

    _delete_person_assignments(cur, soa_id, rid)
    _delete_code(cur, soa_id, code_uid)
    cur.execute(
        "DELETE FROM role WHERE id=? AND soa_id=?",
        (role_id, soa_id),
    )
    cur.execute(
        "SELECT id FROM role WHERE soa_id=? ORDER BY order_index, id",
        (soa_id,),
    )
    for idx, (r,) in enumerate(cur.fetchall(), start=1):
        cur.execute("UPDATE role SET order_index=? WHERE id=?", (idx, r))
    conn.commit()
    conn.close()
    _record_role_audit(soa_id, "delete", rid, before=before)
    return _partial_response(request, soa_id)


@ui_router.post(
    "/ui/soa/{soa_id}/roles/{role_id}/update",
    response_class=HTMLResponse,
)
def ui_roles_update(
    request: Request,
    soa_id: int,
    role_id: int,
    name: str = Form(""),
    label: str = Form(""),
    description: str = Form(""),
    role_type_code: str = Form(""),
    role_type_decode: str = Form(""),
    organization_ids: List[str] = Form(default=[]),
    person_uids: List[str] = Form(default=[]),
    masking: str = Form(""),
):
    if not soa_exists(soa_id):
        raise HTTPException(404, "SOA not found")
    name = name.strip()
    if not name:
        raise HTTPException(400, "name is required")
    conn = _connect()
    cur = conn.cursor()
    cur.execute(
        "SELECT id, role_uid, name, code_uid FROM role WHERE id=? AND soa_id=?",
        (role_id, soa_id),
    )
    row = cur.fetchone()
    if not row:
        conn.close()
        raise HTTPException(404, "Role not found")
    (rid, role_uid, old_name, old_code_uid) = row
    before = {"role_uid": role_uid, "name": old_name}

    org_ids = [o for o in organization_ids if o.strip()]
    clean_person_uids = [p for p in person_uids if p.strip()]
    is_masked = masking.strip().lower() == "on"

    # Validate before any writes
    try:
        _assert_no_role_org_person_org_conflict(cur, soa_id, org_ids, clean_person_uids)

        # Replace role type code record
        _delete_code(cur, soa_id, old_code_uid)
        new_code_uid = None
        if role_type_code.strip():
            version = _ddf_ct_version()
            new_code_uid = _insert_role_code(
                cur,
                soa_id,
                role_type_code.strip(),
                role_type_decode.strip(),
                version,
            )

        cur.execute(
            "UPDATE role SET name=?, label=?, description=?,"
            " code_uid=?, organization_ids=?, masking=?"
            " WHERE id=? AND soa_id=?",
            (
                name,
                label.strip() or None,
                description.strip() or None,
                new_code_uid,
                json.dumps(org_ids) if org_ids else None,
                1 if is_masked else 0,
                role_id,
                soa_id,
            ),
        )

        # Replace person assignments
        _delete_person_assignments(cur, soa_id, rid)
        _assign_persons_to_role(cur, soa_id, rid, clean_person_uids)

        conn.commit()
    except Exception:
        conn.rollback()
        conn.close()
        raise
    conn.close()
    _record_role_audit(
        soa_id,
        "update",
        rid,
        before=before,
        after={"role_uid": role_uid, "name": name},
    )
    return _partial_response(request, soa_id)
