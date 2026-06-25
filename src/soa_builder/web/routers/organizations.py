import json
import logging
import os

from fastapi import APIRouter, Form, HTTPException, Request
from fastapi.responses import HTMLResponse, JSONResponse
from fastapi.templating import Jinja2Templates

from ..audit import _record_organization_audit
from ..db import _connect
from ..utils import (
    get_ddf_ct_rows,
    get_latest_ddf_ct_href,
    get_next_code_uid,
    soa_exists,
)

router = APIRouter(prefix="/soa/{soa_id}")
ui_router = APIRouter()
logger = logging.getLogger("soa_builder.web.routers.organizations")
templates = Jinja2Templates(
    directory=os.path.join(os.path.dirname(__file__), "..", "templates")
)

_ORG_TYPE_CODELIST = "C215480"


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _next_org_uid(cur, soa_id: int) -> str:
    """Return next Organization_N UID, never reusing deleted UIDs."""
    max_n = 0
    cur.execute(
        "SELECT organization_uid FROM organization WHERE soa_id=? "
        "AND organization_uid LIKE 'Organization_%'",
        (soa_id,),
    )
    for (uid,) in cur.fetchall():
        if isinstance(uid, str) and uid.startswith("Organization_"):
            try:
                n = int(uid.split("_")[-1])
                if n > max_n:
                    max_n = n
            except (ValueError, IndexError):
                pass
    cur.execute(
        "SELECT before_json, after_json FROM organization_audit WHERE soa_id=?",
        (soa_id,),
    )
    for before_raw, after_raw in cur.fetchall():
        for raw in (before_raw, after_raw):
            if not raw:
                continue
            try:
                uid = json.loads(raw).get("organization_uid", "")
                if isinstance(uid, str) and uid.startswith("Organization_"):
                    n = int(uid.split("_")[-1])
                    if n > max_n:
                        max_n = n
            except Exception:
                pass
    return f"Organization_{max_n + 1}"


def _get_org_type_options() -> list:
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
        if r.get("codelist_code") == _ORG_TYPE_CODELIST and r.get("code")
    ]
    return sorted(options, key=lambda o: o["label"].lower())


def _get_countries_options() -> list:
    """Return [{name, code}] from the country_codes table."""
    conn = _connect()
    cur = conn.cursor()
    cur.execute(
        "SELECT country_name, country_numeric_code "
        "FROM country_codes ORDER BY country_name"
    )
    rows = [{"name": r[0], "code": r[1]} for r in cur.fetchall()]
    conn.close()
    return rows


def _list_organizations(soa_id: int) -> list:
    """Return all organizations for an SOA as a list of dicts."""
    conn = _connect()
    cur = conn.cursor()
    cur.execute(
        "SELECT o.id, o.organization_uid, o.name, o.label, "
        "o.identifier, o.identifier_scheme, o.type_code_uid, "
        "c.decode AS type_decode, c.code AS type_code_value, "
        "o.addr_text, o.addr_lines, o.addr_city, o.addr_district, "
        "o.addr_state, o.addr_postal_code, o.addr_country_code_uid, "
        "cc.decode AS country_decode, cc.code AS country_numeric "
        "FROM organization o "
        "LEFT JOIN code c "
        "ON c.code_uid = o.type_code_uid AND c.soa_id = o.soa_id "
        "LEFT JOIN code cc "
        "ON cc.code_uid = o.addr_country_code_uid AND cc.soa_id = o.soa_id "
        "WHERE o.soa_id=? ORDER BY o.order_index, o.id",
        (soa_id,),
    )
    result = [
        {
            "id": r[0],
            "organization_uid": r[1],
            "name": r[2],
            "label": r[3],
            "identifier": r[4],
            "identifier_scheme": r[5],
            "type_code_uid": r[6],
            "type_decode": r[7] or "",
            "type_code_value": r[8] or "",
            "addr_text": r[9],
            "addr_lines": json.loads(r[10]) if r[10] else [],
            "addr_city": r[11],
            "addr_district": r[12],
            "addr_state": r[13],
            "addr_postal_code": r[14],
            "addr_country_code_uid": r[15],
            "country_decode": r[16] or "",
            "country_numeric": r[17] or "",
        }
        for r in cur.fetchall()
    ]
    conn.close()
    return result


def _row_to_dict(row) -> dict:
    keys = [
        "id",
        "soa_id",
        "organization_uid",
        "name",
        "label",
        "identifier",
        "identifier_scheme",
        "type_code_uid",
        "addr_text",
        "addr_lines",
        "addr_city",
        "addr_district",
        "addr_state",
        "addr_postal_code",
        "addr_country_code_uid",
        "order_index",
    ]
    return dict(zip(keys, row))


def _insert_type_code(
    cur, soa_id: int, concept_id: str, preferred_term: str, version: str
) -> str:
    """Insert a code-table row for the org type and return the Code_N UID."""
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


def _insert_country_code(cur, soa_id: int, numeric_code: str, country_name: str) -> str:
    """Insert a code-table row for the country and return the Code_N UID."""
    code_uid = get_next_code_uid(cur, soa_id)
    cur.execute(
        "INSERT INTO code (soa_id, code_uid, code, decode, code_system, "
        "code_system_version) VALUES (?,?,?,?,?,?)",
        (
            soa_id,
            code_uid,
            numeric_code,
            country_name,
            "ISO 3166 1 Numeric Code",
            "2026",
        ),
    )
    return code_uid


def _delete_code(cur, soa_id: int, code_uid: str | None) -> None:
    if not code_uid:
        return
    cur.execute(
        "DELETE FROM code WHERE soa_id=? AND code_uid=?",
        (soa_id, code_uid),
    )


def _ddf_ct_version() -> str:
    slug = get_latest_ddf_ct_href() or ""
    parts = slug.split("-")
    return "-".join(parts[-3:]) if len(parts) >= 3 else ""


# ---------------------------------------------------------------------------
# JSON API endpoints
# ---------------------------------------------------------------------------


@router.get("/organizations", response_class=JSONResponse)
def list_organizations(soa_id: int):
    if not soa_exists(soa_id):
        raise HTTPException(404, "SOA not found")
    return _list_organizations(soa_id)


@router.post("/organizations", status_code=201, response_class=JSONResponse)
def create_organization(soa_id: int, body: dict):
    if not soa_exists(soa_id):
        raise HTTPException(404, "SOA not found")
    name = (body.get("name") or "").strip()
    if not name:
        raise HTTPException(400, "name is required")

    type_concept_id = (body.get("type_concept_id") or "").strip()
    type_preferred_term = (body.get("type_preferred_term") or "").strip()
    type_version = (body.get("type_version") or _ddf_ct_version()).strip()
    addr_country_numeric = (body.get("addr_country_numeric") or "").strip()
    addr_country_name = (body.get("addr_country_name") or "").strip()
    addr_lines_raw = body.get("addr_lines") or []
    addr_lines = (
        [ln for ln in addr_lines_raw if ln.strip()]
        if isinstance(addr_lines_raw, list)
        else []
    )

    conn = _connect()
    cur = conn.cursor()
    try:
        type_code_uid = None
        if type_concept_id:
            type_code_uid = _insert_type_code(
                cur, soa_id, type_concept_id, type_preferred_term, type_version
            )

        addr_country_code_uid = None
        if addr_country_numeric:
            addr_country_code_uid = _insert_country_code(
                cur, soa_id, addr_country_numeric, addr_country_name
            )

        cur.execute(
            "SELECT COALESCE(MAX(order_index), 0) FROM organization WHERE soa_id=?",
            (soa_id,),
        )
        order_index = (cur.fetchone()[0] or 0) + 1

        org_uid = _next_org_uid(cur, soa_id)
        cur.execute(
            "INSERT INTO organization "
            "(soa_id, organization_uid, name, label, identifier, "
            "identifier_scheme, type_code_uid, addr_text, addr_lines, "
            "addr_city, addr_district, addr_state, addr_postal_code, "
            "addr_country_code_uid, order_index) "
            "VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
            (
                soa_id,
                org_uid,
                name,
                body.get("label") or None,
                body.get("identifier") or None,
                body.get("identifier_scheme") or None,
                type_code_uid,
                body.get("addr_text") or None,
                json.dumps(addr_lines) if addr_lines else None,
                body.get("addr_city") or None,
                body.get("addr_district") or None,
                body.get("addr_state") or None,
                body.get("addr_postal_code") or None,
                addr_country_code_uid,
                order_index,
            ),
        )
        org_id = cur.lastrowid
        conn.commit()
    except Exception:
        conn.rollback()
        conn.close()
        raise
    conn.close()

    _record_organization_audit(
        soa_id,
        "create",
        org_id,
        after={
            "organization_uid": org_uid,
            "name": name,
        },
    )
    return {"id": org_id, "organization_uid": org_uid, "name": name}


@router.delete(
    "/organizations/{org_id}",
    status_code=200,
    response_class=JSONResponse,
)
def delete_organization(soa_id: int, org_id: int):
    if not soa_exists(soa_id):
        raise HTTPException(404, "SOA not found")
    conn = _connect()
    cur = conn.cursor()
    cur.execute(
        "SELECT id, organization_uid, name, type_code_uid, "
        "addr_country_code_uid FROM organization "
        "WHERE id=? AND soa_id=?",
        (org_id, soa_id),
    )
    row = cur.fetchone()
    if not row:
        conn.close()
        raise HTTPException(404, "Organization not found")
    (oid, org_uid, name, type_code_uid, country_code_uid) = row
    before = {"organization_uid": org_uid, "name": name}

    _delete_code(cur, soa_id, type_code_uid)
    _delete_code(cur, soa_id, country_code_uid)
    cur.execute(
        "DELETE FROM organization WHERE id=? AND soa_id=?",
        (org_id, soa_id),
    )
    # Re-index remaining rows
    cur.execute(
        "SELECT id FROM organization WHERE soa_id=? ORDER BY order_index, id",
        (soa_id,),
    )
    for idx, (rid,) in enumerate(cur.fetchall(), start=1):
        cur.execute("UPDATE organization SET order_index=? WHERE id=?", (idx, rid))
    conn.commit()
    conn.close()
    _record_organization_audit(soa_id, "delete", oid, before=before)
    return {"deleted": org_uid}


# ---------------------------------------------------------------------------
# HTMX UI endpoints
# ---------------------------------------------------------------------------


def _partial_response(request: Request, soa_id: int) -> HTMLResponse:
    from .roles import _get_role_type_options, _list_roles
    from .persons import _list_persons

    organizations = _list_organizations(soa_id)
    roles = _list_roles(soa_id)
    persons = _list_persons(soa_id)
    return templates.TemplateResponse(
        request,
        "organizations_oob_response.html",
        {
            "soa_id": soa_id,
            "organizations": organizations,
            "org_type_options": _get_org_type_options(),
            "countries_options": _get_countries_options(),
            "roles": roles,
            "role_type_options": _get_role_type_options(),
            "persons": persons,
        },
    )


@ui_router.post(
    "/ui/soa/{soa_id}/organizations-add",
    response_class=HTMLResponse,
)
def ui_organizations_add(
    request: Request,
    soa_id: int,
    name: str = Form(""),
    label: str = Form(""),
    identifier: str = Form(""),
    identifier_scheme: str = Form(""),
    type_code: str = Form(""),
    type_decode: str = Form(""),
    addr_text: str = Form(""),
    addr_lines: str = Form(""),
    addr_city: str = Form(""),
    addr_district: str = Form(""),
    addr_state: str = Form(""),
    addr_postal_code: str = Form(""),
    addr_country_numeric: str = Form(""),
    addr_country_name: str = Form(""),
):
    if not soa_exists(soa_id):
        raise HTTPException(404, "SOA not found")
    name = name.strip()
    if not name:
        raise HTTPException(400, "name is required")

    lines = [ln for ln in addr_lines.splitlines() if ln.strip()]
    type_version = _ddf_ct_version()

    conn = _connect()
    cur = conn.cursor()
    try:
        type_code_uid = None
        if type_code.strip():
            type_code_uid = _insert_type_code(
                cur,
                soa_id,
                type_code.strip(),
                type_decode.strip(),
                type_version,
            )

        addr_country_code_uid = None
        if addr_country_numeric.strip():
            addr_country_code_uid = _insert_country_code(
                cur,
                soa_id,
                addr_country_numeric.strip(),
                addr_country_name.strip(),
            )

        cur.execute(
            "SELECT COALESCE(MAX(order_index), 0) FROM organization WHERE soa_id=?",
            (soa_id,),
        )
        order_index = (cur.fetchone()[0] or 0) + 1
        org_uid = _next_org_uid(cur, soa_id)
        cur.execute(
            "INSERT INTO organization "
            "(soa_id, organization_uid, name, label, identifier, "
            "identifier_scheme, type_code_uid, addr_text, addr_lines, "
            "addr_city, addr_district, addr_state, addr_postal_code, "
            "addr_country_code_uid, order_index) "
            "VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
            (
                soa_id,
                org_uid,
                name,
                label.strip() or None,
                identifier.strip() or None,
                identifier_scheme.strip() or None,
                type_code_uid,
                addr_text.strip() or None,
                json.dumps(lines) if lines else None,
                addr_city.strip() or None,
                addr_district.strip() or None,
                addr_state.strip() or None,
                addr_postal_code.strip() or None,
                addr_country_code_uid,
                order_index,
            ),
        )
        org_id = cur.lastrowid
        conn.commit()
    except Exception:
        conn.rollback()
        conn.close()
        raise
    conn.close()

    _record_organization_audit(
        soa_id,
        "create",
        org_id,
        after={"organization_uid": org_uid, "name": name},
    )
    return _partial_response(request, soa_id)


@ui_router.post(
    "/ui/soa/{soa_id}/organizations/{org_id}/delete",
    response_class=HTMLResponse,
)
def ui_organizations_delete(
    request: Request,
    soa_id: int,
    org_id: int,
):
    if not soa_exists(soa_id):
        raise HTTPException(404, "SOA not found")
    conn = _connect()
    cur = conn.cursor()
    cur.execute(
        "SELECT id, organization_uid, name, type_code_uid, "
        "addr_country_code_uid FROM organization "
        "WHERE id=? AND soa_id=?",
        (org_id, soa_id),
    )
    row = cur.fetchone()
    if not row:
        conn.close()
        raise HTTPException(404, "Organization not found")
    (oid, org_uid, name, type_code_uid, country_code_uid) = row
    before = {"organization_uid": org_uid, "name": name}

    _delete_code(cur, soa_id, type_code_uid)
    _delete_code(cur, soa_id, country_code_uid)
    cur.execute(
        "DELETE FROM organization WHERE id=? AND soa_id=?",
        (org_id, soa_id),
    )
    cur.execute(
        "SELECT id FROM organization WHERE soa_id=? ORDER BY order_index, id",
        (soa_id,),
    )
    for idx, (rid,) in enumerate(cur.fetchall(), start=1):
        cur.execute("UPDATE organization SET order_index=? WHERE id=?", (idx, rid))
    conn.commit()
    conn.close()
    _record_organization_audit(soa_id, "delete", oid, before=before)
    return _partial_response(request, soa_id)


@ui_router.post(
    "/ui/soa/{soa_id}/organizations/{org_id}/update",
    response_class=HTMLResponse,
)
def ui_organizations_update(
    request: Request,
    soa_id: int,
    org_id: int,
    name: str = Form(""),
    label: str = Form(""),
    identifier_scheme: str = Form(""),
    identifier: str = Form(""),
    type_code: str = Form(""),
    type_decode: str = Form(""),
    addr_city: str = Form(""),
    addr_district: str = Form(""),
    addr_state: str = Form(""),
    addr_postal_code: str = Form(""),
    addr_country_numeric: str = Form(""),
    addr_country_name: str = Form(""),
):
    if not soa_exists(soa_id):
        raise HTTPException(404, "SOA not found")
    name = name.strip()
    if not name:
        raise HTTPException(400, "name is required")
    conn = _connect()
    cur = conn.cursor()
    cur.execute(
        "SELECT id, organization_uid, name, type_code_uid,"
        " addr_country_code_uid"
        " FROM organization WHERE id=? AND soa_id=?",
        (org_id, soa_id),
    )
    row = cur.fetchone()
    if not row:
        conn.close()
        raise HTTPException(404, "Organization not found")
    (oid, org_uid, old_name, old_type_uid, old_country_uid) = row
    before = {"organization_uid": org_uid, "name": old_name}

    # Replace type code record
    _delete_code(cur, soa_id, old_type_uid)
    new_type_uid = None
    if type_code.strip():
        version = _ddf_ct_version()
        new_type_uid = _insert_type_code(
            cur, soa_id, type_code.strip(), type_decode.strip(), version
        )

    # Replace country code record
    _delete_code(cur, soa_id, old_country_uid)
    new_country_uid = None
    if addr_country_numeric.strip():
        new_country_uid = _insert_country_code(
            cur,
            soa_id,
            addr_country_numeric.strip(),
            addr_country_name.strip(),
        )

    cur.execute(
        "UPDATE organization SET name=?, label=?,"
        " identifier=?, identifier_scheme=?,"
        " type_code_uid=?,"
        " addr_city=?, addr_district=?, addr_state=?,"
        " addr_postal_code=?, addr_country_code_uid=?"
        " WHERE id=? AND soa_id=?",
        (
            name,
            label.strip() or None,
            identifier.strip() or None,
            identifier_scheme.strip() or None,
            new_type_uid,
            addr_city.strip() or None,
            addr_district.strip() or None,
            addr_state.strip() or None,
            addr_postal_code.strip() or None,
            new_country_uid,
            org_id,
            soa_id,
        ),
    )
    conn.commit()
    conn.close()
    _record_organization_audit(
        soa_id,
        "update",
        oid,
        before=before,
        after={"organization_uid": org_uid, "name": name},
    )
    return _partial_response(request, soa_id)
