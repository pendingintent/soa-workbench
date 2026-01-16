import logging
import os

from typing import Optional

from fastapi import APIRouter, HTTPException, Request, Form
from fastapi.responses import JSONResponse, HTMLResponse, RedirectResponse
from fastapi.templating import Jinja2Templates

from ..audit import _record_transition_rule_audit
from ..db import _connect
from ..utils import (
    soa_exists,
)
from ..schemas import RuleCreate, RuleUpdate

router = APIRouter()
logger = logging.getLogger("soa_builder.web.routers.rules")
templates = Jinja2Templates(
    directory=os.path.join(os.path.dirname(__file__), "..", "templates")
)


def _nz(s: Optional[str]) -> Optional[str]:
    s = (s or "").strip()
    return s or None


# API endpoint for listing transition rules
@router.get("/soa/{soa_id}/rules", response_class=JSONResponse, response_model=None)
def list_rules(soa_id: int):
    if not soa_exists(soa_id):
        raise HTTPException(404, "SOA not found")

    conn = _connect()
    cur = conn.cursor()
    cur.execute(
        """
        SELECT id, transition_rule_uid,name,label,description,text,order_index
        FROM transition_rule WHERE soa_id=? ORDER BY order_index
        """,
        (soa_id,),
    )
    rows = [
        {
            "id": row[0],
            "transition_rule_uid": row[1],
            "name": row[2],
            "label": row[3],
            "description": row[4],
            "text": row[5],
            "order_index": row[6],
        }
        for row in cur.fetchall()
    ]
    conn.close()
    return rows


# UI code for listing transition rules
@router.get("/ui/soa/{soa_id}/rules", response_class=HTMLResponse)
def ui_list_rules(request: Request, soa_id: int):
    if not soa_exists(soa_id):
        raise HTTPException(404, "SOA not found")

    rules = list_rules(soa_id)

    return templates.TemplateResponse(
        request,
        "rules.html",
        {
            "request": request,
            "soa_id": soa_id,
            "rules": rules,
        },
    )


# API endpoint for creating transition rule
@router.post(
    "/soa/{soa_id}/rules",
    response_class=JSONResponse,
    status_code=201,
    response_model=None,
)
def add_rule(soa_id: int, payload: RuleCreate):
    if not soa_exists(soa_id):
        raise HTTPException(404, "SOA not found")

    name = (payload.name or "").strip()
    if not name:
        raise HTTPException(400, "Transition Rule name required")

    conn = _connect()
    cur = conn.cursor()

    # order_index
    cur.execute(
        "SELECT COALESCE(MAX(order_index),0) FROM transition_rule WHERE soa_id=?",
        (soa_id,),
    )
    next_ord = (cur.fetchone() or [0])[0] + 1

    # create transition_rule_uid and increment order_index
    cur.execute(
        "SELECT transition_rule_uid FROM transition_rule WHERE soa_id=? and transition_rule_uid LIKE 'TransitionRule_%'",
        (soa_id,),
    )
    existing_uids = [r[0] for r in cur.fetchall() if r[0]]
    used_nums = set()
    for uid in existing_uids:
        if uid.startswith("TransitionRule_"):
            tail = uid[len("TransitionRule_") :]
            if tail.isdigit():
                used_nums.add(int(tail))
            else:
                logger.warning(
                    "Invalid transition_rule_uid format encountered (ignored): %s", uid
                )

    # always pick max(existing) + 1, do not fill gaps
    next_n = (max(used_nums) if used_nums else 0) + 1
    new_uid = f"TransitionRule_{next_n}"

    cur.execute(
        """
        INSERT INTO transition_rule (soa_id,transition_rule_uid,name,label,description,text,order_index)
        VALUES (?,?,?,?,?,?,?)
        """,
        (
            soa_id,
            new_uid,
            name,
            _nz(payload.label),
            _nz(payload.description),
            _nz(payload.text),
            next_ord,
        ),
    )
    id = cur.lastrowid
    conn.commit()
    conn.close()
    after = {
        "id": id,
        "transition_rule_uid": new_uid,
        "name": payload.name,
        "label": (payload.label or "").strip() or None,
        "description": (payload.description or "").strip() or None,
        "text": (payload.text or "").strip() or None,
    }
    _record_transition_rule_audit(soa_id, "create", id, before=None, after=after)
    return after


# UI code for creating transition rule
@router.post("/ui/soa/{soa_id}/rules/create")
def ui_create_rule(
    request: Request,
    soa_id: int,
    name: str = Form(...),
    label: Optional[str] = Form(None),
    description: Optional[str] = Form(None),
    text: Optional[str] = Form(None),
):
    if not soa_exists(soa_id):
        raise HTTPException(404, "SOA not found")

    payload = RuleCreate(
        name=name,
        label=label,
        description=description,
        text=text,
    )
    add_rule(soa_id, payload)
    return RedirectResponse(url=f"/ui/soa/{int(soa_id)}/rules", status_code=303)


# API endpoint for updating transition rule
@router.patch("/soa/{soa_id}/rules/{rule_id}", response_class=JSONResponse)
def update_rule(soa_id: int, rule_id: int, payload: RuleUpdate):
    if not soa_exists(soa_id):
        raise HTTPException(404, "SOA not found")

    conn = _connect()
    cur = conn.cursor()
    cur.execute(
        """
        SELECT id,transition_rule_uid,name,label,description,text
        FROM transition_rule WHERE id=? AND soa_id=?
        """,
        (rule_id, soa_id),
    )
    row = cur.fetchone()
    if not row:
        raise HTTPException(404, f"Transition Rule id={int(rule_id)} not found")

    before = {
        "id": row[0],
        "transition_rule_uid": row[1],
        "name": row[2],
        "label": row[3],
        "description": row[4],
        "text": row[5],
    }

    new_name = (payload.name if payload.name is not None else before["name"]) or ""
    new_label = payload.label if payload.label is not None else before["label"]
    new_description = (
        payload.description
        if payload.description is not None
        else before["description"]
    )
    new_text = payload.text if payload.text is not None else before["text"]

    cur.execute(
        """
        UPDATE transition_rule SET name=?, label=?, description=?, text=?
        WHERE id=? AND soa_id=?
        """,
        (
            _nz(new_name),
            _nz(new_label),
            _nz(new_description),
            _nz(new_text),
            rule_id,
            soa_id,
        ),
    )
    conn.commit()
    cur.execute(
        """
        SELECT id,transition_rule_uid,name,label,description,text
        FROM transition_rule WHERE id=? AND soa_id=?
        """,
        (rule_id, soa_id),
    )
    r = cur.fetchone()
    conn.close()
    after = {
        "id": r[0],
        "transition_rule_uid": r[1],
        "name": r[2],
        "label": r[3],
        "description": r[4],
        "text": r[5],
    }
    mutable = [
        "name",
        "label",
        "description",
        "text",
    ]
    updated_fields = [
        f for f in mutable if (before.get(f) or None) != (after.get(f) or None)
    ]
    _record_transition_rule_audit(
        soa_id,
        "update",
        rule_id,
        before=before,
        after={**after, "updated_fields": updated_fields},
    )
    return {**after, "updated_fields": updated_fields}


# UI code for updating transition rule
@router.post("/ui/soa/{soa_id}/rules/{rule_id}/update")
def ui_update_rule(
    request: Request,
    soa_id: int,
    rule_id: int,
    name: Optional[str] = Form(None),
    label: Optional[str] = Form(None),
    description: Optional[str] = Form(None),
    text: Optional[str] = Form(None),
):
    payload = RuleUpdate(
        name=name,
        label=label,
        description=description,
        text=text,
    )
    update_rule(soa_id, rule_id, payload)
    return RedirectResponse(url=f"/ui/soa/{int(soa_id)}/rules", status_code=303)


# API endpoint for deleting transition rule
@router.delete(
    "/soa/{soa_id}/rules/{rule_id}",
    response_class=JSONResponse,
    response_model=None,
)
def delete_rule(soa_id: int, rule_id: int):
    if not soa_exists(soa_id):
        raise HTTPException(404, "SOA not found")

    conn = _connect()
    cur = conn.cursor()
    cur.execute(
        "SELECT id, transition_rule_uid,name,label FROM transition_rule WHERE soa_id=? AND id=?",
        (soa_id, rule_id),
    )
    row = cur.fetchone()
    if not row:
        raise HTTPException(404, f"Transition Rule id={int(rule_id)} not found")

    before = {
        "id": row[0],
        "transition_rule_uid": row[1],
        "name": row[2],
        "label": row[3],
    }
    cur.execute(
        "DELETE FROM transition_rule WHERE soa_id=? AND id=?",
        (soa_id, rule_id),
    )
    conn.commit()
    # reindex remaining rules order_index sequentially
    cur.execute(
        "SELECT id FROM transition_rule WHERE soa_id=? ORDER BY order_index",
        (soa_id,),
    )
    remaining = [r[0] for r in cur.fetchall()]
    for idx, rid in enumerate(remaining, start=1):
        cur.execute(
            "UPDATE transition_rule SET order_index=? WHERE id=?",
            (idx, rid),
        )
    conn.commit()
    conn.close()
    _record_transition_rule_audit(soa_id, "delete", rule_id, before=before, after=None)
    return {"deleted": True, "id": rule_id}


# UI code for deleting transition rule
@router.post("/ui/soa/{soa_id}/rules/{rule_id}/delete", response_class=HTMLResponse)
def ui_delete_rule(request: Request, soa_id: int, rule_id: int):
    delete_rule(soa_id, rule_id)
    return RedirectResponse(url=f"/ui/soa/{int(soa_id)}/rules", status_code=303)
