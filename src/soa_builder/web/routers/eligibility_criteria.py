"""Eligibility criteria: API and UI (USDM v4.0, CORE DDF00126).

Each row of ``eligibility_criterion`` is one EligibilityCriterion and
its EligibilityCriterionItem (the text). The category is a term from
the Category of Inclusion/Exclusion codelist
(ELIGIBILITY_CATEGORY_CODELIST, default C66797), checked against the
latest SDTM CT package. Criteria are kept in ``order_index`` order;
the USDM export links them with nextId/previousId.

``name`` is exported as the criterion name, which the Define-JSON
generator uses as IETESTCD, and ``label`` as IETEST.
"""

import logging
import os
import sqlite3
from datetime import datetime, timezone
from typing import List, Optional

from fastapi import APIRouter, Form, HTTPException, Request, Response
from fastapi.responses import HTMLResponse, RedirectResponse
from fastapi.templating import Jinja2Templates
from pydantic import BaseModel

from .. import utils as web_utils
from ..audit import _record_eligibility_criterion_audit
from ..codelist_config import ELIGIBILITY_CATEGORY_CODELIST
from ..db import _connect
from ..utils import _max_suffix, _next_uid_from_counter, soa_exists

logger = logging.getLogger("soa_builder.web.routers.eligibility_criteria")

router = APIRouter()
templates = Jinja2Templates(
    directory=os.path.join(os.path.dirname(__file__), "..", "templates")
)

_COLUMNS = (
    "id",
    "criterion_uid",
    "item_uid",
    "name",
    "label",
    "description",
    "identifier",
    "category_code",
    "category_decode",
    "category_version",
    "text",
    "order_index",
)


class EligibilityCriterionIn(BaseModel):
    name: str
    identifier: str
    category_code: str
    text: str
    label: Optional[str] = None
    description: Optional[str] = None


class EligibilityCriterionPatch(BaseModel):
    name: Optional[str] = None
    identifier: Optional[str] = None
    category_code: Optional[str] = None
    text: Optional[str] = None
    label: Optional[str] = None
    description: Optional[str] = None


class EligibilityOrderIn(BaseModel):
    criterion_uids: List[str]


def _clean(value: Optional[str]) -> Optional[str]:
    value = (value or "").strip()
    return value or None


def _require_soa(soa_id: int) -> None:
    if not soa_exists(soa_id):
        raise HTTPException(404, "SOA not found")


def _categories() -> tuple[dict[str, str], str]:
    """Return ({code: submission value}, package date) for the codelist.

    Called through the module so tests can patch get_sdtm_ct_rows.
    """
    payload = web_utils.get_sdtm_ct_rows() or {}
    parts = (payload.get("slug") or "").split("-")
    version = "-".join(parts[-3:]) if len(parts) >= 4 else ""
    terms = {
        str(r.get("code")): str(r.get("submission_value") or "")
        for r in payload.get("rows") or []
        if r.get("codelist_code") == ELIGIBILITY_CATEGORY_CODELIST and r.get("code")
    }
    return terms, version


def _next_uid(cur, soa_id: int, prefix: str, column: str) -> str:
    def _seed() -> int:
        cur.execute(
            f"SELECT {column} FROM eligibility_criterion WHERE soa_id=?",
            (soa_id,),
        )
        return _max_suffix(cur.fetchall())

    # resync: rollback can restore rows without going through the counter.
    return _next_uid_from_counter(cur, soa_id, prefix, _seed, resync=True)


def list_criteria(soa_id: int) -> list:
    conn = _connect()
    try:
        rows = conn.execute(
            f"SELECT {','.join(_COLUMNS)} FROM eligibility_criterion"
            " WHERE soa_id=? ORDER BY order_index, id",
            (soa_id,),
        ).fetchall()
    finally:
        conn.close()
    return [dict(zip(_COLUMNS, row)) for row in rows]


def _get(soa_id: int, criterion_id: int) -> dict:
    match = [c for c in list_criteria(soa_id) if c["id"] == criterion_id]
    if not match:
        raise HTTPException(404, "Eligibility criterion not found")
    return match[0]


def _check_unique(soa_id: int, field: str, value: str, exclude_id=None) -> None:
    for c in list_criteria(soa_id):
        if c["id"] != exclude_id and (c[field] or "").lower() == value.lower():
            label = "Name" if field == "name" else "Identifier"
            raise HTTPException(400, f"{label} {value} is already used")


def _category(code: str) -> tuple[str, str, str]:
    terms, version = _categories()
    code = (code or "").strip()
    if code not in terms:
        raise HTTPException(
            400,
            f"{code or 'Category'} is not in the eligibility category codelist"
            f" {ELIGIBILITY_CATEGORY_CODELIST}",
        )
    return code, terms[code], version


def _required(value: Optional[str], label: str) -> str:
    value = _clean(value)
    if not value:
        raise HTTPException(400, f"{label} is required")
    return value


def create_criterion(soa_id: int, payload: EligibilityCriterionIn) -> dict:
    name = _required(payload.name, "Name")
    identifier = _required(payload.identifier, "Identifier")
    text = _required(payload.text, "Text")
    _check_unique(soa_id, "name", name)
    _check_unique(soa_id, "identifier", identifier)
    code, decode, version = _category(payload.category_code)
    conn = _connect()
    try:
        cur = conn.cursor()
        criterion_uid = _next_uid(cur, soa_id, "EligibilityCriterion_", "criterion_uid")
        item_uid = _next_uid(cur, soa_id, "EligibilityCriterionItem_", "item_uid")
        cur.execute(
            "SELECT COALESCE(MAX(order_index), 0) FROM eligibility_criterion"
            " WHERE soa_id=?",
            (soa_id,),
        )
        order_index = cur.fetchone()[0] + 1
        cur.execute(
            "INSERT INTO eligibility_criterion (soa_id, criterion_uid, item_uid,"
            " name, label, description, identifier, category_code,"
            " category_decode, category_version, text, order_index, created_at)"
            " VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)",
            (
                soa_id,
                criterion_uid,
                item_uid,
                name,
                _clean(payload.label),
                _clean(payload.description),
                identifier,
                code,
                decode,
                version,
                text,
                order_index,
                datetime.now(timezone.utc).isoformat(),
            ),
        )
        new_id = cur.lastrowid
        conn.commit()
    finally:
        conn.close()
    created = _get(soa_id, new_id)
    _record_eligibility_criterion_audit(soa_id, "create", new_id, after=created)
    return created


def update_criterion(
    soa_id: int, criterion_id: int, payload: EligibilityCriterionPatch
) -> dict:
    before = _get(soa_id, criterion_id)
    changes: dict = {}
    if payload.name is not None:
        changes["name"] = _required(payload.name, "Name")
        _check_unique(soa_id, "name", changes["name"], exclude_id=criterion_id)
    if payload.identifier is not None:
        changes["identifier"] = _required(payload.identifier, "Identifier")
        _check_unique(
            soa_id, "identifier", changes["identifier"], exclude_id=criterion_id
        )
    if payload.text is not None:
        changes["text"] = _required(payload.text, "Text")
    if payload.category_code is not None:
        code, decode, version = _category(payload.category_code)
        changes.update(
            category_code=code, category_decode=decode, category_version=version
        )
    if payload.label is not None:
        changes["label"] = _clean(payload.label)
    if payload.description is not None:
        changes["description"] = _clean(payload.description)
    if changes:
        conn = _connect()
        try:
            conn.execute(
                "UPDATE eligibility_criterion SET "
                + ", ".join(f"{col}=?" for col in changes)
                + " WHERE soa_id=? AND id=?",
                (*changes.values(), soa_id, criterion_id),
            )
            conn.commit()
        finally:
            conn.close()
    after = _get(soa_id, criterion_id)
    _record_eligibility_criterion_audit(
        soa_id, "update", criterion_id, before=before, after=after
    )
    return after


def delete_criterion(soa_id: int, criterion_id: int) -> None:
    before = _get(soa_id, criterion_id)
    conn = _connect()
    try:
        conn.execute(
            "DELETE FROM eligibility_criterion WHERE soa_id=? AND id=?",
            (soa_id, criterion_id),
        )
        conn.commit()
    finally:
        conn.close()
    _record_eligibility_criterion_audit(soa_id, "delete", criterion_id, before=before)


def reorder_criteria(soa_id: int, criterion_uids: List[str]) -> list:
    current = list_criteria(soa_id)
    if sorted(criterion_uids) != sorted(c["criterion_uid"] for c in current):
        raise HTTPException(400, "Give every eligibility criterion UID exactly once")
    conn = _connect()
    try:
        conn.executemany(
            "UPDATE eligibility_criterion SET order_index=?"
            " WHERE soa_id=? AND criterion_uid=?",
            [(n, soa_id, uid) for n, uid in enumerate(criterion_uids, start=1)],
        )
        conn.commit()
    finally:
        conn.close()
    after = list_criteria(soa_id)
    _record_eligibility_criterion_audit(
        soa_id,
        "reorder",
        None,
        before={"order": [c["criterion_uid"] for c in current]},
        after={"order": criterion_uids},
    )
    return after


# --- API ----------------------------------------------------------------


@router.get("/soa/{soa_id}/eligibility_criteria")
def api_list_criteria(soa_id: int):
    _require_soa(soa_id)
    return list_criteria(soa_id)


@router.post("/soa/{soa_id}/eligibility_criteria", status_code=201)
def api_create_criterion(soa_id: int, payload: EligibilityCriterionIn):
    _require_soa(soa_id)
    return create_criterion(soa_id, payload)


@router.put("/soa/{soa_id}/eligibility_criteria/order")
def api_reorder_criteria(soa_id: int, payload: EligibilityOrderIn):
    _require_soa(soa_id)
    return reorder_criteria(soa_id, payload.criterion_uids)


@router.patch("/soa/{soa_id}/eligibility_criteria/{criterion_id}")
def api_update_criterion(
    soa_id: int, criterion_id: int, payload: EligibilityCriterionPatch
):
    _require_soa(soa_id)
    return update_criterion(soa_id, criterion_id, payload)


@router.delete("/soa/{soa_id}/eligibility_criteria/{criterion_id}", status_code=204)
def api_delete_criterion(soa_id: int, criterion_id: int):
    _require_soa(soa_id)
    delete_criterion(soa_id, criterion_id)
    return Response(status_code=204)


# --- UI -----------------------------------------------------------------


def _page(request: Request, soa_id: int, error: str = "", form: dict = None):
    conn = _connect()
    try:
        row = conn.execute(
            "SELECT name, study_label FROM soa WHERE id=?", (soa_id,)
        ).fetchone()
    finally:
        conn.close()
    terms, _ = _categories()
    return templates.TemplateResponse(
        request,
        "eligibility_criteria.html",
        {
            "soa_id": soa_id,
            "study_name": row[0] if row else "",
            "study_label": row[1] if row else "",
            "criteria": list_criteria(soa_id),
            "category_options": sorted(
                ({"code": c, "decode": d} for c, d in terms.items()),
                key=lambda t: t["decode"],
            ),
            "codelist": ELIGIBILITY_CATEGORY_CODELIST,
            "error": error,
            "form": form or {},
        },
    )


def _back(soa_id: int) -> RedirectResponse:
    return RedirectResponse(
        f"/ui/soa/{int(soa_id)}/eligibility_criteria", status_code=303
    )


@router.get(
    "/ui/soa/{soa_id}/eligibility_criteria",
    response_class=HTMLResponse,
    name="ui_list_eligibility_criteria",
)
def ui_list_criteria(request: Request, soa_id: int):
    _require_soa(soa_id)
    return _page(request, soa_id)


@router.post("/ui/soa/{soa_id}/eligibility_criteria/create")
def ui_create_criterion(
    request: Request,
    soa_id: int,
    name: str = Form(""),
    identifier: str = Form(""),
    category_code: str = Form(""),
    text: str = Form(""),
    label: Optional[str] = Form(None),
    description: Optional[str] = Form(None),
):
    _require_soa(soa_id)
    form = {
        "name": name,
        "identifier": identifier,
        "category_code": category_code,
        "text": text,
        "label": label,
        "description": description,
    }
    try:
        create_criterion(soa_id, EligibilityCriterionIn(**form))
    except HTTPException as exc:
        return _page(request, soa_id, str(exc.detail), form)
    except sqlite3.Error:
        logger.exception("Creating eligibility criterion failed")
        return _page(request, soa_id, "Could not save the criterion", form)
    return _back(soa_id)


@router.post("/ui/soa/{soa_id}/eligibility_criteria/{criterion_id}/update")
def ui_update_criterion(
    request: Request,
    soa_id: int,
    criterion_id: int,
    name: Optional[str] = Form(None),
    identifier: Optional[str] = Form(None),
    category_code: Optional[str] = Form(None),
    text: Optional[str] = Form(None),
    label: Optional[str] = Form(None),
    description: Optional[str] = Form(None),
):
    _require_soa(soa_id)
    try:
        update_criterion(
            soa_id,
            criterion_id,
            EligibilityCriterionPatch(
                name=name,
                identifier=identifier,
                category_code=category_code,
                text=text,
                label=label if label is not None else "",
                description=description if description is not None else "",
            ),
        )
    except HTTPException as exc:
        if exc.status_code == 404:
            raise
        return _page(request, soa_id, str(exc.detail))
    return _back(soa_id)


def _move(soa_id: int, criterion_id: int, step: int) -> None:
    uids = [c["criterion_uid"] for c in list_criteria(soa_id)]
    target = _get(soa_id, criterion_id)["criterion_uid"]
    i = uids.index(target)
    j = i + step
    if 0 <= j < len(uids):
        uids[i], uids[j] = uids[j], uids[i]
        reorder_criteria(soa_id, uids)


@router.post("/ui/soa/{soa_id}/eligibility_criteria/{criterion_id}/move_up")
def ui_move_up(soa_id: int, criterion_id: int):
    _require_soa(soa_id)
    _move(soa_id, criterion_id, -1)
    return _back(soa_id)


@router.post("/ui/soa/{soa_id}/eligibility_criteria/{criterion_id}/move_down")
def ui_move_down(soa_id: int, criterion_id: int):
    _require_soa(soa_id)
    _move(soa_id, criterion_id, 1)
    return _back(soa_id)


@router.post("/ui/soa/{soa_id}/eligibility_criteria/{criterion_id}/delete")
def ui_delete_criterion(soa_id: int, criterion_id: int):
    _require_soa(soa_id)
    delete_criterion(soa_id, criterion_id)
    return _back(soa_id)
