import logging

from fastapi import APIRouter, Form, HTTPException, Request
from fastapi.responses import HTMLResponse, JSONResponse

from ..audit import _record_footnote_audit
from ..db import _connect
from ..schemas import FootnoteCreate, FootnoteUpdate
from ..utils import soa_exists

router = APIRouter(prefix="/soa/{soa_id}")
ui_router = APIRouter()
logger = logging.getLogger("soa_builder.web.routers.footnotes")


def _next_footnote_uid(soa_id: int) -> str:
    """Return next Footnote_N UID, never reusing deleted UIDs."""
    conn = _connect()
    cur = conn.cursor()
    cur.execute("SELECT MAX(id) FROM footnote WHERE soa_id=?", (soa_id,))
    row = cur.fetchone()
    live_max = row[0] or 0
    cur.execute("SELECT MAX(footnote_id) FROM footnote_audit WHERE soa_id=?", (soa_id,))
    row = cur.fetchone()
    audit_max = row[0] or 0
    conn.close()
    return f"Footnote_{max(live_max, audit_max) + 1}"


def _row_to_dict(row) -> dict:
    keys = [
        "id",
        "soa_id",
        "footnote_uid",
        "name",
        "label",
        "description",
        "text",
        "dictionary_uid",
    ]
    return dict(zip(keys, row))


# ---------------------------------------------------------------------------
# JSON API endpoints
# ---------------------------------------------------------------------------


@router.get("/footnotes", response_class=JSONResponse)
def list_footnotes(soa_id: int):
    if not soa_exists(soa_id):
        raise HTTPException(404, "SOA not found")
    conn = _connect()
    cur = conn.cursor()
    cur.execute(
        "SELECT id,soa_id,footnote_uid,name,label,description,text,dictionary_uid FROM footnote WHERE soa_id=? ORDER BY id",
        (soa_id,),
    )
    rows = [_row_to_dict(r) for r in cur.fetchall()]
    conn.close()
    return JSONResponse(rows)


@router.post("/footnotes", response_class=JSONResponse)
def create_footnote(soa_id: int, body: FootnoteCreate):
    if not soa_exists(soa_id):
        raise HTTPException(404, "SOA not found")
    uid = _next_footnote_uid(soa_id)
    conn = _connect()
    cur = conn.cursor()
    cur.execute(
        "INSERT INTO footnote (soa_id, footnote_uid, name, label, description, text, dictionary_uid) VALUES (?,?,?,?,?,?,?)",
        (
            soa_id,
            uid,
            body.name,
            body.label or None,
            body.description or None,
            body.text or None,
            body.dictionary_uid or None,
        ),
    )
    conn.commit()
    footnote_id = cur.lastrowid
    after = {
        "footnote_uid": uid,
        "name": body.name,
        "label": body.label,
        "description": body.description,
        "text": body.text,
        "dictionary_uid": body.dictionary_uid,
    }
    conn.close()
    _record_footnote_audit(soa_id, "create", footnote_id, before=None, after=after)
    return JSONResponse(
        {"id": footnote_id, "footnote_uid": uid, **after}, status_code=201
    )


@router.patch("/footnotes/{footnote_id}", response_class=JSONResponse)
def update_footnote(
    soa_id: int,
    footnote_id: int,
    body: FootnoteUpdate,
):
    if not soa_exists(soa_id):
        raise HTTPException(404, "SOA not found")
    conn = _connect()
    cur = conn.cursor()
    cur.execute(
        "SELECT id,soa_id,footnote_uid,name,label,description,text,dictionary_uid FROM footnote WHERE id=? AND soa_id=?",
        (footnote_id, soa_id),
    )
    row = cur.fetchone()
    if not row:
        conn.close()
        raise HTTPException(404, "Footnote not found")
    before = _row_to_dict(row)
    new_name = body.name if body.name is not None else before["name"]
    new_label = body.label if body.label is not None else before["label"]
    new_desc = (
        body.description if body.description is not None else before["description"]
    )
    new_text = body.text if body.text is not None else before["text"]
    new_dict_uid = (
        body.dictionary_uid
        if body.dictionary_uid is not None
        else before["dictionary_uid"]
    )
    cur.execute(
        "UPDATE footnote SET name=?, label=?, description=?, text=?, dictionary_uid=? WHERE id=? AND soa_id=?",
        (
            new_name,
            new_label or None,
            new_desc or None,
            new_text or None,
            new_dict_uid or None,
            footnote_id,
            soa_id,
        ),
    )
    conn.commit()
    conn.close()
    after = {
        **before,
        "name": new_name,
        "label": new_label,
        "description": new_desc,
        "text": new_text,
        "dictionary_uid": new_dict_uid,
    }
    _record_footnote_audit(soa_id, "update", footnote_id, before=before, after=after)
    return JSONResponse(after)


@router.delete("/footnotes/{footnote_id}", response_class=JSONResponse)
def delete_footnote(soa_id: int, footnote_id: int):
    if not soa_exists(soa_id):
        raise HTTPException(404, "SOA not found")
    conn = _connect()
    cur = conn.cursor()
    cur.execute(
        "SELECT id,soa_id,footnote_uid,name,label,description,text,dictionary_uid FROM footnote WHERE id=? AND soa_id=?",
        (footnote_id, soa_id),
    )
    row = cur.fetchone()
    if not row:
        conn.close()
        raise HTTPException(404, "Footnote not found")
    before = _row_to_dict(row)
    cur.execute("DELETE FROM footnote WHERE id=? AND soa_id=?", (footnote_id, soa_id))
    conn.commit()
    conn.close()
    _record_footnote_audit(soa_id, "delete", footnote_id, before=before, after=None)
    return JSONResponse({"deleted": footnote_id})


# ---------------------------------------------------------------------------
# UI form endpoints
# ---------------------------------------------------------------------------


@ui_router.post("/ui/soa/{soa_id}/footnotes/create", response_class=HTMLResponse)
def ui_create_footnote(
    request: Request,
    soa_id: int,
    name: str = Form(...),
    label: str | None = Form(None),
    description: str | None = Form(None),
    text: str | None = Form(None),
    dictionary_uid: str | None = Form(None),
):
    if not soa_exists(soa_id):
        raise HTTPException(404, "SOA not found")
    create_footnote(
        soa_id,
        FootnoteCreate(
            name=name,
            label=label,
            description=description,
            text=text,
            dictionary_uid=dictionary_uid,
        ),
    )
    redirect_url = f"/ui/soa/{soa_id}/edit"
    if request.headers.get("HX-Request") == "true":
        return HTMLResponse("", headers={"HX-Redirect": redirect_url})
    return HTMLResponse(f"<script>window.location='{redirect_url}';</script>")


@ui_router.post(
    "/ui/soa/{soa_id}/footnotes/{footnote_id}/update", response_class=HTMLResponse
)
def ui_update_footnote(
    request: Request,
    soa_id: int,
    footnote_id: int,
    name: str | None = Form(None),
    label: str | None = Form(None),
    description: str | None = Form(None),
    text: str | None = Form(None),
    dictionary_uid: str | None = Form(None),
):
    if not soa_exists(soa_id):
        raise HTTPException(404, "SOA not found")
    update_footnote(
        soa_id,
        footnote_id,
        FootnoteUpdate(
            name=name,
            label=label,
            description=description,
            text=text,
            dictionary_uid=dictionary_uid,
        ),
    )
    redirect_url = f"/ui/soa/{soa_id}/edit"
    if request.headers.get("HX-Request") == "true":
        return HTMLResponse("", headers={"HX-Redirect": redirect_url})
    return HTMLResponse(f"<script>window.location='{redirect_url}';</script>")


@ui_router.post(
    "/ui/soa/{soa_id}/footnotes/{footnote_id}/delete", response_class=HTMLResponse
)
def ui_delete_footnote(
    request: Request,
    soa_id: int,
    footnote_id: int,
):
    if not soa_exists(soa_id):
        raise HTTPException(404, "SOA not found")
    delete_footnote(soa_id, footnote_id)
    redirect_url = f"/ui/soa/{soa_id}/edit"
    if request.headers.get("HX-Request") == "true":
        return HTMLResponse("", headers={"HX-Redirect": redirect_url})
    return HTMLResponse(f"<script>window.location='{redirect_url}';</script>")
