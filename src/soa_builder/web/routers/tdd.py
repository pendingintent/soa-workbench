"""Routes for generating SDTM Trial Design Domains (TA, TE)."""

import csv
import io
import json
import logging
import os
import re

from fastapi import APIRouter, HTTPException, Request
from fastapi.responses import HTMLResponse, StreamingResponse
from fastapi.templating import Jinja2Templates

from ..utils import soa_exists

router = APIRouter()
logger = logging.getLogger("soa_builder.web.routers.tdd")
templates = Jinja2Templates(
    directory=os.path.join(os.path.dirname(__file__), "..", "templates")
)

_DOMAINS = [
    ("ta", "Trial Arms", "ta.json", "ta.csv"),
    ("te", "Trial Elements", "te.json", "te.csv"),
    ("tv", "Trial Visits", "tv.json", "tv.csv"),
]

_FIELDNAMES: dict[str, list[str]] = {
    "ta": [
        "STUDYID",
        "DOMAIN",
        "ARMCD",
        "ARM",
        "TAETORD",
        "ETCD",
        "ELEMENT",
        "TABRANCH",
        "TATRANS",
        "EPOCH",
    ],
    "te": ["STUDYID", "DOMAIN", "ETCD", "ELEMENT", "TESTRL", "TEENRL", "TEDUR"],
    "tv": [
        "STUDYID",
        "DOMAIN",
        "VISITNUM",
        "VISIT",
        "VISITDY",
        "ARMCD",
        "ARM",
        "TVSTRL",
        "TVENRL",
    ],
}


_UNICODE_ESCAPE_RE = re.compile(r"\\u([0-9a-fA-F]{4})")


def _decode_unicode_escapes(rows: list[dict]) -> list[dict]:
    """Replace literal \\uXXXX sequences with their Unicode characters."""

    def _fix(v):
        if isinstance(v, str):
            return _UNICODE_ESCAPE_RE.sub(lambda m: chr(int(m.group(1), 16)), v)
        return v

    return [{k: _fix(v) for k, v in row.items()} for row in rows]


def _build(domain: str, soa_id: int) -> list[dict]:
    """Delegate to the appropriate SDTM TDD generator."""
    if domain == "ta":
        from sdtm.generate_ta import build_sdtm_ta

        return build_sdtm_ta(soa_id)
    if domain == "te":
        from sdtm.generate_te import build_sdtm_te

        return build_sdtm_te(soa_id)
    if domain == "tv":
        from sdtm.generate_tv import build_sdtm_tv

        return build_sdtm_tv(soa_id)
    raise ValueError(f"Unknown domain: {domain}")


@router.get("/ui/soa/{soa_id}/tdd", response_class=HTMLResponse)
def ui_tdd(request: Request, soa_id: int):
    if not soa_exists(soa_id):
        raise HTTPException(404, "SOA not found")
    return templates.TemplateResponse(
        request,
        "tdd.html",
        {"soa_id": soa_id, "domains": _DOMAINS},
    )


@router.get("/soa/{soa_id}/tdd/{domain}/json")
def download_tdd_json(soa_id: int, domain: str):
    if not soa_exists(soa_id):
        raise HTTPException(404, "SOA not found")
    valid_keys = {d[0] for d in _DOMAINS}
    if domain not in valid_keys:
        raise HTTPException(400, f"Unknown domain '{domain}'")
    try:
        data = _build(domain, soa_id)
    except Exception as exc:
        logger.exception("Failed to build TDD domain %s for soa_id=%s", domain, soa_id)
        raise HTTPException(500, f"Failed to generate {domain}: {exc}") from exc
    filename = next(d[2] for d in _DOMAINS if d[0] == domain)
    payload = json.dumps(data, indent=2) + "\n"
    buf = io.BytesIO(payload.encode("utf-8"))
    return StreamingResponse(
        buf,
        media_type="application/json",
        headers={"Content-Disposition": f'attachment; filename="{filename}"'},
    )


@router.get("/ui/soa/{soa_id}/tdd/{domain}/view", response_class=HTMLResponse)
def ui_tdd_domain_view(request: Request, soa_id: int, domain: str):
    """Return an HTML partial rendering domain rows in a styled table."""
    if not soa_exists(soa_id):
        raise HTTPException(404, "SOA not found")
    valid_keys = {d[0] for d in _DOMAINS}
    if domain not in valid_keys:
        raise HTTPException(404, f"Unknown domain '{domain}'")
    try:
        rows = _decode_unicode_escapes(_build(domain, soa_id))
    except Exception as exc:
        logger.exception("Failed to build TDD domain %s for soa_id=%s", domain, soa_id)
        raise HTTPException(500, f"Failed to generate {domain}: {exc}") from exc
    return templates.TemplateResponse(
        request,
        "tdd_view_partial.html",
        {
            "soa_id": soa_id,
            "domain": domain.upper(),
            "fieldnames": _FIELDNAMES[domain],
            "rows": rows,
        },
    )


@router.get("/soa/{soa_id}/tdd/{domain}/csv")
def download_tdd_csv(soa_id: int, domain: str):
    if not soa_exists(soa_id):
        raise HTTPException(404, "SOA not found")
    valid_keys = {d[0] for d in _DOMAINS}
    if domain not in valid_keys:
        raise HTTPException(400, f"Unknown domain '{domain}'")
    try:
        data = _build(domain, soa_id)
    except Exception as exc:
        logger.exception("Failed to build TDD domain %s for soa_id=%s", domain, soa_id)
        raise HTTPException(500, f"Failed to generate {domain}: {exc}") from exc
    filename = next(d[3] for d in _DOMAINS if d[0] == domain)
    fieldnames = _FIELDNAMES[domain]
    out = io.StringIO()
    writer = csv.DictWriter(out, fieldnames=fieldnames)
    writer.writeheader()
    writer.writerows(data)
    buf = io.BytesIO(out.getvalue().encode("utf-8"))
    return StreamingResponse(
        buf,
        media_type="text/csv",
        headers={"Content-Disposition": f'attachment; filename="{filename}"'},
    )
