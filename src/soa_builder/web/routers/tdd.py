"""Routes for generating SDTM Trial Design Domains (TA, TE)."""

import csv
import io
import json
import logging
import os
import re
from datetime import datetime, timezone

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

_COLUMN_DEFS: dict[str, list[dict]] = {
    "ta": [
        {
            "itemOID": "TA.STUDYID",
            "name": "STUDYID",
            "label": "Study Identifier",
            "dataType": "string",
            "length": 200,
            "keySequence": 1,
        },
        {
            "itemOID": "TA.DOMAIN",
            "name": "DOMAIN",
            "label": "Domain Abbreviation",
            "dataType": "string",
            "length": 2,
        },
        {
            "itemOID": "TA.ARMCD",
            "name": "ARMCD",
            "label": "Planned Arm Code",
            "dataType": "string",
            "length": 20,
            "keySequence": 2,
        },
        {
            "itemOID": "TA.ARM",
            "name": "ARM",
            "label": "Description of Planned Arm",
            "dataType": "string",
            "length": 200,
        },
        {
            "itemOID": "TA.TAETORD",
            "name": "TAETORD",
            "label": "Order of Element within Arm",
            "dataType": "integer",
            "keySequence": 3,
        },
        {
            "itemOID": "TA.ETCD",
            "name": "ETCD",
            "label": "Element Code",
            "dataType": "string",
            "length": 200,
        },
        {
            "itemOID": "TA.ELEMENT",
            "name": "ELEMENT",
            "label": "Description of Element",
            "dataType": "string",
            "length": 200,
        },
        {
            "itemOID": "TA.TABRANCH",
            "name": "TABRANCH",
            "label": "Branch",
            "dataType": "string",
            "length": 200,
        },
        {
            "itemOID": "TA.TATRANS",
            "name": "TATRANS",
            "label": "Transition Rule",
            "dataType": "string",
            "length": 200,
        },
        {
            "itemOID": "TA.EPOCH",
            "name": "EPOCH",
            "label": "Epoch",
            "dataType": "string",
            "length": 200,
        },
    ],
    "te": [
        {
            "itemOID": "TE.STUDYID",
            "name": "STUDYID",
            "label": "Study Identifier",
            "dataType": "string",
            "length": 200,
            "keySequence": 1,
        },
        {
            "itemOID": "TE.DOMAIN",
            "name": "DOMAIN",
            "label": "Domain Abbreviation",
            "dataType": "string",
            "length": 2,
        },
        {
            "itemOID": "TE.ETCD",
            "name": "ETCD",
            "label": "Element Code",
            "dataType": "string",
            "length": 200,
            "keySequence": 2,
        },
        {
            "itemOID": "TE.ELEMENT",
            "name": "ELEMENT",
            "label": "Description of Element",
            "dataType": "string",
            "length": 200,
        },
        {
            "itemOID": "TE.TESTRL",
            "name": "TESTRL",
            "label": "Rule for Start of Element",
            "dataType": "string",
            "length": 200,
        },
        {
            "itemOID": "TE.TEENRL",
            "name": "TEENRL",
            "label": "Rule for End of Element",
            "dataType": "string",
            "length": 200,
        },
        {
            "itemOID": "TE.TEDUR",
            "name": "TEDUR",
            "label": "Planned Duration of Element",
            "dataType": "string",
            "length": 200,
        },
    ],
    "tv": [
        {
            "itemOID": "TV.STUDYID",
            "name": "STUDYID",
            "label": "Study Identifier",
            "dataType": "string",
            "length": 200,
            "keySequence": 1,
        },
        {
            "itemOID": "TV.DOMAIN",
            "name": "DOMAIN",
            "label": "Domain Abbreviation",
            "dataType": "string",
            "length": 2,
        },
        {
            "itemOID": "TV.VISITNUM",
            "name": "VISITNUM",
            "label": "Visit Number",
            "dataType": "decimal",
            "displayFormat": "8.1",
            "keySequence": 2,
        },
        {
            "itemOID": "TV.VISIT",
            "name": "VISIT",
            "label": "Visit Name",
            "dataType": "string",
            "length": 90,
        },
        {
            "itemOID": "TV.VISITDY",
            "name": "VISITDY",
            "label": "Planned Study Day of Visit",
            "dataType": "integer",
        },
        {
            "itemOID": "TV.ARMCD",
            "name": "ARMCD",
            "label": "Planned Arm Code",
            "dataType": "string",
            "length": 8,
        },
        {
            "itemOID": "TV.ARM",
            "name": "ARM",
            "label": "Description of Planned Arm",
            "dataType": "string",
            "length": 200,
        },
        {
            "itemOID": "TV.TVSTRL",
            "name": "TVSTRL",
            "label": "Visit Start Rule",
            "dataType": "string",
            "length": 200,
        },
        {
            "itemOID": "TV.TVENRL",
            "name": "TVENRL",
            "label": "Visit End Rule",
            "dataType": "string",
            "length": 200,
        },
    ],
}

_NUMERIC_TYPES = {"integer", "decimal", "float"}


def _to_dataset_json(domain: str, records: list[dict]) -> dict:
    """Wrap list[dict] records in a dataset-json 1.1 envelope."""
    col_defs = _COLUMN_DEFS[domain]
    col_names = [c["name"] for c in col_defs]
    study_id = records[0]["STUDYID"] if records else ""
    domain_upper = domain.upper()
    domain_label = next(d[1] for d in _DOMAINS if d[0] == domain)

    def _coerce(col_def, val):
        if col_def["dataType"] in _NUMERIC_TYPES:
            if val == "" or val is None:
                return None
            try:
                if col_def["dataType"] == "integer":
                    return int(val)
                return float(val)
            except (ValueError, TypeError):
                return None
        return val

    rows = [
        [_coerce(col_defs[i], rec.get(name, "")) for i, name in enumerate(col_names)]
        for rec in records
    ]
    file_oid = f"{study_id}.{domain}" if study_id else domain
    return {
        "datasetJSONCreationDateTime": datetime.now(timezone.utc).strftime(
            "%Y-%m-%dT%H:%M:%S"
        ),
        "datasetJSONVersion": "1.1.0",
        "fileOID": file_oid,
        "studyOID": study_id,
        "itemGroupOID": domain_upper,
        "records": len(rows),
        "name": domain_upper,
        "label": domain_label,
        "columns": col_defs,
        "rows": rows,
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
    payload = json.dumps(_to_dataset_json(domain, data), indent=2) + "\n"
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
