from collections import defaultdict
from datetime import datetime, timezone
from typing import Any, Dict, List

from soa_builder.web.db import _connect
from .generate_biomedical_concept_properties import (
    build_usdm_biomedical_concept_properties_for_soa as _ensure_bcp_populated,
)

_VALID_DATATYPES = {
    "text",
    "integer",
    "float",
    "date",
    "time",
    "datetime",
    "boolean",
    "double",
    "hex",
    "base64",
    "hexBinary",
    "durationDatetime",
}
_DATATYPE_MAP = {
    "char": "text",
    "string": "text",
    "num": "float",
}


def _safe_datatype(raw: str) -> str:
    raw = (raw or "").lower().strip()
    if raw in _VALID_DATATYPES:
        return raw
    return _DATATYPE_MAP.get(raw, "text")


def _query_study(cur, soa_id: int) -> Dict:
    cur.execute(
        "SELECT name, study_id, study_label FROM soa WHERE id = ?",
        (soa_id,),
    )
    row = cur.fetchone()
    return {"name": row[0], "study_id": row[1], "study_label": row[2]}


def _query_bcs(cur, soa_id: int) -> List[Dict]:
    cur.execute(
        """
        SELECT
            bc.biomedical_concept_uid,
            bc.name,
            bc.label,
            c.code         ncit_code,
            c.decode,
            c.code_system,
            c.code_system_version
        FROM biomedical_concept bc
        LEFT JOIN alias_code a
               ON bc.code = a.alias_code_uid AND bc.soa_id = a.soa_id
        LEFT JOIN code c
               ON a.standard_code = c.code_uid AND a.soa_id = c.soa_id
        WHERE bc.soa_id = ?
        ORDER BY bc.id
        """,
        (soa_id,),
    )
    cols = [d[0] for d in cur.description]
    return [dict(zip(cols, row)) for row in cur.fetchall()]


def _query_bc_props(cur, soa_id: int) -> Dict[str, List[Dict]]:
    cur.execute(
        """
        SELECT
            bcp.biomedical_concept_uid  parent_bc_uid,
            bcp.biomedical_concept_property_uid,
            bcp.name,
            bcp.label,
            bcp.datatype,
            bcp.isRequired,
            bcp.isEnabled,
            c.code                      ncit_code,
            c.decode,
            c.code_system,
            c.code_system_version
        FROM biomedical_concept_property bcp
        LEFT JOIN alias_code a
               ON bcp.code = a.alias_code_uid AND bcp.soa_id = a.soa_id
        LEFT JOIN code c
               ON a.standard_code = c.code_uid AND a.soa_id = c.soa_id
        WHERE bcp.soa_id = ?
        ORDER BY bcp.id
        """,
        (soa_id,),
    )
    cols = [d[0] for d in cur.description]
    result: Dict[str, List[Dict]] = defaultdict(list)
    for row in cur.fetchall():
        d = dict(zip(cols, row))
        result[d["parent_bc_uid"]].append(d)
    return result


def _coding(row: Dict) -> Dict[str, Any]:
    c: Dict[str, Any] = {
        "code": row["ncit_code"],
        "codeSystem": row["code_system"],
    }
    if row.get("code_system_version"):
        c["codeSystemVersion"] = row["code_system_version"]
    if row.get("decode"):
        c["decode"] = row["decode"]
    return c


def _build_item_group(bc: Dict, props: List[Dict]) -> Dict[str, Any]:
    ig: Dict[str, Any] = {
        "OID": f"IG.{bc['biomedical_concept_uid']}",
        "name": bc["name"],
        "type": "DatasetSpecialization",
    }
    if bc.get("label"):
        ig["label"] = bc["label"]
    if bc.get("ncit_code"):
        ig["coding"] = [_coding(bc)]

    items = [_build_item(p) for p in props if p["isEnabled"]]
    if items:
        ig["items"] = items

    return ig


def _build_item(prop: Dict) -> Dict[str, Any]:
    item: Dict[str, Any] = {
        "OID": f"IT.{prop['biomedical_concept_property_uid']}",
        "name": prop["name"],
        "dataType": _safe_datatype(prop["datatype"]),
        "mandatory": bool(prop["isRequired"]),
    }
    if prop.get("label"):
        item["label"] = prop["label"]
    if prop.get("ncit_code"):
        item["coding"] = [_coding(prop)]

    return item


def build_define_json(soa_id: int) -> Dict[str, Any]:
    _ensure_bcp_populated(soa_id)

    conn = _connect()
    cur = conn.cursor()
    study = _query_study(cur, soa_id)
    bcs = _query_bcs(cur, soa_id)
    props = _query_bc_props(cur, soa_id)
    conn.close()

    study_key = study["study_id"] or soa_id
    item_groups = [
        _build_item_group(bc, props.get(bc["biomedical_concept_uid"], [])) for bc in bcs
    ]

    mdv: Dict[str, Any] = {
        "OID": f"MDV.{study_key}.001",
        "fileOID": f"FILE.{study_key}.001",
        "creationDateTime": datetime.now(timezone.utc).isoformat(),
        "odmVersion": "1.3.2",
        "fileType": "Snapshot",
        "studyOID": f"STUDY.{study_key}",
        "name": study["name"],
        "studyName": study["study_label"] or study["name"],
        "sourceSystem": "SOA Workbench",
    }
    if item_groups:
        mdv["itemGroups"] = item_groups

    return mdv
