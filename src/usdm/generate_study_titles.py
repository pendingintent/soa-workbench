"""USDM StudyTitle generator."""

from typing import Any, Dict, List

from soa_builder.web.app import _connect


def build_usdm_titles(soa_id: int) -> List[Dict[str, Any]]:
    """Build StudyTitle-Output list from DB for the given SOA."""
    conn = _connect()
    cur = conn.cursor()
    cur.execute(
        "SELECT study_title_uid, text, type_code_uid "
        "FROM study_title WHERE soa_id=? ORDER BY order_index, id",
        (soa_id,),
    )
    rows = cur.fetchall()
    conn.close()

    if not rows:
        return _fallback_title(soa_id)

    return [
        {
            "id": uid,
            "extensionAttributes": [],
            "text": text or "",
            "type": _build_type_code(soa_id, type_code_uid),
            "instanceType": "StudyTitle",
        }
        for uid, text, type_code_uid in rows
    ]


def _build_type_code(soa_id: int, code_uid: str | None) -> Dict[str, Any]:
    """Build a USDM Code-Output from the code table (conceptId + preferredTerm)."""
    empty: Dict[str, Any] = {
        "id": code_uid or "Code_unknown",
        "extensionAttributes": [],
        "code": "",
        "codeSystem": "",
        "codeSystemVersion": "",
        "decode": "",
        "instanceType": "Code",
    }
    if not code_uid:
        return empty

    conn = _connect()
    cur = conn.cursor()
    cur.execute(
        "SELECT code, code_system, code_system_version, decode "
        "FROM code WHERE soa_id=? AND code_uid=? LIMIT 1",
        (soa_id, code_uid),
    )
    row = cur.fetchone()
    conn.close()

    if not row:
        return empty

    code, cs, csv, decode = row
    return {
        "id": code_uid,
        "extensionAttributes": [],
        "code": code or "",
        "codeSystem": cs or "http://www.cdisc.org",
        "codeSystemVersion": csv or "",
        "decode": decode or "",
        "instanceType": "Code",
    }


def _fallback_title(soa_id: int) -> List[Dict[str, Any]]:
    """Minimal placeholder used when no titles are stored (USDM requires ≥1)."""
    conn = _connect()
    cur = conn.cursor()
    cur.execute("SELECT name, study_label FROM soa WHERE id=?", (soa_id,))
    row = cur.fetchone()
    conn.close()
    name = (row[1] or row[0] or "") if row else ""
    return [
        {
            "id": "StudyTitle_1",
            "extensionAttributes": [],
            "text": name,
            "type": {
                "id": "Code_StudyTitleType",
                "extensionAttributes": [],
                "code": "C99905x2",
                "codeSystem": "http://www.cdisc.org",
                "codeSystemVersion": "",
                "decode": "Official Study Title",
                "instanceType": "Code",
            },
            "instanceType": "StudyTitle",
        }
    ]
