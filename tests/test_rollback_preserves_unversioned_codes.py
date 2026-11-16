"""Rollback must not orphan code rows owned by entities it never restores.

Rollback resets the SoA's ``code`` / ``alias_code`` rows so concept
codes can be rebuilt. Study titles, identifiers, interventions,
indications, organizations, roles and amendments also store their
codes in ``code`` but are not part of the freeze snapshot, so wiping
those rows leaves them pointing at nothing.
"""

from fastapi.testclient import TestClient

from soa_builder.web.app import app
from soa_builder.web.db import _connect
from soa_builder.web.routers._freeze_helpers import (
    _create_freeze,
    _delete_bc_code_chain,
    _rollback_freeze,
)

client = TestClient(app)


def _create_soa(name: str) -> int:
    r = client.post("/soa", json={"name": name})
    assert r.status_code in (200, 201), r.text
    return r.json()["id"]


def _title_type_code(soa_id: int, title_uid: str):
    conn = _connect()
    try:
        cur = conn.cursor()
        cur.execute(
            "SELECT c.code FROM study_title st"
            " LEFT JOIN code c"
            "   ON c.code_uid = st.type_code_uid AND c.soa_id = st.soa_id"
            " WHERE st.soa_id=? AND st.study_title_uid=?",
            (soa_id, title_uid),
        )
        row = cur.fetchone()
    finally:
        conn.close()
    return row


def test_rollback_keeps_study_title_type_code():
    soa_id = _create_soa("Rollback keeps title codes")
    r = client.post(
        f"/soa/{soa_id}/titles",
        params={
            "text": "Official title",
            "type_concept_id": "C99905x2",
            "type_preferred_term": "Official Study Title",
            "type_version": "2022-09-30",
        },
    )
    assert r.status_code == 201, r.text
    title_uid = r.json()["study_title_uid"]
    assert _title_type_code(soa_id, title_uid) == ("C99905x2",)

    freeze_id, _ = _create_freeze(soa_id, "v1")
    _rollback_freeze(soa_id, freeze_id)

    assert _title_type_code(soa_id, title_uid) == ("C99905x2",)


def test_delete_bc_code_chain_scopes_to_bc_codes():
    """BC-only codes go; a code shared with a title survives."""
    soa_id = _create_soa("BC code chain scope")
    conn = _connect()
    try:
        cur = conn.cursor()
        cur.executemany(
            "INSERT INTO code (soa_id, code_uid, code) VALUES (?,?,?)",
            [
                (soa_id, "Code_1", "C100"),  # BC only
                (soa_id, "Code_2", "C200"),  # BC + study title
                (soa_id, "Code_3", "C300"),  # title only
            ],
        )
        cur.executemany(
            "INSERT INTO alias_code (soa_id, alias_code_uid, standard_code)"
            " VALUES (?,?,?)",
            [
                (soa_id, "AliasCode_1", "Code_1"),
                (soa_id, "AliasCode_2", "Code_2"),
            ],
        )
        cur.executemany(
            "INSERT INTO biomedical_concept"
            " (soa_id, biomedical_concept_uid, name, code) VALUES (?,?,?,?)",
            [
                (soa_id, "BiomedicalConcept_1", "BC one", "AliasCode_1"),
                (soa_id, "BiomedicalConcept_2", "BC two", "AliasCode_2"),
            ],
        )
        cur.executemany(
            "INSERT INTO study_title"
            " (soa_id, study_title_uid, text, type_code_uid) VALUES (?,?,?,?)",
            [
                (soa_id, "StudyTitle_1", "Shared", "Code_2"),
                (soa_id, "StudyTitle_2", "Own", "Code_3"),
            ],
        )

        _delete_bc_code_chain(cur, soa_id)

        cur.execute("SELECT code_uid FROM code WHERE soa_id=?", (soa_id,))
        assert {r[0] for r in cur.fetchall()} == {"Code_2", "Code_3"}
        cur.execute("SELECT COUNT(*) FROM alias_code WHERE soa_id=?", (soa_id,))
        assert cur.fetchone()[0] == 0
    finally:
        conn.rollback()
        conn.close()
