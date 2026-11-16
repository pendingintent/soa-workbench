"""Rollback re-inserts activities with new ids; rows keyed on the old
activity id (DSS and CRF assignments) must follow them, and rows for
activities that the rollback removes must not be left orphaned."""

from fastapi.testclient import TestClient

from soa_builder.web.app import app
from soa_builder.web.db import _connect
from soa_builder.web.routers._freeze_helpers import (
    _create_freeze,
    _rollback_freeze,
)

client = TestClient(app)


def _create_soa(name: str) -> int:
    r = client.post("/soa", json={"name": name})
    assert r.status_code in (200, 201), r.text
    return r.json()["id"]


def _create_activity(soa_id: int, name: str) -> int:
    r = client.post(f"/soa/{soa_id}/activities", json={"name": name})
    assert r.status_code in (200, 201), r.text
    return r.json()["activity_id"]


def _assign(soa_id: int, activity_id: int, title: str) -> None:
    conn = _connect()
    try:
        conn.execute(
            "INSERT INTO activity_concept_dss"
            " (soa_id, activity_id, concept_code, dss_title, dss_href)"
            " VALUES (?,?,?,?,?)",
            (soa_id, activity_id, "C49676", f"DSS {title}", f"/dss/{title}"),
        )
        conn.execute(
            "INSERT INTO activity_concept_crf"
            " (soa_id, activity_id, concept_code, crf_title, crf_href)"
            " VALUES (?,?,?,?,?)",
            (soa_id, activity_id, "C49676", f"CRF {title}", f"/crf/{title}"),
        )
        conn.commit()
    finally:
        conn.close()


def _assignments(soa_id: int, table: str) -> list:
    """(activity name, title) pairs, joined through the live activity."""
    title_col = "dss_title" if table == "activity_concept_dss" else "crf_title"
    conn = _connect()
    try:
        cur = conn.cursor()
        cur.execute(
            f"SELECT a.name, t.{title_col} FROM {table} t"
            " LEFT JOIN activity a ON a.id = t.activity_id"
            " WHERE t.soa_id=? ORDER BY t.id",
            (soa_id,),
        )
        return cur.fetchall()
    finally:
        conn.close()


def test_dss_and_crf_follow_restored_activity():
    soa_id = _create_soa("Rollback DSS/CRF remap")
    vitals = _create_activity(soa_id, "Vitals")
    _assign(soa_id, vitals, "vitals")
    freeze_id, _ = _create_freeze(soa_id, "v1")

    _rollback_freeze(soa_id, freeze_id)

    assert _assignments(soa_id, "activity_concept_dss") == [("Vitals", "DSS vitals")]
    assert _assignments(soa_id, "activity_concept_crf") == [("Vitals", "CRF vitals")]


def test_assignments_survive_repeated_rollback():
    """Live ids drift from the snapshot's after the first rollback."""
    soa_id = _create_soa("Rollback DSS/CRF twice")
    vitals = _create_activity(soa_id, "Vitals")
    _assign(soa_id, vitals, "vitals")
    freeze_id, _ = _create_freeze(soa_id, "v1")

    _rollback_freeze(soa_id, freeze_id)
    _rollback_freeze(soa_id, freeze_id)

    assert _assignments(soa_id, "activity_concept_dss") == [("Vitals", "DSS vitals")]
    assert _assignments(soa_id, "activity_concept_crf") == [("Vitals", "CRF vitals")]


def test_assignments_of_removed_activity_are_dropped():
    soa_id = _create_soa("Rollback DSS/CRF orphans")
    _create_activity(soa_id, "Vitals")
    freeze_id, _ = _create_freeze(soa_id, "v1")
    labs = _create_activity(soa_id, "Labs")  # added after the freeze
    _assign(soa_id, labs, "labs")

    _rollback_freeze(soa_id, freeze_id)

    assert _assignments(soa_id, "activity_concept_dss") == []
    assert _assignments(soa_id, "activity_concept_crf") == []
