"""Rollback re-inserts elements with new row ids; element_intervention
rows hold the integer element.id, so they must follow their element
(matched through its immutable StudyElement_N uid), and rows for
elements that the rollback removes must not be left orphaned."""

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


def _create_element(soa_id: int, uid: str, name: str) -> int:
    conn = _connect()
    try:
        cur = conn.cursor()
        cur.execute(
            "INSERT INTO element (soa_id, element_id, name, order_index)"
            " VALUES (?,?,?,?)",
            (soa_id, uid, name, int(uid.rsplit("_", 1)[1])),
        )
        conn.commit()
        return cur.lastrowid
    finally:
        conn.close()


def _link(soa_id: int, element_row_id: int, intervention_uid: str) -> None:
    conn = _connect()
    try:
        conn.execute(
            "INSERT INTO element_intervention"
            " (soa_id, element_id, intervention_uid, order_index)"
            " VALUES (?,?,?,1)",
            (soa_id, element_row_id, intervention_uid),
        )
        conn.commit()
    finally:
        conn.close()


def _links(soa_id: int) -> list:
    """(element uid, intervention uid) pairs, joined through element."""
    conn = _connect()
    try:
        cur = conn.cursor()
        cur.execute(
            "SELECT e.element_id, x.intervention_uid FROM element_intervention x"
            " LEFT JOIN element e ON e.id = x.element_id AND e.soa_id = x.soa_id"
            " WHERE x.soa_id=? ORDER BY x.intervention_uid",
            (soa_id,),
        )
        return cur.fetchall()
    finally:
        conn.close()


def test_element_interventions_follow_restored_element():
    soa_id = _create_soa("Rollback element_intervention remap")
    el = _create_element(soa_id, "StudyElement_1", "Induction")
    _link(soa_id, el, "StudyIntervention_1")
    _link(soa_id, el, "StudyIntervention_2")
    freeze_id, _ = _create_freeze(soa_id, "v1")

    _rollback_freeze(soa_id, freeze_id)

    assert _links(soa_id) == [
        ("StudyElement_1", "StudyIntervention_1"),
        ("StudyElement_1", "StudyIntervention_2"),
    ]


def test_element_interventions_survive_repeated_rollback():
    """Live ids drift from the snapshot's after the first rollback."""
    soa_id = _create_soa("Rollback element_intervention twice")
    el = _create_element(soa_id, "StudyElement_1", "Induction")
    _link(soa_id, el, "StudyIntervention_1")
    freeze_id, _ = _create_freeze(soa_id, "v1")

    _rollback_freeze(soa_id, freeze_id)
    _rollback_freeze(soa_id, freeze_id)

    assert _links(soa_id) == [("StudyElement_1", "StudyIntervention_1")]


def test_element_interventions_of_removed_element_are_dropped():
    soa_id = _create_soa("Rollback element_intervention orphans")
    el1 = _create_element(soa_id, "StudyElement_1", "Induction")
    _link(soa_id, el1, "StudyIntervention_1")
    freeze_id, _ = _create_freeze(soa_id, "v1")
    el2 = _create_element(soa_id, "StudyElement_2", "Continuation")
    _link(soa_id, el2, "StudyIntervention_2")  # added after the freeze

    _rollback_freeze(soa_id, freeze_id)

    assert _links(soa_id) == [("StudyElement_1", "StudyIntervention_1")]
