"""Shared rules and scopes embedded under several owners get unique ids.

The workbench stores one TransitionRule row that several elements and
encounters can use, and one GeographicScope row that an amendment, its
governance dates and its enrollments can all point at. USDM embeds
these objects under each owner, and every id in a document must be
unique. One owner keeps the stored uid; every other embedded copy gets
its own {Entity}_N uid from the counter, persisted in
embedded_copy_uid so it is the same in every export.
"""

import re
from collections import Counter

import pytest
from fastapi.testclient import TestClient

from soa_builder.web.app import app
from soa_builder.web.db import _connect
from usdm.generate_amendments import build_usdm_amendments
from usdm.generate_elements import build_usdm_elements
from usdm.generate_encounters import build_usdm_encounters

client = TestClient(app)

_created: list = []


@pytest.fixture(autouse=True)
def _cleanup():
    yield
    conn = _connect()
    tables = [
        r[0] for r in conn.execute("SELECT name FROM sqlite_master WHERE type='table'")
    ]
    for table in tables:
        cols = [c[1] for c in conn.execute(f"PRAGMA table_info({table})")]
        if "soa_id" in cols:
            for soa_id in _created:
                conn.execute(f"DELETE FROM {table} WHERE soa_id=?", (soa_id,))
    for soa_id in _created:
        conn.execute("DELETE FROM soa WHERE id=?", (soa_id,))
    conn.commit()
    conn.close()
    _created.clear()


@pytest.fixture
def offline_ct(monkeypatch):
    """Encounter types resolve through CDISC Library DDF CT; stub it."""
    from soa_builder.web import utils
    import usdm.generate_encounters as enc

    monkeypatch.setattr(utils, "get_ddf_ct_rows", lambda *a, **k: {"slug": ""})
    monkeypatch.setattr(
        utils, "get_ddf_ct_term", lambda *a, **k: {"submission_value": "Visit"}
    )
    monkeypatch.setattr(enc, "get_submission_value_for_code", lambda *a, **k: "")


def _new_soa(name: str) -> int:
    r = client.post("/soa", json={"name": name})
    assert r.status_code in (200, 201), r.text
    _created.append(r.json()["id"])
    return r.json()["id"]


def _exec(sql: str, args=()) -> None:
    conn = _connect()
    try:
        conn.execute(sql, args)
        conn.commit()
    finally:
        conn.close()


def _rule(soa_id: int, n: int) -> str:
    uid = f"TransitionRule_{n}"
    _exec(
        "INSERT INTO transition_rule (soa_id, transition_rule_uid, name, text,"
        " order_index) VALUES (?, ?, ?, ?, ?)",
        (soa_id, uid, f"Rule {n}", f"Rule {n} text", n),
    )
    return uid


def _element(soa_id: int, n: int, start: str, end: str) -> None:
    _exec(
        "INSERT INTO element (soa_id, element_id, name, testrl, teenrl,"
        " order_index) VALUES (?, ?, ?, ?, ?, ?)",
        (soa_id, f"StudyElement_{n}", f"E{n}", start, end, n),
    )


def _visit(soa_id: int, n: int, start: str, end: str) -> None:
    _exec(
        "INSERT INTO code_association (soa_id, code_uid, codelist_table,"
        " codelist_code, code, decode) VALUES (?, ?, 'http://www.cdisc.org',"
        " 'C188728', 'C25716', 'Visit')",
        (soa_id, f"Code_{900 + n}"),
    )
    _exec(
        "INSERT INTO visit (soa_id, name, order_index, encounter_uid, type,"
        " transitionStartRule, transitionEndRule) VALUES (?, ?, ?, ?, ?, ?, ?)",
        (soa_id, f"V{n}", n, f"Encounter_{n}", f"Code_{900 + n}", start, end),
    )


def _ids(obj, acc=None) -> Counter:
    acc = Counter() if acc is None else acc
    if isinstance(obj, dict):
        if "id" in obj and "instanceType" in obj:
            acc[obj["id"]] += 1
        for v in obj.values():
            _ids(v, acc)
    elif isinstance(obj, list):
        for v in obj:
            _ids(v, acc)
    return acc


def _rule_ids(owners: list) -> list:
    return [
        (o["transitionStartRule"] or {}).get("id")
        for o in owners
        if o.get("transitionStartRule")
    ] + [
        (o["transitionEndRule"] or {}).get("id")
        for o in owners
        if o.get("transitionEndRule")
    ]


def _number(uid: str) -> int:
    return int(re.search(r"_(\d+)$", uid).group(1))


def test_elements_sharing_a_rule_get_unique_rule_ids():
    soa_id = _new_soa("Shared rule elements")
    start, end = _rule(soa_id, 1), _rule(soa_id, 2)
    _element(soa_id, 1, start, end)
    _element(soa_id, 2, start, end)

    elements = build_usdm_elements(soa_id)

    first, second = elements
    assert first["transitionStartRule"]["id"] == start
    assert first["transitionEndRule"]["id"] == end
    copies = [second["transitionStartRule"]["id"], second["transitionEndRule"]["id"]]
    assert all(c.startswith("TransitionRule_") for c in copies)
    assert all(_number(c) > 2 for c in copies)
    assert len(set(copies)) == 2
    # The copy keeps the rule's content.
    assert second["transitionStartRule"]["text"] == "Rule 1 text"
    # Stable across exports.
    assert build_usdm_elements(soa_id) == elements


def test_elements_and_encounters_never_share_a_rule_id(offline_ct):
    soa_id = _new_soa("Shared rule encounters")
    start, end = _rule(soa_id, 1), _rule(soa_id, 2)
    _element(soa_id, 1, start, end)
    _element(soa_id, 2, start, end)
    _visit(soa_id, 1, start, None)
    _visit(soa_id, 2, None, end)

    # Encounters first: the element still keeps the stored uid.
    encounters = build_usdm_encounters(soa_id)
    elements = build_usdm_elements(soa_id)

    assert elements[0]["transitionStartRule"]["id"] == start
    ids = _rule_ids(elements) + _rule_ids(encounters)
    assert len(ids) == 6
    assert len(set(ids)) == 6
    assert _ids(elements + encounters).most_common(1)[0][1] == 1


def test_a_rule_used_only_by_encounters_keeps_its_id_on_the_first(offline_ct):
    soa_id = _new_soa("Encounter-only rule")
    rule = _rule(soa_id, 1)
    _visit(soa_id, 1, None, rule)
    _visit(soa_id, 2, rule, None)

    encounters = build_usdm_encounters(soa_id)

    assert encounters[0]["transitionEndRule"]["id"] == rule
    assert encounters[1]["transitionStartRule"]["id"] != rule


def test_new_rules_never_take_a_copy_id():
    soa_id = _new_soa("Rule after copies")
    start, end = _rule(soa_id, 1), _rule(soa_id, 2)
    _element(soa_id, 1, start, end)
    _element(soa_id, 2, start, end)
    copies = _rule_ids(build_usdm_elements(soa_id)[1:])

    r = client.post(f"/soa/{soa_id}/rules", json={"name": "Later rule"})
    assert r.status_code in (200, 201), r.text
    conn = _connect()
    (new_uid,) = conn.execute(
        "SELECT transition_rule_uid FROM transition_rule"
        " WHERE soa_id=? AND name='Later rule'",
        (soa_id,),
    ).fetchone()
    conn.close()
    assert new_uid not in copies + [start, end]


def _amendment_with_shared_scope(soa_id: int) -> None:
    _exec(
        "INSERT INTO study_amendment (soa_id, freeze_id, amendment_uid, name,"
        " number, summary) VALUES (?, 0, 'StudyAmendment_1', 'A1', '1', 's')",
        (soa_id,),
    )
    _exec(
        "INSERT INTO code_association (soa_id, code_uid, codelist_table,"
        " codelist_code, code, decode) VALUES (?, 'Code_1',"
        " '/mdr/ct/packages/ddfct-2026-03-27', 'C207412', 'C68846', 'Global')",
        (soa_id,),
    )
    _exec(
        "INSERT INTO code_association (soa_id, code_uid, codelist_table,"
        " codelist_code, code, decode) VALUES (?, 'Code_2',"
        " '/mdr/ct/packages/ddfct-2026-03-27', 'C207413', 'C215663',"
        " 'Effective Date')",
        (soa_id,),
    )
    _exec(
        "INSERT INTO amendment_geographic_scope (soa_id, amendment_uid,"
        " scope_uid, type_code_uid) VALUES (?, 'StudyAmendment_1',"
        " 'GeographicScope_1', 'Code_1')",
        (soa_id,),
    )
    _exec(
        "INSERT INTO amendment_governance_date (soa_id, amendment_uid,"
        " date_uid, name, type_code_uid, date_value) VALUES (?,"
        " 'StudyAmendment_1', 'GovernanceDate_1', 'd', 'Code_2', '2026-01-01')",
        (soa_id,),
    )
    _exec(
        "INSERT INTO governance_date_geographic_scope (soa_id, date_uid,"
        " scope_uid) VALUES (?, 'GovernanceDate_1', 'GeographicScope_1')",
        (soa_id,),
    )
    _exec(
        "INSERT INTO amendment_subject_enrollment (soa_id, amendment_uid,"
        " enrollment_uid, name, quantity_value, for_scope_uid) VALUES"
        " (?, 'StudyAmendment_1', 'SubjectEnrollment_1', 'e', 10,"
        " 'GeographicScope_1')",
        (soa_id,),
    )


def test_a_scope_reused_by_dates_and_enrollments_gets_unique_ids():
    soa_id = _new_soa("Shared scope")
    _amendment_with_shared_scope(soa_id)

    amendments = build_usdm_amendments(soa_id)

    (am,) = amendments
    own = am["geographicScopes"][0]
    on_date = am["dateValues"][0]["geographicScopes"][0]
    on_enrollment = am["enrollments"][0]["forGeographicScope"]
    assert own["id"] == "GeographicScope_1"
    assert own["type"]["id"] == "Code_1"
    for copy in (on_date, on_enrollment):
        assert copy["id"].startswith("GeographicScope_")
        assert copy["type"]["id"].startswith("Code_")
        assert copy["type"]["code"] == "C68846"
    dupes = {k: n for k, n in _ids(amendments).items() if n > 1}
    assert dupes == {}
    assert build_usdm_amendments(soa_id) == amendments


def test_new_scopes_and_codes_never_take_a_copy_id():
    soa_id = _new_soa("Scope after copies")
    _amendment_with_shared_scope(soa_id)
    am = build_usdm_amendments(soa_id)[0]
    copies = {
        am["dateValues"][0]["geographicScopes"][0]["id"],
        am["dateValues"][0]["geographicScopes"][0]["type"]["id"],
        am["enrollments"][0]["forGeographicScope"]["id"],
        am["enrollments"][0]["forGeographicScope"]["type"]["id"],
    }

    from soa_builder.web.routers.amendments.helpers import _next_scope_uid
    from soa_builder.web.utils import get_next_code_uid

    conn = _connect()
    cur = conn.cursor()
    fresh = {_next_scope_uid(cur, soa_id), get_next_code_uid(cur, soa_id)}
    conn.close()
    assert not fresh & copies
