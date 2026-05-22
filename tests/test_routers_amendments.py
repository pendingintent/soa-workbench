"""Tests for the amendments router (API + UI + audit + USDM generator)."""

from unittest.mock import patch

from fastapi.testclient import TestClient

from soa_builder.web.app import app
from soa_builder.web.db import _connect

client = TestClient(app)

_SLUG_PATCH = "soa_builder.web.routers.amendments.get_latest_ddf_ct_href"
_CT_MAP_PATCH = "soa_builder.web.routers.freezes.get_ddf_ct_codelist_map"


def _new_soa(name: str) -> int:
    r = client.post("/soa", json={"name": name})
    return r.json()["id"]


def _freeze(soa_id: int, label: str = "") -> int:
    client.post(
        f"/ui/soa/{soa_id}/freeze",
        data={"version_label": label},
    )
    conn = _connect()
    cur = conn.cursor()
    cur.execute(
        "SELECT id FROM soa_freeze WHERE soa_id=? ORDER BY id DESC LIMIT 1",
        (soa_id,),
    )
    row = cur.fetchone()
    cur.close()
    conn.rollback()
    conn.close()
    return row[0]


def _db_query_one(sql: str, params: tuple):
    """Run a single-row SELECT and return the row (or None), closing properly."""
    conn = _connect()
    cur = conn.cursor()
    cur.execute(sql, params)
    row = cur.fetchone()
    cur.close()
    conn.rollback()
    conn.close()
    return row


def _db_query_all(sql: str, params: tuple):
    """Run a multi-row SELECT and return the rows, closing properly."""
    conn = _connect()
    cur = conn.cursor()
    cur.execute(sql, params)
    rows = cur.fetchall()
    cur.close()
    conn.rollback()
    conn.close()
    return rows


# ---------------------------------------------------------------------------
# Freeze without amendment
# ---------------------------------------------------------------------------


def test_freeze_without_amendment_creates_no_amendment():
    soa_id = _new_soa("Freeze No Amendment")
    freeze_id = _freeze(soa_id, "v1")
    row = _db_query_one(
        "SELECT id FROM study_amendment WHERE freeze_id=?", (freeze_id,)
    )
    assert row is None


# ---------------------------------------------------------------------------
# Create amendment via JSON API
# ---------------------------------------------------------------------------


@patch(_SLUG_PATCH, return_value="ddfct-2024-01-01")
def test_create_amendment_api(_mock):
    soa_id = _new_soa("Amendment API Create")
    freeze_id = _freeze(soa_id, "v1")
    resp = client.post(
        f"/soa/{soa_id}/freeze/{freeze_id}/amendment",
        json={
            "name": "Safety Update",
            "number": "1",
            "summary": "Minor safety update",
            "primary_reason_code": "C1234",
        },
    )
    assert resp.status_code == 201
    body = resp.json()
    assert body["amendment_uid"] == "StudyAmendment_1"

    code_row = _db_query_one(
        "SELECT code FROM code_association WHERE soa_id=? AND code='C1234'",
        (soa_id,),
    )
    assert code_row is not None

    am_row = _db_query_one(
        "SELECT amendment_uid,name FROM study_amendment WHERE freeze_id=?",
        (freeze_id,),
    )
    assert am_row[0] == "StudyAmendment_1"
    assert am_row[1] == "Safety Update"


@patch(_SLUG_PATCH, return_value="ddfct-2024-01-01")
def test_create_amendment_duplicate_returns_409(_mock):
    soa_id = _new_soa("Amendment Duplicate")
    freeze_id = _freeze(soa_id, "v1")
    payload = {
        "name": "X",
        "number": "1",
        "summary": "s",
        "primary_reason_code": "C1234",
    }
    r1 = client.post(f"/soa/{soa_id}/freeze/{freeze_id}/amendment", json=payload)
    assert r1.status_code == 201
    r2 = client.post(f"/soa/{soa_id}/freeze/{freeze_id}/amendment", json=payload)
    assert r2.status_code == 409


def test_create_amendment_missing_primary_reason_returns_422():
    soa_id = _new_soa("Amendment Missing Reason")
    freeze_id = _freeze(soa_id, "v1")
    resp = client.post(
        f"/soa/{soa_id}/freeze/{freeze_id}/amendment",
        json={"name": "X", "number": "1", "summary": "s"},
    )
    assert resp.status_code == 422


@patch(_SLUG_PATCH, return_value="ddfct-2024-01-01")
def test_create_amendment_c17649_requires_other_reason(_mock):
    soa_id = _new_soa("Amendment Other Reason Required")
    freeze_id = _freeze(soa_id, "v1")
    resp = client.post(
        f"/soa/{soa_id}/freeze/{freeze_id}/amendment",
        json={
            "name": "X",
            "number": "1",
            "summary": "s",
            "primary_reason_code": "C17649",
        },
    )
    assert resp.status_code == 422


@patch(_SLUG_PATCH, return_value="ddfct-2024-01-01")
def test_create_amendment_c17649_with_other_reason_succeeds(_mock):
    soa_id = _new_soa("Amendment C17649 With Other")
    freeze_id = _freeze(soa_id, "v1")
    resp = client.post(
        f"/soa/{soa_id}/freeze/{freeze_id}/amendment",
        json={
            "name": "X",
            "number": "1",
            "summary": "s",
            "primary_reason_code": "C17649",
            "primary_reason_other": "Regulatory request",
        },
    )
    assert resp.status_code == 201


# ---------------------------------------------------------------------------
# Secondary reasons
# ---------------------------------------------------------------------------


@patch(_SLUG_PATCH, return_value="ddfct-2024-01-01")
def test_add_and_remove_secondary_reason(_mock):
    soa_id = _new_soa("Secondary Reasons")
    freeze_id = _freeze(soa_id, "v1")
    am_resp = client.post(
        f"/soa/{soa_id}/freeze/{freeze_id}/amendment",
        json={
            "name": "X",
            "number": "1",
            "summary": "s",
            "primary_reason_code": "C111",
        },
    )
    amendment_id = am_resp.json()["id"]

    add_resp = client.post(
        f"/soa/{soa_id}/amendment/{amendment_id}/reasons",
        json={"code": "C222"},
    )
    assert add_resp.status_code == 201
    reason_id = add_resp.json()["id"]
    assert add_resp.json()["reason_uid"] == "StudyAmendmentReason_2"

    del_resp = client.delete(
        f"/soa/{soa_id}/amendment/{amendment_id}/reason/{reason_id}"
    )
    assert del_resp.status_code == 204

    row = _db_query_one(
        "SELECT id FROM study_amendment_reason WHERE id=?", (reason_id,)
    )
    assert row is None


@patch(_SLUG_PATCH, return_value="ddfct-2024-01-01")
def test_secondary_reason_c17649_requires_other(_mock):
    soa_id = _new_soa("Secondary C17649")
    freeze_id = _freeze(soa_id, "v1")
    am_resp = client.post(
        f"/soa/{soa_id}/freeze/{freeze_id}/amendment",
        json={
            "name": "X",
            "number": "1",
            "summary": "s",
            "primary_reason_code": "C111",
        },
    )
    amendment_id = am_resp.json()["id"]
    resp = client.post(
        f"/soa/{soa_id}/amendment/{amendment_id}/reasons",
        json={"code": "C17649"},
    )
    assert resp.status_code == 422


@patch(_SLUG_PATCH, return_value="ddfct-2024-01-01")
def test_cannot_delete_primary_reason(_mock):
    soa_id = _new_soa("Cannot Delete Primary")
    freeze_id = _freeze(soa_id, "v1")
    am_resp = client.post(
        f"/soa/{soa_id}/freeze/{freeze_id}/amendment",
        json={
            "name": "X",
            "number": "1",
            "summary": "s",
            "primary_reason_code": "C111",
        },
    )
    amendment_id = am_resp.json()["id"]
    amendment_uid = am_resp.json()["amendment_uid"]
    row = _db_query_one(
        "SELECT id FROM study_amendment_reason "
        "WHERE role='primary' AND soa_id=? AND amendment_uid=?",
        (soa_id, amendment_uid),
    )
    primary_id = row[0]
    resp = client.delete(f"/soa/{soa_id}/amendment/{amendment_id}/reason/{primary_id}")
    assert resp.status_code == 400


# ---------------------------------------------------------------------------
# Impacts
# ---------------------------------------------------------------------------


@patch(_SLUG_PATCH, return_value="ddfct-2024-01-01")
def test_add_and_remove_impact(_mock):
    soa_id = _new_soa("Amendment Impacts")
    freeze_id = _freeze(soa_id, "v1")
    am_resp = client.post(
        f"/soa/{soa_id}/freeze/{freeze_id}/amendment",
        json={
            "name": "X",
            "number": "1",
            "summary": "s",
            "primary_reason_code": "C111",
        },
    )
    amendment_id = am_resp.json()["id"]

    add_resp = client.post(
        f"/soa/{soa_id}/amendment/{amendment_id}/impacts",
        json={
            "type_code": "C555",
            "text": "Affects eligibility criteria",
            "is_substantial": True,
        },
    )
    assert add_resp.status_code == 201
    impact_id = add_resp.json()["id"]
    assert add_resp.json()["impact_uid"] == "StudyAmendmentImpact_1"

    del_resp = client.delete(
        f"/soa/{soa_id}/amendment/{amendment_id}/impact/{impact_id}"
    )
    assert del_resp.status_code == 204

    row = _db_query_one(
        "SELECT id FROM study_amendment_impact WHERE id=?", (impact_id,)
    )
    assert row is None


# ---------------------------------------------------------------------------
# Changes and sections
# ---------------------------------------------------------------------------


@patch(_SLUG_PATCH, return_value="ddfct-2024-01-01")
def test_add_and_remove_change_with_section(_mock):
    soa_id = _new_soa("Amendment Changes")
    freeze_id = _freeze(soa_id, "v1")
    am_resp = client.post(
        f"/soa/{soa_id}/freeze/{freeze_id}/amendment",
        json={
            "name": "X",
            "number": "1",
            "summary": "s",
            "primary_reason_code": "C111",
        },
    )
    amendment_id = am_resp.json()["id"]

    ch_resp = client.post(
        f"/soa/{soa_id}/amendment/{amendment_id}/changes",
        json={
            "name": "Eligibility update",
            "summary": "Changed inclusion criterion",
            "rationale": "New safety data",
        },
    )
    assert ch_resp.status_code == 201
    change_id = ch_resp.json()["id"]
    assert ch_resp.json()["change_uid"] == "StudyChange_1"

    sec_resp = client.post(
        f"/soa/{soa_id}/change/{change_id}/sections",
        json={
            "section_number": "5.1",
            "section_title": "Inclusion Criteria",
            "applies_to_id": "doc-001",
        },
    )
    assert sec_resp.status_code == 201
    ref_id = sec_resp.json()["id"]
    assert sec_resp.json()["ref_uid"] == "DocumentContentReference_1"

    del_sec = client.delete(f"/soa/{soa_id}/change/{change_id}/section/{ref_id}")
    assert del_sec.status_code == 204

    del_ch = client.delete(f"/soa/{soa_id}/amendment/{amendment_id}/change/{change_id}")
    assert del_ch.status_code == 204

    row = _db_query_one("SELECT id FROM study_change WHERE id=?", (change_id,))
    assert row is None


# ---------------------------------------------------------------------------
# Delete cascade
# ---------------------------------------------------------------------------


@patch(_SLUG_PATCH, return_value="ddfct-2024-01-01")
def test_delete_amendment_cascades(_mock):
    soa_id = _new_soa("Amendment Cascade Delete")
    freeze_id = _freeze(soa_id, "v1")
    am_resp = client.post(
        f"/soa/{soa_id}/freeze/{freeze_id}/amendment",
        json={
            "name": "X",
            "number": "1",
            "summary": "s",
            "primary_reason_code": "C111",
        },
    )
    amendment_id = am_resp.json()["id"]
    amendment_uid = am_resp.json()["amendment_uid"]

    client.post(
        f"/soa/{soa_id}/amendment/{amendment_id}/reasons",
        json={"code": "C222"},
    )
    client.post(
        f"/soa/{soa_id}/amendment/{amendment_id}/impacts",
        json={"type_code": "C555", "text": "desc", "is_substantial": False},
    )
    ch_resp = client.post(
        f"/soa/{soa_id}/amendment/{amendment_id}/changes",
        json={"name": "Ch", "summary": "s", "rationale": "r"},
    )
    change_uid_val = ch_resp.json()["change_uid"]
    change_id = ch_resp.json()["id"]
    client.post(
        f"/soa/{soa_id}/change/{change_id}/sections",
        json={
            "section_number": "1",
            "section_title": "T",
            "applies_to_id": "doc",
        },
    )

    del_resp = client.delete(f"/soa/{soa_id}/amendment/{amendment_id}")
    assert del_resp.status_code == 204

    assert (
        _db_query_one(
            "SELECT id FROM study_amendment WHERE id=? AND soa_id=?",
            (amendment_id, soa_id),
        )
        is None
    )
    assert (
        _db_query_one(
            "SELECT id FROM study_amendment_reason WHERE amendment_uid=? AND soa_id=?",
            (amendment_uid, soa_id),
        )
        is None
    )
    assert (
        _db_query_one(
            "SELECT id FROM study_amendment_impact WHERE amendment_uid=? AND soa_id=?",
            (amendment_uid, soa_id),
        )
        is None
    )
    assert (
        _db_query_one(
            "SELECT id FROM study_change WHERE amendment_uid=? AND soa_id=?",
            (amendment_uid, soa_id),
        )
        is None
    )
    assert (
        _db_query_one(
            "SELECT id FROM document_content_reference WHERE change_uid=? AND soa_id=?",
            (change_uid_val, soa_id),
        )
        is None
    )


# ---------------------------------------------------------------------------
# UID monotonicity (never recycles deleted UIDs)
# ---------------------------------------------------------------------------


@patch(_SLUG_PATCH, return_value="ddfct-2024-01-01")
def test_amendment_uid_monotonic(_mock):
    soa_id = _new_soa("Amendment UID Monotonic")
    freeze_id1 = _freeze(soa_id, "v1")
    r1 = client.post(
        f"/soa/{soa_id}/freeze/{freeze_id1}/amendment",
        json={
            "name": "X",
            "number": "1",
            "summary": "s",
            "primary_reason_code": "C111",
        },
    )
    assert r1.json()["amendment_uid"] == "StudyAmendment_1"
    amendment_id = r1.json()["id"]

    client.delete(f"/soa/{soa_id}/amendment/{amendment_id}")

    freeze_id2 = _freeze(soa_id, "v2")
    r2 = client.post(
        f"/soa/{soa_id}/freeze/{freeze_id2}/amendment",
        json={
            "name": "Y",
            "number": "2",
            "summary": "s",
            "primary_reason_code": "C222",
        },
    )
    assert r2.json()["amendment_uid"] == "StudyAmendment_2"


# ---------------------------------------------------------------------------
# USDM generator
# ---------------------------------------------------------------------------


@patch(_SLUG_PATCH, return_value="ddfct-2024-01-01")
def test_usdm_generator_returns_correct_instance_types(_mock):
    from usdm.generate_amendments import build_usdm_amendments

    soa_id = _new_soa("USDM Generator Test")
    freeze_id = _freeze(soa_id, "v1")
    am_resp = client.post(
        f"/soa/{soa_id}/freeze/{freeze_id}/amendment",
        json={
            "name": "Efficacy Update",
            "number": "1",
            "summary": "Updated efficacy endpoint",
            "primary_reason_code": "C111",
        },
    )
    amendment_id = am_resp.json()["id"]
    client.post(
        f"/soa/{soa_id}/amendment/{amendment_id}/impacts",
        json={"type_code": "C555", "text": "desc", "is_substantial": True},
    )
    ch_resp = client.post(
        f"/soa/{soa_id}/amendment/{amendment_id}/changes",
        json={"name": "Ch", "summary": "s", "rationale": "r"},
    )
    change_id = ch_resp.json()["id"]
    client.post(
        f"/soa/{soa_id}/change/{change_id}/sections",
        json={
            "section_number": "3.2",
            "section_title": "Endpoints",
            "applies_to_id": "doc-v1",
        },
    )

    amendments = build_usdm_amendments(soa_id)
    assert len(amendments) == 1
    am = amendments[0]
    assert am["instanceType"] == "StudyAmendment"
    assert am["primaryReason"]["instanceType"] == "StudyAmendmentReason"
    assert am["impacts"][0]["instanceType"] == "StudyAmendmentImpact"
    assert am["changes"][0]["instanceType"] == "StudyChange"
    assert (
        am["changes"][0]["changedSections"][0]["instanceType"]
        == "DocumentContentReference"
    )
    assert am["geographicScopes"] == []
    assert am["enrollments"] == []
    assert am["dateValues"] == []


@patch(_SLUG_PATCH, return_value="ddfct-2024-01-01")
def test_usdm_generator_empty_for_soa_with_no_amendments(_mock):
    from usdm.generate_amendments import build_usdm_amendments

    soa_id = _new_soa("USDM Generator Empty")
    _freeze(soa_id, "v1")
    amendments = build_usdm_amendments(soa_id)
    assert amendments == []


# ---------------------------------------------------------------------------
# UI: freeze form creates amendment when is_amendment is set
# ---------------------------------------------------------------------------


@patch(_SLUG_PATCH, return_value="ddfct-2024-01-01")
def test_ui_freeze_with_amendment_checkbox(_mock_slug):
    soa_id = _new_soa("UI Freeze + Amendment")
    resp = client.post(
        f"/ui/soa/{soa_id}/freeze",
        data={
            "version_label": "v1",
            "is_amendment": "1",
            "amendment_name": "Safety Amendment",
            "amendment_number": "1",
            "amendment_summary": "Updated safety criteria",
            "primary_reason_code": "C99",
        },
        headers={"HX-Request": "true"},
    )
    assert resp.status_code == 200
    assert resp.headers.get("HX-Redirect")

    row = _db_query_one(
        "SELECT amendment_uid FROM study_amendment WHERE soa_id=?", (soa_id,)
    )
    assert row is not None
    assert row[0] == "StudyAmendment_1"


def test_ui_freeze_without_amendment_checkbox():
    soa_id = _new_soa("UI Freeze No Amendment Checkbox")
    resp = client.post(
        f"/ui/soa/{soa_id}/freeze",
        data={"version_label": "v1"},
        headers={"HX-Request": "true"},
    )
    assert resp.status_code == 200

    row = _db_query_one("SELECT id FROM study_amendment WHERE soa_id=?", (soa_id,))
    assert row is None
