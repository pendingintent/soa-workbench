"""Tests for materializing CDISC classification groups (from the
cdisc-biomedical-concept-groupings service) into the concept_group /
concept_group_concept tables, so they appear alongside Custom Concept
Groups in the "Add group" dropdown — while Custom Concept Groups
behavior stays completely unaffected.
"""

import pytest
import requests
from fastapi.testclient import TestClient

from soa_builder.web.app import _get_concept_group_sections
from soa_builder.web.db import _connect
from soa_builder.web.app import app
from soa_builder.web.routers.concept_groups import (
    _bc_grouping_cache,
    sync_cdisc_concept_groups,
)

client = TestClient(app)


@pytest.fixture(autouse=True)
def clean_concept_group_tables():
    _bc_grouping_cache["data"] = None
    _bc_grouping_cache["fetched_at"] = 0
    conn = _connect()
    cur = conn.cursor()
    cur.execute("DELETE FROM concept_group_concept")
    cur.execute("DELETE FROM concept_group")
    conn.commit()
    conn.close()
    yield
    conn = _connect()
    cur = conn.cursor()
    cur.execute("DELETE FROM concept_group_concept")
    cur.execute("DELETE FROM concept_group")
    conn.commit()
    conn.close()


SCHEMES = [
    {
        "scheme_id": "coa_type",
        "name": "COA Type",
        "description": "",
        "purpose": "",
        "intended_use": "",
    },
    {
        "scheme_id": "concept_group",
        "name": "Concept Group",
        "description": "",
        "purpose": "",
        "intended_use": "",
    },
]
BCS = [
    {"bc_id": "C1", "short_name": "Weight", "ncit_code": "C1"},
    {"bc_id": "NEW_1", "short_name": "Retired Thing [RETIRED]", "ncit_code": "nan"},
]


def _values_and_assignments(include_cg_value=True, include_new1_assignment=True):
    values = [
        {
            "value_id": "coa_v1",
            "scheme_id": "coa_type",
            "label": "Clinician Reported",
            "description": "",
        },
    ]
    assignments = [
        {
            "assignment_id": "a1",
            "bc_id": "C1",
            "scheme_id": "coa_type",
            "value_id": "coa_v1",
        },
    ]
    if include_cg_value:
        values.append(
            {
                "value_id": "cg_v1",
                "scheme_id": "concept_group",
                "label": "Vital Signs",
                "description": "",
            }
        )
        assignments.append(
            {
                "assignment_id": "a2",
                "bc_id": "C1",
                "scheme_id": "concept_group",
                "value_id": "cg_v1",
            }
        )
        if include_new1_assignment:
            assignments.append(
                {
                    "assignment_id": "a3",
                    "bc_id": "NEW_1",
                    "scheme_id": "concept_group",
                    "value_id": "cg_v1",
                }
            )
    return values, assignments


class DummyResponse:
    def __init__(self, payload, status_code=200):
        self._payload = payload
        self.status_code = status_code

    def json(self):
        return self._payload

    def raise_for_status(self):
        if self.status_code >= 400:
            raise requests.exceptions.HTTPError(f"HTTP {self.status_code}")


def _fake_get(schemes, values, assignments, bcs):
    def fake_get(url, params=None, timeout=None):
        if url.endswith("/biomedical-concepts"):
            return DummyResponse({"items": bcs, "total": len(bcs)})
        if url.endswith("/classification-schemes"):
            return DummyResponse({"items": schemes, "total": len(schemes)})
        if url.endswith("/classification-values"):
            return DummyResponse({"items": values, "total": len(values)})
        if url.endswith("/classification-assignments"):
            return DummyResponse({"items": assignments, "total": len(assignments)})
        raise AssertionError(f"Unexpected URL: {url}")

    return fake_get


def _row_for_uid(uid):
    conn = _connect()
    cur = conn.cursor()
    cur.execute(
        "SELECT id, name, source, cdisc_scheme_id, cdisc_scheme_name, cdisc_value_id "
        "FROM concept_group WHERE concept_group_uid=?",
        (uid,),
    )
    row = cur.fetchone()
    conn.close()
    return row


def _members_for_uid(uid):
    conn = _connect()
    cur = conn.cursor()
    cur.execute(
        "SELECT concept_code FROM concept_group_concept WHERE concept_group_uid=?",
        (uid,),
    )
    codes = {r[0] for r in cur.fetchall()}
    conn.close()
    return codes


def test_sync_materializes_groups_and_prefers_ncit_code(monkeypatch):
    values, assignments = _values_and_assignments()
    monkeypatch.setattr("requests.get", _fake_get(SCHEMES, values, assignments, BCS))

    summary = sync_cdisc_concept_groups(force=True)
    assert summary["synced"] is True
    assert summary["groups"] == 2

    coa_row = _row_for_uid("cdisc:coa_type:coa_v1")
    assert coa_row is not None
    assert coa_row[1] == "Clinician Reported"
    assert coa_row[2] == "cdisc"
    assert coa_row[4] == "COA Type"

    cg_row = _row_for_uid("cdisc:concept_group:cg_v1")
    assert cg_row is not None
    assert cg_row[1] == "Vital Signs"

    # C1's ncit_code ("C1") is used directly; NEW_1's ncit_code ("nan")
    # doesn't match ^C\d+$ so it falls back to bc_id ("NEW_1").
    assert _members_for_uid("cdisc:concept_group:cg_v1") == {"C1", "NEW_1"}
    assert _members_for_uid("cdisc:coa_type:coa_v1") == {"C1"}


def test_sync_idempotent_and_preserves_custom_groups(monkeypatch):
    # Seed a custom group first — must survive untouched across syncs.
    resp = client.post("/concept-groups", json={"name": "My Custom Group"})
    assert resp.status_code == 200
    custom_id = resp.json()["id"]

    values, assignments = _values_and_assignments()
    monkeypatch.setattr("requests.get", _fake_get(SCHEMES, values, assignments, BCS))

    sync_cdisc_concept_groups(force=True)
    row1 = _row_for_uid("cdisc:concept_group:cg_v1")
    id_after_first_sync = row1[0]

    sync_cdisc_concept_groups(force=True)
    row2 = _row_for_uid("cdisc:concept_group:cg_v1")
    assert row2[0] == id_after_first_sync  # stable id: real upsert, not delete+reinsert

    # Custom group untouched.
    resp = client.get("/concept-groups")
    assert resp.status_code == 200
    custom_groups = [g for g in resp.json() if g["id"] == custom_id]
    assert len(custom_groups) == 1
    assert custom_groups[0]["name"] == "My Custom Group"


def test_sync_bidirectional_stale_cleanup(monkeypatch):
    values, assignments = _values_and_assignments()
    monkeypatch.setattr("requests.get", _fake_get(SCHEMES, values, assignments, BCS))
    sync_cdisc_concept_groups(force=True)
    assert _row_for_uid("cdisc:concept_group:cg_v1") is not None

    # Re-sync with the concept_group value removed entirely upstream.
    values2, assignments2 = _values_and_assignments(include_cg_value=False)
    monkeypatch.setattr("requests.get", _fake_get(SCHEMES, values2, assignments2, BCS))
    sync_cdisc_concept_groups(force=True)
    assert _row_for_uid("cdisc:concept_group:cg_v1") is None
    assert _members_for_uid("cdisc:concept_group:cg_v1") == set()
    # The still-current coa_type group is untouched.
    assert _row_for_uid("cdisc:coa_type:coa_v1") is not None


def test_sync_stale_member_cleanup(monkeypatch):
    values, assignments = _values_and_assignments()
    monkeypatch.setattr("requests.get", _fake_get(SCHEMES, values, assignments, BCS))
    sync_cdisc_concept_groups(force=True)
    assert _members_for_uid("cdisc:concept_group:cg_v1") == {"C1", "NEW_1"}

    # Re-sync with NEW_1's assignment to cg_v1 removed.
    values2, assignments2 = _values_and_assignments(include_new1_assignment=False)
    monkeypatch.setattr("requests.get", _fake_get(SCHEMES, values2, assignments2, BCS))
    sync_cdisc_concept_groups(force=True)
    assert _members_for_uid("cdisc:concept_group:cg_v1") == {"C1"}


def test_sync_resilience_on_service_unavailable(monkeypatch):
    values, assignments = _values_and_assignments()
    monkeypatch.setattr("requests.get", _fake_get(SCHEMES, values, assignments, BCS))
    sync_cdisc_concept_groups(force=True)
    assert _row_for_uid("cdisc:concept_group:cg_v1") is not None

    def raising_get(url, params=None, timeout=None):
        raise requests.exceptions.ConnectionError("boom")

    monkeypatch.setattr("requests.get", raising_get)
    summary = sync_cdisc_concept_groups(force=True)
    assert summary["synced"] is False

    # Previously-synced rows survive the outage untouched.
    assert _row_for_uid("cdisc:concept_group:cg_v1") is not None
    assert _members_for_uid("cdisc:concept_group:cg_v1") == {"C1", "NEW_1"}


def test_migration_idempotent():
    from soa_builder.web.migrate_database import (
        _migrate_concept_group_add_cdisc_source_columns,
    )

    _migrate_concept_group_add_cdisc_source_columns()
    _migrate_concept_group_add_cdisc_source_columns()

    conn = _connect()
    cur = conn.cursor()
    cur.execute("PRAGMA table_info(concept_group)")
    cols = [r[1] for r in cur.fetchall()]
    conn.close()
    for expected in (
        "source",
        "cdisc_scheme_id",
        "cdisc_scheme_name",
        "cdisc_value_id",
    ):
        assert cols.count(expected) == 1


def test_get_concept_group_sections_ordering_and_filtering(monkeypatch):
    resp = client.post("/concept-groups", json={"name": "My Custom Group"})
    assert resp.status_code == 200

    values, assignments = _values_and_assignments()
    monkeypatch.setattr("requests.get", _fake_get(SCHEMES, values, assignments, BCS))
    sync_cdisc_concept_groups(force=True)

    sections = _get_concept_group_sections()
    headings = [s["heading"] for s in sections]

    # COA Type appears before Concept Group (fixed order), schemes with
    # no values (Collection Method, etc.) are omitted, and Custom
    # Concept Groups comes last.
    assert headings == ["COA Type", "Concept Group", "Custom Concept Groups"]

    coa_section = sections[0]
    assert [g["name"] for g in coa_section["groups"]] == ["Clinician Reported"]

    custom_section = sections[-1]
    assert any(g["name"] == "My Custom Group" for g in custom_section["groups"])


def test_admin_page_excludes_cdisc_groups(monkeypatch):
    resp = client.post("/concept-groups", json={"name": "My Custom Group"})
    assert resp.status_code == 200

    values, assignments = _values_and_assignments()
    monkeypatch.setattr("requests.get", _fake_get(SCHEMES, values, assignments, BCS))
    sync_cdisc_concept_groups(force=True)

    resp = client.get("/ui/concept-groups")
    assert resp.status_code == 200
    assert "Vital Signs" not in resp.text
    assert "Clinician Reported" not in resp.text
    assert "My Custom Group" in resp.text

    # The plain JSON listing API is likewise scoped to custom groups.
    resp = client.get("/concept-groups")
    assert resp.status_code == 200
    names = [g["name"] for g in resp.json()]
    assert "Vital Signs" not in names
    assert "My Custom Group" in names


def test_admin_crud_guard_rejects_cdisc_group(monkeypatch):
    values, assignments = _values_and_assignments()
    monkeypatch.setattr("requests.get", _fake_get(SCHEMES, values, assignments, BCS))
    sync_cdisc_concept_groups(force=True)
    cdisc_id = _row_for_uid("cdisc:concept_group:cg_v1")[0]

    resp = client.patch(f"/concept-groups/{cdisc_id}", json={"name": "Hacked"})
    assert resp.status_code == 400

    resp = client.delete(f"/concept-groups/{cdisc_id}")
    assert resp.status_code == 400

    resp = client.post(
        f"/ui/concept-groups/{cdisc_id}/update",
        data={"name": "Hacked"},
    )
    assert resp.status_code == 400

    resp = client.post(f"/ui/concept-groups/{cdisc_id}/delete")
    assert resp.status_code == 400

    # The group survives all of the above untouched.
    assert _row_for_uid("cdisc:concept_group:cg_v1")[1] == "Vital Signs"


def test_end_to_end_assign_cdisc_group_to_activity(monkeypatch):
    values, assignments = _values_and_assignments()
    monkeypatch.setattr("requests.get", _fake_get(SCHEMES, values, assignments, BCS))
    sync_cdisc_concept_groups(force=True)

    soa_resp = client.post("/soa", json={"name": "CDISC Group E2E Test"})
    soa_id = soa_resp.json()["id"]
    act_resp = client.post(f"/soa/{soa_id}/activities", json={"name": "Vitals"})
    activity_id = act_resp.json()["activity_id"]

    resp = client.post(
        f"/ui/soa/{soa_id}/activity/{activity_id}/concept-groups/add",
        data={"concept_group_uid": "cdisc:concept_group:cg_v1"},
    )
    assert resp.status_code == 200
    assert "Concept Group: Vital Signs" in resp.text

    conn = _connect()
    cur = conn.cursor()
    cur.execute(
        "SELECT concept_code, concept_group_uid FROM activity_concept "
        "WHERE activity_id=? AND soa_id=?",
        (activity_id, soa_id),
    )
    rows = {r[0]: r[1] for r in cur.fetchall()}
    conn.close()
    assert rows == {
        "C1": "cdisc:concept_group:cg_v1",
        "NEW_1": "cdisc:concept_group:cg_v1",
    }

    # Removal (×all) works unchanged too.
    resp = client.post(
        f"/ui/soa/{soa_id}/activity/{activity_id}/concept-groups/remove",
        data={"concept_group_uid": "cdisc:concept_group:cg_v1"},
    )
    assert resp.status_code == 200
    conn = _connect()
    cur = conn.cursor()
    cur.execute(
        "SELECT COUNT(*) FROM activity_concept WHERE activity_id=? AND soa_id=?",
        (activity_id, soa_id),
    )
    count = cur.fetchone()[0]
    conn.close()
    assert count == 0
