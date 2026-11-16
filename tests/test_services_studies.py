"""Shared study create/update helpers (issue #382).

The index-page form, the JSON API and the setup wizard all create a
study through ``create_soa_record`` so the uniqueness rule lives in one
place.
"""

import uuid

import pytest

from fastapi.testclient import TestClient

from soa_builder.web.app import app
from soa_builder.web.db import _connect
from soa_builder.web.services.studies import DuplicateStudyIdError
from soa_builder.web.services.studies import MissingStudyIdError
from soa_builder.web.services.studies import create_soa_record
from soa_builder.web.services.studies import update_soa_record

client = TestClient(app)


def _sid():
    return f"STUDY-{uuid.uuid4().hex[:8]}"


def _row(soa_id):
    conn = _connect()
    try:
        return conn.execute(
            "SELECT name, study_id, study_label, study_description FROM soa WHERE id=?",
            (soa_id,),
        ).fetchone()
    finally:
        conn.close()


def test_create_soa_record_inserts_and_trims():
    study_id = _sid()
    soa_id = create_soa_record("Wizard", f" {study_id} ", " Lbl ", "  ")
    assert _row(soa_id) == ("Wizard", study_id, "Lbl", None)


def test_create_soa_record_rejects_duplicate_study_id():
    study_id = _sid()
    create_soa_record("First", study_id)
    with pytest.raises(DuplicateStudyIdError):
        create_soa_record("Second", study_id)


def test_create_soa_record_allows_blank_study_id_twice():
    first = create_soa_record("A")
    second = create_soa_record("B", "  ")
    assert first != second


def test_update_soa_record_changes_fields():
    soa_id = create_soa_record("Old", _sid())
    new_id = _sid()
    update_soa_record(soa_id, "New", new_id, "L", "D")
    assert _row(soa_id) == ("New", new_id, "L", "D")


def test_update_soa_record_keeps_existing_study_id_when_blank():
    study_id = _sid()
    soa_id = create_soa_record("Keep", study_id)
    update_soa_record(soa_id, "Keep", "", None, None)
    assert _row(soa_id)[1] == study_id


def test_update_soa_record_rejects_duplicate():
    taken = _sid()
    create_soa_record("Owner", taken)
    soa_id = create_soa_record("Other", _sid())
    with pytest.raises(DuplicateStudyIdError):
        update_soa_record(soa_id, "Other", taken, None, None)


def test_update_soa_record_requires_study_id():
    soa_id = create_soa_record("NoId")
    with pytest.raises(MissingStudyIdError):
        update_soa_record(soa_id, "NoId", "", None, None)


def test_api_create_still_rejects_duplicate():
    study_id = _sid()
    assert client.post("/soa", json={"name": "A", "study_id": study_id}).status_code
    resp = client.post("/soa", json={"name": "B", "study_id": study_id})
    assert resp.status_code == 400
    assert "study_id already exists" in resp.text


def test_ui_create_still_rejects_duplicate():
    study_id = _sid()
    client.post("/ui/soa/create", data={"name": "A", "study_id": study_id})
    resp = client.post("/ui/soa/create", data={"name": "B", "study_id": study_id})
    assert "study_id already exists" in resp.text
