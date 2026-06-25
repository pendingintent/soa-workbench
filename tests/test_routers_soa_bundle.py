"""Tests for SOA bundle export and import endpoints."""

import io
import json

from fastapi.testclient import TestClient

from soa_builder.web.app import app
from soa_builder.web.db import _connect

client = TestClient(app)


def _new_soa(name: str) -> int:
    r = client.post(
        "/soa",
        json={"name": name, "study_id": f"TEST-{name[:8].upper()}"},
    )
    assert r.status_code in (200, 201), r.text
    return r.json()["id"]


def _populate_soa(soa_id: int):
    """Add epochs, visits, activities, cells, arm, and an objective."""
    # Epochs
    ep1 = client.post(
        f"/soa/{soa_id}/epochs",
        json={"name": "Screening", "type": "SCREENING"},
    )
    assert ep1.status_code in (200, 201)
    ep1_uid = ep1.json()["epoch_uid"]

    ep2 = client.post(
        f"/soa/{soa_id}/epochs",
        json={"name": "Treatment", "type": "TREATMENT"},
    )
    assert ep2.status_code in (200, 201)
    ep2_uid = ep2.json()["epoch_uid"]

    # Visits
    v1 = client.post(
        f"/soa/{soa_id}/visits",
        json={"name": "Screening Visit", "epoch_uid": ep1_uid},
    )
    assert v1.status_code in (200, 201)
    v1_id = v1.json()["id"]

    v2 = client.post(
        f"/soa/{soa_id}/visits",
        json={"name": "Week 1", "epoch_uid": ep2_uid},
    )
    assert v2.status_code in (200, 201)
    v2_id = v2.json()["id"]

    # Activities
    a1 = client.post(f"/soa/{soa_id}/activities", json={"name": "Informed Consent"})
    assert a1.status_code in (200, 201)
    a1_id = a1.json()["activity_id"]

    a2 = client.post(f"/soa/{soa_id}/activities", json={"name": "Vital Signs"})
    assert a2.status_code in (200, 201)
    a2_id = a2.json()["activity_id"]

    # Cells
    for v_id in [v1_id, v2_id]:
        for a_id in [a1_id, a2_id]:
            r = client.post(
                f"/soa/{soa_id}/cells",
                json={"visit_id": v_id, "activity_id": a_id, "status": "X"},
            )
            assert r.status_code in (200, 201), r.text

    # Arm
    arm_r = client.post(
        f"/soa/{soa_id}/arms",
        json={"name": "Treatment Arm", "type": "INVESTIGATIONAL"},
    )
    assert arm_r.status_code in (200, 201)

    return {
        "epochs": [ep1_uid, ep2_uid],
        "visit_ids": [v1_id, v2_id],
        "activity_ids": [a1_id, a2_id],
    }


def _export_bundle(soa_id: int) -> dict:
    r = client.get(f"/soa/{soa_id}/export/bundle")
    assert r.status_code == 200, r.text
    assert "attachment" in r.headers.get("content-disposition", "")
    return json.loads(r.content)


def _import_bundle(bundle: dict, name: str = None) -> dict:
    buf = io.BytesIO(json.dumps(bundle).encode())
    data = {}
    if name:
        data["name"] = name
    r = client.post(
        "/soa/import/bundle",
        files={"file": ("bundle.json", buf, "application/json")},
        data=data,
    )
    assert r.status_code == 201, r.text
    return r.json()


def _count(soa_id: int, table: str) -> int:
    conn = _connect()
    cur = conn.cursor()
    cur.execute(f"SELECT COUNT(*) FROM {table} WHERE soa_id=?", (soa_id,))
    n = cur.fetchone()[0]
    conn.close()
    return n


def test_export_bundle_has_required_keys():
    soa_id = _new_soa("ExportKeysTest")
    bundle = _export_bundle(soa_id)
    assert bundle["format_version"] == "1.0"
    assert "exported_at" in bundle
    assert bundle["soa"]["name"] == "ExportKeysTest"
    for key in ["epoch", "visit", "activity", "matrix_cells", "arm"]:
        assert key in bundle, f"Missing key: {key}"


def test_export_bundle_roundtrip():
    soa_id = _new_soa("RoundtripTest")
    _populate_soa(soa_id)

    bundle = _export_bundle(soa_id)

    # Verify bundle content counts
    assert len(bundle["epoch"]) == 2
    assert len(bundle["visit"]) == 2
    assert len(bundle["activity"]) == 2
    assert len(bundle["matrix_cells"]) == 4
    assert len(bundle["arm"]) == 1

    # Import into a new SOA
    result = _import_bundle(bundle)
    new_soa_id = result["soa_id"]
    assert new_soa_id != soa_id

    # Verify counts match
    assert _count(new_soa_id, "epoch") == 2
    assert _count(new_soa_id, "visit") == 2
    assert _count(new_soa_id, "activity") == 2
    assert _count(new_soa_id, "matrix_cells") == 4
    assert _count(new_soa_id, "arm") == 1

    # Verify cells reference the new SOA's visit/activity IDs
    conn = _connect()
    cur = conn.cursor()
    cur.execute(
        "SELECT visit_id, activity_id FROM matrix_cells WHERE soa_id=?",
        (new_soa_id,),
    )
    cells = cur.fetchall()
    conn.close()
    new_visit_ids = {r[0] for r in cells}
    new_act_ids = {r[1] for r in cells}

    # IDs must belong to the new SOA, not the old one
    conn = _connect()
    cur = conn.cursor()
    cur.execute("SELECT id FROM visit WHERE soa_id=?", (new_soa_id,))
    expected_visit_ids = {r[0] for r in cur.fetchall()}
    cur.execute("SELECT id FROM activity WHERE soa_id=?", (new_soa_id,))
    expected_act_ids = {r[0] for r in cur.fetchall()}
    conn.close()

    assert new_visit_ids == expected_visit_ids
    assert new_act_ids == expected_act_ids


def test_export_empty_soa_bundle():
    soa_id = _new_soa("EmptyBundleTest")
    bundle = _export_bundle(soa_id)
    assert bundle["epoch"] == []
    assert bundle["visit"] == []
    assert bundle["activity"] == []

    result = _import_bundle(bundle)
    new_soa_id = result["soa_id"]
    assert _count(new_soa_id, "epoch") == 0
    assert _count(new_soa_id, "visit") == 0
    assert _count(new_soa_id, "activity") == 0


def test_import_name_override():
    soa_id = _new_soa("OriginalName")
    bundle = _export_bundle(soa_id)

    result = _import_bundle(bundle, name="Custom Name")
    assert result["name"] == "Custom Name"

    new_soa_id = result["soa_id"]
    conn = _connect()
    cur = conn.cursor()
    cur.execute("SELECT name FROM soa WHERE id=?", (new_soa_id,))
    db_name = cur.fetchone()[0]
    conn.close()
    assert db_name == "Custom Name"


def test_import_invalid_format_version():
    bundle = {
        "format_version": "99.0",
        "soa": {"name": "Bad"},
        "epoch": [],
        "visit": [],
        "activity": [],
        "matrix_cells": [],
    }
    buf = io.BytesIO(json.dumps(bundle).encode())
    r = client.post(
        "/soa/import/bundle",
        files={"file": ("bad.json", buf, "application/json")},
    )
    assert r.status_code == 422


def test_export_not_found():
    r = client.get("/soa/999999/export/bundle")
    assert r.status_code == 404


def test_import_invalid_json():
    buf = io.BytesIO(b"not json at all {{{")
    r = client.post(
        "/soa/import/bundle",
        files={"file": ("bad.json", buf, "application/json")},
    )
    assert r.status_code == 422
