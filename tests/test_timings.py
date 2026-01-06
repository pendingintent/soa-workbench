from typing import Dict, Any, Tuple

from fastapi.testclient import TestClient

from soa_builder.web.app import app
from soa_builder.web.db import _connect

client = TestClient(app)


def _ensure_soa(soa_id: int = 999) -> int:
    conn = _connect()
    cur = conn.cursor()
    cur.execute(
        "INSERT OR IGNORE INTO soa (id, name) VALUES (?, ?)",
        (soa_id, f"Test SOA {soa_id}"),
    )
    cur.execute("DELETE FROM timing WHERE soa_id=?", (soa_id,))
    conn.commit()
    conn.close()
    return soa_id


def _create_timing(
    soa_id: int, name: str = "Baseline", **kwargs
) -> Tuple[int, Dict[str, Any]]:
    payload = {"name": name, **kwargs}
    r = client.post(f"/soa/{soa_id}/timings", json=payload)
    assert r.status_code == 201, r.text
    data = r.json()
    return data["id"], data


def test_list_timings_404_for_missing_soa():
    r = client.get("/soa/123456/timings")
    assert r.status_code == 404


def test_create_timing_requires_name():
    soa_id = _ensure_soa(1001)
    r = client.post(f"/soa/{soa_id}/timings", json={"name": "   "})
    assert r.status_code == 400
    assert "Timing name required" in r.text


def test_update_timing_mutable_fields_and_updated_fields():
    soa_id = _ensure_soa(1003)
    tid, before = _create_timing(soa_id, name="Baseline", label=None, description=None)

    payload = {
        "name": "  Baseline Updated  ",
        "label": "  Label X ",
        "description": "  Desc Y ",
        "type": "  relative ",
        "value": "  5 ",
        "value_label": "  days ",
        "relative_to_from": "  from ",
        "relative_from_schedule_instance": "  Arm A ",
        "relative_to_schedule_instance": "  Epoch 1 ",
        "window_label": "  Window ",
        "window_upper": "  +2 ",
        "window_lower": "  -1 ",
    }
    r = client.patch(f"/soa/{soa_id}/timings/{tid}", json=payload)
    assert r.status_code == 200, r.text
    data = r.json()

    # Trimmed values stored (empty -> None)
    assert data["name"] == "Baseline Updated"
    assert data["label"] == "Label X"
    assert data["description"] == "Desc Y"
    assert data["type"] == "relative"
    assert data["value"] == "5"
    assert data["value_label"] == "days"
    assert data["relative_to_from"] == "from"
    assert data["relative_from_schedule_instance"] == "Arm A"
    assert data["relative_to_schedule_instance"] == "Epoch 1"
    assert data["window_label"] == "Window"
    assert data["window_upper"] == "+2"
    assert data["window_lower"] == "-1"

    # updated_fields must include changed keys
    uf = set(data["updated_fields"])
    expected = {
        "name",
        "label",
        "description",
        "type",
        "value",
        "value_label",
        "relative_to_from",
        "relative_from_schedule_instance",
        "relative_to_schedule_instance",
        "window_label",
        "window_upper",
        "window_lower",
    }
    assert expected.issubset(uf)


def test_update_timing_404_for_missing_id():
    soa_id = _ensure_soa(1004)
    r = client.patch(f"/soa/{soa_id}/timings/999999", json={"name": "x"})
    assert r.status_code == 404


def test_delete_timing_happy_path_and_404():
    soa_id = _ensure_soa(1005)
    tid, _ = _create_timing(soa_id, name="Delete Me")
    r = client.delete(f"/soa/{soa_id}/timings/{tid}")
    assert r.status_code == 200
    data = r.json()
    assert data["deleted"] is True
    assert data["id"] == tid

    # Gone
    r2 = client.delete(f"/soa/{soa_id}/timings/{tid}")
    assert r2.status_code == 404
