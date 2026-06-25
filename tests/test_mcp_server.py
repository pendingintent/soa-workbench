"""Tests for the soa-workbench MCP server tool handlers.

Tests call _dispatch() directly — no MCP transport layer needed.
All tests use the pytest-isolated test DB (soa_builder_web_tests.db).
"""

import pytest

from soa_builder.mcp.server import _dispatch
from soa_builder.web.app import app  # noqa: F401 — triggers DB migrations
from soa_builder.web.db import _connect

SOA_ID = 7701
ACTIVITY_ID = 6601


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _clean():
    conn = _connect()
    cur = conn.cursor()
    cur.execute("DELETE FROM matrix_cells WHERE soa_id=?", (SOA_ID,))
    cur.execute("DELETE FROM activity WHERE soa_id=?", (SOA_ID,))
    cur.execute("DELETE FROM visit WHERE soa_id=?", (SOA_ID,))
    cur.execute("DELETE FROM instances WHERE soa_id=?", (SOA_ID,))
    cur.execute("DELETE FROM soa WHERE id=?", (SOA_ID,))
    conn.commit()
    conn.close()


def _seed_soa():
    _clean()
    conn = _connect()
    cur = conn.cursor()
    cur.execute(
        "INSERT INTO soa (id, name, study_id, study_label) VALUES (?,?,?,?)",
        (SOA_ID, "MCP Test Study", "MCP-001", "MCP Test Label"),
    )
    conn.commit()
    conn.close()


def _seed_activity(name="Vital Signs"):
    conn = _connect()
    cur = conn.cursor()
    cur.execute(
        "INSERT INTO activity (soa_id, name, order_index, activity_uid)"
        " VALUES (?,?,?,?)",
        (SOA_ID, name, 1, "Activity_1"),
    )
    aid = cur.lastrowid
    conn.commit()
    conn.close()
    return aid


def _seed_instance(name="Screening"):
    conn = _connect()
    cur = conn.cursor()
    cur.execute(
        "INSERT INTO instances"
        " (soa_id, name, instance_uid, member_of_timeline)"
        " VALUES (?,?,?,?)",
        (SOA_ID, name, "ScheduledActivityInstance_1", "Timeline_1"),
    )
    iid = cur.lastrowid
    conn.commit()
    conn.close()
    return iid


# ---------------------------------------------------------------------------
# list_soas
# ---------------------------------------------------------------------------


def test_list_soas_returns_list():
    _seed_soa()
    result = _dispatch("list_soas", {})
    assert isinstance(result, list)
    ids = [r["id"] for r in result]
    assert SOA_ID in ids


def test_list_soas_fields():
    _seed_soa()
    result = _dispatch("list_soas", {})
    match = next(r for r in result if r["id"] == SOA_ID)
    assert match["name"] == "MCP Test Study"
    assert match["study_id"] == "MCP-001"


# ---------------------------------------------------------------------------
# create_soa
# ---------------------------------------------------------------------------


def test_create_soa_inserts_row():
    conn = _connect()
    cur = conn.cursor()
    cur.execute("DELETE FROM soa WHERE study_id=?", ("MCP-NEW-001",))
    conn.commit()
    conn.close()

    result = _dispatch(
        "create_soa",
        {"name": "New Study", "study_id": "MCP-NEW-001", "study_label": "New"},
    )
    assert "id" in result
    new_id = result["id"]

    conn = _connect()
    cur = conn.cursor()
    cur.execute("SELECT name FROM soa WHERE id=?", (new_id,))
    assert cur.fetchone()[0] == "New Study"
    cur.execute("DELETE FROM soa WHERE id=?", (new_id,))
    conn.commit()
    conn.close()


def test_create_soa_requires_name():
    with pytest.raises(ValueError, match="name is required"):
        _dispatch("create_soa", {"name": ""})


def test_create_soa_duplicate_study_id_raises():
    _seed_soa()
    with pytest.raises(ValueError, match="already exists"):
        _dispatch("create_soa", {"name": "Dup", "study_id": "MCP-001"})


# ---------------------------------------------------------------------------
# get_soa
# ---------------------------------------------------------------------------


def test_get_soa_returns_metadata():
    _seed_soa()
    result = _dispatch("get_soa", {"soa_id": SOA_ID})
    assert result["id"] == SOA_ID
    assert result["name"] == "MCP Test Study"
    assert result["study_id"] == "MCP-001"
    assert result["study_label"] == "MCP Test Label"


def test_get_soa_missing_raises():
    with pytest.raises(ValueError, match="not found"):
        _dispatch("get_soa", {"soa_id": 999999})


# ---------------------------------------------------------------------------
# list_visits / create_visit
# ---------------------------------------------------------------------------


def test_create_visit_inserts_row():
    _seed_soa()
    result = _dispatch("create_visit", {"soa_id": SOA_ID, "name": "Screening"})
    assert result["name"] == "Screening"
    assert result["encounter_uid"].startswith("Encounter_")
    assert result["order_index"] == 1


def test_list_visits_round_trip():
    _seed_soa()
    _dispatch("create_visit", {"soa_id": SOA_ID, "name": "Visit 1", "label": "V1"})
    _dispatch("create_visit", {"soa_id": SOA_ID, "name": "Visit 2"})
    visits = _dispatch("list_visits", {"soa_id": SOA_ID})
    assert len(visits) == 2
    assert visits[0]["name"] == "Visit 1"
    assert visits[0]["label"] == "V1"
    assert visits[1]["name"] == "Visit 2"


def test_create_visit_requires_name():
    _seed_soa()
    with pytest.raises(ValueError, match="name is required"):
        _dispatch("create_visit", {"soa_id": SOA_ID, "name": ""})


# ---------------------------------------------------------------------------
# list_activities / create_activity
# ---------------------------------------------------------------------------


def test_create_activity_inserts_row():
    _seed_soa()
    result = _dispatch("create_activity", {"soa_id": SOA_ID, "name": "CBC"})
    assert result["name"] == "CBC"
    assert result["activity_uid"].startswith("Activity_")
    assert result["order_index"] == 1


def test_list_activities_round_trip():
    _seed_soa()
    _dispatch("create_activity", {"soa_id": SOA_ID, "name": "Vital Signs"})
    _dispatch(
        "create_activity",
        {"soa_id": SOA_ID, "name": "ECG", "label": "12-lead ECG"},
    )
    acts = _dispatch("list_activities", {"soa_id": SOA_ID})
    assert len(acts) == 2
    assert acts[0]["name"] == "Vital Signs"
    assert acts[1]["name"] == "ECG"
    assert acts[1]["label"] == "12-lead ECG"


def test_create_activity_requires_name():
    _seed_soa()
    with pytest.raises(ValueError, match="name is required"):
        _dispatch("create_activity", {"soa_id": SOA_ID, "name": ""})


# ---------------------------------------------------------------------------
# assign_instance_activity + get_soa_matrix
# ---------------------------------------------------------------------------


def test_assign_instance_activity_creates_cell():
    _seed_soa()
    aid = _seed_activity()
    iid = _seed_instance()

    result = _dispatch(
        "assign_instance_activity",
        {"soa_id": SOA_ID, "instance_id": iid, "activity_id": aid},
    )
    assert result["status"] == "X"
    assert result["instance_id"] == iid
    assert result["activity_id"] == aid


def test_get_soa_matrix_reflects_assignment():
    _seed_soa()
    aid = _seed_activity()
    iid = _seed_instance()
    _dispatch(
        "assign_instance_activity",
        {"soa_id": SOA_ID, "instance_id": iid, "activity_id": aid},
    )
    matrix = _dispatch("get_soa_matrix", {"soa_id": SOA_ID})
    assert any(
        c["instance_id"] == iid and c["activity_id"] == aid for c in matrix["cells"]
    )
    assert any(a["id"] == aid for a in matrix["activities"])
    assert any(i["id"] == iid for i in matrix["instances"])


def test_assign_instance_activity_remove():
    _seed_soa()
    aid = _seed_activity()
    iid = _seed_instance()
    _dispatch(
        "assign_instance_activity",
        {"soa_id": SOA_ID, "instance_id": iid, "activity_id": aid},
    )
    result = _dispatch(
        "assign_instance_activity",
        {"soa_id": SOA_ID, "instance_id": iid, "activity_id": aid, "status": ""},
    )
    assert result["deleted"] is True
    matrix = _dispatch("get_soa_matrix", {"soa_id": SOA_ID})
    assert not any(
        c["instance_id"] == iid and c["activity_id"] == aid for c in matrix["cells"]
    )


# ---------------------------------------------------------------------------
# get_usdm_json
# ---------------------------------------------------------------------------


def test_get_usdm_json_encounters():
    _seed_soa()
    result = _dispatch("get_usdm_json", {"soa_id": SOA_ID, "component": "encounters"})
    assert isinstance(result, list)


def test_get_usdm_json_unknown_component():
    _seed_soa()
    with pytest.raises(ValueError, match="Unknown component"):
        _dispatch("get_usdm_json", {"soa_id": SOA_ID, "component": "bogus"})


# ---------------------------------------------------------------------------
# unknown tool
# ---------------------------------------------------------------------------


def test_unknown_tool_raises():
    with pytest.raises(ValueError, match="Unknown tool"):
        _dispatch("no_such_tool", {})
