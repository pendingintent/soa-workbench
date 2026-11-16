"""MCP workflow tools (freeze/rollback, design entities, audit, files).

They call the web API in-process, so these exercise the full stack.
"""

import pytest
from fastapi.testclient import TestClient

from soa_builder.mcp.server import _TOOLS, _dispatch
from soa_builder.web.app import app

client = TestClient(app)


def _new_soa(name: str) -> int:
    return _dispatch("create_soa", {"name": name})["id"]


def _instance(soa_id: int, name: str) -> int:
    r = client.post(f"/soa/{soa_id}/instances", json={"name": name})
    assert r.status_code in (200, 201), r.text
    return r.json()["id"]


def test_every_tool_has_a_handler():
    for tool in _TOOLS:
        try:
            _dispatch(tool.name, {})
        except ValueError as exc:
            assert "Unknown tool" not in str(exc), tool.name
        except (KeyError, TypeError):
            pass  # handler exists; required args missing


def test_freeze_rollback_workflow():
    soa_id = _new_soa("MCP freeze workflow")
    _dispatch("create_activity", {"soa_id": soa_id, "name": "Vitals"})
    v1 = _dispatch("create_freeze", {"soa_id": soa_id, "version_label": "v1"})
    _dispatch("create_activity", {"soa_id": soa_id, "name": "Labs"})
    v2 = _dispatch("create_freeze", {"soa_id": soa_id})
    assert v2["version_label"] == "v2"

    freezes = _dispatch("list_freezes", {"soa_id": soa_id})
    assert [f["id"] for f in freezes] == [v2["id"], v1["id"]]

    diff = _dispatch(
        "diff_freezes",
        {"soa_id": soa_id, "left_freeze_id": v1["id"], "right_freeze_id": v2["id"]},
    )
    assert "Labs" in str(diff)

    preview = _dispatch("rollback_preview", {"soa_id": soa_id, "freeze_id": v1["id"]})
    assert preview["activities_to_restore"] == 1

    result = _dispatch("rollback_freeze", {"soa_id": soa_id, "freeze_id": v1["id"]})
    assert result["safety_freeze_id"]
    names = [a["name"] for a in _dispatch("list_activities", {"soa_id": soa_id})]
    assert names == ["Vitals"]


def test_rollback_unknown_freeze_raises():
    soa_id = _new_soa("MCP rollback 404")
    with pytest.raises(ValueError, match="404"):
        _dispatch("rollback_freeze", {"soa_id": soa_id, "freeze_id": 987654})


def test_design_entity_crud_is_audited():
    soa_id = _new_soa("MCP design CRUD")
    created = _dispatch(
        "create_design_entity",
        {"soa_id": soa_id, "entity": "arms", "fields": {"name": "Placebo"}},
    )
    arm_id = created["id"]
    listed = _dispatch("list_design_entities", {"soa_id": soa_id, "entity": "arms"})
    assert [a["name"] for a in listed] == ["Placebo"]

    _dispatch(
        "update_design_entity",
        {
            "soa_id": soa_id,
            "entity": "arms",
            "entity_id": arm_id,
            "fields": {"label": "PBO"},
        },
    )
    _dispatch(
        "delete_design_entity",
        {"soa_id": soa_id, "entity": "arms", "entity_id": arm_id},
    )
    assert _dispatch("list_design_entities", {"soa_id": soa_id, "entity": "arms"}) == []

    history = _dispatch("get_audit_history", {"soa_id": soa_id, "entity_type": "arm"})
    assert [h["action"] for h in history] == ["delete", "update", "create"]


def test_design_entity_validation_errors():
    soa_id = _new_soa("MCP design errors")
    with pytest.raises(ValueError, match="Unknown entity"):
        _dispatch("list_design_entities", {"soa_id": soa_id, "entity": "widgets"})
    with pytest.raises(ValueError, match="fields must be"):
        _dispatch(
            "create_design_entity",
            {"soa_id": soa_id, "entity": "epochs", "fields": "Screening"},
        )
    with pytest.raises(ValueError, match="422"):
        _dispatch(
            "create_design_entity",
            {"soa_id": soa_id, "entity": "epochs", "fields": {}},
        )


def test_export_excel_writes_file(tmp_path):
    soa_id = _new_soa("MCP excel")
    activity = _dispatch("create_activity", {"soa_id": soa_id, "name": "Vitals"})
    iid = _instance(soa_id, "Day 1")
    _dispatch(
        "assign_instance_activity",
        {"soa_id": soa_id, "instance_id": iid, "activity_id": activity["id"]},
    )
    result = _dispatch("export_excel", {"soa_id": soa_id, "output_dir": str(tmp_path)})
    written = tmp_path / result["path"].split("/")[-1]
    assert written.exists() and written.read_bytes()[:2] == b"PK"


def test_export_excel_empty_matrix_raises(tmp_path):
    soa_id = _new_soa("MCP excel empty")
    with pytest.raises(ValueError, match="empty matrix"):
        _dispatch("export_excel", {"soa_id": soa_id, "output_dir": str(tmp_path)})


def test_generate_dair_writes_docx(tmp_path):
    soa_id = _new_soa("MCP DAIR")
    _dispatch("create_activity", {"soa_id": soa_id, "name": "Vitals"})
    base = _dispatch("create_freeze", {"soa_id": soa_id})["id"]
    _dispatch("create_activity", {"soa_id": soa_id, "name": "Labs"})
    revised = _dispatch("create_freeze", {"soa_id": soa_id})["id"]
    result = _dispatch(
        "generate_dair",
        {
            "soa_id": soa_id,
            "base_freeze_id": base,
            "revised_freeze_id": revised,
            "output_dir": str(tmp_path),
        },
    )
    assert result["path"].startswith(str(tmp_path))
    assert result["path"].endswith(".docx")
    with open(result["path"], "rb") as fh:
        assert fh.read(2) == b"PK"
