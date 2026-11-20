"""Organization.type comes from the DDF organization type codelist.

USDM v4 DDF00200: an organization type must come from codelist
C188724. The workbench used to offer the StudyRole codelist (C215480)
instead, so stored types can include role codes such as Sponsor.

All CT comes from the CDISC Library (``get_ddf_ct_rows``); these tests
mock that fetch with a tiny fake package rather than embedding CT.
"""

from unittest.mock import patch

from fastapi.testclient import TestClient

from soa_builder.web import codelist_config
from soa_builder.web.app import app
from soa_builder.web.db import _connect
from soa_builder.web.routers import organizations

client = TestClient(app)

_ORG_TYPE = "C188724"
_ROLE = "C215480"
# Fake Library package: two org types and one role code.
_FAKE_ROWS = [
    {"codelist_code": _ORG_TYPE, "code": "OT1", "preferred_term": "Org Type One"},
    {"codelist_code": _ORG_TYPE, "code": "OT2", "preferred_term": "Org Type Two"},
    {"codelist_code": _ROLE, "code": "RL1", "preferred_term": "Role One"},
]


def _ct(rows):
    """Patch the Library CT fetch used by the organizations router."""
    return patch.object(
        organizations, "get_ddf_ct_rows", return_value={"rows": rows, "error": None}
    )


def _version():
    return patch.object(
        organizations, "get_latest_ddf_ct_href", return_value="ddfct-2026-09-25"
    )


def _new_soa(name: str) -> int:
    return client.post("/soa", json={"name": name}).json()["id"]


def _api_create(soa_id: int, code: str, term: str = "x", version: str = "2026-09-25"):
    return client.post(
        f"/soa/{soa_id}/organizations",
        json={
            "name": f"Org {code}",
            "type_concept_id": code,
            "type_preferred_term": term,
            "type_version": version,
        },
    )


def _stored_type(soa_id: int, org_uid: str) -> tuple:
    conn = _connect()
    try:
        return conn.execute(
            "SELECT c.code, c.decode, c.code_system_version FROM organization o"
            " JOIN code c ON c.code_uid = o.type_code_uid AND c.soa_id = o.soa_id"
            " WHERE o.soa_id = ? AND o.organization_uid = ?",
            (soa_id, org_uid),
        ).fetchone()
    finally:
        conn.close()


def _legacy_org(soa_id: int) -> dict:
    """An org typed with a role code, as stored before the fix.

    Created while the Library CT is unavailable, so no check runs."""
    with _ct([]):
        r = _api_create(soa_id, "RL1", "Role One", version="2025-03-28")
    assert r.status_code == 201, r.text
    return r.json()


def _ui_update(soa_id: int, org_id: int, type_code: str, type_decode: str):
    return client.post(
        f"/ui/soa/{soa_id}/organizations/{org_id}/update",
        data={
            "name": "Renamed",
            "type_code": type_code,
            "type_decode": type_decode,
        },
    )


# --- configuration -------------------------------------------------------


def test_default_codelist_is_organization_type():
    assert codelist_config.ORGANIZATION_TYPE_CODELIST == _ORG_TYPE


def test_options_come_from_organization_type_codelist():
    with _ct(_FAKE_ROWS):
        codes = [o["code"] for o in organizations._get_org_type_options()]
    assert codes == ["OT1", "OT2"]


# --- API create ------------------------------------------------------------


def test_api_create_accepts_codelist_type():
    soa_id = _new_soa("OrgType API ok")
    with _ct(_FAKE_ROWS):
        assert _api_create(soa_id, "OT1").status_code == 201


def test_api_create_rejects_role_code():
    soa_id = _new_soa("OrgType API role code")
    with _ct(_FAKE_ROWS):
        r = _api_create(soa_id, "RL1")
    assert r.status_code == 422
    assert "DDF00200" in r.json()["detail"]
    assert client.get(f"/soa/{soa_id}/organizations").json() == []


def test_api_create_without_type_ok():
    soa_id = _new_soa("OrgType API no type")
    with _ct(_FAKE_ROWS):
        r = client.post(f"/soa/{soa_id}/organizations", json={"name": "No Type"})
    assert r.status_code == 201


def test_check_skipped_when_library_unavailable():
    soa_id = _new_soa("OrgType API offline")
    with _ct([]):
        assert _api_create(soa_id, "RL1").status_code == 201


# --- UI add ----------------------------------------------------------------


def test_ui_add_rejects_role_code_and_shows_error():
    soa_id = _new_soa("OrgType UI add role code")
    with _ct(_FAKE_ROWS), _version():
        r = client.post(
            f"/ui/soa/{soa_id}/organizations-add",
            data={"name": "Bad Type", "type_code": "RL1", "type_decode": "Role One"},
        )
    assert r.status_code == 422
    assert "organizations-section" in r.text
    assert "am-api-notice-error" in r.text
    assert "DDF00200" in r.text
    assert "before-swap" in r.text
    assert client.get(f"/soa/{soa_id}/organizations").json() == []


# --- UI update and legacy types -------------------------------------------


def test_ui_update_keeps_unchanged_legacy_type():
    soa_id = _new_soa("OrgType UI keep legacy")
    org = _legacy_org(soa_id)
    before = _stored_type(soa_id, org["organization_uid"])
    with _ct(_FAKE_ROWS), _version():
        r = _ui_update(soa_id, org["id"], "RL1", "Role One")
    assert r.status_code == 200, r.text
    # Same code, decode and CT version: the code row is not re-stamped.
    assert _stored_type(soa_id, org["organization_uid"]) == before


def test_ui_update_rejects_switch_to_other_non_codelist_type():
    soa_id = _new_soa("OrgType UI switch bad")
    with _ct(_FAKE_ROWS):
        org = _api_create(soa_id, "OT1", "Org Type One").json()
    with _ct(_FAKE_ROWS), _version():
        r = _ui_update(soa_id, org["id"], "RL1", "Role One")
    assert r.status_code == 422
    assert "DDF00200" in r.text
    assert _stored_type(soa_id, org["organization_uid"])[0] == "OT1"


def test_ui_update_retype_legacy_to_codelist_type():
    soa_id = _new_soa("OrgType UI retype")
    org = _legacy_org(soa_id)
    with _ct(_FAKE_ROWS), _version():
        r = _ui_update(soa_id, org["id"], "OT2", "Org Type Two")
    assert r.status_code == 200, r.text
    assert _stored_type(soa_id, org["organization_uid"])[:2] == (
        "OT2",
        "Org Type Two",
    )


def _page(soa_id: int) -> str:
    return client.get(f"/ui/soa/{soa_id}/orgs-roles-persons").text


def test_legacy_type_stays_selected_and_is_flagged():
    soa_id = _new_soa("OrgType render legacy")
    _legacy_org(soa_id)
    with _ct(_FAKE_ROWS):
        html = _page(soa_id)
    assert 'value="RL1"' in html
    assert "not in C188724" in html
    assert "DDF00200" in html


def test_stored_type_kept_selectable_when_library_unavailable():
    # Without the Library CT the dropdown has no options, but the
    # stored type must still be selected so saving does not wipe it.
    soa_id = _new_soa("OrgType render offline")
    with _ct([]):
        _api_create(soa_id, "OT1", "Org Type One")
        html = _page(soa_id)
    assert 'value="OT1"' in html
    assert "DDF00200" not in html
