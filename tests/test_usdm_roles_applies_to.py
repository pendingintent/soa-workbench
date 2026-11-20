"""USDM StudyRole.appliesToIds.

DDF00189 needs every StudyRole to apply to the study version or to at
least one study design (not both, not neither). DDF00203 needs a
sponsor role (C70793) to apply to the study version. The workbench
has no per-role scope, so each role applies to the study version.
"""

from fastapi.testclient import TestClient

from soa_builder.web.app import app
from soa_builder.web.db import _connect
from usdm.generate_roles import build_usdm_roles

client = TestClient(app)


def _soa_with_roles(name: str) -> int:
    soa_id = client.post("/soa", json={"name": name}).json()["id"]
    conn = _connect()
    try:
        cur = conn.cursor()
        cur.execute(
            "INSERT INTO code (soa_id, code_uid, code, code_system,"
            " code_system_version, decode)"
            " VALUES (?, 'Code_501', 'C70793', 'http://www.cdisc.org',"
            " '2026-09-25', 'Clinical Study Sponsor')",
            (soa_id,),
        )
        cur.executemany(
            "INSERT INTO role (soa_id, role_uid, name, code_uid,"
            " organization_ids, order_index) VALUES (?, ?, ?, ?, ?, ?)",
            [
                (soa_id, "StudyRole_1", "Sponsor", "Code_501", '["Organization_1"]', 1),
                (soa_id, "StudyRole_2", "Investigator", None, None, 2),
            ],
        )
        conn.commit()
    finally:
        conn.close()
    return soa_id


def test_every_role_applies_to_the_study_version():
    soa_id = _soa_with_roles("Roles apply to version")

    roles = build_usdm_roles(soa_id)

    assert [r["appliesToIds"] for r in roles] == [
        ["StudyVersion_1"],
        ["StudyVersion_1"],
    ]


def test_role_applies_to_ids_match_exported_study_version():
    soa_id = _soa_with_roles("Roles match version id")

    resp = client.get(f"/soa/{soa_id}/usdm_json/full")
    assert resp.status_code == 200
    version = resp.json()["study"]["versions"][0]

    sponsor = next(r for r in version["roles"] if r["code"]["code"] == "C70793")
    design_ids = {d["id"] for d in version["studyDesigns"]}
    assert sponsor["appliesToIds"] == [version["id"]]
    assert not design_ids & set(sponsor["appliesToIds"])
