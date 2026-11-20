"""USDM StudyArm.type codeSystemVersion.

DDF00073 needs one version per code system. Arm types (C174222) are
in both Protocol CT and DDF CT, and Protocol CT is published less
often, so the arm type version is taken from DDF CT when the term is
there, matching the other DDF codes. Protocol CT is the fallback.
"""

import pytest

from soa_builder.web import utils
from soa_builder.web.db import _connect
from usdm.usdm_utils import _get_type_code_tuple

ARM_TYPE = "C174222"


def _row(code: str, value: str) -> dict:
    return {"codelist_code": ARM_TYPE, "code": code, "submission_value": value}


@pytest.fixture
def ct_packages(monkeypatch):
    ddf = {
        "slug": "ddfct-2026-09-25",
        "rows": [_row("C174266", "Experimental Arm")],
    }
    protocol = {
        "slug": "protocolct-2025-09-26",
        "rows": [
            _row("C174266", "Experimental Arm"),
            _row("C999999", "Protocol Only Arm"),
        ],
    }
    monkeypatch.setattr(utils, "get_ddf_ct_rows", lambda *a, **k: ddf)
    monkeypatch.setattr(utils, "get_protocol_ct_rows", lambda *a, **k: protocol)


def _arm_type(code: str) -> tuple[int, str]:
    from fastapi.testclient import TestClient

    from soa_builder.web.app import app

    soa_id = (
        TestClient(app).post("/soa", json={"name": f"Arm type {code}"}).json()["id"]
    )
    conn = _connect()
    try:
        conn.execute(
            "INSERT INTO code_association (soa_id, code_uid, codelist_table,"
            " codelist_code, code) VALUES (?, 'Code_11',"
            " 'db://protocol_terminology', ?, ?)",
            (soa_id, ARM_TYPE, code),
        )
        conn.commit()
    finally:
        conn.close()
    return soa_id, "Code_11"


def test_arm_type_in_ddf_ct_uses_ddf_version(ct_packages):
    soa_id, code_uid = _arm_type("C174266")

    codes, decodes, _, versions = _get_type_code_tuple(soa_id, code_uid)

    assert codes == ["C174266"]
    assert decodes == ["Experimental Arm"]
    assert versions == ["2026-09-25"]


def test_arm_type_only_in_protocol_ct_falls_back(ct_packages):
    soa_id, code_uid = _arm_type("C999999")

    codes, decodes, _, versions = _get_type_code_tuple(soa_id, code_uid)

    assert codes == ["C999999"]
    assert decodes == ["Protocol Only Arm"]
    assert versions == ["2025-09-26"]
