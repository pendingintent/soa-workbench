"""USDM StudyVersion.studyIdentifiers.

A study version may hold several identifiers (1..*), each scoped to
its own organization. DDF00172 needs exactly one sponsor-scoped
identifier: one scoped to the organization of the sponsor StudyRole.
The generator must never rewrite a stored identifier's scopeId to
make that hold: an NCT number scoped to ClinicalTrials.gov must stay
scoped to ClinicalTrials.gov.
"""

from fastapi.testclient import TestClient

from soa_builder.web.app import app
from soa_builder.web.db import _connect
from usdm.generate_study_identifiers import build_usdm_study_identifiers

client = TestClient(app)


def _soa(name: str, study_id: str, identifiers: list, sponsor_role: bool = True) -> int:
    """Create an SoA with a pharmaceutical company (Organization_1), a
    registry org (Organization_2) and the given (text, scope_org_uid)
    rows. With ``sponsor_role``, a sponsor StudyRole points at
    Organization_1; otherwise Organization_1 is only *typed* C70793."""
    soa_id = client.post("/soa", json={"name": name}).json()["id"]
    conn = _connect()
    try:
        cur = conn.cursor()
        cur.execute("UPDATE soa SET study_id=? WHERE id=?", (study_id or None, soa_id))
        cur.executemany(
            "INSERT INTO code (soa_id, code_uid, code, code_system)"
            " VALUES (?, ?, ?, 'http://www.cdisc.org')",
            [
                (soa_id, "Code_901", "C54149" if sponsor_role else "C70793"),
                (soa_id, "Code_902", "C93453"),
                (soa_id, "Code_903", "C70793"),
            ],
        )
        if sponsor_role:
            cur.execute(
                "INSERT INTO role (soa_id, role_uid, name, code_uid,"
                " organization_ids, order_index)"
                " VALUES (?, 'StudyRole_1', 'Sponsor', 'Code_903',"
                " '[\"Organization_1\"]', 1)",
                (soa_id,),
            )
        cur.executemany(
            "INSERT INTO organization"
            " (soa_id, organization_uid, name, type_code_uid, order_index)"
            " VALUES (?, ?, ?, ?, ?)",
            [
                (soa_id, "Organization_1", "Sponsor Inc", "Code_901", 1),
                (soa_id, "Organization_2", "ClinicalTrials.gov", "Code_902", 2),
            ],
        )
        cur.executemany(
            "INSERT INTO study_identifier"
            " (soa_id, study_identifier_uid, text, scope_org_uid,"
            " order_index) VALUES (?, ?, ?, ?, ?)",
            [
                (soa_id, f"StudyIdentifier_{n}", text, org, n)
                for n, (text, org) in enumerate(identifiers, start=1)
            ],
        )
        conn.commit()
    finally:
        conn.close()
    return soa_id


def _scopes(identifiers: list) -> dict:
    return {si["text"]: si["scopeId"] for si in identifiers}


def test_multiple_identifiers_keep_their_own_scope():
    soa_id = _soa(
        "USDM SI multiple",
        "SI-MULTI",
        [("NCT00000001", "Organization_2"), ("PROTO-001", "Organization_1")],
    )
    result = build_usdm_study_identifiers(soa_id)
    assert [si["id"] for si in result] == [
        "StudyIdentifier_1",
        "StudyIdentifier_2",
    ]
    assert _scopes(result) == {
        "NCT00000001": "Organization_2",
        "PROTO-001": "Organization_1",
    }


def test_missing_sponsor_identifier_is_added_not_rewritten():
    soa_id = _soa(
        "USDM SI no sponsor",
        "SI-NOSPONSOR",
        [("NCT00000001", "Organization_2")],
    )
    result = build_usdm_study_identifiers(soa_id)
    # The registry identifier is untouched...
    assert result[0]["id"] == "StudyIdentifier_1"
    assert result[0]["scopeId"] == "Organization_2"
    # ...and a sponsor identifier is derived from soa.study_id.
    sponsor = [si for si in result if si["scopeId"] == "Organization_1"]
    assert len(sponsor) == 1
    assert sponsor[0]["text"] == "SI-NOSPONSOR"
    assert sponsor[0]["id"] == "StudyIdentifier_2"


def test_no_sponsor_identifier_added_without_study_id():
    soa_id = _soa(
        "USDM SI no sponsor no study id",
        "",
        [("NCT00000001", "Organization_2")],
    )
    result = build_usdm_study_identifiers(soa_id)
    assert _scopes(result) == {"NCT00000001": "Organization_2"}


def test_no_rows_falls_back_to_study_id():
    soa_id = _soa("USDM SI empty", "SI-EMPTY", [])
    result = build_usdm_study_identifiers(soa_id)
    assert len(result) == 1
    assert result[0]["text"] == "SI-EMPTY"
    assert result[0]["scopeId"] == "Organization_1"


def test_org_typed_sponsor_without_sponsor_role_is_not_sponsor():
    # USDM v4: the sponsor comes from the sponsor StudyRole only.
    soa_id = _soa(
        "USDM SI typed sponsor no role",
        "SI-TYPEDONLY",
        [("NCT00000001", "Organization_2")],
        sponsor_role=False,
    )
    # No sponsor role, so no sponsor identifier is derived for the
    # org that is merely typed C70793.
    result = build_usdm_study_identifiers(soa_id)
    assert _scopes(result) == {"NCT00000001": "Organization_2"}


def test_no_rows_and_no_sponsor_role_leaves_scope_empty():
    soa_id = _soa("USDM SI empty no role", "SI-EMPTYNOROLE", [], sponsor_role=False)
    result = build_usdm_study_identifiers(soa_id)
    assert result[0]["text"] == "SI-EMPTYNOROLE"
    assert result[0]["scopeId"] == ""
