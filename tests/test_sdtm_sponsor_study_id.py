"""SDTM STUDYID: the sponsor StudyIdentifier text.

Order, matching ``build_usdm_study_identifiers``:
1. the identifier scoped to the sponsor StudyRole's organization
2. ``soa.study_id`` (what USDM derives the sponsor identifier from)
3. the first stored identifier
4. ``soa.name``

``soa.study_id`` comes before the first identifier because, with
several identifiers, the first may be a registry number (NCT, EU CT)
that must not become STUDYID.
"""

from fastapi.testclient import TestClient

from sdtm._common import sponsor_study_id
from soa_builder.web.app import app
from soa_builder.web.db import _connect

client = TestClient(app)


def _soa(name: str, study_id: str, identifiers: list, sponsor_role: bool = True) -> int:
    """Organization_1 holds the sponsor role (when ``sponsor_role``);
    Organization_2 is a registry. ``identifiers`` is (text, org) rows."""
    soa_id = client.post("/soa", json={"name": name}).json()["id"]
    conn = _connect()
    try:
        cur = conn.cursor()
        cur.execute("UPDATE soa SET study_id=? WHERE id=?", (study_id or None, soa_id))
        cur.executemany(
            "INSERT INTO code (soa_id, code_uid, code, code_system)"
            " VALUES (?, ?, ?, 'http://www.cdisc.org')",
            [
                (soa_id, "Code_901", "C54149"),
                (soa_id, "Code_902", "C93453"),
                (soa_id, "Code_903", "C70793"),
            ],
        )
        cur.executemany(
            "INSERT INTO organization"
            " (soa_id, organization_uid, name, type_code_uid, order_index)"
            " VALUES (?, ?, ?, ?, ?)",
            [
                (soa_id, "Organization_1", "Pharma Inc", "Code_901", 1),
                (soa_id, "Organization_2", "ClinicalTrials.gov", "Code_902", 2),
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


def _studyid(soa_id: int) -> str:
    conn = _connect()
    try:
        return sponsor_study_id(conn.cursor(), soa_id)
    finally:
        conn.close()


def test_sponsor_scoped_identifier_wins_even_when_not_first():
    soa_id = _soa(
        "STUDYID sponsor second",
        "STUDYID-A",
        [("NCT00000001", "Organization_2"), ("PROTO-001", "Organization_1")],
    )
    assert _studyid(soa_id) == "PROTO-001"


def test_study_id_before_registry_identifier():
    soa_id = _soa(
        "STUDYID no sponsor identifier",
        "STUDYID-B",
        [("NCT00000001", "Organization_2")],
    )
    assert _studyid(soa_id) == "STUDYID-B"


def test_first_identifier_when_no_study_id():
    soa_id = _soa(
        "STUDYID no study id",
        "",
        [("NCT00000001", "Organization_2")],
    )
    assert _studyid(soa_id) == "NCT00000001"


def test_name_when_nothing_else():
    soa_id = _soa("STUDYID name only", "", [])
    assert _studyid(soa_id) == "STUDYID name only"


def test_org_typed_sponsor_without_role_is_not_sponsor():
    # Organization_1 has no sponsor role here, so its identifier is
    # not the sponsor identifier; soa.study_id is used instead.
    soa_id = _soa(
        "STUDYID no sponsor role",
        "STUDYID-C",
        [("PROTO-001", "Organization_1")],
        sponsor_role=False,
    )
    assert _studyid(soa_id) == "STUDYID-C"
