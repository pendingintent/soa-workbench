"""The sponsor organization comes from the sponsor StudyRole.

USDM v4: C70793 "Sponsor" is a StudyRole code (codelist C215480), not
an Organization type. DDF00172 defines the sponsor study identifier as
the one scoped to "the organization for the sponsor study role", so
the sponsor organization is the one in that role's organizationIds.
An organization *typed* C70793 does not make it the sponsor.
"""

import json

from fastapi.testclient import TestClient

from soa_builder.web.app import app
from soa_builder.web.db import _connect
from soa_builder.web.utils import sponsor_org_uid

client = TestClient(app)


def _soa(name: str, orgs: list, roles: list) -> int:
    """Seed orgs [(uid, type_code)] and roles [(uid, code, org_ids)]."""
    soa_id = client.post("/soa", json={"name": name}).json()["id"]
    conn = _connect()
    try:
        cur = conn.cursor()
        n = 0
        for uid, type_code in orgs:
            n += 1
            cur.execute(
                "INSERT INTO code (soa_id, code_uid, code, code_system)"
                " VALUES (?, ?, ?, 'http://www.cdisc.org')",
                (soa_id, f"Code_{900 + n}", type_code),
            )
            cur.execute(
                "INSERT INTO organization (soa_id, organization_uid, name,"
                " type_code_uid, order_index) VALUES (?, ?, ?, ?, ?)",
                (soa_id, uid, uid, f"Code_{900 + n}", n),
            )
        for i, (uid, code, org_ids) in enumerate(roles, start=1):
            n += 1
            cur.execute(
                "INSERT INTO code (soa_id, code_uid, code, code_system)"
                " VALUES (?, ?, ?, 'http://www.cdisc.org')",
                (soa_id, f"Code_{900 + n}", code),
            )
            cur.execute(
                "INSERT INTO role (soa_id, role_uid, name, code_uid,"
                " organization_ids, order_index) VALUES (?, ?, ?, ?, ?, ?)",
                (soa_id, uid, uid, f"Code_{900 + n}", json.dumps(org_ids), i),
            )
        conn.commit()
    finally:
        conn.close()
    return soa_id


def _lookup(soa_id: int) -> str:
    conn = _connect()
    try:
        return sponsor_org_uid(conn.cursor(), soa_id)
    finally:
        conn.close()


def test_sponsor_is_the_sponsor_role_organization():
    # Organization type is Pharmaceutical Company, not C70793.
    soa_id = _soa(
        "Sponsor via role",
        orgs=[("Organization_1", "C93453"), ("Organization_2", "C54149")],
        roles=[
            ("StudyRole_1", "C25936", ["Organization_1"]),  # Investigator
            ("StudyRole_2", "C70793", ["Organization_2"]),  # Sponsor
        ],
    )
    assert _lookup(soa_id) == "Organization_2"


def test_org_typed_sponsor_without_role_is_not_sponsor():
    soa_id = _soa(
        "Sponsor type only",
        orgs=[("Organization_1", "C70793")],
        roles=[],
    )
    assert _lookup(soa_id) == ""


def test_co_sponsor_role_is_not_sponsor():
    soa_id = _soa(
        "Co-sponsor only",
        orgs=[("Organization_1", "C54149")],
        roles=[("StudyRole_1", "C215669", ["Organization_1"])],
    )
    assert _lookup(soa_id) == ""


def test_sponsor_role_without_organization():
    soa_id = _soa(
        "Sponsor role no org",
        orgs=[("Organization_1", "C54149")],
        roles=[("StudyRole_1", "C70793", [])],
    )
    assert _lookup(soa_id) == ""


def test_sponsor_role_org_must_exist_in_study():
    soa_id = _soa(
        "Sponsor role dangling org",
        orgs=[("Organization_1", "C54149")],
        roles=[("StudyRole_1", "C70793", ["Organization_9"])],
    )
    assert _lookup(soa_id) == ""
