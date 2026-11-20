"""Define-JSON TS SPONSOR comes from the sponsor StudyRole.

USDM v4: C70793 "Sponsor" is a StudyRole code, not an Organization
type, so the SPONSOR value-level entry is driven by a role whose
code is C70793 and that points to an organization.
"""

from usdm.create_define_json import _has_sponsor_role


def _role(code: str, org_ids: list) -> dict:
    return {
        "code": {"code": code, "decode": "x"},
        "organizationIds": org_ids,
        "instanceType": "StudyRole",
    }


def test_sponsor_role_with_organization():
    version = {"roles": [_role("C70793", ["Organization_1"])]}
    assert _has_sponsor_role(version) is True


def test_sponsor_role_without_organization():
    version = {"roles": [_role("C70793", [])]}
    assert _has_sponsor_role(version) is False


def test_org_typed_sponsor_is_not_enough():
    version = {
        "roles": [_role("C25936", ["Organization_1"])],
        "organizations": [{"id": "Organization_1", "type": {"code": "C70793"}}],
    }
    assert _has_sponsor_role(version) is False


def test_no_roles():
    assert _has_sponsor_role({}) is False
    assert _has_sponsor_role({"roles": None}) is False
