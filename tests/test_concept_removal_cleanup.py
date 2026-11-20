"""Removing concepts from an activity must not leave biomedical
concepts, or the code/alias_code chain each one owns, behind.

Every path that deletes activity_concept rows (concept groups, BC
categories, single concepts) has to run the orphan cleanup, and the
cleanup must delete exactly the removed concept's own chain: never a
chain that another concept or entity still uses.
"""

from unittest.mock import patch

import pytest
from fastapi.testclient import TestClient

from soa_builder.web.app import app
from soa_builder.web.db import _connect

client = TestClient(app)

CODES = ["C11111", "C22222"]

_created_soas: list = []
_created_groups: list = []


@pytest.fixture(autouse=True)
def _remove_test_data():
    """Delete this module's SoAs and groups afterwards, so concepts
    left without properties don't leak into tests that back-fill
    properties for every SoA in the shared test database."""
    yield
    conn = _connect()
    cur = conn.cursor()
    cur.execute("SELECT name FROM sqlite_master WHERE type='table'")
    tables = [r[0] for r in cur.fetchall()]
    for table in tables:
        cur.execute(f"PRAGMA table_info({table})")
        if any(r[1] == "soa_id" for r in cur.fetchall()):
            for soa_id in _created_soas:
                cur.execute(f"DELETE FROM {table} WHERE soa_id=?", (soa_id,))
    for soa_id in _created_soas:
        cur.execute("DELETE FROM soa WHERE id=?", (soa_id,))
    for uid in _created_groups:
        cur.execute(
            "DELETE FROM concept_group_concept WHERE concept_group_uid=?", (uid,)
        )
        cur.execute("DELETE FROM concept_group WHERE concept_group_uid=?", (uid,))
    conn.commit()
    conn.close()
    _created_soas.clear()
    _created_groups.clear()


def _new_soa(name: str) -> int:
    r = client.post("/soa", json={"name": name})
    assert r.status_code == 200, r.text
    _created_soas.append(r.json()["id"])
    return r.json()["id"]


def _new_activity(soa_id: int, name: str) -> int:
    r = client.post(f"/soa/{soa_id}/activities", json={"name": name})
    assert r.status_code == 200, r.text
    return r.json()["activity_id"]


def _new_group(name: str) -> tuple:
    """A custom concept group holding CODES; returns (id, uid)."""
    r = client.post("/concept-groups", json={"name": name})
    assert r.status_code in (200, 201), r.text
    group_id, uid = r.json()["id"], r.json()["concept_group_uid"]
    _created_groups.append(uid)
    conn = _connect()
    for code in CODES:
        conn.execute(
            "INSERT INTO concept_group_concept"
            " (concept_group_uid, concept_code, concept_title) VALUES (?,?,?)",
            (uid, code, f"Concept {code}"),
        )
    conn.commit()
    conn.close()
    return group_id, uid


def _assign_group(soa_id: int, activity_id: int, group_id: int) -> None:
    r = client.post(f"/soa/{soa_id}/activities/{activity_id}/concept-groups/{group_id}")
    assert r.status_code == 200, r.text


def _chain(soa_id: int) -> dict:
    """Counts of the concept rows of CODES in this SoA."""
    conn = _connect()
    cur = conn.cursor()
    ph = ",".join("?" * len(CODES))
    counts = {}
    cur.execute(
        f"SELECT COUNT(*) FROM code WHERE soa_id=? AND code IN ({ph})",
        (soa_id, *CODES),
    )
    counts["code"] = cur.fetchone()[0]
    cur.execute(
        "SELECT COUNT(*) FROM alias_code a JOIN code c"
        " ON c.code_uid=a.standard_code AND c.soa_id=a.soa_id"
        f" WHERE a.soa_id=? AND c.code IN ({ph})",
        (soa_id, *CODES),
    )
    counts["alias_code"] = cur.fetchone()[0]
    cur.execute("SELECT COUNT(*) FROM biomedical_concept WHERE soa_id=?", (soa_id,))
    counts["biomedical_concept"] = cur.fetchone()[0]
    cur.execute(
        "SELECT COUNT(*) FROM alias_code a WHERE soa_id=? AND NOT EXISTS"
        " (SELECT 1 FROM code c WHERE c.soa_id=a.soa_id"
        " AND c.code_uid=a.standard_code)",
        (soa_id,),
    )
    counts["alias_without_code"] = cur.fetchone()[0]
    conn.close()
    return counts


EMPTY = {"code": 0, "alias_code": 0, "biomedical_concept": 0, "alias_without_code": 0}


def _remove_group_api(soa_id, activity_id, group_id, group_uid):
    r = client.delete(
        f"/soa/{soa_id}/activities/{activity_id}/concept-groups/{group_id}"
    )
    assert r.status_code == 200, r.text


def _remove_group_ui(soa_id, activity_id, group_id, group_uid):
    r = client.post(
        f"/ui/soa/{soa_id}/activity/{activity_id}/concept-groups/remove",
        data={"concept_group_uid": group_uid},
    )
    assert r.status_code == 200, r.text


def _delete_group_api(soa_id, activity_id, group_id, group_uid):
    r = client.delete(f"/concept-groups/{group_id}")
    assert r.status_code == 200, r.text


def _delete_group_ui(soa_id, activity_id, group_id, group_uid):
    r = client.post(f"/ui/concept-groups/{group_id}/delete", follow_redirects=False)
    assert r.status_code in (200, 303), r.text


@pytest.mark.parametrize(
    "remove",
    [_remove_group_api, _remove_group_ui, _delete_group_api, _delete_group_ui],
    ids=["unassign-api", "unassign-ui", "delete-group-api", "delete-group-ui"],
)
def test_removing_a_concept_group_cleans_up_its_concepts(remove):
    soa_id = _new_soa(f"Group removal {remove.__name__}")
    activity_id = _new_activity(soa_id, "Scale")
    group_id, group_uid = _new_group(f"Group {remove.__name__}")
    _assign_group(soa_id, activity_id, group_id)
    assert _chain(soa_id) == {
        "code": 2,
        "alias_code": 2,
        "biomedical_concept": 2,
        "alias_without_code": 0,
    }

    remove(soa_id, activity_id, group_id, group_uid)

    assert _chain(soa_id) == EMPTY


def test_deleting_a_group_cleans_up_every_soa_using_it():
    group_id, group_uid = _new_group("Group used by two SoAs")
    soas = []
    for name in ("Group delete SoA A", "Group delete SoA B"):
        soa_id = _new_soa(name)
        _assign_group(soa_id, _new_activity(soa_id, "Scale"), group_id)
        soas.append(soa_id)

    _delete_group_api(None, None, group_id, group_uid)

    assert [_chain(s) for s in soas] == [EMPTY, EMPTY]


def test_removing_a_bc_category_cleans_up_its_concepts():
    soa_id = _new_soa("Category removal")
    activity_id = _new_activity(soa_id, "Vitals")
    fake = [{"code": c, "title": f"Concept {c}", "href": f"/mdr/bc/{c}"} for c in CODES]
    with patch(
        "soa_builder.web.services.concepts.fetch_biomedical_concepts_by_category",
        return_value=fake,
    ):
        r = client.post(
            f"/ui/soa/{soa_id}/activity/{activity_id}/bc-categories/add",
            data={"category_name": "Vital Signs"},
        )
    assert r.status_code == 200, r.text
    assert _chain(soa_id)["biomedical_concept"] == 2

    r = client.post(
        f"/ui/soa/{soa_id}/activity/{activity_id}/bc-categories/remove",
        data={"category_name": "Vital Signs"},
    )
    assert r.status_code == 200, r.text

    assert _chain(soa_id) == EMPTY


def test_removing_a_shared_concept_keeps_the_other_activitys_chain():
    """Each concept owns its own code chain. Removing it from one
    activity must delete that chain only, even though another activity
    still uses the same concept code."""
    soa_id = _new_soa("Shared concept removal")
    group_id, group_uid = _new_group("Group on two activities")
    first = _new_activity(soa_id, "Visit A scale")
    second = _new_activity(soa_id, "Visit B scale")
    _assign_group(soa_id, first, group_id)
    _assign_group(soa_id, second, group_id)
    assert _chain(soa_id)["biomedical_concept"] == 4

    _remove_group_api(soa_id, first, group_id, group_uid)

    assert _chain(soa_id) == {
        "code": 2,
        "alias_code": 2,
        "biomedical_concept": 2,
        "alias_without_code": 0,
    }


def test_cleanup_never_deletes_another_entitys_code_with_the_same_value():
    """A code row is only deleted through the concept that owns it,
    never found by its C-code value."""
    soa_id = _new_soa("Unrelated code survives")
    conn = _connect()
    conn.execute(
        "INSERT INTO code (soa_id, code_uid, code, decode) VALUES (?,?,?,?)",
        (soa_id, "Code_UNRELATED", CODES[0], "Owned by something else"),
    )
    conn.commit()
    conn.close()
    activity_id = _new_activity(soa_id, "Scale")
    group_id, group_uid = _new_group("Group vs unrelated code")
    _assign_group(soa_id, activity_id, group_id)

    _remove_group_api(soa_id, activity_id, group_id, group_uid)

    conn = _connect()
    row = conn.execute(
        "SELECT code FROM code WHERE soa_id=? AND code_uid='Code_UNRELATED'",
        (soa_id,),
    ).fetchone()
    conn.close()
    assert row == (CODES[0],)


def test_every_activity_concept_delete_cleans_up():
    """A module that deletes activity_concept rows must also clean up
    the concepts it unlinks: through _delete_activity_concepts, or by
    calling _cleanup_orphaned_concept_rows itself. Rollback is exempt:
    it rebuilds every biomedical concept from the restored snapshot."""
    import re
    from pathlib import Path

    from soa_builder.web import db as web_db

    exempt = {"routers/_freeze_helpers/rollback.py", "services/concepts.py"}
    delete = re.compile(r"DELETE FROM activity_concept\b(?!_)")
    offenders = []
    root = Path(web_db.__file__).parent
    for path in root.rglob("*.py"):
        rel = path.relative_to(root).as_posix()
        text = path.read_text()
        if rel in exempt or not delete.search(text):
            continue
        if "_cleanup_orphaned_concept_rows" not in text:
            offenders.append(rel)
    assert not offenders, (
        "Modules delete activity_concept rows without cleaning up the"
        f" concepts they unlink: {offenders}. Use _delete_activity_concepts."
    )
