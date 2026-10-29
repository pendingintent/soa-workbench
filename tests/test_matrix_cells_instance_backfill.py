"""Legacy visit-only matrix cells gain an instance_id."""

from fastapi.testclient import TestClient

from soa_builder.web.app import _fetch_matrix, app
from soa_builder.web.db import _connect
from soa_builder.web.migrate_database import (
    _migrate_matrix_cells_backfill_instance_id,
)

client = TestClient(app)


def _new_soa():
    resp = client.post("/soa", json={"name": "Backfill Test"})
    return resp.json()["id"]


def _seed(soa_id):
    """Encounter_1 -> one instance; Encounter_2 -> two instances."""
    conn = _connect()
    cur = conn.cursor()
    ids = {"visit": {}, "instance": {}, "activity": {}}
    for enc in ("Encounter_1", "Encounter_2", "Encounter_3"):
        cur.execute(
            "INSERT INTO visit (soa_id, name, encounter_uid) VALUES (?,?,?)",
            (soa_id, enc, enc),
        )
        ids["visit"][enc] = cur.lastrowid
    layout = [
        ("ScheduledActivityInstance_1", "Encounter_1"),
        ("ScheduledActivityInstance_2", "Encounter_2"),
        ("ScheduledActivityInstance_3", "Encounter_2"),
    ]
    for uid, enc in layout:
        cur.execute(
            "INSERT INTO instances (soa_id, instance_uid, name, "
            "encounter_uid) VALUES (?,?,?,?)",
            (soa_id, uid, uid, enc),
        )
        ids["instance"][uid] = cur.lastrowid
    for num in (1, 2):
        cur.execute(
            "INSERT INTO activity (soa_id, name, activity_uid) VALUES (?,?,?)",
            (soa_id, f"A{num}", f"Activity_{num}"),
        )
        ids["activity"][num] = cur.lastrowid
    conn.commit()
    conn.close()
    return ids


def _legacy_cell(soa_id, visit_id, activity_id):
    conn = _connect()
    cur = conn.cursor()
    cur.execute(
        "INSERT INTO matrix_cells (soa_id, visit_id, activity_id, "
        "status) VALUES (?,?,?,?)",
        (soa_id, visit_id, activity_id, "X"),
    )
    conn.commit()
    cell_id = cur.lastrowid
    conn.close()
    return cell_id


def _instance_of(cell_id):
    conn = _connect()
    row = conn.execute(
        "SELECT instance_id FROM matrix_cells WHERE id=?", (cell_id,)
    ).fetchone()
    conn.close()
    return row[0]


def test_backfill_resolves_cell_on_unique_encounter():
    soa_id = _new_soa()
    ids = _seed(soa_id)
    cell = _legacy_cell(soa_id, ids["visit"]["Encounter_1"], ids["activity"][1])
    _migrate_matrix_cells_backfill_instance_id()
    assert _instance_of(cell) == ids["instance"]["ScheduledActivityInstance_1"]


def test_backfill_leaves_ambiguous_and_unmatched_cells_alone():
    soa_id = _new_soa()
    ids = _seed(soa_id)
    shared = _legacy_cell(soa_id, ids["visit"]["Encounter_2"], ids["activity"][1])
    no_instance = _legacy_cell(soa_id, ids["visit"]["Encounter_3"], ids["activity"][1])
    _migrate_matrix_cells_backfill_instance_id()
    assert _instance_of(shared) is None
    assert _instance_of(no_instance) is None


def test_backfill_skips_cell_that_would_duplicate_instance_cell():
    soa_id = _new_soa()
    ids = _seed(soa_id)
    inst = ids["instance"]["ScheduledActivityInstance_1"]
    conn = _connect()
    conn.execute(
        "INSERT INTO matrix_cells (soa_id, instance_id, activity_id, "
        "status) VALUES (?,?,?,?)",
        (soa_id, inst, ids["activity"][1], "X"),
    )
    conn.commit()
    conn.close()
    legacy = _legacy_cell(soa_id, ids["visit"]["Encounter_1"], ids["activity"][1])
    _migrate_matrix_cells_backfill_instance_id()
    assert _instance_of(legacy) is None


def test_backfill_is_idempotent():
    soa_id = _new_soa()
    ids = _seed(soa_id)
    _legacy_cell(soa_id, ids["visit"]["Encounter_1"], ids["activity"][1])
    first = _migrate_matrix_cells_backfill_instance_id()
    second = _migrate_matrix_cells_backfill_instance_id()
    assert first >= 1
    assert second == 0


def test_set_cell_writes_instance_id_when_visit_resolves():
    soa_id = _new_soa()
    ids = _seed(soa_id)
    resp = client.post(
        f"/soa/{soa_id}/cells",
        json={
            "visit_id": ids["visit"]["Encounter_1"],
            "activity_id": ids["activity"][1],
            "status": "X",
        },
    )
    assert resp.status_code == 200, resp.text
    assert (
        _instance_of(resp.json()["cell_id"])
        == ids["instance"]["ScheduledActivityInstance_1"]
    )


def test_set_cell_ambiguous_visit_still_succeeds_without_instance():
    soa_id = _new_soa()
    ids = _seed(soa_id)
    resp = client.post(
        f"/soa/{soa_id}/cells",
        json={
            "visit_id": ids["visit"]["Encounter_2"],
            "activity_id": ids["activity"][1],
            "status": "X",
        },
    )
    assert resp.status_code == 200, resp.text
    assert _instance_of(resp.json()["cell_id"]) is None


def test_legacy_toggle_writes_instance_id_when_visit_resolves():
    soa_id = _new_soa()
    ids = _seed(soa_id)
    resp = client.post(
        f"/ui/soa/{soa_id}/toggle_cell",
        data={
            "visit_id": ids["visit"]["Encounter_1"],
            "activity_id": ids["activity"][1],
        },
    )
    assert resp.status_code == 200, resp.text
    conn = _connect()
    row = conn.execute(
        "SELECT instance_id FROM matrix_cells WHERE soa_id=? AND activity_id=?",
        (soa_id, ids["activity"][1]),
    ).fetchone()
    conn.close()
    assert row[0] == ids["instance"]["ScheduledActivityInstance_1"]


def _cell_count(soa_id):
    conn = _connect()
    n = conn.execute(
        "SELECT COUNT(*) FROM matrix_cells WHERE soa_id=?", (soa_id,)
    ).fetchone()[0]
    conn.close()
    return n


def test_legacy_toggle_rejects_ambiguous_visit_and_writes_nothing():
    soa_id = _new_soa()
    ids = _seed(soa_id)
    resp = client.post(
        f"/ui/soa/{soa_id}/toggle_cell",
        data={
            "visit_id": ids["visit"]["Encounter_2"],
            "activity_id": ids["activity"][1],
        },
    )
    assert resp.status_code == 409, resp.text
    assert _cell_count(soa_id) == 0


def test_legacy_toggle_rejects_visit_without_instances():
    soa_id = _new_soa()
    ids = _seed(soa_id)
    resp = client.post(
        f"/ui/soa/{soa_id}/toggle_cell",
        data={
            "visit_id": ids["visit"]["Encounter_3"],
            "activity_id": ids["activity"][1],
        },
    )
    assert resp.status_code == 409, resp.text
    assert _cell_count(soa_id) == 0


def test_set_cell_flags_whether_the_cell_will_render():
    soa_id = _new_soa()
    ids = _seed(soa_id)
    resolved = client.post(
        f"/soa/{soa_id}/cells",
        json={
            "visit_id": ids["visit"]["Encounter_1"],
            "activity_id": ids["activity"][1],
            "status": "X",
        },
    ).json()
    orphan = client.post(
        f"/soa/{soa_id}/cells",
        json={
            "visit_id": ids["visit"]["Encounter_2"],
            "activity_id": ids["activity"][1],
            "status": "X",
        },
    ).json()
    assert resolved["instance_resolved"] is True
    assert orphan["instance_resolved"] is False


def test_set_cell_update_reports_existing_resolution():
    soa_id = _new_soa()
    ids = _seed(soa_id)
    payload = {
        "visit_id": ids["visit"]["Encounter_2"],
        "activity_id": ids["activity"][1],
        "status": "X",
    }
    client.post(f"/soa/{soa_id}/cells", json=payload)
    payload["status"] = "Y"
    again = client.post(f"/soa/{soa_id}/cells", json=payload).json()
    assert again["instance_resolved"] is False


def test_matrix_read_shows_resolved_but_not_orphan_cells():
    soa_id = _new_soa()
    ids = _seed(soa_id)
    for enc in ("Encounter_1", "Encounter_2"):
        client.post(
            f"/soa/{soa_id}/cells",
            json={
                "visit_id": ids["visit"][enc],
                "activity_id": ids["activity"][1],
                "status": "X",
            },
        )
    _, _, cells = _fetch_matrix(soa_id)
    assert [c["instance_id"] for c in cells] == [
        ids["instance"]["ScheduledActivityInstance_1"]
    ]


def test_backfill_creates_encounter_lookup_indexes():
    _migrate_matrix_cells_backfill_instance_id()
    conn = _connect()
    names = {
        r[1]
        for table in ("instances", "visit")
        for r in conn.execute(f"PRAGMA index_list({table})")
    }
    conn.close()
    assert "idx_instances_soa_encounter" in names
    assert "idx_visit_soa_encounter" in names
