from fastapi.testclient import TestClient

from soa_builder.web.app import app, _connect

client = TestClient(app)


def test_study_cell_uid_unique_on_later_addition():
    # Create study
    r = client.post("/soa", json={"name": "UID Unique Later Study"})
    assert r.status_code == 200
    soa_id = r.json()["id"]

    # Create an arm
    arm_resp = client.post(f"/soa/{soa_id}/arms", json={"name": "Arm B"})
    assert arm_resp.status_code in (200, 201)
    arm_id = arm_resp.json().get("arm_id") or arm_resp.json().get("id")

    # Fetch arm_uid
    conn = _connect()
    cur = conn.cursor()
    cur.execute("SELECT arm_uid FROM arm WHERE id=?", (arm_id,))
    arm_uid = cur.fetchone()[0]

    # Create one epoch
    e1 = client.post(f"/soa/{soa_id}/epochs", json={"name": "Baseline"})
    assert e1.status_code in (200, 201)

    # Get epoch_uid
    cur.execute(
        "SELECT id, epoch_uid FROM epoch WHERE soa_id=? ORDER BY order_index", (soa_id,)
    )
    rows = cur.fetchall()
    epoch_uid = rows[0][1] or ("StudyEpoch_" + str(rows[0][0]))

    # Create three elements
    el1 = client.post(
        f"/soa/{soa_id}/elements",
        json={"name": "Vitals", "label": "V", "description": ""},
    )
    assert el1.status_code in (200, 201)
    el2 = client.post(
        f"/soa/{soa_id}/elements", json={"name": "ECG", "label": "E", "description": ""}
    )
    assert el2.status_code in (200, 201)
    el3 = client.post(
        f"/soa/{soa_id}/elements", json={"name": "PK", "label": "P", "description": ""}
    )
    assert el3.status_code in (200, 201)

    # Get element_uids (element_id)
    cur.execute("SELECT element_id FROM element WHERE soa_id=? ORDER BY id", (soa_id,))
    element_ids = [r[0] for r in cur.fetchall()]
    assert len(element_ids) >= 3
    el_a, el_b, el_c = element_ids[:3]
    conn.close()

    # First submission: add Vitals+ECG
    form1 = {"arm_uid": arm_uid, "epoch_uid": epoch_uid, "element_uids": [el_a, el_b]}
    resp1 = client.post(f"/ui/soa/{soa_id}/study_cells/create", data=form1)
    assert resp1.status_code in (200, 201)

    # Capture the assigned study_cell_uids
    conn = _connect()
    cur = conn.cursor()
    cur.execute(
        "SELECT study_cell_uid FROM study_cell WHERE soa_id=? AND arm_uid=? AND epoch_uid=? ORDER BY id",
        (soa_id, arm_uid, epoch_uid),
    )
    first_uids = [r[0] for r in cur.fetchall()]
    conn.close()
    assert len(first_uids) == 2
    assert first_uids[0] != first_uids[1], "Each row must have a unique study_cell_uid"

    # Second submission later: add PK only, should get a new unique UID
    form2 = {"arm_uid": arm_uid, "epoch_uid": epoch_uid, "element_uids": [el_c]}
    resp2 = client.post(f"/ui/soa/{soa_id}/study_cells/create", data=form2)
    assert resp2.status_code in (200, 201)

    # Verify all three rows have unique study_cell_uids
    conn = _connect()
    cur = conn.cursor()
    cur.execute(
        "SELECT study_cell_uid, element_uid FROM study_cell WHERE soa_id=? AND arm_uid=? AND epoch_uid=? ORDER BY id",
        (soa_id, arm_uid, epoch_uid),
    )
    rows = cur.fetchall()
    conn.close()
    assert len(rows) >= 3
    uids = [r[0] for r in rows]
    assert len(uids) == len(set(uids)), "All study_cell_uids must be unique"

    # Idempotence check: submitting the same element again should not create a duplicate row
    form_dup = {"arm_uid": arm_uid, "epoch_uid": epoch_uid, "element_uids": [el_c]}
    resp_dup = client.post(f"/ui/soa/{soa_id}/study_cells/create", data=form_dup)
    assert resp_dup.status_code in (200, 201)
    conn = _connect()
    cur = conn.cursor()
    cur.execute(
        "SELECT COUNT(*) FROM study_cell WHERE soa_id=? AND arm_uid=? AND epoch_uid=? AND element_uid=?",
        (soa_id, arm_uid, epoch_uid, el_c),
    )
    cnt = cur.fetchone()[0]
    conn.close()
    assert (
        cnt == 1
    ), "Duplicate submission should not create a second row for the same element"
