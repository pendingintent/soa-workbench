from fastapi.testclient import TestClient

from soa_builder.web.app import app, _connect

client = TestClient(app)


def test_study_cell_uid_reuse_same_arm_epoch():
    # Create study
    r = client.post("/soa", json={"name": "UID Reuse Study"})
    assert r.status_code == 200
    soa_id = r.json()["id"]

    # Create an arm (UI API is in routers.arms; use JSON route)
    arm_resp = client.post(f"/soa/{soa_id}/arms", json={"name": "Arm A"})
    assert arm_resp.status_code in (200, 201)
    arm_id = arm_resp.json().get("arm_id") or arm_resp.json().get("id")

    # Fetch arm_uid
    conn = _connect()
    cur = conn.cursor()
    cur.execute("SELECT arm_uid FROM arm WHERE id=?", (arm_id,))
    arm_uid = cur.fetchone()[0]

    # Create two epochs via JSON API
    e1 = client.post(f"/soa/{soa_id}/epochs", json={"name": "Screening"})
    assert e1.status_code in (200, 201)
    e2 = client.post(f"/soa/{soa_id}/epochs", json={"name": "Treatment"})
    assert e2.status_code in (200, 201)

    # Pick first epoch and get its epoch_uid
    cur.execute(
        "SELECT id, epoch_uid FROM epoch WHERE soa_id=? ORDER BY order_index", (soa_id,)
    )
    rows = cur.fetchall()
    epoch_id = rows[0][0]
    epoch_uid = rows[0][1] or ("StudyEpoch_" + str(epoch_id))

    # Create two elements
    el1 = client.post(
        f"/soa/{soa_id}/elements",
        json={"name": "Dose", "label": "D", "description": ""},
    )
    assert el1.status_code in (200, 201)
    el2 = client.post(
        f"/soa/{soa_id}/elements", json={"name": "Lab", "label": "L", "description": ""}
    )
    assert el2.status_code in (200, 201)

    # Fetch element logical ids (element_id)
    cur.execute("PRAGMA table_info(element)")
    cols = {r[1] for r in cur.fetchall()}
    assert "element_id" in cols, "element_id column expected in tests"
    cur.execute("SELECT element_id FROM element WHERE soa_id=? ORDER BY id", (soa_id,))
    element_ids = [r[0] for r in cur.fetchall()]
    assert len(element_ids) >= 2
    el_a, el_b = element_ids[0], element_ids[1]

    conn.close()

    # Call UI endpoint to add study cells with multiple elements; reuse safeguard should apply
    form = {
        "arm_uid": arm_uid,
        "epoch_uid": epoch_uid,
        "element_uids": [el_a, el_b],
    }
    resp = client.post(f"/ui/soa/{soa_id}/add_study_cell", data=form)
    assert resp.status_code in (200, 201)

    # Verify rows share the same study_cell_uid
    conn = _connect()
    cur = conn.cursor()
    cur.execute(
        "SELECT study_cell_uid, element_uid FROM study_cell WHERE soa_id=? AND arm_uid=? AND epoch_uid=? ORDER BY id",
        (soa_id, arm_uid, epoch_uid),
    )
    sc_rows = cur.fetchall()
    conn.close()
    assert len(sc_rows) >= 2
    uids = {r[0] for r in sc_rows}
    assert (
        len(uids) == 1
    ), "Expected all StudyCell rows to reuse the same study_cell_uid"

    # Idempotence check: submitting the same element again should not create a duplicate row
    form_dup = {"arm_uid": arm_uid, "epoch_uid": epoch_uid, "element_uids": [el_b]}
    resp_dup = client.post(f"/ui/soa/{soa_id}/add_study_cell", data=form_dup)
    assert resp_dup.status_code in (200, 201)
    conn = _connect()
    cur = conn.cursor()
    cur.execute(
        "SELECT COUNT(*) FROM study_cell WHERE soa_id=? AND arm_uid=? AND epoch_uid=? AND element_uid=?",
        (soa_id, arm_uid, epoch_uid, el_b),
    )
    cnt = cur.fetchone()[0]
    conn.close()
    assert (
        cnt == 1
    ), "Duplicate submission should not create a second row for the same element"
