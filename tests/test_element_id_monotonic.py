from fastapi.testclient import TestClient

from soa_builder.web.app import _connect, app

client = TestClient(app)
PREFIX = "StudyElement_"


def create_soa(name="ElementIDMonotonicTest"):
    resp = client.post("/soa", json={"name": name})
    assert resp.status_code == 200
    return resp.json()["id"]


def get_first_element(soa_id):
    conn = _connect()
    cur = conn.cursor()
    cur.execute(
        "SELECT id, element_id FROM element WHERE soa_id=? ORDER BY id LIMIT 1",
        (soa_id,),
    )
    row = cur.fetchone()
    conn.close()
    return row


def get_last_element(soa_id):
    conn = _connect()
    cur = conn.cursor()
    cur.execute(
        "SELECT id, element_id FROM element WHERE soa_id=? ORDER BY id DESC LIMIT 1",
        (soa_id,),
    )
    row = cur.fetchone()
    conn.close()
    return row


def test_element_id_monotonic_after_delete():
    soa_id = create_soa()
    # Create first element
    r1 = client.post(f"/ui/soa/{soa_id}/add_element", data={"name": "Elem A"})
    assert r1.status_code == 200
    first = get_first_element(soa_id)
    # If column absent or value None, skip monotonic assertion
    if not first or first[1] is None:
        return
    assert first[1].startswith(PREFIX)
    n1 = int(first[1][len(PREFIX) :])
    assert n1 == 1

    # Delete the first element via UI endpoint
    del_resp = client.post(
        f"/ui/soa/{soa_id}/delete_element", data={"element_id": first[0]}
    )
    assert del_resp.status_code == 200

    # Create another element and ensure ID increments to 2 (monotonic)
    r2 = client.post(f"/ui/soa/{soa_id}/add_element", data={"name": "Elem B"})
    assert r2.status_code == 200
    last = get_last_element(soa_id)
    assert last is not None
    assert last[1] is not None
    assert last[1].startswith(PREFIX)
    n2 = int(last[1][len(PREFIX) :])
    assert n2 == 2
