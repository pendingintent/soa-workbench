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
