import os
import json
from importlib import reload
from fastapi.testclient import TestClient
import soa_builder.web.app as webapp
from soa_builder.web.app import app, DB_PATH, _connect

client = TestClient(app)


def reset_db_with_concepts_env():
    if os.path.exists(DB_PATH):
        os.remove(DB_PATH)
    # Provide environment override concepts JSON
    sample = {
        "items": [
            {"concept_code": "BC001", "title": "Body Weight"},
            {"concept_code": "BC002", "title": "Height"},
            {"concept_code": "BC003", "title": "Blood Pressure"},
        ]
    }
    os.environ["CDISC_CONCEPTS_JSON"] = json.dumps(sample)
    reload(webapp)


def test_set_activity_concepts_and_render():
    reset_db_with_concepts_env()
    # create soa and activity
    r = client.post("/soa", json={"name": "Concept Trial"})
    soa_id = r.json()["id"]
    act = client.post(f"/soa/{soa_id}/activities", json={"name": "Vitals"}).json()[
        "activity_id"
    ]
    # associate concepts
    resp = client.post(
        f"/soa/{soa_id}/activities/{act}/concepts",
        json={"concept_codes": ["BC001", "BC003"]},
    )
    assert resp.status_code == 200
    assert resp.json()["concepts_set"] == 2
    # verify DB rows
    conn = _connect()
    cur = conn.cursor()
    cur.execute(
        "SELECT concept_code, concept_title FROM activity_concept WHERE activity_id=? ORDER BY concept_code",
        (act,),
    )
    rows = cur.fetchall()
    conn.close()
    assert rows == [("BC001", "Body Weight"), ("BC003", "Blood Pressure")]
    # render edit page and confirm titles appear
    edit_html = client.get(f"/ui/soa/{soa_id}/edit").text
    assert "Body Weight" in edit_html
    assert "Blood Pressure" in edit_html
    # ensure select options present
    assert 'option value="BC002"' in edit_html


def test_update_concepts_replaces_previous():
    reset_db_with_concepts_env()
    r = client.post("/soa", json={"name": "Replace Trial"})
    soa_id = r.json()["id"]
    act = client.post(f"/soa/{soa_id}/activities", json={"name": "Labs"}).json()[
        "activity_id"
    ]
    client.post(
        f"/soa/{soa_id}/activities/{act}/concepts",
        json={"concept_codes": ["BC001", "BC002"]},
    )
    # second update with different set
    client.post(
        f"/soa/{soa_id}/activities/{act}/concepts", json={"concept_codes": ["BC003"]}
    )
    conn = _connect()
    cur = conn.cursor()
    cur.execute("SELECT concept_code FROM activity_concept WHERE activity_id=?", (act,))
    codes = sorted(c[0] for c in cur.fetchall())
    conn.close()
    assert codes == ["BC003"]
