import os
from importlib import reload
from fastapi.testclient import TestClient
import soa_builder.web.app as webapp
from soa_builder.web.app import app, DB_PATH

client = TestClient(app)


def reset_db_empty_concepts():
    if os.path.exists(DB_PATH):
        os.remove(DB_PATH)
    # ensure no override and skip remote
    os.environ.pop("CDISC_CONCEPTS_JSON", None)
    os.environ["CDISC_SKIP_REMOTE"] = "1"
    reload(webapp)


def test_empty_concepts_message_present():
    reset_db_empty_concepts()
    r = client.post("/soa", json={"name": "Empty Concepts Trial"})
    soa_id = r.json()["id"]
    client.post(f"/soa/{soa_id}/activities", json={"name": "Vitals"})
    # render edit page
    html = client.get(f"/ui/soa/{soa_id}/edit").text
    assert "No biomedical concepts loaded." in html
