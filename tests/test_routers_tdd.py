"""Tests for the SDTM Trial Design Domains (TDD) generation routes."""

from fastapi.testclient import TestClient

from soa_builder.web.app import app

client = TestClient(app)


def test_ui_tdd_200():
    """GET /ui/soa/{soa_id}/tdd returns 200 HTML for a valid SoA."""
    r = client.post("/soa", json={"name": "TDD 200 Test"})
    soa_id = r.json()["id"]

    resp = client.get(f"/ui/soa/{soa_id}/tdd")
    assert resp.status_code == 200
    assert "text/html" in resp.headers.get("content-type", "")


def test_ui_tdd_contains_all_domains():
    """Response HTML lists all three domain rows."""
    r = client.post("/soa", json={"name": "TDD Domains Test"})
    soa_id = r.json()["id"]

    resp = client.get(f"/ui/soa/{soa_id}/tdd")
    assert resp.status_code == 200
    assert "Trial Arms" in resp.text
    assert "Trial Elements" in resp.text
    assert "Trial Visits" in resp.text


def test_ui_tdd_404_nonexistent_soa():
    """GET /ui/soa/999999/tdd returns 404 for a nonexistent SoA."""
    resp = client.get("/ui/soa/999999/tdd")
    assert resp.status_code == 404


def test_download_tdd_ta_json():
    """Download TA domain as JSON returns attachment with list payload."""
    r = client.post("/soa", json={"name": "TDD TA JSON Test"})
    soa_id = r.json()["id"]

    resp = client.get(f"/soa/{soa_id}/tdd/ta/json")
    assert resp.status_code == 200
    assert "application/json" in resp.headers.get("content-type", "")
    assert "attachment" in resp.headers.get("content-disposition", "")
    assert "ta.json" in resp.headers.get("content-disposition", "")
    assert isinstance(resp.json(), list)


def test_download_tdd_ta_csv():
    """Download TA domain as CSV returns attachment with STUDYID header."""
    r = client.post("/soa", json={"name": "TDD TA CSV Test"})
    soa_id = r.json()["id"]

    resp = client.get(f"/soa/{soa_id}/tdd/ta/csv")
    assert resp.status_code == 200
    assert "text/csv" in resp.headers.get("content-type", "")
    assert "attachment" in resp.headers.get("content-disposition", "")
    assert "ta.csv" in resp.headers.get("content-disposition", "")
    assert "STUDYID" in resp.text


def test_download_tdd_te_json():
    """Download TE domain as JSON returns attachment with list payload."""
    r = client.post("/soa", json={"name": "TDD TE JSON Test"})
    soa_id = r.json()["id"]

    resp = client.get(f"/soa/{soa_id}/tdd/te/json")
    assert resp.status_code == 200
    assert "application/json" in resp.headers.get("content-type", "")
    assert "attachment" in resp.headers.get("content-disposition", "")
    assert "te.json" in resp.headers.get("content-disposition", "")
    assert isinstance(resp.json(), list)


def test_download_tdd_te_csv():
    """Download TE domain as CSV returns attachment with STUDYID header."""
    r = client.post("/soa", json={"name": "TDD TE CSV Test"})
    soa_id = r.json()["id"]

    resp = client.get(f"/soa/{soa_id}/tdd/te/csv")
    assert resp.status_code == 200
    assert "text/csv" in resp.headers.get("content-type", "")
    assert "attachment" in resp.headers.get("content-disposition", "")
    assert "te.csv" in resp.headers.get("content-disposition", "")
    assert "STUDYID" in resp.text


def test_download_tdd_404_nonexistent_soa():
    """Download endpoint returns 404 for a nonexistent SoA."""
    resp = client.get("/soa/999999/tdd/ta/json")
    assert resp.status_code == 404


def test_download_tdd_400_unknown_domain():
    """Download endpoint returns 400 for an unknown domain key."""
    r = client.post("/soa", json={"name": "TDD Bad Domain Test"})
    soa_id = r.json()["id"]

    resp = client.get(f"/soa/{soa_id}/tdd/bogus/json")
    assert resp.status_code == 400


def test_download_tdd_csv_header_when_no_data():
    """CSV for TE with no elements still returns the header row."""
    r = client.post("/soa", json={"name": "TDD TE Empty CSV Test"})
    soa_id = r.json()["id"]

    resp = client.get(f"/soa/{soa_id}/tdd/te/csv")
    assert resp.status_code == 200
    assert "text/csv" in resp.headers.get("content-type", "")
    assert "STUDYID" in resp.text


def test_download_tdd_ta_with_data():
    """TA JSON contains correct SDTM fields when arms/epochs/elements exist."""
    r = client.post("/soa", json={"name": "TDD TA Data Test"})
    soa_id = r.json()["id"]

    # Create arm, epoch, element, then link via study_cell
    arm_r = client.post(f"/soa/{soa_id}/arms", json={"name": "ArmA"})
    arm_uid = arm_r.json()["arm_uid"]

    epoch_r = client.post(f"/soa/{soa_id}/epochs", json={"name": "Screening"})
    epoch_uid = epoch_r.json()["epoch_uid"]

    el_r = client.post(f"/soa/{soa_id}/elements", json={"name": "EL-SCREEN"})
    el_uid = el_r.json()["element_uid"]

    client.post(
        f"/soa/{soa_id}/study_cells",
        json={"arm_uid": arm_uid, "epoch_uid": epoch_uid, "element_uid": el_uid},
    )

    resp = client.get(f"/soa/{soa_id}/tdd/ta/json")
    assert resp.status_code == 200
    records = resp.json()
    assert len(records) >= 1
    rec = records[0]
    assert rec["DOMAIN"] == "TA"
    assert rec["ARMCD"] == "ArmA"
    assert rec["ETCD"] == "EL-SCREEN"
    assert rec["EPOCH"] == "Screening"
    assert rec["TAETORD"] == 1


def test_download_tdd_te_with_data():
    """TE JSON contains correct SDTM fields when elements exist."""
    r = client.post("/soa", json={"name": "TDD TE Data Test"})
    soa_id = r.json()["id"]

    client.post(f"/soa/{soa_id}/elements", json={"name": "EL-RUN-IN"})

    resp = client.get(f"/soa/{soa_id}/tdd/te/json")
    assert resp.status_code == 200
    records = resp.json()
    assert len(records) >= 1
    rec = records[0]
    assert rec["DOMAIN"] == "TE"
    assert rec["ETCD"] == "EL-RUN-IN"
    assert "TESTRL" in rec
    assert "TEENRL" in rec
    assert rec["TEDUR"] == ""


def test_download_tdd_tv_json():
    """Download TV domain as JSON returns attachment with list payload."""
    r = client.post("/soa", json={"name": "TDD TV JSON Test"})
    soa_id = r.json()["id"]

    resp = client.get(f"/soa/{soa_id}/tdd/tv/json")
    assert resp.status_code == 200
    assert "application/json" in resp.headers.get("content-type", "")
    assert "attachment" in resp.headers.get("content-disposition", "")
    assert "tv.json" in resp.headers.get("content-disposition", "")
    assert isinstance(resp.json(), list)


def test_download_tdd_tv_csv():
    """Download TV domain as CSV returns attachment with STUDYID header."""
    r = client.post("/soa", json={"name": "TDD TV CSV Test"})
    soa_id = r.json()["id"]

    resp = client.get(f"/soa/{soa_id}/tdd/tv/csv")
    assert resp.status_code == 200
    assert "text/csv" in resp.headers.get("content-type", "")
    assert "attachment" in resp.headers.get("content-disposition", "")
    assert "tv.csv" in resp.headers.get("content-disposition", "")
    assert "STUDYID" in resp.text


def test_download_tdd_tv_with_data():
    """TV JSON contains correct SDTM fields when visits exist."""
    r = client.post("/soa", json={"name": "TDD TV Data Test"})
    soa_id = r.json()["id"]

    client.post(f"/soa/{soa_id}/visits", json={"name": "Visit 1"})
    client.post(f"/soa/{soa_id}/visits", json={"name": "Visit 2"})

    resp = client.get(f"/soa/{soa_id}/tdd/tv/json")
    assert resp.status_code == 200
    records = resp.json()
    assert len(records) == 2
    rec = records[0]
    assert rec["DOMAIN"] == "TV"
    assert rec["VISIT"] == "Visit 1"
    assert rec["VISITNUM"] == 1
    assert rec["VISITDY"] == ""
    assert rec["ARMCD"] == ""
    assert "ARM" in rec
    assert "TVSTRL" in rec
    assert "TVENRL" in rec


def test_download_tdd_tv_visitdy_and_arm():
    """TV JSON populates VISITDY from timing and ARMCD/ARM from instance linkage."""
    r = client.post("/soa", json={"name": "TDD TV VISITDY+ARM Test"})
    soa_id = r.json()["id"]

    # Create a timing record with ISO 8601 duration value
    t_r = client.post(f"/soa/{soa_id}/timings", json={"name": "Day 1", "value": "P1D"})
    timing_id = t_r.json()["id"]

    # Create a visit and link it to the timing via scheduledAtId
    v_r = client.post(f"/soa/{soa_id}/visits", json={"name": "Screening"})
    visit_id = v_r.json()["id"]
    client.patch(
        f"/soa/{soa_id}/visits/{visit_id}", json={"scheduledAtId": str(timing_id)}
    )

    # Get the encounter_uid assigned to the visit
    visits_list = client.get(f"/soa/{soa_id}/visits").json()
    enc_uid = visits_list[0]["encounter_uid"]

    # Create arm, epoch, element, study_cell to establish arm→epoch link
    arm_r = client.post(f"/soa/{soa_id}/arms", json={"name": "TrtArm"})
    arm_uid = arm_r.json()["arm_uid"]
    epoch_r = client.post(f"/soa/{soa_id}/epochs", json={"name": "Screen"})
    epoch_uid = epoch_r.json()["epoch_uid"]
    el_r = client.post(f"/soa/{soa_id}/elements", json={"name": "EL-S"})
    el_uid = el_r.json()["element_uid"]
    client.post(
        f"/soa/{soa_id}/study_cells",
        json={"arm_uid": arm_uid, "epoch_uid": epoch_uid, "element_uid": el_uid},
    )

    # Create an instance linking the encounter to the epoch
    inst_r = client.post(f"/soa/{soa_id}/instances", json={"name": "SAI-Screen"})
    inst_id = inst_r.json()["id"]
    client.patch(
        f"/soa/{soa_id}/instances/{inst_id}",
        json={"encounter_uid": enc_uid, "epoch_uid": epoch_uid},
    )

    resp = client.get(f"/soa/{soa_id}/tdd/tv/json")
    assert resp.status_code == 200
    records = resp.json()
    assert len(records) == 1
    rec = records[0]
    assert rec["DOMAIN"] == "TV"
    assert rec["VISIT"] == "Screening"
    assert rec["VISITDY"] == "1"
    assert rec["ARMCD"] == "TrtArm"
    assert rec["ARM"] == "TrtArm"  # falls back to arm.name when no description set
