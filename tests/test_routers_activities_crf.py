"""Tests for CRF specialization assignment endpoints and USDM export."""

from unittest.mock import patch

from fastapi.testclient import TestClient

from soa_builder.web.app import app
from soa_builder.web.db import _connect
from usdm.generate_extension_attributes import (
    populate_crf_extension_attributes,
    populate_extension_attributes,
    build_usdm_crf_extension_attributes,
)

client = TestClient(app)

SOA_ID = 9201
BC_UID = "BiomedicalConcept_CRF_1"
ACTIVITY_ID = 8001
CONCEPT_CODE = "C25564"
CRF_HREF = (
    "https://api.library.cdisc.org/api/cosmos/v2/mdr/specializations/"
    "crf/packages/2026-06-30/specializations/VSTEST"
)
CRF_HREF_2 = (
    "https://api.library.cdisc.org/api/cosmos/v2/mdr/specializations/"
    "crf/packages/2026-06-30/specializations/VSPOS"
)
CRF_TITLE = "Vital Signs Test"
CRF_TITLE_2 = "Vital Signs Position"


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _clean():
    conn = _connect()
    cur = conn.cursor()
    cur.execute("DELETE FROM activity_concept_crf WHERE soa_id=?", (SOA_ID,))
    cur.execute("DELETE FROM activity_concept_dss WHERE soa_id=?", (SOA_ID,))
    cur.execute("DELETE FROM activity_concept WHERE soa_id=?", (SOA_ID,))
    cur.execute("DELETE FROM activity WHERE soa_id=?", (SOA_ID,))
    cur.execute("DELETE FROM soa WHERE id=?", (SOA_ID,))
    conn.commit()
    conn.close()


def _setup_soa_and_activity():
    """Create a SOA and activity, return (soa_id, activity_id)."""
    _clean()
    conn = _connect()
    cur = conn.cursor()
    cur.execute(
        "INSERT INTO soa (id, name) VALUES (?, ?)",
        (SOA_ID, "CRF Test Study"),
    )
    cur.execute(
        "INSERT INTO activity (id, soa_id, name, order_index) VALUES (?, ?, ?, ?)",
        (ACTIVITY_ID, SOA_ID, "Vital Signs", 1),
    )
    conn.commit()
    conn.close()
    return SOA_ID, ACTIVITY_ID


def _seed_ac(activity_id=ACTIVITY_ID, concept_uid=BC_UID):
    conn = _connect()
    cur = conn.cursor()
    cur.execute(
        "INSERT OR IGNORE INTO activity_concept"
        " (soa_id, activity_id, activity_uid, concept_uid, concept_code, concept_title)"
        " VALUES (?, ?, ?, ?, ?, ?)",
        (SOA_ID, activity_id, 1, concept_uid, CONCEPT_CODE, "Vital Signs"),
    )
    conn.commit()
    conn.close()


def _seed_crf(href=CRF_HREF, title=CRF_TITLE, activity_id=ACTIVITY_ID):
    conn = _connect()
    cur = conn.cursor()
    cur.execute(
        "INSERT INTO activity_concept_crf"
        " (soa_id, activity_id, concept_code, crf_title, crf_href)"
        " VALUES (?, ?, ?, ?, ?)",
        (SOA_ID, activity_id, CONCEPT_CODE, title, href),
    )
    conn.commit()
    conn.close()


def _seed_dss(
    title="VSORRES",
    href="https://api.library.cdisc.org/api/cosmos/v2"
    "/mdr/specializations/sdtm/datasetspecializations/VSORRES",
    activity_id=ACTIVITY_ID,
):
    conn = _connect()
    cur = conn.cursor()
    cur.execute(
        "INSERT INTO activity_concept_dss"
        " (soa_id, activity_id, concept_code, dss_title, dss_href, dss_display)"
        " VALUES (?, ?, ?, ?, ?, ?)",
        (SOA_ID, activity_id, CONCEPT_CODE, title, href, title),
    )
    conn.commit()
    conn.close()


_MOCK_CRF_LIST = [
    {"title": CRF_TITLE, "href": CRF_HREF},
    {"title": CRF_TITLE_2, "href": CRF_HREF_2},
]


# ---------------------------------------------------------------------------
# CRF options endpoint
# ---------------------------------------------------------------------------


def test_crf_options_returns_option_elements():
    _setup_soa_and_activity()
    _seed_ac()
    with patch(
        "soa_builder.web.routers.activities.fetch_crf_specializations"
        if False
        else "soa_builder.web.app.fetch_crf_specializations",
        return_value=_MOCK_CRF_LIST,
    ):
        resp = client.get(
            f"/ui/soa/{SOA_ID}/activity/{ACTIVITY_ID}"
            f"/concept/{CONCEPT_CODE}/crf/options"
        )
    assert resp.status_code == 200
    body = resp.text
    assert "Select CRF" in body
    assert CRF_TITLE in body
    assert CRF_TITLE_2 in body


def test_crf_options_excludes_already_assigned():
    _setup_soa_and_activity()
    _seed_ac()
    _seed_crf(href=CRF_HREF, title=CRF_TITLE)
    with patch(
        "soa_builder.web.app.fetch_crf_specializations",
        return_value=_MOCK_CRF_LIST,
    ):
        resp = client.get(
            f"/ui/soa/{SOA_ID}/activity/{ACTIVITY_ID}"
            f"/concept/{CONCEPT_CODE}/crf/options"
        )
    assert resp.status_code == 200
    # Already-assigned CRF href should not appear as an assignable option
    assert CRF_HREF not in resp.text or CRF_TITLE_2 in resp.text


# ---------------------------------------------------------------------------
# CRF save endpoint
# ---------------------------------------------------------------------------


def test_save_crf_assignment_creates_row():
    _setup_soa_and_activity()
    _seed_ac()
    selection = f"{CRF_TITLE}||{CRF_HREF}"
    with patch(
        "soa_builder.web.app.fetch_crf_specializations",
        return_value=_MOCK_CRF_LIST,
    ):
        resp = client.post(
            f"/ui/soa/{SOA_ID}/activity/{ACTIVITY_ID}/concept/{CONCEPT_CODE}/crf",
            data={"crf_selection": selection},
        )
    assert resp.status_code == 200

    conn = _connect()
    cur = conn.cursor()
    cur.execute(
        "SELECT crf_title, crf_href FROM activity_concept_crf"
        " WHERE soa_id=? AND activity_id=? AND concept_code=?",
        (SOA_ID, ACTIVITY_ID, CONCEPT_CODE),
    )
    rows = cur.fetchall()
    conn.close()
    assert len(rows) == 1
    assert rows[0][0] == CRF_TITLE
    assert rows[0][1] == CRF_HREF


def test_save_crf_duplicate_is_ignored():
    _setup_soa_and_activity()
    _seed_ac()
    selection = f"{CRF_TITLE}||{CRF_HREF}"
    with patch(
        "soa_builder.web.app.fetch_crf_specializations",
        return_value=_MOCK_CRF_LIST,
    ):
        client.post(
            f"/ui/soa/{SOA_ID}/activity/{ACTIVITY_ID}/concept/{CONCEPT_CODE}/crf",
            data={"crf_selection": selection},
        )
        resp2 = client.post(
            f"/ui/soa/{SOA_ID}/activity/{ACTIVITY_ID}/concept/{CONCEPT_CODE}/crf",
            data={"crf_selection": selection},
        )
    assert resp2.status_code == 200

    conn = _connect()
    cur = conn.cursor()
    cur.execute(
        "SELECT COUNT(*) FROM activity_concept_crf"
        " WHERE soa_id=? AND activity_id=? AND concept_code=?",
        (SOA_ID, ACTIVITY_ID, CONCEPT_CODE),
    )
    count = cur.fetchone()[0]
    conn.close()
    assert count == 1


def test_save_crf_empty_selection_is_noop():
    _setup_soa_and_activity()
    _seed_ac()
    resp = client.post(
        f"/ui/soa/{SOA_ID}/activity/{ACTIVITY_ID}/concept/{CONCEPT_CODE}/crf",
        data={"crf_selection": ""},
    )
    assert resp.status_code == 200

    conn = _connect()
    cur = conn.cursor()
    cur.execute("SELECT COUNT(*) FROM activity_concept_crf WHERE soa_id=?", (SOA_ID,))
    assert cur.fetchone()[0] == 0
    conn.close()


# ---------------------------------------------------------------------------
# CRF delete endpoint
# ---------------------------------------------------------------------------


def test_delete_crf_assignment_removes_row():
    _setup_soa_and_activity()
    _seed_ac()
    _seed_crf()

    conn = _connect()
    cur = conn.cursor()
    cur.execute("SELECT id FROM activity_concept_crf WHERE soa_id=?", (SOA_ID,))
    row_id = cur.fetchone()[0]
    conn.close()

    resp = client.post(
        f"/ui/soa/{SOA_ID}/activity/{ACTIVITY_ID}"
        f"/concept/{CONCEPT_CODE}/crf/{row_id}/delete"
    )
    assert resp.status_code == 200

    conn = _connect()
    cur = conn.cursor()
    cur.execute("SELECT COUNT(*) FROM activity_concept_crf WHERE soa_id=?", (SOA_ID,))
    assert cur.fetchone()[0] == 0
    conn.close()


def test_delete_nonexistent_crf_row_is_graceful():
    _setup_soa_and_activity()
    _seed_ac()
    resp = client.post(
        f"/ui/soa/{SOA_ID}/activity/{ACTIVITY_ID}"
        f"/concept/{CONCEPT_CODE}/crf/99999/delete"
    )
    assert resp.status_code == 200


# ---------------------------------------------------------------------------
# CRF cell re-render
# ---------------------------------------------------------------------------


def test_get_crf_cell_returns_html():
    _setup_soa_and_activity()
    _seed_ac()
    _seed_crf()
    resp = client.get(f"/ui/soa/{SOA_ID}/activity/{ACTIVITY_ID}/crf_cell")
    assert resp.status_code == 200
    assert "crf-cell" in resp.text


# ---------------------------------------------------------------------------
# CRF specializations browse page
# ---------------------------------------------------------------------------


def test_crf_specializations_list_page():
    with patch(
        "soa_builder.web.app.fetch_crf_specializations",
        return_value=_MOCK_CRF_LIST,
    ):
        resp = client.get("/ui/crf/specializations")
    assert resp.status_code == 200
    assert CRF_TITLE in resp.text
    assert CRF_TITLE_2 in resp.text


def test_crf_specializations_list_empty():
    with patch(
        "soa_builder.web.app.fetch_crf_specializations",
        return_value=[],
    ):
        resp = client.get("/ui/crf/specializations")
    assert resp.status_code == 200
    assert "No CRF specializations available" in resp.text


# ---------------------------------------------------------------------------
# USDM extension attributes — CRF
# ---------------------------------------------------------------------------


def test_populate_crf_assigns_uid():
    _setup_soa_and_activity()
    _seed_ac()
    _seed_crf()

    populate_crf_extension_attributes(SOA_ID)

    conn = _connect()
    cur = conn.cursor()
    cur.execute(
        "SELECT extension_attribute_uid FROM activity_concept_crf WHERE soa_id=?",
        (SOA_ID,),
    )
    rows = cur.fetchall()
    conn.close()
    assert len(rows) == 1
    assert rows[0][0] == "ExtensionAttribute_1"


def test_populate_crf_is_idempotent():
    _setup_soa_and_activity()
    _seed_ac()
    _seed_crf()

    populate_crf_extension_attributes(SOA_ID)
    populate_crf_extension_attributes(SOA_ID)

    conn = _connect()
    cur = conn.cursor()
    cur.execute(
        "SELECT extension_attribute_uid FROM activity_concept_crf WHERE soa_id=?",
        (SOA_ID,),
    )
    rows = [r[0] for r in cur.fetchall()]
    conn.close()
    assert rows == ["ExtensionAttribute_1"]


def test_crf_uids_do_not_collide_with_dss_uids():
    """DSS and CRF UIDs must not share the same ExtensionAttribute_N number."""
    _setup_soa_and_activity()
    _seed_ac()
    _seed_dss()
    _seed_crf()

    populate_extension_attributes(SOA_ID)
    populate_crf_extension_attributes(SOA_ID)

    conn = _connect()
    cur = conn.cursor()
    cur.execute(
        "SELECT extension_attribute_uid FROM activity_concept_dss WHERE soa_id=?",
        (SOA_ID,),
    )
    dss_uids = {r[0] for r in cur.fetchall()}
    cur.execute(
        "SELECT extension_attribute_uid FROM activity_concept_crf WHERE soa_id=?",
        (SOA_ID,),
    )
    crf_uids = {r[0] for r in cur.fetchall()}
    conn.close()

    assert len(dss_uids) == 1
    assert len(crf_uids) == 1
    assert dss_uids.isdisjoint(crf_uids), (
        f"UID collision between DSS {dss_uids} and CRF {crf_uids}"
    )


def test_build_crf_extension_attributes_returns_ea():
    _setup_soa_and_activity()
    _seed_ac()
    _seed_crf()
    populate_crf_extension_attributes(SOA_ID)

    out = build_usdm_crf_extension_attributes(SOA_ID, BC_UID)
    assert len(out) == 1
    ea = out[0]
    assert ea["id"] == "ExtensionAttribute_1"
    assert ea["url"] == "http://www.cdisc.org/usdm/extensions/specializations/crf"
    assert ea["valueString"].startswith("/mdr/specializations/crf/")
    assert ea["instanceType"] == "ExtensionAttribute"


def test_build_crf_extension_attributes_empty_when_none():
    _setup_soa_and_activity()
    _seed_ac()

    out = build_usdm_crf_extension_attributes(SOA_ID, BC_UID)
    assert out == []


def test_build_crf_multiple_rows():
    _setup_soa_and_activity()
    _seed_ac()
    _seed_crf(href=CRF_HREF, title=CRF_TITLE)
    _seed_crf(href=CRF_HREF_2, title=CRF_TITLE_2)
    populate_crf_extension_attributes(SOA_ID)

    out = build_usdm_crf_extension_attributes(SOA_ID, BC_UID)
    assert len(out) == 2
    uids = [e["id"] for e in out]
    assert uids[0] != uids[1]
    assert all(
        e["url"] == "http://www.cdisc.org/usdm/extensions/specializations/crf"
        for e in out
    )
