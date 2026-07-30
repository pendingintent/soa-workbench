"""Tests for bc_surrogates router endpoints."""

from fastapi.testclient import TestClient

from soa_builder.web.app import app

client = TestClient(app)


def _make_soa(name="BC Surrogate Test"):
    r = client.post("/soa", json={"name": name})
    assert r.status_code == 200
    return r.json()["id"]


def _make_activity(soa_id, name="Test Activity"):
    r = client.post(f"/soa/{soa_id}/activities", json={"name": name})
    assert r.status_code == 200
    return r.json()["activity_id"]


# ---------------------------------------------------------------------------
# CRUD
# ---------------------------------------------------------------------------


def test_list_bc_surrogates_empty():
    soa_id = _make_soa()
    r = client.get(f"/soa/{soa_id}/bc-surrogates")
    assert r.status_code == 200
    assert r.json() == []


def test_create_bc_surrogate():
    soa_id = _make_soa()
    r = client.post(
        f"/soa/{soa_id}/bc-surrogates",
        json={"name": "TumorLength", "label": "Tumor longest diameter"},
    )
    assert r.status_code == 200
    data = r.json()
    assert data["name"] == "TumorLength"
    assert data["label"] == "Tumor longest diameter"
    assert data["surrogate_uid"].startswith("BiomedicalConceptSurrogate_")
    assert "id" in data


def test_create_bc_surrogate_uid_monotonic():
    soa_id = _make_soa()
    r1 = client.post(f"/soa/{soa_id}/bc-surrogates", json={"name": "S1"})
    r2 = client.post(f"/soa/{soa_id}/bc-surrogates", json={"name": "S2"})
    uid1 = r1.json()["surrogate_uid"]
    uid2 = r2.json()["surrogate_uid"]
    n1 = int(uid1.split("_")[-1])
    n2 = int(uid2.split("_")[-1])
    assert n2 > n1


def test_list_bc_surrogates():
    soa_id = _make_soa()
    client.post(f"/soa/{soa_id}/bc-surrogates", json={"name": "Alpha"})
    client.post(f"/soa/{soa_id}/bc-surrogates", json={"name": "Beta"})
    r = client.get(f"/soa/{soa_id}/bc-surrogates")
    assert r.status_code == 200
    names = [s["name"] for s in r.json()]
    assert "Alpha" in names
    assert "Beta" in names


def test_update_bc_surrogate():
    soa_id = _make_soa()
    create_r = client.post(
        f"/soa/{soa_id}/bc-surrogates",
        json={"name": "OldName", "label": "Old Label"},
    )
    sur_id = create_r.json()["id"]

    r = client.patch(
        f"/soa/{soa_id}/bc-surrogates/{sur_id}",
        json={
            "name": "NewName",
            "label": "New Label",
            "reference": "https://example.com",
        },
    )
    assert r.status_code == 200
    data = r.json()
    assert data["name"] == "NewName"
    assert data["label"] == "New Label"
    assert data["reference"] == "https://example.com"


def test_delete_bc_surrogate():
    soa_id = _make_soa()
    create_r = client.post(f"/soa/{soa_id}/bc-surrogates", json={"name": "ToDelete"})
    sur_id = create_r.json()["id"]

    r = client.delete(f"/soa/{soa_id}/bc-surrogates/{sur_id}")
    assert r.status_code == 200
    assert r.json()["deleted"] is True

    # Should be gone from list
    list_r = client.get(f"/soa/{soa_id}/bc-surrogates")
    assert all(s["id"] != sur_id for s in list_r.json())


def test_create_surrogate_missing_name():
    soa_id = _make_soa()
    r = client.post(f"/soa/{soa_id}/bc-surrogates", json={"label": "No name"})
    assert r.status_code == 422


def test_surrogate_not_found_on_update():
    soa_id = _make_soa()
    r = client.patch(f"/soa/{soa_id}/bc-surrogates/99999", json={"name": "X"})
    assert r.status_code == 404


def test_surrogate_not_found_on_delete():
    soa_id = _make_soa()
    r = client.delete(f"/soa/{soa_id}/bc-surrogates/99999")
    assert r.status_code == 404


# ---------------------------------------------------------------------------
# Link / Unlink
# ---------------------------------------------------------------------------


def test_link_surrogate_to_activity():
    soa_id = _make_soa()
    act_id = _make_activity(soa_id)
    create_r = client.post(f"/soa/{soa_id}/bc-surrogates", json={"name": "SurrA"})
    sur_id = create_r.json()["id"]

    r = client.post(f"/soa/{soa_id}/activities/{act_id}/bc-surrogates/{sur_id}")
    assert r.status_code == 200
    data = r.json()
    assert data["linked"] is True
    assert "surrogate_uid" in data


def test_link_surrogate_idempotent():
    soa_id = _make_soa()
    act_id = _make_activity(soa_id)
    create_r = client.post(f"/soa/{soa_id}/bc-surrogates", json={"name": "SurrB"})
    sur_id = create_r.json()["id"]

    client.post(f"/soa/{soa_id}/activities/{act_id}/bc-surrogates/{sur_id}")
    # Second link should not error
    r = client.post(f"/soa/{soa_id}/activities/{act_id}/bc-surrogates/{sur_id}")
    assert r.status_code == 200


def test_unlink_surrogate_from_activity():
    soa_id = _make_soa()
    act_id = _make_activity(soa_id)
    create_r = client.post(f"/soa/{soa_id}/bc-surrogates", json={"name": "SurrC"})
    sur_id = create_r.json()["id"]

    client.post(f"/soa/{soa_id}/activities/{act_id}/bc-surrogates/{sur_id}")
    r = client.delete(f"/soa/{soa_id}/activities/{act_id}/bc-surrogates/{sur_id}")
    assert r.status_code == 200
    assert r.json()["unlinked"] is True


def test_delete_surrogate_removes_activity_links():
    soa_id = _make_soa()
    act_id = _make_activity(soa_id)
    create_r = client.post(f"/soa/{soa_id}/bc-surrogates", json={"name": "SurrD"})
    sur_id = create_r.json()["id"]
    sur_uid = create_r.json()["surrogate_uid"]

    client.post(f"/soa/{soa_id}/activities/{act_id}/bc-surrogates/{sur_id}")
    client.delete(f"/soa/{soa_id}/bc-surrogates/{sur_id}")

    # After delete, re-query activity — the link should be gone
    # Verify via USDM output
    from usdm.generate_activities import build_usdm_activities

    acts = build_usdm_activities(soa_id)
    for a in acts:
        assert sur_uid not in a["bcSurrogateIds"]


# ---------------------------------------------------------------------------
# USDM output
# ---------------------------------------------------------------------------


def test_surrogate_appears_in_usdm_output():
    soa_id = _make_soa()
    act_id = _make_activity(soa_id, "SoA Activity")

    # Create surrogate and link to activity
    create_r = client.post(
        f"/soa/{soa_id}/bc-surrogates",
        json={"name": "PupilDilation", "label": "Pupil dilation measurement"},
    )
    sur = create_r.json()
    sur_uid = sur["surrogate_uid"]
    sur_id = sur["id"]
    client.post(f"/soa/{soa_id}/activities/{act_id}/bc-surrogates/{sur_id}")

    # Check USDM activities bcSurrogateIds
    from usdm.generate_activities import build_usdm_activities

    acts = build_usdm_activities(soa_id)
    assert any(sur_uid in a["bcSurrogateIds"] for a in acts), (
        "surrogate_uid missing from bcSurrogateIds"
    )

    # Check USDM bcSurrogates list
    from usdm.generate_bc_surrogates import build_usdm_bc_surrogates

    surrogates = build_usdm_bc_surrogates(soa_id)
    assert any(s["id"] == sur_uid for s in surrogates), (
        "surrogate missing from bcSurrogates"
    )
    matching = next(s for s in surrogates if s["id"] == sur_uid)
    assert matching["name"] == "PupilDilation"
    assert matching["instanceType"] == "BiomedicalConceptSurrogate"


def test_unlinked_surrogate_excluded_from_usdm_output():
    soa_id = _make_soa()
    act_id = _make_activity(soa_id, "SoA Activity")

    create_r = client.post(
        f"/soa/{soa_id}/bc-surrogates", json={"name": "OrphanSurrogate"}
    )
    sur_id = create_r.json()["id"]
    sur_uid = create_r.json()["surrogate_uid"]

    from usdm.generate_bc_surrogates import build_usdm_bc_surrogates

    # Never linked to an activity — must not appear in the USDM export.
    surrogates = build_usdm_bc_surrogates(soa_id)
    assert not any(s["id"] == sur_uid for s in surrogates)

    # Link then unlink — still must not appear.
    client.post(f"/soa/{soa_id}/activities/{act_id}/bc-surrogates/{sur_id}")
    client.delete(f"/soa/{soa_id}/activities/{act_id}/bc-surrogates/{sur_id}")
    surrogates = build_usdm_bc_surrogates(soa_id)
    assert not any(s["id"] == sur_uid for s in surrogates)
