"""JSON API for freezes/rollback and the combined audit history."""

from fastapi.testclient import TestClient

from soa_builder.web.app import app

client = TestClient(app)


def _create_soa(name: str) -> int:
    r = client.post("/soa", json={"name": name})
    assert r.status_code in (200, 201), r.text
    return r.json()["id"]


def test_freeze_list_create_and_get():
    soa_id = _create_soa("API freezes")
    r = client.post(f"/soa/{soa_id}/freezes", json={"version_label": "v1"})
    assert r.status_code == 201, r.text
    created = r.json()
    assert created["version_label"] == "v1"

    listed = client.get(f"/soa/{soa_id}/freezes").json()
    assert [f["id"] for f in listed] == [created["id"]]

    detail = client.get(f"/soa/{soa_id}/freeze/{created['id']}")
    assert detail.status_code == 200


def test_freeze_auto_label_and_duplicate_label():
    soa_id = _create_soa("API freeze labels")
    first = client.post(f"/soa/{soa_id}/freezes", json={}).json()
    assert first["version_label"] == "v1"
    dup = client.post(f"/soa/{soa_id}/freezes", json={"version_label": "v1"})
    assert dup.status_code == 400


def test_freeze_unknown_soa_404():
    assert client.post("/soa/999999/freezes", json={}).status_code == 404
    assert client.get("/soa/999999/freezes").status_code == 404


def test_rollback_restores_and_is_audited():
    soa_id = _create_soa("API rollback")
    client.post(f"/soa/{soa_id}/activities", json={"name": "Vitals"})
    freeze_id = client.post(f"/soa/{soa_id}/freezes", json={}).json()["id"]
    client.post(f"/soa/{soa_id}/activities", json={"name": "Added later"})

    preview = client.get(f"/soa/{soa_id}/freeze/{freeze_id}/rollback_preview")
    assert preview.status_code == 200
    assert preview.json()["activities_to_restore"] == 1

    r = client.post(f"/soa/{soa_id}/freeze/{freeze_id}/rollback")
    assert r.status_code == 200, r.text
    result = r.json()
    assert result["activities_restored"] == 1
    assert result["safety_freeze_id"]

    names = [a["name"] for a in client.get(f"/soa/{soa_id}/activities").json()]
    assert names == ["Vitals"]
    audit = client.get(f"/soa/{soa_id}/rollback_audit").json()["audit"]
    assert audit[0]["freeze_id"] == freeze_id


def test_rollback_unknown_freeze_404():
    soa_id = _create_soa("API rollback 404")
    assert client.post(f"/soa/{soa_id}/freeze/987654/rollback").status_code == 404


def test_audit_history_filters_and_pages():
    soa_id = _create_soa("API audit history")
    for name in ("A", "B", "C"):
        client.post(f"/soa/{soa_id}/activities", json={"name": name})
    client.post(f"/soa/{soa_id}/visits", json={"name": "Screening"})

    everything = client.get(f"/soa/{soa_id}/audit").json()
    assert {e["entity_type"] for e in everything} >= {"activity", "visit"}

    acts = client.get(f"/soa/{soa_id}/audit", params={"entity_type": "activity"})
    assert acts.status_code == 200
    rows = acts.json()
    assert len(rows) == 3
    assert all(r["entity_type"] == "activity" and r["action"] == "create" for r in rows)
    assert rows[0]["after"]["name"] == "C"  # newest first

    page = client.get(
        f"/soa/{soa_id}/audit",
        params={"entity_type": "activity", "limit": 1, "offset": 1},
    ).json()
    assert [r["id"] for r in page] == [rows[1]["id"]]


def test_audit_history_rejects_unknown_entity_type():
    soa_id = _create_soa("API audit bad type")
    r = client.get(f"/soa/{soa_id}/audit", params={"entity_type": "nope"})
    assert r.status_code == 400
    assert "activity" in r.json()["detail"]


def test_freeze_diff_json_is_reachable():
    """Regression: /freeze/{freeze_id} used to shadow /freeze/diff.json."""
    soa_id = _create_soa("API freeze diff")
    left = client.post(f"/soa/{soa_id}/freezes", json={}).json()["id"]
    client.post(f"/soa/{soa_id}/activities", json={"name": "Labs"})
    right = client.post(f"/soa/{soa_id}/freezes", json={}).json()["id"]
    r = client.get(
        f"/soa/{soa_id}/freeze/diff.json", params={"left": left, "right": right}
    )
    assert r.status_code == 200, r.text
