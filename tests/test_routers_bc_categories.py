"""Tests for bc_categories router — add/remove CDISC BC categories to activities."""

from unittest.mock import patch

from fastapi.testclient import TestClient

from soa_builder.web.app import app

client = TestClient(app)

_FAKE_CONCEPTS = [
    {"code": "C12345", "title": "Systolic Blood Pressure", "href": "/mdr/bc/C12345"},
    {"code": "C67890", "title": "Diastolic Blood Pressure", "href": "/mdr/bc/C67890"},
]


def _new_soa(name: str) -> int:
    r = client.post("/soa", json={"name": name})
    assert r.status_code == 200
    return r.json()["id"]


def _new_activity(soa_id: int, name: str = "Vitals") -> int:
    r = client.post(f"/soa/{soa_id}/activities", json={"name": name})
    assert r.status_code == 200
    return r.json()["activity_id"]


def _add_category(soa_id: int, activity_id: int, name: str = "Vital Signs"):
    return client.post(
        f"/ui/soa/{soa_id}/activity/{activity_id}/bc-categories/add",
        data={"category_name": name},
    )


def _remove_category(soa_id: int, activity_id: int, name: str = "Vital Signs"):
    return client.post(
        f"/ui/soa/{soa_id}/activity/{activity_id}/bc-categories/remove",
        data={"category_name": name},
    )


def _row_count(soa_id: int, activity_id: int, category_name: str) -> int:
    from soa_builder.web.db import _connect

    conn = _connect()
    cur = conn.cursor()
    cur.execute(
        "SELECT COUNT(*) FROM activity_concept "
        "WHERE soa_id=? AND activity_id=? AND bc_category_name=?",
        (soa_id, activity_id, category_name),
    )
    n = cur.fetchone()[0]
    conn.close()
    return n


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


def test_add_bc_category_inserts_concepts():
    """Adding a category bulk-inserts all its concepts with bc_category_name."""
    soa_id = _new_soa("Cat Add Test")
    activity_id = _new_activity(soa_id)

    with patch(
        "soa_builder.web.app.fetch_biomedical_concepts_by_category",
        return_value=_FAKE_CONCEPTS,
    ):
        resp = _add_category(soa_id, activity_id)

    assert resp.status_code == 200
    assert _row_count(soa_id, activity_id, "Vital Signs") == 2


def test_add_bc_category_skips_duplicates():
    """Adding the same category twice does not double-insert rows."""
    soa_id = _new_soa("Cat Dupe Test")
    activity_id = _new_activity(soa_id)

    with patch(
        "soa_builder.web.app.fetch_biomedical_concepts_by_category",
        return_value=_FAKE_CONCEPTS,
    ):
        _add_category(soa_id, activity_id)
        _add_category(soa_id, activity_id)

    assert _row_count(soa_id, activity_id, "Vital Signs") == 2


def test_remove_bc_category_deletes_all_rows():
    """Removing a category deletes all its activity_concept rows."""
    soa_id = _new_soa("Cat Remove Test")
    activity_id = _new_activity(soa_id)

    with patch(
        "soa_builder.web.app.fetch_biomedical_concepts_by_category",
        return_value=_FAKE_CONCEPTS,
    ):
        _add_category(soa_id, activity_id)

    assert _row_count(soa_id, activity_id, "Vital Signs") == 2

    resp = _remove_category(soa_id, activity_id)
    assert resp.status_code == 200
    assert _row_count(soa_id, activity_id, "Vital Signs") == 0


def test_remove_individual_concept_after_category_add():
    """Removing one concept via /concepts/remove leaves the other intact."""
    soa_id = _new_soa("Cat Individual Remove")
    activity_id = _new_activity(soa_id)

    with patch(
        "soa_builder.web.app.fetch_biomedical_concepts_by_category",
        return_value=_FAKE_CONCEPTS,
    ):
        _add_category(soa_id, activity_id)

    resp = client.post(
        f"/ui/soa/{soa_id}/activity/{activity_id}/concepts/remove",
        data={"concept_code": "C12345"},
    )
    assert resp.status_code == 200
    assert _row_count(soa_id, activity_id, "Vital Signs") == 1

    from soa_builder.web.db import _connect

    conn = _connect()
    cur = conn.cursor()
    cur.execute(
        "SELECT concept_code FROM activity_concept "
        "WHERE soa_id=? AND activity_id=? AND bc_category_name=?",
        (soa_id, activity_id, "Vital Signs"),
    )
    remaining_codes = [r[0] for r in cur.fetchall()]
    conn.close()
    assert remaining_codes == ["C67890"]


def test_add_category_404_on_bad_soa():
    resp = client.post(
        "/ui/soa/999999/activity/1/bc-categories/add",
        data={"category_name": "Vital Signs"},
    )
    assert resp.status_code == 404


def test_add_category_404_on_bad_activity():
    soa_id = _new_soa("Cat 404 Activity")
    resp = client.post(
        f"/ui/soa/{soa_id}/activity/999999/bc-categories/add",
        data={"category_name": "Vital Signs"},
    )
    assert resp.status_code == 404


def test_remove_category_404_on_bad_soa():
    resp = client.post(
        "/ui/soa/999999/activity/1/bc-categories/remove",
        data={"category_name": "Vital Signs"},
    )
    assert resp.status_code == 404


def test_cell_html_contains_category_dropdown():
    """concepts_cell partial includes the Add category dropdown when categories exist."""
    soa_id = _new_soa("Cat Dropdown Test")
    activity_id = _new_activity(soa_id)

    fake_categories = [{"name": "Vital Signs", "title": "Vital Signs", "href": ""}]
    with patch(
        "soa_builder.web.app.fetch_biomedical_concept_categories",
        return_value=fake_categories,
    ):
        resp = client.get(f"/ui/soa/{soa_id}/activity/{activity_id}/concepts_cell")

    assert resp.status_code == 200
    assert "bc-categories/add" in resp.text
    assert "Add category" in resp.text


def test_assigned_category_excluded_from_dropdown():
    """Once a category is assigned it no longer appears in the dropdown."""
    soa_id = _new_soa("Cat Excluded Dropdown")
    activity_id = _new_activity(soa_id)

    fake_categories = [{"name": "Vital Signs", "title": "Vital Signs", "href": ""}]

    with patch(
        "soa_builder.web.app.fetch_biomedical_concepts_by_category",
        return_value=_FAKE_CONCEPTS,
    ):
        _add_category(soa_id, activity_id)

    with patch(
        "soa_builder.web.app.fetch_biomedical_concept_categories",
        return_value=fake_categories,
    ):
        resp = client.get(f"/ui/soa/{soa_id}/activity/{activity_id}/concepts_cell")

    assert resp.status_code == 200
    # The category is assigned, so it should not appear as an option
    assert 'value="Vital Signs"' not in resp.text
