"""HTMX fragments returned by the SoA editor (superscript editors, the
legacy matrix cell toggle and the metadata-form alerts).

Assertions are structural (parsed tags and attributes) so markup can be
reformatted, while the HTMX contract and HTML escaping are pinned.
"""

import json
from html.parser import HTMLParser

import pytest
from fastapi.testclient import TestClient

from soa_builder.web.app import app
from soa_builder.web.db import _connect

client = TestClient(app)
PAYLOAD = '"><script>alert(1)</script>'


class _Tags(HTMLParser):
    def __init__(self):
        super().__init__()
        self.tags = []
        self.text = []

    def handle_starttag(self, tag, attrs):
        self.tags.append((tag, dict(attrs)))

    def handle_data(self, data):
        self.text.append(data)


def _parse(html):
    parser = _Tags()
    parser.feed(html)
    return parser


def _first(parsed, tag):
    return next(attrs for t, attrs in parsed.tags if t == tag)


def _exec(sql, params=()):
    conn = _connect()
    try:
        cur = conn.cursor()
        cur.execute(sql, params)
        conn.commit()
        return cur.lastrowid
    finally:
        conn.close()


@pytest.fixture
def soa():
    r = client.post("/soa", json={"name": "HTMX fragments"})
    soa_id = r.json()["id"]
    activity_id = client.post(
        f"/soa/{soa_id}/activities", json={"name": "Vitals"}
    ).json()["activity_id"]
    visit_id = _exec(
        "INSERT INTO visit (soa_id, name, order_index, encounter_uid, superscript)"
        " VALUES (?, 'V1', 1, 'Encounter_901', ?)",
        (soa_id, PAYLOAD),
    )
    instance_id = _exec(
        "INSERT INTO instances (soa_id, name, instance_uid, encounter_uid)"
        " VALUES (?, 'I1', 'ScheduledActivityInstance_901', 'Encounter_901')",
        (soa_id,),
    )
    epoch_id = _exec(
        "INSERT INTO epoch (soa_id, name, order_index, superscript)"
        " VALUES (?, 'Screening', 1, ?)",
        (soa_id, PAYLOAD),
    )
    _exec("UPDATE activity SET superscript=? WHERE id=?", (PAYLOAD, activity_id))
    _exec(
        "INSERT INTO matrix_cells (soa_id, instance_id, activity_id, status,"
        " superscript) VALUES (?, ?, ?, 'X', ?)",
        (soa_id, instance_id, activity_id, PAYLOAD),
    )
    return {
        "soa": soa_id,
        "activity": activity_id,
        "visit": visit_id,
        "instance": instance_id,
        "epoch": epoch_id,
    }


def _editor_case(ids, kind):
    s = ids["soa"]
    if kind == "cell":
        path = f"{ids['instance']}/{ids['activity']}"
        return (
            "td",
            f"/ui/soa/{s}/cell_superscript_edit/{path}",
            f"/ui/soa/{s}/cell_superscript/{path}",
            f"/ui/soa/{s}/cell_superscript_view/{path}",
        )
    entity_id = ids["visit" if kind == "encounter" else kind]
    tag = "th" if kind in ("encounter", "epoch") else "td"
    return (
        tag,
        f"/ui/soa/{s}/{kind}_superscript_edit/{entity_id}",
        f"/ui/soa/{s}/{kind}_superscript/{entity_id}",
        f"/ui/soa/{s}/{kind}_superscript_view/{entity_id}",
    )


@pytest.mark.parametrize("kind", ["cell", "activity", "encounter", "epoch"])
def test_superscript_editor_fragment(soa, kind):
    tag, url, post_url, view_url = _editor_case(soa, kind)
    if kind == "epoch":
        url += "?colspan=3"
        view_url += "?colspan=3"
    r = client.get(url)
    assert r.status_code == 200
    assert "<script>" not in r.text  # stored superscript is escaped
    parsed = _parse(r.text)
    root_tag, root = parsed.tags[0]
    assert root_tag == tag
    assert "cell-editing" in root["class"].split()
    form = _first(parsed, "form")
    assert form["hx-post"] == post_url
    assert (form["hx-swap"], form["hx-target"]) == ("outerHTML", f"closest {tag}")
    inputs = [a for t, a in parsed.tags if t == "input"]
    value_input = next(a for a in inputs if a.get("name") == "superscript")
    assert value_input["value"] == PAYLOAD
    assert "autofocus" in value_input
    cancel = _first(parsed, "span")
    assert cancel["hx-get"] == view_url
    assert cancel["onclick"] == "event.stopPropagation()"
    if kind == "epoch":
        assert root["colspan"] == "3"
        hidden = next(a for a in inputs if a.get("type") == "hidden")
        assert (hidden["name"], hidden["value"]) == ("colspan", "3")
    if kind == "cell":
        assert "X" in "".join(parsed.text)


def test_superscript_editor_missing_row_404(soa):
    r = client.get(f"/ui/soa/{soa['soa']}/activity_superscript_edit/987654")
    assert r.status_code == 404


def test_legacy_visit_toggle_fragment(soa):
    _exec("DELETE FROM matrix_cells WHERE soa_id=?", (soa["soa"],))
    r = client.post(
        f"/ui/soa/{soa['soa']}/toggle_cell",
        data={"visit_id": soa["visit"], "activity_id": soa["activity"]},
    )
    assert r.status_code == 200
    parsed = _parse(r.text)
    tag, td = parsed.tags[0]
    assert tag == "td" and "cell" in td["class"].split()
    assert td["hx-post"] == f"/ui/soa/{soa['soa']}/toggle_cell"
    assert td["hx-swap"] == "outerHTML"
    assert json.loads(td["hx-vals"]) == {
        "visit_id": soa["visit"],
        "activity_id": soa["activity"],
    }
    assert "".join(parsed.text).strip() == "X"


def _script_of(html):
    assert html.startswith("<script>") and html.endswith("</script>")
    return html[len("<script>") : -len("</script>")]


def test_update_meta_rejects_duplicate_study_id(soa):
    client.post("/soa", json={"name": "Other", "study_id": "HTMX-DUP-1"})
    r = client.post(
        f"/ui/soa/{soa['soa']}/update_meta", data={"study_id": "HTMX-DUP-1"}
    )
    script = _script_of(r.text.strip())
    assert "alert(" in script and "study_id already exists" in script
    assert f"/ui/soa/{soa['soa']}/edit" in script


def test_update_meta_requires_study_id(soa):
    r = client.post(f"/ui/soa/{soa['soa']}/update_meta", data={"study_id": ""})
    script = _script_of(r.text.strip())
    assert "study_id is required" in script
    assert f"/ui/soa/{soa['soa']}/edit" in script


def test_create_soa_rejects_duplicate_study_id():
    client.post("/soa", json={"name": "First", "study_id": "HTMX-DUP-2"})
    r = client.post("/ui/soa/create", data={"name": "Second", "study_id": "HTMX-DUP-2"})
    script = _script_of(r.text.strip())
    assert "study_id already exists" in script and 'window.location="/"' in script
