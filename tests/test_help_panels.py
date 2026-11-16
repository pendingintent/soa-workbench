"""Contextual "What is this?" help panels (issue #382).

Each study-design page carries a collapsible panel that explains the
USDM concept the page edits. The panels come from one macro in
``_usdm_help.html`` so the wording lives in a single place.
"""

import pytest

from fastapi.testclient import TestClient

from soa_builder.web.app import app
from soa_builder.web.templating import templates

client = TestClient(app)


@pytest.fixture
def soa_id():
    resp = client.post("/soa", json={"name": "Help Panel Study"})
    assert resp.status_code in (200, 201)
    return resp.json()["id"]


PAGE_PANELS = [
    ("epochs", "epoch", "StudyEpoch"),
    ("arms", "arm", "StudyArm"),
    ("elements", "element", "StudyElement"),
    ("visits", "encounter", "Encounter"),
    ("activities", "activity", "Activity"),
]


@pytest.mark.parametrize("path, key, usdm_class", PAGE_PANELS)
def test_design_page_shows_help_panel(soa_id, path, key, usdm_class):
    resp = client.get(f"/ui/soa/{soa_id}/{path}")
    assert resp.status_code == 200
    assert 'class="collapsible am-help-panel"' in resp.text
    assert f'data-help-key="{key}"' in resp.text
    assert "What is this?" in resp.text
    assert usdm_class in resp.text


def test_design_page_shows_exactly_one_panel(soa_id):
    resp = client.get(f"/ui/soa/{soa_id}/epochs")
    assert resp.text.count("data-help-key=") == 1


@pytest.mark.parametrize(
    "key, usdm_class",
    [("instance", "ScheduledActivityInstance"), ("matrix", "Schedule")],
)
def test_edit_page_shows_instance_and_matrix_help(soa_id, key, usdm_class):
    resp = client.get(f"/ui/soa/{soa_id}/edit")
    assert resp.status_code == 200
    assert f'data-help-key="{key}"' in resp.text
    assert usdm_class in resp.text


ALL_KEYS = [
    "study",
    "title",
    "epoch",
    "arm",
    "element",
    "encounter",
    "activity",
    "biomedical_concept",
    "instance",
    "matrix",
]


def _render_panel(key, open_panel=False):
    source = (
        '{% from "_usdm_help.html" import help_panel %}'
        "{{ help_panel(key, open=open_panel) }}"
    )
    tmpl = templates.env.from_string(source)
    return tmpl.render(key=key, open_panel=open_panel)


@pytest.mark.parametrize("key", ALL_KEYS)
def test_every_documented_key_renders(key):
    html = _render_panel(key)
    assert f'data-help-key="{key}"' in html
    assert "<strong>" in html


def test_unknown_key_renders_nothing():
    assert _render_panel("no_such_concept").strip() == ""


def test_open_flag_expands_panel():
    assert " open" not in _render_panel("epoch").split(">")[0]
    assert " open" in _render_panel("epoch", open_panel=True).split(">")[0]
