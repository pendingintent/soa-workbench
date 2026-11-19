"""Eligibility criteria (USDM v4.0, CORE DDF00126).

InterventionalStudyDesign.eligibilityCriteria is a required, non-empty
list of EligibilityCriterion. Each criterion has a category from the
Category of Inclusion/Exclusion codelist (C66797), an identifier, a
next/previous order and a criterionItemId pointing to an
EligibilityCriterionItem on the StudyVersion, which holds the text.
StudyDesignPopulation.criterionIds lists the criteria.
"""

import pytest
from fastapi.testclient import TestClient

from soa_builder.web import utils
from soa_builder.web.app import app
from soa_builder.web.db import _connect
from soa_builder.web.routers._freeze_helpers import _rollback_freeze

client = TestClient(app)


def _term(codelist: str, code: str, value: str) -> dict:
    return {"codelist_code": codelist, "code": code, "submission_value": value}


@pytest.fixture(autouse=True)
def sdtm_ct(monkeypatch):
    payload = {
        "slug": "sdtmct-2026-09-25",
        "rows": [
            _term("C66797", "C25532", "INCLUSION"),
            _term("C66797", "C25370", "EXCLUSION"),
            _term("C66781", "C29848", "YEARS"),
        ],
    }
    monkeypatch.setattr(utils, "get_sdtm_ct_rows", lambda *a, **k: payload)


def _new_soa(name: str) -> int:
    return client.post("/soa", json={"name": name}).json()["id"]


def _create(soa_id: int, **fields) -> dict:
    body = {
        "name": "INCL01",
        "label": "Informed consent",
        "identifier": "I1",
        "category_code": "C25532",
        "text": "Informed consent obtained before any trial-related activities.",
        **fields,
    }
    resp = client.post(f"/soa/{soa_id}/eligibility_criteria", json=body)
    assert resp.status_code == 201, resp.text
    return resp.json()


def _usdm(soa_id: int) -> dict:
    resp = client.get(f"/soa/{soa_id}/usdm_json/full")
    assert resp.status_code == 200
    return resp.json()["study"]["versions"][0]


def _seed_three(soa_id: int) -> list:
    return [
        _create(soa_id),
        _create(
            soa_id,
            name="INCL02",
            label="Age",
            identifier="I2",
            text="Male or female, age >= 18 years at the time of consent.",
        ),
        _create(
            soa_id,
            name="EXCL01",
            label="Prior treatment",
            identifier="E1",
            category_code="C25370",
            text="Treatment with any GLP-1 receptor agonist within 90 days.",
        ),
    ]


# --- API ----------------------------------------------------------------


def test_create_assigns_uids_and_category():
    soa_id = _new_soa("Elig create")

    created = _create(soa_id)

    assert created["criterion_uid"] == "EligibilityCriterion_1"
    assert created["item_uid"] == "EligibilityCriterionItem_1"
    assert created["category_code"] == "C25532"
    assert created["category_decode"] == "INCLUSION"
    assert created["category_version"] == "2026-09-25"


@pytest.mark.parametrize(
    "fields",
    [
        {"category_code": "C29848"},
        {"name": ""},
        {"identifier": ""},
        {"text": "   "},
    ],
)
def test_create_rejects_invalid_input(fields):
    soa_id = _new_soa("Elig invalid")
    body = {
        "name": "INCL01",
        "identifier": "I1",
        "category_code": "C25532",
        "text": "Some text",
        **fields,
    }

    resp = client.post(f"/soa/{soa_id}/eligibility_criteria", json=body)

    assert resp.status_code in (400, 422)


def test_name_and_identifier_are_unique_per_study():
    soa_id = _new_soa("Elig unique")
    _create(soa_id)

    same_name = client.post(
        f"/soa/{soa_id}/eligibility_criteria",
        json={
            "name": "INCL01",
            "identifier": "I9",
            "category_code": "C25532",
            "text": "x",
        },
    )
    same_identifier = client.post(
        f"/soa/{soa_id}/eligibility_criteria",
        json={
            "name": "INCL09",
            "identifier": "I1",
            "category_code": "C25532",
            "text": "x",
        },
    )

    assert same_name.status_code == 400
    assert same_identifier.status_code == 400
    # Another study can reuse them.
    _create(_new_soa("Elig unique other"))


def test_update_and_delete_are_audited():
    soa_id = _new_soa("Elig audit")
    created = _create(soa_id)
    url = f"/soa/{soa_id}/eligibility_criteria/{created['id']}"

    updated = client.patch(url, json={"text": "Signed informed consent."})
    assert updated.status_code == 200, updated.text
    assert updated.json()["text"] == "Signed informed consent."
    assert client.delete(url).status_code == 204
    assert client.get(f"/soa/{soa_id}/eligibility_criteria").json() == []

    conn = _connect()
    try:
        actions = [
            r[0]
            for r in conn.execute(
                "SELECT action FROM eligibility_criterion_audit"
                " WHERE soa_id=? ORDER BY id",
                (soa_id,),
            )
        ]
    finally:
        conn.close()
    assert actions == ["create", "update", "delete"]


def test_uids_are_not_reused_after_delete():
    soa_id = _new_soa("Elig monotonic")
    first = _create(soa_id)
    client.delete(f"/soa/{soa_id}/eligibility_criteria/{first['id']}")

    second = _create(soa_id)

    assert second["criterion_uid"] == "EligibilityCriterion_2"
    assert second["item_uid"] == "EligibilityCriterionItem_2"


def test_update_rejects_duplicate_identifier():
    soa_id = _new_soa("Elig update dup")
    first, second, _ = _seed_three(soa_id)

    resp = client.patch(
        f"/soa/{soa_id}/eligibility_criteria/{second['id']}",
        json={"identifier": first["identifier"]},
    )

    assert resp.status_code == 400


def test_reorder_changes_order():
    soa_id = _new_soa("Elig reorder")
    i1, i2, e1 = _seed_three(soa_id)

    resp = client.put(
        f"/soa/{soa_id}/eligibility_criteria/order",
        json={
            "criterion_uids": [
                e1["criterion_uid"],
                i1["criterion_uid"],
                i2["criterion_uid"],
            ]
        },
    )

    assert resp.status_code == 200, resp.text
    listed = client.get(f"/soa/{soa_id}/eligibility_criteria").json()
    assert [c["identifier"] for c in listed] == ["E1", "I1", "I2"]


def test_reorder_needs_every_criterion_once():
    soa_id = _new_soa("Elig reorder bad")
    i1, i2, _ = _seed_three(soa_id)

    resp = client.put(
        f"/soa/{soa_id}/eligibility_criteria/order",
        json={"criterion_uids": [i1["criterion_uid"], i2["criterion_uid"]]},
    )

    assert resp.status_code == 400


def test_404_for_missing_soa_and_other_studys_criterion():
    owner = _new_soa("Elig owner")
    created = _create(owner)
    other = _new_soa("Elig other")

    assert client.get("/soa/999999/eligibility_criteria").status_code == 404
    resp = client.patch(
        f"/soa/{other}/eligibility_criteria/{created['id']}", json={"text": "x"}
    )
    assert resp.status_code == 404


# --- USDM export --------------------------------------------------------


def test_export_criteria_items_and_population_ids():
    soa_id = _new_soa("Elig export")
    _seed_three(soa_id)

    version = _usdm(soa_id)
    design = version["studyDesigns"][0]
    criteria = design["eligibilityCriteria"]

    assert [c["id"] for c in criteria] == [
        "EligibilityCriterion_1",
        "EligibilityCriterion_2",
        "EligibilityCriterion_3",
    ]
    first = criteria[0]
    assert first == {
        "id": "EligibilityCriterion_1",
        "extensionAttributes": [],
        "name": "INCL01",
        "label": "Informed consent",
        "description": None,
        "category": {
            "id": "EligibilityCriterion_1_Category",
            "extensionAttributes": [],
            "code": "C25532",
            "codeSystem": "http://www.cdisc.org",
            "codeSystemVersion": "2026-09-25",
            "decode": "INCLUSION",
            "instanceType": "Code",
        },
        "identifier": "I1",
        "criterionItemId": "EligibilityCriterionItem_1",
        "nextId": "EligibilityCriterion_2",
        "previousId": None,
        "notes": [],
        "instanceType": "EligibilityCriterion",
    }
    assert criteria[1]["previousId"] == "EligibilityCriterion_1"
    assert criteria[2]["nextId"] is None
    assert criteria[2]["category"]["decode"] == "EXCLUSION"

    items = version["eligibilityCriterionItems"]
    assert [i["id"] for i in items] == [
        "EligibilityCriterionItem_1",
        "EligibilityCriterionItem_2",
        "EligibilityCriterionItem_3",
    ]
    assert items[0]["name"] == "INCL01"
    assert items[0]["instanceType"] == "EligibilityCriterionItem"
    assert design["population"]["criterionIds"] == [c["id"] for c in criteria]


def test_export_escapes_text_as_xhtml():
    soa_id = _new_soa("Elig xhtml")
    _create(soa_id, text="HbA1c 7.0-10.5% & eGFR < 30 or > 90")

    item = _usdm(soa_id)["eligibilityCriterionItems"][0]

    assert item["text"] == "HbA1c 7.0-10.5% &amp; eGFR &lt; 30 or &gt; 90"


def test_export_without_criteria_is_empty():
    version = _usdm(_new_soa("Elig none"))

    assert version["studyDesigns"][0]["eligibilityCriteria"] == []
    assert version["eligibilityCriterionItems"] == []


# --- Freeze and rollback -----------------------------------------------


def _freeze(soa_id: int) -> int:
    client.post(f"/ui/soa/{soa_id}/freeze", data={"version_label": "v1"})
    conn = _connect()
    try:
        return conn.execute(
            "SELECT id FROM soa_freeze WHERE soa_id=? ORDER BY id DESC", (soa_id,)
        ).fetchone()[0]
    finally:
        conn.close()


def test_rollback_restores_criteria():
    soa_id = _new_soa("Elig rollback")
    i1, i2, e1 = _seed_three(soa_id)
    freeze_id = _freeze(soa_id)
    client.delete(f"/soa/{soa_id}/eligibility_criteria/{i2['id']}")
    client.patch(
        f"/soa/{soa_id}/eligibility_criteria/{i1['id']}", json={"text": "Changed"}
    )

    _rollback_freeze(soa_id, freeze_id)

    listed = client.get(f"/soa/{soa_id}/eligibility_criteria").json()
    assert [c["criterion_uid"] for c in listed] == [
        i1["criterion_uid"],
        i2["criterion_uid"],
        e1["criterion_uid"],
    ]
    assert listed[0]["text"] == i1["text"]


def test_rollback_of_older_freeze_keeps_criteria():
    """A freeze taken before this feature has no criteria key; rollback
    must leave the live criteria alone rather than delete them."""
    soa_id = _new_soa("Elig old freeze")
    freeze_id = _freeze(soa_id)
    conn = _connect()
    try:
        row = conn.execute(
            "SELECT snapshot_json FROM soa_freeze WHERE id=?", (freeze_id,)
        ).fetchone()
        import json

        snap = json.loads(row[0])
        snap.pop("eligibility_criteria", None)
        conn.execute(
            "UPDATE soa_freeze SET snapshot_json=? WHERE id=?",
            (json.dumps(snap), freeze_id),
        )
        conn.commit()
    finally:
        conn.close()
    _create(soa_id)

    _rollback_freeze(soa_id, freeze_id)

    assert len(client.get(f"/soa/{soa_id}/eligibility_criteria").json()) == 1


# --- UI -----------------------------------------------------------------


def test_ui_page_create_move_and_delete():
    soa_id = _new_soa("Elig UI")
    page = client.get(f"/ui/soa/{soa_id}/eligibility_criteria")
    assert page.status_code == 200
    assert "INCLUSION" in page.text

    for name, ident in (("INCL01", "I1"), ("INCL02", "I2")):
        resp = client.post(
            f"/ui/soa/{soa_id}/eligibility_criteria/create",
            data={
                "name": name,
                "identifier": ident,
                "category_code": "C25532",
                "text": f"{ident} text",
            },
        )
        assert resp.status_code in (200, 303), resp.text
    listed = client.get(f"/soa/{soa_id}/eligibility_criteria").json()
    second = listed[1]

    client.post(f"/ui/soa/{soa_id}/eligibility_criteria/{second['id']}/move_up")
    listed = client.get(f"/soa/{soa_id}/eligibility_criteria").json()
    assert [c["identifier"] for c in listed] == ["I2", "I1"]

    client.post(f"/ui/soa/{soa_id}/eligibility_criteria/{second['id']}/delete")
    listed = client.get(f"/soa/{soa_id}/eligibility_criteria").json()
    assert [c["identifier"] for c in listed] == ["I1"]

    page = client.get(f"/ui/soa/{soa_id}/eligibility_criteria")
    assert "I1 text" in page.text


def test_ui_create_shows_validation_error():
    soa_id = _new_soa("Elig UI error")
    _create(soa_id)

    resp = client.post(
        f"/ui/soa/{soa_id}/eligibility_criteria/create",
        data={
            "name": "INCL01",
            "identifier": "I5",
            "category_code": "C25532",
            "text": "Duplicate name",
        },
    )

    assert resp.status_code == 200
    assert "already used" in resp.text
