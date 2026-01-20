"""Comprehensive tests for rules (transition_rule) router endpoints."""

from fastapi.testclient import TestClient

from soa_builder.web.app import app

client = TestClient(app)


def test_list_rules_empty():
    """Test listing rules for a new SoA returns empty list."""
    r = client.post("/soa", json={"name": "Rules Test Study"})
    soa_id = r.json()["id"]

    resp = client.get(f"/soa/{soa_id}/rules")
    assert resp.status_code == 200
    assert resp.json() == []


def test_list_rules_nonexistent_soa():
    """Test listing rules for nonexistent SoA returns 404."""
    resp = client.get("/soa/999999/rules")
    assert resp.status_code == 404


def test_create_rule():
    """Test creating a rule via API."""
    r = client.post("/soa", json={"name": "Rule Create Test"})
    soa_id = r.json()["id"]

    rule_data = {
        "name": "Eligibility Rule",
        "description": "Patient must be 18+",
        "label": "Eligibility",
    }
    resp = client.post(f"/soa/{soa_id}/rules", json=rule_data)
    assert resp.status_code == 201
    data = resp.json()
    assert "id" in data
    assert data["name"] == "Eligibility Rule"
    assert "transition_rule_uid" in data


def test_create_rule_minimal():
    """Test creating rule with only required name field."""
    r = client.post("/soa", json={"name": "Minimal Rule Test"})
    soa_id = r.json()["id"]

    rule_data = {"name": "Basic Rule"}
    resp = client.post(f"/soa/{soa_id}/rules", json=rule_data)
    assert resp.status_code == 201
    data = resp.json()
    assert data["name"] == "Basic Rule"


def test_create_rule_with_text():
    """Test creating rule with text field."""
    r = client.post("/soa", json={"name": "Text Test"})
    soa_id = r.json()["id"]

    rule_data = {"name": "Age Check", "text": "age >= 18"}
    resp = client.post(f"/soa/{soa_id}/rules", json=rule_data)
    assert resp.status_code == 201
    data = resp.json()
    assert data["text"] == "age >= 18"


def test_list_rules_with_data():
    """Test listing rules returns created rules."""
    r = client.post("/soa", json={"name": "List Test"})
    soa_id = r.json()["id"]

    # Create rule
    client.post(f"/soa/{soa_id}/rules", json={"name": "Test Rule"})

    # List rules
    resp = client.get(f"/soa/{soa_id}/rules")
    assert resp.status_code == 200
    rules = resp.json()
    assert len(rules) == 1
    assert rules[0]["name"] == "Test Rule"


def test_update_rule():
    """Test updating rule via PATCH."""
    r = client.post("/soa", json={"name": "Update Test"})
    soa_id = r.json()["id"]

    # Create rule
    rule_resp = client.post(f"/soa/{soa_id}/rules", json={"name": "Original Name"})
    rule_id = rule_resp.json()["id"]

    # Update it
    update_data = {"name": "Updated Name", "label": "New Label"}
    resp = client.patch(f"/soa/{soa_id}/rules/{rule_id}", json=update_data)
    assert resp.status_code == 200
    data = resp.json()
    assert data["name"] == "Updated Name"
    assert data["label"] == "New Label"
    assert "updated_fields" in data


def test_update_rule_partial():
    """Test partial update (only some fields)."""
    r = client.post("/soa", json={"name": "Partial Update Test"})
    soa_id = r.json()["id"]

    # Create rule with all fields
    rule_resp = client.post(
        f"/soa/{soa_id}/rules",
        json={
            "name": "Original",
            "label": "Label",
            "description": "Desc",
            "text": "Text",
        },
    )
    rule_id = rule_resp.json()["id"]

    # Update only description
    update_data = {"description": "New Description"}
    resp = client.patch(f"/soa/{soa_id}/rules/{rule_id}", json=update_data)
    assert resp.status_code == 200
    data = resp.json()
    assert data["name"] == "Original"  # unchanged
    assert data["description"] == "New Description"  # changed


def test_delete_rule():
    """Test deleting a rule."""
    r = client.post("/soa", json={"name": "Delete Test"})
    soa_id = r.json()["id"]

    # Create rule
    rule_resp = client.post(f"/soa/{soa_id}/rules", json={"name": "To Delete"})
    rule_id = rule_resp.json()["id"]

    # Delete it
    resp = client.delete(f"/soa/{soa_id}/rules/{rule_id}")
    assert resp.status_code == 200
    assert resp.json()["deleted"] is True

    # Verify it's gone
    list_resp = client.get(f"/soa/{soa_id}/rules")
    rules = list_resp.json()
    rule_ids = [r["id"] for r in rules]
    assert rule_id not in rule_ids


def test_delete_nonexistent_rule():
    """Test deleting nonexistent rule returns 404."""
    r = client.post("/soa", json={"name": "Delete Nonexistent Test"})
    soa_id = r.json()["id"]

    resp = client.delete(f"/soa/{soa_id}/rules/999999")
    assert resp.status_code == 404


def test_update_nonexistent_rule():
    """Test updating nonexistent rule returns 404."""
    r = client.post("/soa", json={"name": "Update Nonexistent Test"})
    soa_id = r.json()["id"]

    resp = client.patch(f"/soa/{soa_id}/rules/999999", json={"name": "New Name"})
    assert resp.status_code == 404


def test_ui_create_rule():
    """Test creating rule via UI form."""
    r = client.post("/soa", json={"name": "UI Rule Test"})
    soa_id = r.json()["id"]

    form_data = {"name": "UI Rule", "description": "Created via UI"}
    resp = client.post(f"/ui/soa/{soa_id}/rules/create", data=form_data)
    # Returns redirect
    assert resp.status_code == 200  # TestClient doesn't follow redirects


def test_ui_update_rule():
    """Test updating rule via UI form."""
    r = client.post("/soa", json={"name": "UI Update Test"})
    soa_id = r.json()["id"]

    # Create rule
    rule_resp = client.post(f"/soa/{soa_id}/rules", json={"name": "Original"})
    rule_id = rule_resp.json()["id"]

    # Update via UI
    form_data = {"name": "Updated via UI"}
    resp = client.post(f"/ui/soa/{soa_id}/rules/{rule_id}/update", data=form_data)
    assert resp.status_code == 200  # TestClient doesn't follow redirects


def test_ui_delete_rule():
    """Test deleting rule via UI form."""
    r = client.post("/soa", json={"name": "UI Delete Test"})
    soa_id = r.json()["id"]

    # Create rule
    rule_resp = client.post(f"/soa/{soa_id}/rules", json={"name": "To Delete"})
    rule_id = rule_resp.json()["id"]

    # Delete via UI
    resp = client.post(f"/ui/soa/{soa_id}/rules/{rule_id}/delete")
    assert resp.status_code == 200  # TestClient doesn't follow redirects


def test_ui_list_rules():
    """Test UI view for listing rules."""
    r = client.post("/soa", json={"name": "UI List Test"})
    soa_id = r.json()["id"]

    resp = client.get(f"/ui/soa/{soa_id}/rules")
    assert resp.status_code == 200
    assert "text/html" in resp.headers.get("content-type", "")


def test_rule_uid_generation():
    """Test that transition_rule_uid is auto-generated."""
    r = client.post("/soa", json={"name": "UID Test"})
    soa_id = r.json()["id"]

    # Create first rule
    resp1 = client.post(f"/soa/{soa_id}/rules", json={"name": "Rule 1"})
    uid1 = resp1.json()["transition_rule_uid"]
    assert uid1.startswith("TransitionRule_")

    # Create second rule
    resp2 = client.post(f"/soa/{soa_id}/rules", json={"name": "Rule 2"})
    uid2 = resp2.json()["transition_rule_uid"]
    assert uid2.startswith("TransitionRule_")

    # UIDs should be different
    assert uid1 != uid2


def test_rule_order_index():
    """Test that rules have order_index."""
    r = client.post("/soa", json={"name": "Order Test"})
    soa_id = r.json()["id"]

    # Create multiple rules
    client.post(f"/soa/{soa_id}/rules", json={"name": "Rule 1"})
    client.post(f"/soa/{soa_id}/rules", json={"name": "Rule 2"})

    # List rules
    resp = client.get(f"/soa/{soa_id}/rules")
    rules = resp.json()

    # Should have order_index
    assert "order_index" in rules[0]
    assert "order_index" in rules[1]


def test_rule_order_index_resequenced_after_delete():
    """Test that order_index is resequenced after delete."""
    r = client.post("/soa", json={"name": "Resequence Test"})
    soa_id = r.json()["id"]

    # Create 3 rules
    client.post(f"/soa/{soa_id}/rules", json={"name": "Rule 1"})
    r2 = client.post(f"/soa/{soa_id}/rules", json={"name": "Rule 2"})
    client.post(f"/soa/{soa_id}/rules", json={"name": "Rule 3"})

    id2 = r2.json()["id"]

    # Delete middle rule
    client.delete(f"/soa/{soa_id}/rules/{id2}")

    # List remaining rules
    resp = client.get(f"/soa/{soa_id}/rules")
    rules = resp.json()

    # Should be 2 rules left
    assert len(rules) == 2

    # Order indices should be sequential (1, 2)
    indices = sorted([r["order_index"] for r in rules])
    assert indices == [1, 2]


def test_create_rule_empty_name():
    """Test creating rule with empty name fails."""
    r = client.post("/soa", json={"name": "Empty Name Test"})
    soa_id = r.json()["id"]

    rule_data = {"name": ""}
    resp = client.post(f"/soa/{soa_id}/rules", json=rule_data)
    assert resp.status_code == 400


def test_create_rule_nonexistent_soa():
    """Test creating rule for nonexistent SoA returns 404."""
    rule_data = {"name": "Test Rule"}
    resp = client.post("/soa/999999/rules", json=rule_data)
    assert resp.status_code == 404


def test_rule_all_fields():
    """Test creating rule with all fields populated."""
    r = client.post("/soa", json={"name": "All Fields Test"})
    soa_id = r.json()["id"]

    rule_data = {
        "name": "Complete Rule",
        "label": "Test Label",
        "description": "Test Description",
        "text": "Test Text",
    }
    resp = client.post(f"/soa/{soa_id}/rules", json=rule_data)
    assert resp.status_code == 201
    data = resp.json()
    assert data["name"] == "Complete Rule"
    assert data["label"] == "Test Label"
    assert data["description"] == "Test Description"
    assert data["text"] == "Test Text"
