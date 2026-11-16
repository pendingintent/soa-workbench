"""Unit tests for _diff_entity_list, the core of freeze diffs."""

from soa_builder.web.routers._freeze_helpers import _diff_entity_list


def _uids(items):
    return sorted(i["arm_uid"] for i in items)


def test_added_removed_changed():
    left = [
        {"arm_uid": "Arm_1", "name": "A", "label": "x"},
        {"arm_uid": "Arm_2", "name": "B", "label": "y"},
    ]
    right = [
        {"arm_uid": "Arm_2", "name": "B2", "label": "y"},
        {"arm_uid": "Arm_3", "name": "C", "label": "z"},
    ]
    added, removed, changed = _diff_entity_list(left, right, "arm_uid")
    assert _uids(added) == ["Arm_3"]
    assert _uids(removed) == ["Arm_1"]
    assert changed == [{"uid": "Arm_2", "changes": {"name": {"old": "B", "new": "B2"}}}]


def test_identical_lists_have_no_differences():
    rows = [{"arm_uid": "Arm_1", "name": "A"}]
    assert _diff_entity_list(rows, list(rows), "arm_uid") == ([], [], [])


def test_display_fields_limit_change_detection():
    left = [{"arm_uid": "Arm_1", "name": "A", "order_index": 1}]
    right = [{"arm_uid": "Arm_1", "name": "A", "order_index": 2}]
    assert _diff_entity_list(left, right, "arm_uid", ["name"])[2] == []
    changed = _diff_entity_list(left, right, "arm_uid", ["order_index"])[2]
    assert changed[0]["changes"] == {"order_index": {"old": 1, "new": 2}}


def test_field_present_on_one_side_only_counts_as_change():
    left = [{"arm_uid": "Arm_1", "name": "A"}]
    right = [{"arm_uid": "Arm_1", "name": "A", "label": "new"}]
    changed = _diff_entity_list(left, right, "arm_uid")[2]
    assert changed[0]["changes"] == {"label": {"old": None, "new": "new"}}


def test_none_inputs_and_rows_without_uid_are_ignored():
    right = [{"arm_uid": None, "name": "orphan"}, "not-a-dict", {"name": "no key"}]
    assert _diff_entity_list(None, right, "arm_uid") == ([], [], [])
    assert _diff_entity_list(None, None, "arm_uid") == ([], [], [])
