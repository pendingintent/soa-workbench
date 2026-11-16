"""SoA matrix assembly, export helpers and matrix cell rendering.

Moved from app.py; app.py re-exports the public names.
"""

from ..errors import MALFORMED_JSON_ERRORS
import sqlite3
from ..initialize_database import _connect
import csv
import html as _html
import json
import os
import tempfile


NORMALIZED_ROOT = os.environ.get("SOA_BUILDER_NORMALIZED_ROOT", "normalized")


def _list_reorder_audit(soa_id: int) -> list[dict]:
    conn = _connect()
    cur = conn.cursor()
    cur.execute(
        "SELECT id, entity_type, old_order_json, new_order_json, performed_at FROM reorder_audit WHERE soa_id=? ORDER BY id DESC",
        (soa_id,),
    )
    rows = [
        {
            "id": r[0],
            "entity_type": r[1],
            "old_order": json.loads(r[2]) if r[2] else [],
            "new_order": json.loads(r[3]) if r[3] else [],
            "performed_at": r[4],
        }
        for r in cur.fetchall()
    ]
    conn.close()
    return rows


def _fetch_arms_for_edit(soa_id: int) -> list[dict]:
    """Return ordered arms for edit template."""
    try:
        conn = _connect()
        cur = conn.cursor()
        cur.execute(
            "SELECT id,name,label,description,order_index,arm_uid,COALESCE(type,''),COALESCE(data_origin_type,'') FROM arm WHERE soa_id=? ORDER BY order_index",
            (soa_id,),
        )
        rows = [
            {
                "id": r[0],
                "name": r[1],
                "label": r[2],
                "description": r[3],
                "order_index": r[4],
                "arm_uid": r[5],
                "type": r[6] or None,
                "data_origin_type": r[7] or None,
            }
            for r in cur.fetchall()
        ]
        conn.close()
        return rows
    except sqlite3.Error:
        return []


def _fetch_matrix(soa_id: int):
    conn = _connect()
    cur = conn.cursor()
    # Axis now uses ScheduledActivityInstance rows
    cur.execute(
        """
    SELECT id,name,epoch_uid,encounter_uid,instance_uid,member_of_timeline FROM instances
    WHERE soa_id=? ORDER BY member_of_timeline,id
    """,
        (soa_id,),
    )
    instances = [
        dict(
            id=r[0],
            name=r[1],
            epoch_uid=r[2],
            encounter_uid=r[3],
            instance_uid=r[4],
            member_of_timeline=r[5],
        )
        for r in cur.fetchall()
    ]
    # Activities: include optional label/description/superscript if schema supports them
    cur.execute("PRAGMA table_info(activity)")
    act_cols = {r[1] for r in cur.fetchall()}
    has_superscript = "superscript" in act_cols
    if "label" in act_cols and "description" in act_cols:
        sup_select = ",superscript" if has_superscript else ""
        cur.execute(
            f"SELECT id,name,order_index,activity_uid,label,description{sup_select}"
            " FROM activity WHERE soa_id=? ORDER BY order_index",
            (soa_id,),
        )
        activities = [
            dict(
                id=r[0],
                name=r[1],
                order_index=r[2],
                activity_uid=r[3],
                label=r[4],
                description=r[5],
                superscript=r[6] if has_superscript else None,
            )
            for r in cur.fetchall()
        ]
    else:
        cur.execute(
            "SELECT id,name,order_index,activity_uid FROM activity WHERE soa_id=? ORDER BY order_index",
            (soa_id,),
        )
        activities = [
            dict(
                id=r[0],
                name=r[1],
                order_index=r[2],
                activity_uid=r[3],
                label=None,
                description=None,
                superscript=None,
            )
            for r in cur.fetchall()
        ]
    cur.execute(
        """
        SELECT instance_id, activity_id, status, superscript FROM matrix_cells WHERE soa_id=? AND instance_id IS NOT NULL
        """,
        (soa_id,),
    )
    cells = [
        dict(instance_id=r[0], activity_id=r[1], status=r[2], superscript=r[3])
        for r in cur.fetchall()
    ]
    conn.close()
    return instances, activities, cells


# Deprecated: implemented in routers/cells.py
def _list_study_cells(soa_id: int) -> list[dict]:
    """List study_cell rows, including element and arm names filtered by soa_id.

    Returns: [{id, study_cell_uid, arm_uid, epoch_uid, element_uid, element_name, arm_name, epoch_name}]
    """
    conn = _connect()
    cur = conn.cursor()
    cur.execute(
        "SELECT sc.id, sc.study_cell_uid, sc.arm_uid, sc.epoch_uid, sc.element_uid, "
        "       e.name AS element_name, a.name AS arm_name, ep.name AS epoch_name "
        "FROM study_cell sc "
        "LEFT JOIN element e ON e.element_id = sc.element_uid AND e.soa_id = sc.soa_id "
        "LEFT JOIN arm a ON a.arm_uid = sc.arm_uid AND a.soa_id = sc.soa_id "
        "LEFT JOIN epoch ep ON ep.epoch_uid = sc.epoch_uid AND ep.soa_id = sc.soa_id "
        "WHERE sc.soa_id=? ORDER BY sc.id",
        (soa_id,),
    )
    rows = [
        {
            "id": r[0],
            "study_cell_uid": r[1],
            "arm_uid": r[2],
            "epoch_uid": r[3],
            "element_uid": r[4],
            "element_name": r[5],
            "arm_name": r[6],
            "epoch_name": r[7],
        }
        for r in cur.fetchall()
    ]
    conn.close()
    return rows


def _wide_csv_path(soa_id: int) -> str:
    return os.path.join(tempfile.gettempdir(), f"soa_{soa_id}_wide.csv")


def _generate_wide_csv(soa_id: int) -> str:
    instances, activities, cells = _fetch_matrix(soa_id)
    if not instances or not activities:
        raise ValueError(
            "Cannot generate CSV: need at least one scheduled instance and one activity"
        )
    # Build matrix with first column Activity, subsequent visit headers using label or name
    instance_headers = [i["name"] for i in instances]
    matrix = []
    for a in activities:
        row = [a["name"]]
        for inst in instances:
            match = next(
                (
                    c["status"]
                    for c in cells
                    if c["instance_id"] == inst["id"] and c["activity_id"] == a["id"]
                ),
                "",
            )
            row.append(match)
        matrix.append(row)
    path = _wide_csv_path(soa_id)
    with open(path, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(["Activity"] + instance_headers)
        writer.writerows(matrix)
    return path


def _matrix_arrays(soa_id: int):
    """Return schedule instance headers list and rows (activity name + statuses)."""
    instances, activities, cells = _fetch_matrix(soa_id)
    instance_headers = [i["name"] for i in instances]
    cell_lookup = {
        (c["instance_id"], c["activity_id"]): c.get("status", "")
        for c in cells
        if c.get("instance_id") is not None and c.get("activity_id") is not None
    }
    rows = []
    for a in activities:
        row = [a["name"]]
        for inst in instances:
            row.append(cell_lookup.get((inst["id"], a["id"]), ""))
        rows.append(row)
    return instance_headers, rows


def _fetch_enriched_instances(soa_id: int):
    """Return enriched instance data with all header information for XLSX export."""
    conn = _connect()
    cur = conn.cursor()
    cur.execute(
        """
        SELECT i.id,i.name,i.instance_uid,i.label,i.member_of_timeline,
        v.id AS encounter_id,v.name AS encounter_name,v.label AS encounter_label,v.superscript AS encounter_superscript,
        e.id AS epoch_id,e.name AS epoch_name,e.epoch_label as epoch_label,e.superscript AS epoch_superscript,
        tm.window_label,tm.label AS timing_label,tm.name AS timing_name,tm.value AS study_day
        FROM instances i
        LEFT JOIN visit v ON v.encounter_uid = i.encounter_uid AND v.soa_id = i.soa_id
        LEFT JOIN epoch e ON e.epoch_uid = i.epoch_uid AND e.soa_id = i.soa_id
        LEFT JOIN timing tm ON tm.id = v.scheduledAtId AND tm.soa_id = v.soa_id
        WHERE i.soa_id=?
        ORDER BY COALESCE(i.member_of_timeline, 'zzz'), i.order_index, i.id
        """,
        (soa_id,),
    )
    instances = [
        {
            "id": r[0],
            "name": r[1],
            "instance_uid": r[2],
            "label": r[3],
            "member_of_timeline": r[4],
            "encounter_id": r[5],
            "encounter_name": r[6],
            "encounter_label": r[7],
            "encounter_superscript": r[8],
            "epoch_id": r[9],
            "epoch_name": r[10],
            "epoch_label": r[11],
            "epoch_superscript": r[12],
            "window_label": r[13],
            "timing_label": r[14],
            "timing_name": r[15],
            "study_day": r[16],
        }
        for r in cur.fetchall()
    ]
    conn.close()
    return instances


def _add_header_rows_to_worksheet(worksheet, enriched_instances):
    """Add header rows to a worksheet with instance metadata."""
    # Insert 6 rows at the top for header rows
    worksheet.insert_rows(1, 6)

    # Build header rows
    # Row 1: Epoch (with merged cells for consecutive same values)
    worksheet.cell(1, 1, "")
    worksheet.cell(1, 2, "Epoch:")
    col_idx = 3
    epoch_groups = []  # Track (value, start_col, end_col) for merging
    prev_epoch = None
    start_col = 3
    for i, inst in enumerate(enriched_instances):
        epoch_val = inst.get("epoch_label") or inst.get("epoch_name") or ""
        if prev_epoch is None:
            prev_epoch = epoch_val
            start_col = col_idx
        elif prev_epoch != epoch_val:
            epoch_groups.append((prev_epoch, start_col, col_idx - 1))
            prev_epoch = epoch_val
            start_col = col_idx
        col_idx += 1
    # Add last group
    if prev_epoch is not None:
        epoch_groups.append((prev_epoch, start_col, col_idx - 1))

    # Write and merge epoch cells
    for epoch_val, start, end in epoch_groups:
        worksheet.cell(1, start, epoch_val)
        if start != end:
            worksheet.merge_cells(
                start_row=1, start_column=start, end_row=1, end_column=end
            )

    # Row 2: Encounter
    worksheet.cell(2, 1, "")
    worksheet.cell(2, 2, "Encounter:")
    for i, inst in enumerate(enriched_instances):
        encounter_val = inst.get("encounter_label") or inst.get("encounter_name") or ""
        worksheet.cell(2, i + 3, encounter_val)

    # Row 3: Instance (ScheduledActivityInstance)
    worksheet.cell(3, 1, "")
    worksheet.cell(3, 2, "Instance:")
    for i, inst in enumerate(enriched_instances):
        instance_val = inst.get("label") or inst.get("name") or ""
        worksheet.cell(3, i + 3, instance_val)

    # Row 4: Study Day
    worksheet.cell(4, 1, "")
    worksheet.cell(4, 2, "Study Day:")
    for i, inst in enumerate(enriched_instances):
        study_day_val = inst.get("study_day") or ""
        worksheet.cell(4, i + 3, study_day_val)

    # Row 5: Timing
    worksheet.cell(5, 1, "")
    worksheet.cell(5, 2, "Timing:")
    for i, inst in enumerate(enriched_instances):
        timing_val = inst.get("timing_label") or inst.get("timing_name") or ""
        worksheet.cell(5, i + 3, timing_val)

    # Row 6: Visit Window
    worksheet.cell(6, 1, "")
    worksheet.cell(6, 2, "Visit Window:")
    for i, inst in enumerate(enriched_instances):
        window_val = inst.get("window_label") or ""
        worksheet.cell(6, i + 3, window_val)


def instance_cell_exists(soa_id: int, instance_id: int, activity_id: int) -> bool:
    """Return True if the activity is scheduled at the instance."""
    conn = _connect()
    try:
        row = conn.execute(
            "SELECT 1 FROM matrix_cells WHERE soa_id=? AND instance_id=?"
            " AND activity_id=?",
            (soa_id, instance_id, activity_id),
        ).fetchone()
    finally:
        conn.close()
    return row is not None


def set_instance_cell(
    soa_id: int, instance_id: int, activity_id: int, on: bool
) -> None:
    """Schedule (``on``) or unschedule an activity at an instance.

    Idempotent: setting a cell to its current state changes nothing.
    """
    conn = _connect()
    try:
        cur = conn.cursor()
        cur.execute(
            "SELECT id FROM matrix_cells WHERE soa_id=? AND instance_id=?"
            " AND activity_id=?",
            (soa_id, instance_id, activity_id),
        )
        rows = cur.fetchall()
        if on and not rows:
            cur.execute(
                "INSERT INTO matrix_cells (soa_id, instance_id, activity_id,"
                " status) VALUES (?,?,?,?)",
                (soa_id, instance_id, activity_id, "X"),
            )
        elif not on and rows:
            cur.executemany(
                "DELETE FROM matrix_cells WHERE id=?", [(r[0],) for r in rows]
            )
        conn.commit()
    finally:
        conn.close()


def _render_cell_td(
    soa_id: int,
    instance_id: int,
    activity_id: int,
    status: str,
    superscript: str | None,
) -> str:
    """Build the <td> HTML for a matrix cell, including superscript and edit button."""
    soa_id_safe = _html.escape(str(soa_id), quote=True)
    instance_id_safe = _html.escape(str(instance_id), quote=True)
    activity_id_safe = _html.escape(str(activity_id), quote=True)

    if status == "X":
        sup_html = f"<sup>{_html.escape(superscript)}</sup>" if superscript else ""
        edit_btn = (
            f'<span class="sup-edit"'
            f' hx-get="/ui/soa/{soa_id_safe}/cell_superscript_edit/{instance_id_safe}/{activity_id_safe}"'
            f' hx-swap="outerHTML" hx-target="closest td"'
            f' onclick="event.stopPropagation()" title="Edit superscript">\u270e</span>'
        )
        content = f"X{sup_html}{edit_btn}"
    else:
        content = ""

    # Build hx-vals as JSON, then HTML-escape for safe embedding in attribute
    hx_vals_json = json.dumps(
        {"instance_id": instance_id, "activity_id": activity_id},
        separators=(",", ":"),
    )
    hx_vals_attr = _html.escape(hx_vals_json, quote=True)

    return (
        f'<td hx-post="/ui/soa/{soa_id_safe}/toggle_cell"'
        f" hx-vals='{hx_vals_attr}'"
        f' hx-swap="outerHTML" class="cell">{content}</td>'
    )


def _render_activity_td(
    soa_id: int,
    activity_id: int,
    display_name: str,
    superscript: str | None,
) -> str:
    """Build the <td> HTML for an activity row header, including superscript and edit button."""
    soa_id_safe = _html.escape(str(soa_id), quote=True)
    activity_id_safe = _html.escape(str(activity_id), quote=True)
    name_safe = _html.escape(display_name or "")
    sup_html = f"<sup>{_html.escape(superscript)}</sup>" if superscript else ""
    edit_btn = (
        f'<span class="sup-edit"'
        f' hx-get="/ui/soa/{soa_id_safe}/activity_superscript_edit/{activity_id_safe}"'
        f' hx-swap="outerHTML" hx-target="closest td"'
        f' onclick="event.stopPropagation()" title="Edit superscript">✎</span>'
    )
    return f"<td>{name_safe}{sup_html}{edit_btn}</td>"


def _render_encounter_th(
    soa_id: int,
    encounter_id: int,
    display_name: str,
    superscript: str | None,
) -> str:
    """Build the <th> HTML for an encounter column header, including superscript and edit button."""
    soa_id_safe = _html.escape(str(soa_id), quote=True)
    encounter_id_safe = _html.escape(str(encounter_id), quote=True)
    name_safe = _html.escape(display_name or "")
    sup_html = f"<sup>{_html.escape(superscript)}</sup>" if superscript else ""
    edit_btn = (
        f'<span class="sup-edit"'
        f' hx-get="/ui/soa/{soa_id_safe}/encounter_superscript_edit/{encounter_id_safe}"'
        f' hx-swap="outerHTML" hx-target="closest th"'
        f' onclick="event.stopPropagation()" title="Edit superscript">✎</span>'
    )
    return (
        f'<th style="text-align:center;font-size:0.50em;">'
        f"{name_safe}{sup_html}{edit_btn}</th>"
    )


def _render_epoch_th(
    soa_id: int,
    epoch_id: int,
    display_value: str,
    colspan: int,
    superscript: str | None,
) -> str:
    """Build the <th> HTML for a (possibly multi-column) epoch header, including superscript and edit button."""
    soa_id_safe = _html.escape(str(soa_id), quote=True)
    epoch_id_safe = _html.escape(str(epoch_id), quote=True)
    colspan_safe = _html.escape(str(colspan), quote=True)
    value_safe = _html.escape(display_value or "")
    sup_html = f"<sup>{_html.escape(superscript)}</sup>" if superscript else ""
    edit_btn = (
        f'<span class="sup-edit"'
        f' hx-get="/ui/soa/{soa_id_safe}/epoch_superscript_edit/{epoch_id_safe}?colspan={colspan_safe}"'
        f' hx-swap="outerHTML" hx-target="closest th"'
        f' onclick="event.stopPropagation()" title="Edit superscript">✎</span>'
    )
    return (
        f'<th style="text-align:center;font-size:0.50em;" colspan="{colspan_safe}">'
        f"{value_safe}{sup_html}{edit_btn}</th>"
    )


def _reindex(table: str, soa_id: int):
    conn = _connect()
    cur = conn.cursor()
    cur.execute(
        f"SELECT id FROM {table} WHERE soa_id=? ORDER BY order_index", (soa_id,)
    )
    ids = [r[0] for r in cur.fetchall()]
    for idx, _id in enumerate(ids, start=1):
        cur.execute(f"UPDATE {table} SET order_index=? WHERE id=?", (idx, _id))
    conn.commit()
    conn.close()


# Function to compute next available TransitionRule_{N}
def _next_transition_rule_uid(soa_id: int) -> str:
    """Compute next monotonically increasing TransitionRule_N for an SoA.
    Considers existing transition_rule rows and any prior UIDs found in transition_rule_audit.
    This guarantees we never reuse a lower N even after deletes.
    """
    conn = _connect()
    cur = conn.cursor()
    max_n = 0
    try:
        cur.execute(
            "SELECT transition_rule_uid FROM transition_rule WHERE soa_id=?",
            (soa_id,),
        )
        for (uid,) in cur.fetchall():
            if isinstance(uid, str) and uid.startswith("TransitionRule_"):
                tail = uid.split("TransitionRule_")[-1]
                if tail.isdigit():
                    max_n = max(max_n, int(tail))
    except sqlite3.Error:
        pass
    try:
        cur.execute(
            "SELECT before_json, after_json FROM transition_rule_audit WHERE soa_id=?",
            (soa_id,),
        )
        for bjson, ajson in cur.fetchall():
            for js in (bjson, ajson):
                if not js:
                    continue
                try:
                    obj = json.loads(js)
                except MALFORMED_JSON_ERRORS:
                    obj = None
                if isinstance(obj, dict):
                    uid = obj.get("transition_rule_uid") or obj.get(
                        "transition_rule_id"
                    )
                    if isinstance(uid, str) and uid.startswith("TransitionRule_"):
                        tail = uid.split("TransitionRule_")[-1]
                        if tail.isdigit():
                            max_n = max(max_n, int(tail))
    except sqlite3.Error:
        pass
    conn.close()
    return f"TransitionRule_{max_n + 1}"
