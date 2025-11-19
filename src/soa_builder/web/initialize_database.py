from .db import _connect


def _init_db():
    conn = _connect()
    cur = conn.cursor()
    cur.execute(
        """CREATE TABLE IF NOT EXISTS soa (id INTEGER PRIMARY KEY AUTOINCREMENT, name TEXT, created_at TEXT)"""
    )
    cur.execute(
        """CREATE TABLE IF NOT EXISTS visit (id INTEGER PRIMARY KEY AUTOINCREMENT, soa_id INTEGER, name TEXT, raw_header TEXT, order_index INTEGER)"""
    )
    cur.execute(
        """CREATE TABLE IF NOT EXISTS activity (id INTEGER PRIMARY KEY AUTOINCREMENT, soa_id INTEGER, name TEXT, order_index INTEGER, activity_uid TEXT)"""
    )
    # Arms: groupings similar to Visits. (Legacy element linkage removed; schema now only stores intrinsic fields.)
    cur.execute(
        """CREATE TABLE IF NOT EXISTS arm (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            soa_id INTEGER NOT NULL,
            name TEXT NOT NULL,
            label TEXT,
            description TEXT,
            type TEXT, -- classification for the arm (e.g., TREATMENT, CONTROL)
            data_origin_type TEXT, -- origin of the arm definition (e.g., PROTOCOL, IMPORT, MANUAL)
            order_index INTEGER,
            arm_uid TEXT -- immutable StudyArm_N identifier unique within an SOA
        )"""
    )
    # Elements: finer-grained structural units (optional) that can also be ordered
    cur.execute(
        """CREATE TABLE IF NOT EXISTS element (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            soa_id INTEGER NOT NULL,
            name TEXT NOT NULL,
            label TEXT,
            description TEXT,
            testrl TEXT,
            teenrl TEXT,
            order_index INTEGER,
            created_at TEXT
        )"""
    )
    # Element audit table capturing create/update/delete operations
    cur.execute(
        """CREATE TABLE IF NOT EXISTS element_audit (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            soa_id INTEGER NOT NULL,
            element_id INTEGER,
            action TEXT NOT NULL, -- create|update|delete|reorder
            before_json TEXT,
            after_json TEXT,
            performed_at TEXT NOT NULL
        )"""
    )
    # Visit audit table
    cur.execute(
        """CREATE TABLE IF NOT EXISTS visit_audit (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            soa_id INTEGER NOT NULL,
            visit_id INTEGER,
            action TEXT NOT NULL, -- create|update|delete|reorder
            before_json TEXT,
            after_json TEXT,
            performed_at TEXT NOT NULL
        )"""
    )
    # Activity audit table
    cur.execute(
        """CREATE TABLE IF NOT EXISTS activity_audit (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            soa_id INTEGER NOT NULL,
            activity_id INTEGER,
            action TEXT NOT NULL, -- create|update|delete|reorder
            before_json TEXT,
            after_json TEXT,
            performed_at TEXT NOT NULL
        )"""
    )
    # Arm audit table
    cur.execute(
        """CREATE TABLE IF NOT EXISTS arm_audit (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            soa_id INTEGER NOT NULL,
            arm_id INTEGER,
            action TEXT NOT NULL, -- create|update|delete|reorder
            before_json TEXT,
            after_json TEXT,
            performed_at TEXT NOT NULL
        )"""
    )
    # Epoch audit table
    cur.execute(
        """CREATE TABLE IF NOT EXISTS epoch_audit (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            soa_id INTEGER NOT NULL,
            epoch_id INTEGER,
            action TEXT NOT NULL, -- create|update|delete|reorder
            before_json TEXT,
            after_json TEXT,
            performed_at TEXT NOT NULL
        )"""
    )
    # Epochs: high-level study phase grouping (optional). Behaves like visits/activities list ordering.
    cur.execute(
        """CREATE TABLE IF NOT EXISTS epoch (id INTEGER PRIMARY KEY AUTOINCREMENT, soa_id INTEGER, name TEXT, order_index INTEGER)"""
    )
    # Matrix cells table (renamed from legacy 'cell')
    cur.execute(
        """CREATE TABLE IF NOT EXISTS matrix_cells (id INTEGER PRIMARY KEY AUTOINCREMENT, soa_id INTEGER, visit_id INTEGER, activity_id INTEGER, status TEXT)"""
    )
    # Mapping table linking activities to biomedical concepts (concept_code + title stored for snapshot purposes)
    cur.execute(
        """CREATE TABLE IF NOT EXISTS activity_concept (id INTEGER PRIMARY KEY AUTOINCREMENT, activity_id INTEGER, concept_code TEXT, concept_title TEXT)"""
    )
    # Frozen versions (snapshot JSON of current matrix & concepts)
    cur.execute(
        """CREATE TABLE IF NOT EXISTS soa_freeze (id INTEGER PRIMARY KEY AUTOINCREMENT, soa_id INTEGER, version_label TEXT, created_at TEXT, snapshot_json TEXT)"""
    )
    # Unique index to enforce one label per SoA
    cur.execute(
        """CREATE UNIQUE INDEX IF NOT EXISTS idx_soafreeze_unique ON soa_freeze(soa_id, version_label)"""
    )
    # Rollback audit log
    cur.execute(
        """CREATE TABLE IF NOT EXISTS rollback_audit (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            soa_id INTEGER NOT NULL,
            freeze_id INTEGER NOT NULL,
            performed_at TEXT NOT NULL,
            visits_restored INTEGER,
            activities_restored INTEGER,
            cells_restored INTEGER,
            concepts_restored INTEGER,
            elements_restored INTEGER
        )"""
    )
    # Reorder audit (tracks manual drag reorder operations for visits & activities)
    cur.execute(
        """CREATE TABLE IF NOT EXISTS reorder_audit (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            soa_id INTEGER NOT NULL,
            entity_type TEXT NOT NULL, -- 'visit' | 'activity' | 'epoch' | 'arm' | 'element'
            old_order_json TEXT NOT NULL,
            new_order_json TEXT NOT NULL,
            performed_at TEXT NOT NULL
        )"""
    )
    conn.commit()
    conn.close()
