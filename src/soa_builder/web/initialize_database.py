from .db import _connect

"""Script to check and create all database tables required for application"""


def _init_db():
    conn = _connect()
    cur = conn.cursor()

    # activity
    cur.execute(
        """CREATE TABLE IF NOT EXISTS activity (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            soa_id INTEGER,
            name TEXT,
            order_index INTEGER,
            activity_uid TEXT,  -- immutable Activity_N identifier unique within an SOA
            label TEXT,
            description TEXT,
            UNIQUE(soa_id,activity_uid)
        )"""
    )

    # activity_concept
    # Mapping table linking activities to biomedical concepts (concept_code + title stored for snapshot purposes)
    cur.execute(
        """CREATE TABLE IF NOT EXISTS activity_concept (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            activity_id INTEGER,
            concept_code TEXT,
            concept_title TEXT,
            concept_uid TEXT,    -- immutable BiomedicalConcept_N identifier unique within an SOA
            activity_uid TEXT,   -- joins to the activity table using this uid unique within an SOA
            soa_id INT,
            href TEXT               -- stores the API address where the BC exists; codeSystem & codeSystemVersion
        )"""
    )

    # arm
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

    # cell
    # Matrix cells table (renamed from legacy 'cell')
    cur.execute(
        """CREATE TABLE IF NOT EXISTS matrix_cells (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            soa_id INTEGER,
            visit_id INTEGER,
            activity_id INTEGER,
            status TEXT
        )"""
    )

    # code
    # create the code table to store unique Code_uid values associated with study objects
    cur.execute(
        """CREATE TABLE IF NOT EXISTS code (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            soa_id INTEGER NOT NULL,
            code_uid TEXT, -- immutable Code_N identifier unique within an SOA
            codelist_table TEXT,
            codelist_code TEXT NOT NULL,
            code TEXT NOT NULL,
            UNIQUE(soa_id, code_uid)
        )"""
    )

    # ddf_terminology: this table is created dynamically when uploading a new DDF Terminology
    # spreadsheet (app.py:5179-5545)

    # element
    # Elements: finer-grained structural units (optional) that can also be ordered
    cur.execute(
        """CREATE TABLE IF NOT EXISTS element (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            soa_id INTEGER NOT NULL,
            element_id TEXT,
            name TEXT NOT NULL,
            label TEXT,
            description TEXT,
            testrl TEXT,
            teenrl TEXT,
            order_index INTEGER,
            created_at TEXT,
            UNIQUE(soa_id,element_id)
        )"""
    )

    # epoch
    # Epochs: high-level study phase grouping (optional). Behaves like visits/activities list ordering.
    cur.execute(
        """CREATE TABLE IF NOT EXISTS epoch (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            soa_id INTEGER,
            name TEXT,
            order_index INTEGER,
            epoch_seq INTEGER,
            epoch_label TEXT,
            epoch_description TEXT,
            type TEXT,
            epoch_uid TEXT,
            UNIQUE (soa_id, epoch_uid)
        )"""
    )

    # instance
    # create instances table
    cur.execute(
        """CREATE TABLE IF NOT EXISTS instances (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        soa_id INT NOT NULL,
        instance_uid TEXT NOT NULL,     -- immutable ScheduledActivityInstance_N identifier unique within SOA
        name TEXT NOT NULL,
        label TEXT,
        description TEXT,
        default_condition_uid TEXT,
        epoch_uid TEXT,
        timeline_id TEXT,
        timeline_exit_id TEXT,
        order_index INT,
        encounter_uid TEXT,
        UNIQUE(soa_id, instance_uid)
        )"""
    )

    # protocol_terminology: this table is created dynamically when uploading a new Protocol Terminology
    # (app.py:5781-6119)

    # schedule_timelines
    # create schedule_timelines table
    cur.execute(
        """CREATE TABLE IF NOT EXISTS schedule_timelines (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        soa_id INT NOT NULL,
        schedule_timeline_uid TEXT NOT NULL,    -- immutable ScheduleTimeline_N identifier unique within SOA
        name TEXT NOT NULL,
        label TEXT,
        description TEXT,
        main_timeline INT,  -- 1=True|0=False
        entry_condition TEXT,
        entry_id,       -- dropdown select for ScheduledActivityInstance_
        exit_id TEXT,
        order_index INT,
        UNIQUE(soa_id, schedule_timeline_uid)
        )"""
    )

    # soa
    cur.execute(
        """CREATE TABLE IF NOT EXISTS soa (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT,
            created_at TEXT,
            study_id TEXT,
            study_label TEXT,
            study_description TEXT
        )"""
    )

    # study_cell
    # create the study_cell table to store the relationship between Epoch, Arm and related elements
    cur.execute(
        """CREATE TABLE IF NOT EXISTS study_cell (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            soa_id INTEGER NOT NULL,
            study_cell_uid TEXT NOT NULL, --immutable StudyCell_N identifier unique within SOA
            arm_uid TEXT NOT NULL,
            epoch_uid TEXT NOT NULL,
            element_uid TEXT NOT NULL
        )"""
    )

    # timing
    # create the timing table
    cur.execute(
        """CREATE TABLE IF NOT EXISTS timing (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        soa_id INTEGER NOT NULL,
        timing_uid TEXT NOT NULL,   -- immutable Timing_N identifier unique within SOA
        name TEXT NOT NULL,
        label TEXT,
        description TEXT,
        type TEXT,  -- value chosen from submissionValue in codelist_code C201264
        value TEXT,
        value_label TEXT,
        relative_to_from TEXT,  -- value chosen from submissionValue in codelist_code C201265
        relative_from_schedule_instance TEXT,
        relative_to_schedule_instance TEXT,
        window_label TEXT,
        window_upper TEXT,
        window_lower TEXT,
        order_index INTEGER,
        member_of_timeline TEXT,
        UNIQUE(soa_id, timing_uid)
        )"""
    )

    # transition_rule
    # create the transition_rule table to store the transition rules for elements, encounters
    cur.execute(
        """CREATE TABLE IF NOT EXISTS transition_rule (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            soa_id INTEGER NOT NULL,
            transition_rule_uid TEXT NOT NULL,  --immutable TransitionRule_N identifier unique within SOA
            name TEXT NOT NULL,
            label TEXT,
            description TEXT,
            text TEXT,
            order_index INTEGER,
            created_at TEXT,
            UNIQUE(soa_id, transition_rule_uid)
        )"""
    )

    # visit
    # Encounters
    cur.execute(
        """CREATE TABLE IF NOT EXISTS visit (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            soa_id INTEGER,
            name TEXT,
            label TEXT,
            order_index INTEGER,
            epoch_id INTEGER,
            encounter_uid TEXT,
            description TEXT,
            type TEXT,
            environmentalSettings TEXT,
            contactModes TEXT,
            transitionStartRule TEXT,
            transitionEndRule TEXT,
            scheduledAtId TEXT,
            UNIQUE(soa_id,encounter_uid)
        )"""
    )

    # AUDIT TABLES FOR TRACKING ALL CHANGES TO ENTITIES

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
    # Study Cell audit table
    cur.execute(
        """CREATE TABLE IF NOT EXISTS study_cell_audit (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            soa_id INTEGER NOT NULL,
            study_cell_id INTEGER,
            action TEXT NOT NULL, -- create|update|delete
            before_json TEXT,
            after_json TEXT,
            performed_at TEXT NOT NULL
        )"""
    )
    # Transition rule audit table
    cur.execute(
        """CREATE TABLE IF NOT EXISTS transition_rule_audit (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            soa_id INTEGER NOT NULL,
            transition_rule_id INTEGER,
            action TEXT NOT NULL, -- create|update|delete
            before_json TEXT,
            after_json TEXT,
            performed_at TEXT NOT NULL
        )"""
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

    # create timing_audit table
    cur.execute(
        """CREATE TABLE IF NOT EXISTS timing_audit (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        soa_id INT NOT NULL,
        timing_id INT NOT NULL,
        action TEXT NOT NULL,   -- create|update|delete
        before_json TEXT,
        after_json TEXT,
        performed_at TEXT
        )"""
    )

    # create schedule_timelines_audit table
    cur.execute(
        """CREATE TABLE IF NOT EXISTS schedule_timelines_audit (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                soa_id INTEGER NOT NULL,
                instance_id INTEGER,
                action TEXT NOT NULL,
                before_json TEXT,
                after_json TEXT,
                performed_at TEXT NOT NULL
            )"""
    )

    # create instance_audit table
    cur.execute(
        """CREATE TABLE IF NOT EXISTS instance_audit (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                soa_id INTEGER NOT NULL,
                instance_id INTEGER,
                action TEXT NOT NULL,
                before_json TEXT,
                after_json TEXT,
                performed_at TEXT NOT NULL
            )"""
    )

    # Frozen versions (snapshot JSON of current matrix & concepts)
    cur.execute(
        """CREATE TABLE IF NOT EXISTS soa_freeze (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            soa_id INTEGER,
            version_label TEXT,
            created_at TEXT,
            snapshot_json TEXT
        )"""
    )
    # Unique index to enforce one label per SoA
    cur.execute(
        """CREATE UNIQUE INDEX IF NOT EXISTS idx_soafreeze_unique ON soa_freeze(soa_id, version_label)"""
    )

    conn.commit()
    conn.close()
