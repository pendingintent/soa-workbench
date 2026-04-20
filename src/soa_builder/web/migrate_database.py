import logging
import os
from datetime import datetime, timezone

from dotenv import load_dotenv

from .db import _connect

load_dotenv()
DB_PATH = os.environ.get("SOA_BUILDER_DB", "soa_builder_web.db")
logger = logging.getLogger("soa_builder.concepts")
if not logger.handlers:
    _h = logging.StreamHandler()
    _h.setFormatter(logging.Formatter("[%(asctime)s] %(levelname)s %(message)s"))
    logger.addHandler(_h)
logger.setLevel(logging.INFO)


# Migration: rename database table cell to matrix_cells
def _migrate_copy_cell_data():
    try:
        conn = _connect()
        cur = conn.cursor()
        # Check if both tables exist
        cur.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='cell'")
        cell_exists = cur.fetchone() is not None
        cur.execute(
            "SELECT name FROM sqlite_master WHERE type='table' AND name='matrix_cells'"
        )
        matrix_exists = cur.fetchone() is not None
        if not (cell_exists and matrix_exists):
            conn.close()
            return
        # Only copy if matrix_cells is empty
        cur.execute("SELECT COUNT(*) FROM matrix_cells")
        if cur.fetchone()[0] > 0:
            conn.close()
            return
        # Copy data
        cur.execute(
            "INSERT INTO matrix_cells (soa_id, visit_id, activity_id, status) SELECT soa_id, visit_id, activity_id, status FROM cell"
        )
        conn.commit()
        logger.info("Copied data from 'cell' to 'matrix_cells'")
        conn.close()
    except Exception as e:
        logger.warning("cell->matrix_cells data copy error: %s", e)


# Migration: add arm_uid to arm
def _migrate_add_arm_uid():
    """Ensure arm_uid column exists and is populated with StudyArm_<n> unique per soa.
    Backfills existing arms sequentially by id order if missing. Creates unique index (soa_id, arm_uid).
    """
    try:
        conn = _connect()
        cur = conn.cursor()
        cur.execute("PRAGMA table_info(arm)")
        cols = {r[1] for r in cur.fetchall()}
        if "arm_uid" not in cols:
            cur.execute("ALTER TABLE arm ADD COLUMN arm_uid TEXT")
            conn.commit()
        # Backfill any NULL arm_uid values
        cur.execute("SELECT DISTINCT soa_id FROM arm WHERE arm_uid IS NULL")
        soa_ids = [r[0] for r in cur.fetchall()]
        for sid in soa_ids:
            cur.execute(
                "SELECT id FROM arm WHERE soa_id=? AND arm_uid IS NULL ORDER BY id",
                (sid,),
            )
            ids = [r[0] for r in cur.fetchall()]
            # Determine existing numbers to avoid collision (if partial data present)
            cur.execute(
                "SELECT arm_uid FROM arm WHERE soa_id=? AND arm_uid IS NOT NULL", (sid,)
            )
            existing_uids = {r[0] for r in cur.fetchall() if r[0]}
            used_nums = set()
            for uid in existing_uids:
                if uid.startswith("StudyArm_"):
                    try:
                        used_nums.add(int(uid.split("StudyArm_")[-1]))
                    except Exception:
                        pass
            next_n = 1
            for arm_id in ids:
                while next_n in used_nums:
                    next_n += 1
                new_uid = f"StudyArm_{next_n}"
                used_nums.add(next_n)
                next_n += 1
                cur.execute("UPDATE arm SET arm_uid=? WHERE id=?", (new_uid, arm_id))
        # Create unique index
        try:
            cur.execute(
                "CREATE UNIQUE INDEX IF NOT EXISTS idx_arm_soaid_uid ON arm(soa_id, arm_uid)"
            )
            conn.commit()
        except Exception:
            pass
        conn.commit()
        conn.close()
    except Exception as e:
        logger.warning("arm_uid migration failed: %s", e)


# Migration: drop deprecated arm linkage columns
def _migrate_drop_arm_element_link():
    """If legacy columns (element_id, etcd) exist in arm, rebuild table without them.
    SQLite cannot drop columns directly; we create new table, copy data, replace.
    Safe to run multiple times (idempotent)."""
    try:
        conn = _connect()
        cur = conn.cursor()
        cur.execute("PRAGMA table_info(arm)")
        cols = [r[1] for r in cur.fetchall()]
        if "element_id" in cols or "etcd" in cols:
            logger.info(
                "Rebuilding arm table to drop deprecated columns element_id, etcd"
            )
            # Determine if arm_uid index exists to recreate later
            cur.execute(
                "SELECT name FROM sqlite_master WHERE type='index' AND name='idx_arm_soaid_uid'"
            )
            has_uid_index = cur.fetchone() is not None
            # Create new table
            cur.execute(
                """
                CREATE TABLE arm_new (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    soa_id INTEGER NOT NULL,
                    name TEXT NOT NULL,
                    label TEXT,
                    description TEXT,
                    order_index INTEGER,
                    arm_uid TEXT
                )
            """
            )
            # Copy data (ignore legacy columns)
            # Only select columns that persist
            """
            select_cols = [
                c
                for c in [
                    "id",
                    "soa_id",
                    "name",
                    "label",
                    "description",
                    "order_index",
                    "arm_uid",
                ]
                if c in cols
            ]
            """
            cur.execute(
                "INSERT INTO arm_new (id,soa_id,name,label,description,order_index,arm_uid) SELECT id,soa_id,name,label,description,order_index,arm_uid FROM arm"
            )
            # Drop old table, rename
            cur.execute("DROP TABLE arm")
            cur.execute("ALTER TABLE arm_new RENAME TO arm")
            if has_uid_index:
                try:
                    cur.execute(
                        "CREATE UNIQUE INDEX IF NOT EXISTS idx_arm_soaid_uid ON arm(soa_id, arm_uid)"
                    )
                except Exception:
                    pass
            conn.commit()
        conn.close()
    except Exception as e:
        logger.warning("arm linkage drop migration failed: %s", e)


# Migration: add epoch_id to visit
def _migrate_add_epoch_id_to_visit():
    """Add epoch_id column to visit table if missing."""
    try:
        conn = _connect()
        cur = conn.cursor()
        cur.execute("PRAGMA table_info(visit)")
        cols = {r[1] for r in cur.fetchall()}
        if "epoch_id" not in cols:
            cur.execute("ALTER TABLE visit ADD COLUMN epoch_id INTEGER")
            conn.commit()
            logger.info("Added epoch_id column to visit table")
        conn.close()
    except Exception as e:
        logger.warning("epoch_id migration failed: %s", e)


# Migration: add epoch_seq to epoch
def _migrate_add_epoch_seq():
    """Ensure epoch_seq (immutable sequence per SoA) exists; backfill sequential values per study.
    Creates unique index (soa_id, epoch_seq) to guarantee uniqueness inside a study.
    """
    try:
        conn = _connect()
        cur = conn.cursor()
        cur.execute("PRAGMA table_info(epoch)")
        cols = {r[1] for r in cur.fetchall()}
        if "epoch_seq" not in cols:
            cur.execute("ALTER TABLE epoch ADD COLUMN epoch_seq INTEGER")
            conn.commit()
            logger.info("Added epoch_seq column to epoch table")
            # Backfill existing epochs with sequential values by id order per soa
            cur.execute("SELECT DISTINCT soa_id FROM epoch")
            soa_ids = [r[0] for r in cur.fetchall()]
            for sid in soa_ids:
                cur.execute("SELECT id FROM epoch WHERE soa_id=? ORDER BY id", (sid,))
                ids = [r[0] for r in cur.fetchall()]
                for seq, eid in enumerate(ids, start=1):
                    cur.execute("UPDATE epoch SET epoch_seq=? WHERE id=?", (seq, eid))
            conn.commit()
        # Unique index (idempotent)
        try:
            cur.execute(
                "CREATE UNIQUE INDEX IF NOT EXISTS idx_epoch_soaid_seq ON epoch(soa_id, epoch_seq)"
            )
            conn.commit()
        except Exception as ie:  # pragma: no cover
            logger.warning("Failed creating idx_epoch_soaid_seq: %s", ie)
        conn.close()
    except Exception as e:  # pragma: no cover
        logger.warning("epoch_seq migration failed: %s", e)


# Migration: add visit label/description
def _migrate_visit_add_label_desc():
    """Add optional label and description columns to visit if missing."""
    try:
        conn = _connect()
        cur = conn.cursor()
        cur.execute("PRAGMA table_info(visit)")
        cols = {r[1] for r in cur.fetchall()}
        alters: list[str] = []
        if "label" not in cols:
            alters.append("ALTER TABLE visit ADD COLUMN label TEXT")
        if "description" not in cols:
            alters.append("ALTER TABLE visit ADD COLUMN description TEXT")
        for stmt in alters:
            try:
                cur.execute(stmt)
            except Exception as e:
                logger.warning("Failed visit migration '%s': %s", stmt, e)
        if alters:
            conn.commit()
            logger.info(
                "Applied visit label/description migration: %s", ", ".join(alters)
            )
        conn.close()
    except Exception as e:
        logger.warning("visit label/description migration failed: %s", e)


# Migration: add epoch label/description
def _migrate_add_epoch_label_desc():
    """Add optional epoch_label and epoch_description columns if missing."""
    try:
        conn = _connect()
        cur = conn.cursor()
        cur.execute("PRAGMA table_info(epoch)")
        cols = {r[1] for r in cur.fetchall()}
        alters = []
        if "epoch_label" not in cols:
            alters.append("ALTER TABLE epoch ADD COLUMN epoch_label TEXT")
        if "epoch_description" not in cols:
            alters.append("ALTER TABLE epoch ADD COLUMN epoch_description TEXT")
        for stmt in alters:
            try:
                cur.execute(stmt)
            except Exception as e:  # pragma: no cover
                logger.warning(
                    "Failed epoch label/description migration '%s': %s", stmt, e
                )
        if alters:
            conn.commit()
            logger.info(
                "Applied epoch label/description migration: %s", ", ".join(alters)
            )
        conn.close()
    except Exception as e:  # pragma: no cover
        logger.warning("Epoch label/description migration failed: %s", e)


# Migrate: add epoch type (options from SDTM CT codelist_code=C99079)
def _migrate_add_epoch_type():
    """Add optional epoch type column if missing"""
    try:
        conn = _connect()
        cur = conn.cursor()
        cur.execute("PRAGMA table_info(epoch)")
        cols = {r[1] for r in cur.fetchall()}
        alters = []
        if "type" not in cols:
            alters.append("ALTER TABLE epoch ADD COLUMN type TEXT")
        for statement in alters:
            try:
                cur.execute(statement)
            except Exception as e:
                logger.warning("Failed epoch type migration '%s': %s", statement, e)
        if alters:
            conn.commit()
            logger.info("Applied epoch type migration: %s", ", ".join(alters))
        conn.close()
    except Exception as e:
        logger.warning("Epoch type migration failed: %s", e)


# Migration: add epoch_uid to epoch
def _migrate_add_epoch_uid():
    """Ensure epoch_uid column exists and is populated as StudyEpoch_<n> unique per SoA.
    Uses epoch_seq when available to keep numbering stable; otherwise falls back to id order.
    Creates unique index (soa_id, epoch_uid).
    """
    try:
        conn = _connect()
        cur = conn.cursor()
        cur.execute("PRAGMA table_info(epoch)")
        cols = {r[1] for r in cur.fetchall()}
        if "epoch_uid" not in cols:
            cur.execute("ALTER TABLE epoch ADD COLUMN epoch_uid TEXT")
            conn.commit()
            logger.info("Added epoch_uid column to epoch table")
        # Backfill any NULL epoch_uid values
        cur.execute("SELECT DISTINCT soa_id FROM epoch")
        soa_ids = [r[0] for r in cur.fetchall()]
        for sid in soa_ids:
            # Prefer ordering by epoch_seq if present to make UIDs deterministic
            order_col = "epoch_seq" if "epoch_seq" in cols else "id"
            cur.execute(
                f"SELECT id, COALESCE(epoch_seq, 0) FROM epoch WHERE soa_id=? AND epoch_uid IS NULL ORDER BY {order_col}",
                (sid,),
            )
            rows = cur.fetchall()
            if not rows:
                continue
            # Determine used numbers to avoid collisions when partially populated
            cur.execute(
                "SELECT epoch_uid FROM epoch WHERE soa_id=? AND epoch_uid IS NOT NULL",
                (sid,),
            )
            used_nums = set()
            for (uid,) in cur.fetchall():
                if isinstance(uid, str) and uid.startswith("StudyEpoch_"):
                    try:
                        used_nums.add(int(uid.split("StudyEpoch_")[-1]))
                    except Exception:
                        pass
            for eid, seq in rows:
                n = int(seq) if int(seq) > 0 and int(seq) not in used_nums else None
                if n is None:
                    # pick next available number
                    n = 1
                    while n in used_nums:
                        n += 1
                uid = f"StudyEpoch_{n}"
                used_nums.add(n)
                cur.execute("UPDATE epoch SET epoch_uid=? WHERE id=?", (uid, eid))
        # Create unique index
        try:
            cur.execute(
                "CREATE UNIQUE INDEX IF NOT EXISTS idx_epoch_soaid_uid ON epoch(soa_id, epoch_uid)"
            )
            conn.commit()
        except Exception:
            pass
        # Create trigger to auto-fill epoch_uid on insert when NULL
        try:
            cur.execute(
                """
                CREATE TRIGGER IF NOT EXISTS tr_epoch_uid_autofill
                AFTER INSERT ON epoch
                FOR EACH ROW
                WHEN NEW.epoch_uid IS NULL
                BEGIN
                    UPDATE epoch
                    SET epoch_uid = 'StudyEpoch_' || COALESCE(NEW.epoch_seq, NEW.id)
                    WHERE id = NEW.id;
                END;
                """
            )
            conn.commit()
        except Exception:
            pass
        conn.commit()
        conn.close()
    except Exception as e:
        logger.warning("epoch_uid migration failed: %s", e)


# Migrations: add study metadata columns
def _migrate_add_study_fields():
    """Ensure study metadata columns (study_id, study_label, study_description) exist on soa table.
    Safe to run repeatedly; SQLite ADD COLUMN is idempotent when guarded by schema inspection.
    """
    try:
        conn = _connect()
        cur = conn.cursor()
        cur.execute("PRAGMA table_info(soa)")
        existing = {r[1] for r in cur.fetchall()}  # column names
        alters = []
        if "study_id" not in existing:
            alters.append("ALTER TABLE soa ADD COLUMN study_id TEXT")
        if "study_label" not in existing:
            alters.append("ALTER TABLE soa ADD COLUMN study_label TEXT")
        if "study_description" not in existing:
            alters.append("ALTER TABLE soa ADD COLUMN study_description TEXT")
        for stmt in alters:
            try:
                cur.execute(stmt)
            except (
                Exception
            ) as e:  # pragma: no cover - defensive; should not fail normally
                logger.warning("Failed executing migration statement '%s': %s", stmt, e)
        if alters:
            conn.commit()
        # Create unique index on study_id (NULLs allowed multiple times by SQLite)
        try:
            cur.execute(
                "CREATE UNIQUE INDEX IF NOT EXISTS idx_soa_study_id ON soa(study_id)"
            )
            conn.commit()
        except Exception as e:  # pragma: no cover
            logger.warning("Failed creating unique index idx_soa_study_id: %s", e)
        conn.close()
        if alters:
            logger.info("Applied study field migrations: %s", ", ".join(alters))
    except Exception as e:  # pragma: no cover
        logger.warning("Study field migration failed: %s", e)


# Migrations: Drop legacy activity_concept_override table
def _drop_unused_override_table():
    """Drop legacy activity_concept_override table if it still exists.
    This table supported mutable concept titles which are no longer allowed.
    Safe to run repeatedly; will no-op if table absent."""
    try:
        conn = _connect()
        cur = conn.cursor()
        cur.execute(
            "SELECT name FROM sqlite_master WHERE type='table' AND name='activity_concept_override'"
        )
        if cur.fetchone():
            try:
                cur.execute("DROP TABLE activity_concept_override")
                conn.commit()
                logger.info("Dropped obsolete table activity_concept_override")
            except Exception as e:
                logger.warning(
                    "Failed to drop obsolete table activity_concept_override: %s", e
                )
        conn.close()
    except Exception as e:
        logger.warning("Migration check for activity_concept_override failed: %s", e)


# Migration: ensure element table columns exist
def _migrate_element_table():
    """Ensure element table has full expected schema (order_index, label, description, testrl, teenrl, created_at).
    Backfills order_index sequentially by id if missing.
    Safe to run repeatedly."""
    try:
        conn = _connect()
        cur = conn.cursor()
        cur.execute(
            "SELECT name FROM sqlite_master WHERE type='table' AND name='element'"
        )
        if not cur.fetchone():
            conn.close()
            return  # table does not exist yet (fresh init will create with full schema)
        cur.execute("PRAGMA table_info(element)")
        cols = {r[1] for r in cur.fetchall()}
        alters = []
        # Add missing columns
        if "order_index" not in cols:
            alters.append("ALTER TABLE element ADD COLUMN order_index INTEGER")
        if "label" not in cols:
            alters.append("ALTER TABLE element ADD COLUMN label TEXT")
        if "description" not in cols:
            alters.append("ALTER TABLE element ADD COLUMN description TEXT")
        if "testrl" not in cols:
            alters.append("ALTER TABLE element ADD COLUMN testrl TEXT")
        if "teenrl" not in cols:
            alters.append("ALTER TABLE element ADD COLUMN teenrl TEXT")
        if "created_at" not in cols:
            alters.append("ALTER TABLE element ADD COLUMN created_at TEXT")
        for stmt in alters:
            try:
                cur.execute(stmt)
            except Exception as e:  # pragma: no cover
                logger.warning("Element migration failed executing '%s': %s", stmt, e)
        if alters:
            conn.commit()
        # Backfill order_index if newly added
        if "order_index" not in cols:
            cur.execute("SELECT id FROM element ORDER BY id")
            ids = [r[0] for r in cur.fetchall()]
            for idx, eid in enumerate(ids, start=1):
                cur.execute("UPDATE element SET order_index=? WHERE id=?", (idx, eid))
        # Backfill created_at
        if "created_at" not in cols:
            now = datetime.now(timezone.utc).isoformat()
            cur.execute(
                "UPDATE element SET created_at=? WHERE created_at IS NULL", (now,)
            )
        conn.commit()
        conn.close()
        if alters:
            logger.info("Applied element table migration: %s", ", ".join(alters))
    except Exception as e:  # pragma: no cover
        logger.warning("Element table migration encountered error: %s", e)


# Migration: rename legacy 'cell' table to 'matrix_cells'
def _migrate_rename_cell_table():
    try:
        conn = _connect()
        cur = conn.cursor()
        # If new table already exists nothing to do
        cur.execute(
            "SELECT name FROM sqlite_master WHERE type='table' AND name='matrix_cells'"
        )
        if cur.fetchone():
            conn.close()
            return
        # If legacy table exists rename it
        cur.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='cell'")
        if cur.fetchone():
            try:
                cur.execute("ALTER TABLE cell RENAME TO matrix_cells")
                conn.commit()
                logger.info("Renamed legacy table 'cell' to 'matrix_cells'")
            except Exception as e:  # pragma: no cover
                logger.warning("Failed renaming cell table: %s", e)
        else:
            # Create fresh matrix_cells if neither present (defensive)
            cur.execute(
                """CREATE TABLE IF NOT EXISTS matrix_cells (id INTEGER PRIMARY KEY AUTOINCREMENT, soa_id INTEGER, visit_id INTEGER, activity_id INTEGER, status TEXT)"""
            )
            conn.commit()
            logger.info("Created matrix_cells table (no prior cell table found)")
        conn.close()
    except Exception as e:  # pragma: no cover
        logger.warning("cell->matrix_cells migration error: %s", e)


# Migration: ensure element_id column with unique StudyElement_<n> values
def _migrate_element_id():
    """Ensure element.element_id column exists and values follow prefix 'StudyElement_<n>' unique per SOA.

    Steps per SOA:
      - Add column if missing (nullable initially)
      - Collect existing values; parse numbers from well-formed prefixes StudyElement_<n>
      - Reassign malformed/NULL/duplicate values to next available sequential numbers starting at 1.
      - Create unique index (soa_id, element_id).
    Safe to run multiple times; idempotent aside from normalizing malformed values."""
    try:
        conn = _connect()
        cur = conn.cursor()
        cur.execute("PRAGMA table_info(element)")
        cols = {r[1] for r in cur.fetchall()}
        if "element" not in cols and not cols:  # table missing entirely
            conn.close()
            return
        if "element_id" not in cols:
            try:
                cur.execute("ALTER TABLE element ADD COLUMN element_id TEXT")
                conn.commit()
                logger.info("Added element_id column to element table")
            except Exception as e:  # pragma: no cover
                logger.warning("Failed adding element_id column: %s", e)
        # Backfill / normalize per SOA
        cur.execute("SELECT DISTINCT soa_id FROM element")
        soa_ids = [r[0] for r in cur.fetchall()]
        for sid in soa_ids:
            cur.execute(
                "SELECT id, element_id FROM element WHERE soa_id=? ORDER BY id", (sid,)
            )
            rows = cur.fetchall()
            used_nums = set()
            # Capture already valid numbers
            for _id, _eid in rows:
                if _eid and isinstance(_eid, str) and _eid.startswith("StudyElement_"):
                    try:
                        n = int(_eid.split("StudyElement_")[-1])
                        if n > 0:
                            if n not in used_nums:
                                used_nums.add(n)
                            else:
                                # mark duplicate for reassignment by blanking
                                cur.execute(
                                    "UPDATE element SET element_id=NULL WHERE id=?",
                                    (_id,),
                                )
                    except Exception:  # pragma: no cover
                        pass
            # Re-fetch after clearing duplicates
            cur.execute(
                "SELECT id, element_id FROM element WHERE soa_id=? ORDER BY id", (sid,)
            )
            rows = cur.fetchall()
            next_n = 1
            for _id, _eid in rows:
                valid = (
                    _eid
                    and isinstance(_eid, str)
                    and _eid.startswith("StudyElement_")
                    and _eid.split("StudyElement_")[-1].isdigit()
                )
                if valid:
                    continue  # leave intact
                while next_n in used_nums:
                    next_n += 1
                new_val = f"StudyElement_{next_n}"
                used_nums.add(next_n)
                next_n += 1
                cur.execute(
                    "UPDATE element SET element_id=? WHERE id=?", (new_val, _id)
                )
        # Create unique index
        try:
            cur.execute(
                "CREATE UNIQUE INDEX IF NOT EXISTS idx_element_soaid_elementid ON element(soa_id, element_id)"
            )
            conn.commit()
        except Exception as e:  # pragma: no cover
            logger.warning(
                "Failed creating unique index idx_element_soaid_elementid: %s", e
            )
        conn.commit()
        conn.close()
    except Exception as e:  # pragma: no cover
        logger.warning("element_id migration encountered error: %s", e)


# Migration: Add elements_restored to rollback_audit
def _migrate_rollback_add_elements_restored():
    try:
        conn = _connect()
        cur = conn.cursor()
        cur.execute("PRAGMA table_info(rollback_audit)")
        cols = {r[1] for r in cur.fetchall()}
        if "elements_restored" not in cols:
            cur.execute(
                "ALTER TABLE rollback_audit ADD COLUMN elements_restored INTEGER"
            )
            conn.commit()
            logger.info("Added elements_restored column to rollback_audit")
        conn.close()
    except Exception as e:  # pragma: no cover
        logger.warning("rollback_audit migration failed: %s", e)


# Migration: Add activity_uid to activity
def _migrate_activity_add_uid():
    """Add activity_uid column if missing; backfill as Activity_<order_index>."""
    try:
        conn = _connect()
        cur = conn.cursor()
        cur.execute("PRAGMA table_info(activity)")
        cols = {r[1] for r in cur.fetchall()}
        if "activity_uid" not in cols:
            cur.execute("ALTER TABLE activity ADD COLUMN activity_uid TEXT")
            # backfill
            cur.execute("SELECT id, order_index FROM activity")
            for rid, oi in cur.fetchall():
                cur.execute(
                    "UPDATE activity SET activity_uid=? WHERE id=?",
                    (f"Activity_{oi}", rid),
                )
            # create unique index scoped per soa
            cur.execute(
                "CREATE UNIQUE INDEX IF NOT EXISTS idx_activity_soa_uid ON activity(soa_id, activity_uid)"
            )
            conn.commit()
        else:
            # still ensure index exists
            cur.execute(
                "CREATE UNIQUE INDEX IF NOT EXISTS idx_activity_soa_uid ON activity(soa_id, activity_uid)"
            )
            conn.commit()
        conn.close()
    except Exception as e:  # pragma: no cover
        logger.warning("activity_uid migration failed: %s", e)


# Migration: Add type & data_origin_type to arm
def _migrate_arm_add_type_fields():
    """Ensure arm table has type and data_origin_type columns.
    Safe to run multiple times; adds columns if missing. No backfill logic (NULL acceptable).
    """
    try:
        conn = _connect()
        cur = conn.cursor()
        cur.execute("PRAGMA table_info(arm)")
        cols = {r[1] for r in cur.fetchall()}
        alters = []
        if "type" not in cols:
            alters.append("ALTER TABLE arm ADD COLUMN type TEXT")
        if "data_origin_type" not in cols:
            alters.append("ALTER TABLE arm ADD COLUMN data_origin_type TEXT")
        for stmt in alters:
            try:
                cur.execute(stmt)
            except Exception as e:  # pragma: no cover
                logger.warning("Failed arm type field migration '%s': %s", stmt, e)
        if alters:
            conn.commit()
            logger.info(
                "Applied arm type/data_origin_type migration: %s", ", ".join(alters)
            )
        conn.close()
    except Exception as e:  # pragma: no cover
        logger.warning("Arm type/data_origin_type migration failed: %s", e)


# Migration: Ensure element_audit has before_json/after_json columns
def _migrate_element_audit_columns():
    """Add missing columns before_json and after_json to element_audit.

    Handles legacy schemas that only had id, soa_id, element_id, action, performed_at.
    Safe to run multiple times; idempotent via schema inspection.
    """
    try:
        conn = _connect()
        cur = conn.cursor()
        # Ensure table exists; if not present, create with full schema
        cur.execute(
            "SELECT name FROM sqlite_master WHERE type='table' AND name='element_audit'"
        )
        exists = cur.fetchone() is not None
        if not exists:
            cur.execute(
                """CREATE TABLE IF NOT EXISTS element_audit (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    soa_id INTEGER NOT NULL,
                    element_id INTEGER,
                    action TEXT NOT NULL,
                    before_json TEXT,
                    after_json TEXT,
                    performed_at TEXT NOT NULL
                )"""
            )
            conn.commit()
            logger.info("Created element_audit table with full schema")
            conn.close()
            return
        # Add columns if missing
        cur.execute("PRAGMA table_info(element_audit)")
        cols = {r[1] for r in cur.fetchall()}
        alters = []
        if "before_json" not in cols:
            alters.append("ALTER TABLE element_audit ADD COLUMN before_json TEXT")
        if "after_json" not in cols:
            alters.append("ALTER TABLE element_audit ADD COLUMN after_json TEXT")
        for stmt in alters:
            try:
                cur.execute(stmt)
            except Exception as e:  # pragma: no cover
                logger.warning(
                    "Failed element_audit column migration '%s': %s", stmt, e
                )
        if alters:
            conn.commit()
            logger.info(
                "Applied element_audit column migrations: %s", ", ".join(alters)
            )
        conn.close()
    except Exception as e:  # pragma: no cover
        logger.warning("element_audit column migration failed: %s", e)


# Backfill dataset_date for existing terminology tables
def _backfill_dataset_date(table: str, audit_table: str):
    """If terminology table exists and has dataset_date (or sheet_dataset_date) column with blank values,
    attempt to backfill from the latest audit row that has a non-null dataset_date.
    Safe to run multiple times; will no-op if already populated or columns absent."""
    try:
        conn = _connect()
        cur = conn.cursor()
        # Ensure table exists
        cur.execute(
            "SELECT name FROM sqlite_master WHERE type='table' AND name=?", (table,)
        )
        if not cur.fetchone():
            conn.close()
            return
        cur.execute(f"PRAGMA table_info({table})")
        cols = {r[1] for r in cur.fetchall()}
        date_col = None
        # Prefer dataset_date; fallback sheet_dataset_date
        if "dataset_date" in cols:
            date_col = "dataset_date"
        elif "sheet_dataset_date" in cols:
            date_col = "sheet_dataset_date"
        if not date_col:
            conn.close()
            return
        # Check if any non-empty value exists
        cur.execute(
            f"SELECT COUNT(*) FROM {table} WHERE {date_col} IS NOT NULL AND {date_col} != ''"
        )
        if cur.fetchone()[0] > 0:
            conn.close()
            return  # already populated
        # Find latest audit dataset_date
        cur.execute(
            f"SELECT dataset_date FROM {audit_table} WHERE dataset_date IS NOT NULL AND dataset_date != '' ORDER BY loaded_at DESC LIMIT 1"
        )
        row = cur.fetchone()
        if not row or not row[0]:
            conn.close()
            return
        ds_date = row[0]
        cur.execute(
            f"UPDATE {table} SET {date_col}=? WHERE {date_col} IS NULL OR {date_col}=''",
            (ds_date,),
        )
        conn.commit()
        conn.close()
    except Exception as e:  # pragma: no cover
        logger.warning("dataset_date backfill for %s failed: %s", table, e)


def _migrate_visit_columns():
    """Add missing columns to the database table `visit`
    New columns:
    - description: string
    - type: string
    - environmentalSettings: string[]
    - contactModes: string[]
    - transitionStartRule: string
    - transitionEndRule: string

    (environmentalSettings & contactModes are officially list but
    are only single string values in the first iteration of the app
    """
    try:
        conn = _connect()
        cur = conn.cursor()
        cur.execute("PRAGMA table_info(visit)")
        cols = {r[1] for r in cur.fetchall()}
        alters = []
        if "description" not in cols:
            alters.append("ALTER TABLE visit ADD COLUMN description TEXT")
        if "type" not in cols:
            alters.append("ALTER TABLE visit ADD COLUMN type TEXT")
        if "environmentalSettings" not in cols:
            alters.append("ALTER TABLE visit ADD COLUMN environmentalSettings TEXT")
        if "contactModes" not in cols:
            alters.append("ALTER TABLE visit ADD COLUMN contactModes TEXT")
        if "transitionStartRule" not in cols:
            alters.append("ALTER TABLE visit ADD COLUMN transitionStartRule TEXT")
        if "transitionEndRule" not in cols:
            alters.append("ALTER TABLE visit ADD COLUMN transitionEndRule TEXT")
        if "scheduledAtId" not in cols:
            alters.append("ALTER TABLE visit ADD COLUMN scheduledAtId TEXT")
        for stmt in alters:
            try:
                cur.execute(stmt)
            except Exception as e:  # pragma: no cover
                logger.warning("Failed visit field migration '%s': %s", stmt, e)
        if alters:
            conn.commit()
            logger.info("Applied visit column migration: %s", ", ".join(alters))
        conn.close()
    except Exception as e:  # pragma: no cover
        logger.warning("visit table migration failed: %s", e)


def _migrate_timing_add_member_of_timeline():
    """Add optional member_of_timeline column to timing table if missing."""
    try:
        conn = _connect()
        cur = conn.cursor()
        # Ensure timing table exists
        cur.execute(
            "SELECT name FROM sqlite_master WHERE type='table' AND name='timing'"
        )
        if not cur.fetchone():
            conn.close()
            return
        # Check column presence
        cur.execute("PRAGMA table_info(timing)")
        cols = {r[1] for r in cur.fetchall()}
        if "member_of_timeline" not in cols:
            cur.execute("ALTER TABLE timing ADD COLUMN member_of_timeline TEXT")
            conn.commit()
            logger.info("Added member_of_timeline column to timing table")
        conn.close()
    except Exception as e:  # pragma: no cover
        logger.warning("timing member_of_timeline migration failed: %s", e)


def _migrate_instances_add_member_of_timeline():
    """Add optional member_of_timeline"""
    try:
        conn = _connect()
        cur = conn.cursor()
        cur.execute(
            "SELECT name FROM sqlite_master WHERE type='table' AND name='instances'"
        )
        if not cur.fetchone():
            conn.close()
            return
        cur.execute("PRAGMA table_info(instances)")
        cols = {r[1] for r in cur.fetchall()}
        if "member_of_timeline" not in cols:
            cur.execute("ALTER TABLE instances ADD COLUMN member_of_timeline TEXT")
            conn.commit()
            logger.info("Added member_of_timeline column to instances table")
        conn.close()
    except Exception as e:
        logger.warning("instances member_of_timeline migration failed: %s", e)


def _migrate_matrix_cells_add_instance_id():
    """Add instance_id column to matrix_cells if missing (idempotent)"""
    try:
        conn = _connect()
        cur = conn.cursor()
        cur.execute("PRAGMA table_info(matrix_cells)")
        cols = {r[1] for r in cur.fetchall()}
        if "instance_id" not in cols:
            cur.execute("ALTER TABLE matrix_cells ADD COLUMN instance_id INTEGER")
            conn.commit()
            logger.info("Added instance_id column to matrix_cells")
        conn.close()
    except Exception as e:
        logger.warning("matrix_cells instance_id migration failed: %s", e)


def _migrate_activity_concept_add_href():
    """Add href column to store the API URI from which codeSystem and codeSystemVersion USDM properties can be derived"""
    try:
        conn = _connect()
        cur = conn.cursor()
        cur.execute("PRAGMA table_info(activity_concept)")
        cols = {r[1] for r in cur.fetchall()}
        if "href" not in cols:
            cur.execute("ALTER TABLE activity_concept ADD COLUMN href TEXT")
            conn.commit()
            logger.info("Added href column to the activity_concept table")
        conn.close()
    except Exception as e:
        logger.warning("activity_concept href migration failed: %s", e)


def _migrate_activity_concept_add_dss():
    """Add dss_title and dss_href columns to activity_concept for DSS assignment."""
    try:
        conn = _connect()
        cur = conn.cursor()
        cur.execute("PRAGMA table_info(activity_concept)")
        cols = {r[1] for r in cur.fetchall()}
        if "dss_title" not in cols:
            cur.execute("ALTER TABLE activity_concept ADD COLUMN dss_title TEXT")
            conn.commit()
            logger.info("Added dss_title column to activity_concept table")
        if "dss_href" not in cols:
            cur.execute("ALTER TABLE activity_concept ADD COLUMN dss_href TEXT")
            conn.commit()
            logger.info("Added dss_href column to activity_concept table")
        if "dss_domain" not in cols:
            cur.execute("ALTER TABLE activity_concept ADD COLUMN dss_domain TEXT")
            conn.commit()
            logger.info("Added dss_domain column to activity_concept table")
        conn.close()
    except Exception as e:
        logger.warning("activity_concept dss migration failed: %s", e)


def _migrate_study_cell_add_order_index():
    """Add order_index column to study_cell table to support reordering"""
    try:
        conn = _connect()
        cur = conn.cursor()
        cur.execute("PRAGMA table_info(study_cell)")
        cols = {r[1] for r in cur.fetchall()}
        if "order_index" not in cols:
            cur.execute("ALTER TABLE study_cell ADD COLUMN order_index INTEGER")
            conn.commit()
            logger.info("Added order_index column to the study_cell table")
        conn.close()
    except Exception as e:
        logger.warning("order_index migration failed: %s", e)


def _migrate_biomedical_concept_audit():
    """Create biomedical_concept_audit table for tracking create/delete operations."""
    try:
        conn = _connect()
        cur = conn.cursor()
        cur.execute(
            """CREATE TABLE IF NOT EXISTS biomedical_concept_audit (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                soa_id INTEGER NOT NULL,
                biomedical_concept_id INTEGER,
                action TEXT NOT NULL,
                before_json TEXT,
                after_json TEXT,
                performed_at TEXT NOT NULL
            )"""
        )
        conn.commit()
        conn.close()
    except Exception as e:
        logger.warning("_migrate_biomedical_concept_audit: %s", e)


def _migrate_backfill_biomedical_concept_codes():
    """One-time backfill: for biomedical_concept rows that have no matching alias_code entry,
    create code + alias_code rows and update biomedical_concept.code to the alias_code_uid.
    """
    try:
        conn = _connect()
        cur = conn.cursor()

        cur.execute(
            """
            SELECT bc.id, bc.soa_id, bc.biomedical_concept_uid, ac2.concept_code
            FROM biomedical_concept bc
            LEFT JOIN alias_code ac
                   ON ac.alias_code_uid = bc.code AND ac.soa_id = bc.soa_id
            LEFT JOIN activity_concept ac2
                   ON ac2.concept_uid = bc.biomedical_concept_uid
                  AND ac2.soa_id = bc.soa_id
            WHERE ac.id IS NULL
            """
        )
        rows = cur.fetchall()

        for bc_id, soa_id, bc_uid, concept_code in rows:
            if not concept_code:
                continue  # no raw code available — nothing to create

            # get-or-create code row
            cur.execute(
                "SELECT code_uid FROM code WHERE soa_id=? AND code=?",
                (soa_id, concept_code),
            )
            row = cur.fetchone()
            if row:
                code_uid = row[0]
            else:
                cur.execute(
                    "SELECT code_uid FROM code"
                    " WHERE soa_id=? AND code_uid LIKE 'Code_%'",
                    (soa_id,),
                )
                existing = [x[0] for x in cur.fetchall() if x[0]]
                n = 1
                if existing:
                    try:
                        n = max(int(x.split("_")[1]) for x in existing) + 1
                    except Exception:
                        n = len(existing) + 1
                code_uid = f"Code_{n}"
                cur.execute(
                    "INSERT INTO code (soa_id, code_uid, code) VALUES (?,?,?)",
                    (soa_id, code_uid, concept_code),
                )

            # get-or-create alias_code row
            cur.execute(
                "SELECT alias_code_uid FROM alias_code"
                " WHERE soa_id=? AND standard_code=?",
                (soa_id, code_uid),
            )
            row = cur.fetchone()
            if row:
                alias_uid = row[0]
            else:
                cur.execute(
                    "SELECT alias_code_uid FROM alias_code"
                    " WHERE soa_id=? AND alias_code_uid LIKE 'AliasCode_%'",
                    (soa_id,),
                )
                existing = [x[0] for x in cur.fetchall() if x[0]]
                n = 1
                if existing:
                    try:
                        n = max(int(x.split("_")[1]) for x in existing) + 1
                    except Exception:
                        n = len(existing) + 1
                alias_uid = f"AliasCode_{n}"
                cur.execute(
                    "INSERT INTO alias_code"
                    " (soa_id, alias_code_uid, standard_code) VALUES (?,?,?)",
                    (soa_id, alias_uid, code_uid),
                )

            # patch biomedical_concept.code
            cur.execute(
                "UPDATE biomedical_concept SET code=? WHERE id=?",
                (alias_uid, bc_id),
            )

        conn.commit()
        conn.close()
    except Exception as e:
        logger.warning("_migrate_backfill_biomedical_concept_codes: %s", e)


def _migrate_biomedical_concept_property_add_uid():
    """Add biomedical_concept_uid column to biomedical_concept_property table."""
    try:
        conn = _connect()
        cur = conn.cursor()
        cur.execute("PRAGMA table_info(biomedical_concept_property)")
        cols = {r[1] for r in cur.fetchall()}
        if "biomedical_concept_uid" not in cols:
            cur.execute(
                "ALTER TABLE biomedical_concept_property"
                " ADD COLUMN biomedical_concept_uid TEXT"
            )
            conn.commit()
            logger.info(
                "Added biomedical_concept_uid column to biomedical_concept_property"
            )
        conn.close()
    except Exception as e:
        logger.warning("_migrate_biomedical_concept_property_add_uid: %s", e)


def _migrate_truncate_biomedical_concept_property_data():
    """Truncate biomedical_concept_property rows that were populated from DSS.

    The prior DSS-based writer produced incorrect BiomedicalConceptProperty
    values. This one-time migration clears those rows and the alias_code/code
    rows that were created solely to back them. Idempotent: a no-op on an
    empty table.
    """
    try:
        conn = _connect()
        cur = conn.cursor()
        cur.execute("PRAGMA table_info(biomedical_concept_property)")
        cols = {r[1] for r in cur.fetchall()}
        if not cols:
            conn.close()
            return
        cur.execute(
            "SELECT DISTINCT soa_id, code FROM biomedical_concept_property"
            " WHERE code IS NOT NULL"
        )
        alias_refs = cur.fetchall()
        cur.execute("DELETE FROM biomedical_concept_property")
        for soa_id, alias_uid in alias_refs:
            cur.execute(
                "SELECT 1 FROM biomedical_concept_property"
                " WHERE soa_id=? AND code=? LIMIT 1",
                (soa_id, alias_uid),
            )
            if cur.fetchone():
                continue
            cur.execute(
                "SELECT 1 FROM biomedical_concept WHERE soa_id=? AND code=? LIMIT 1",
                (soa_id, alias_uid),
            )
            if cur.fetchone():
                continue
            cur.execute(
                "SELECT standard_code FROM alias_code"
                " WHERE alias_code_uid=? AND soa_id=?",
                (alias_uid, soa_id),
            )
            code_row = cur.fetchone()
            cur.execute(
                "DELETE FROM alias_code WHERE alias_code_uid=? AND soa_id=?",
                (alias_uid, soa_id),
            )
            if code_row:
                cur.execute(
                    "DELETE FROM code WHERE code_uid=? AND soa_id=?",
                    (code_row[0], soa_id),
                )
        conn.commit()
        conn.close()
    except Exception as e:
        logger.warning("_migrate_truncate_biomedical_concept_property_data: %s", e)


def _migrate_repoint_stale_bc_code_chains():
    """Realign biomedical_concept.code chain with activity_concept.concept_code.

    Legacy data-loading paths left some biomedical_concept rows whose
    code/alias_code/code chain points at a different concept than the one
    recorded in activity_concept. For each mismatch, this migration:

      1. Ensures a (code, alias_code) chain exists for the correct
         activity_concept.concept_code in the same soa.
      2. Repoints biomedical_concept.code to the correct alias_code_uid
         and rewrites biomedical_concept.name to activity_concept.concept_title.
      3. Drops the now-orphaned old alias_code and code rows.
      4. Fires a synchronous CDISC BC API call for each repointed
         concept_code to populate code.decode/code_system/code_system_version
         and biomedical_concept.label/description/name. Network or API
         failures are logged and the migration continues — the normal
         enrichment path will backfill on next user interaction.

    Idempotent: re-running finds no mismatches and is a no-op.
    """
    try:
        import requests

        conn = _connect()
        cur = conn.cursor()
        cur.execute(
            """
            SELECT bc.id, bc.soa_id, bc.biomedical_concept_uid,
                   bc.code, ac.concept_code, ac.concept_title
            FROM biomedical_concept bc
            INNER JOIN activity_concept ac
              ON bc.biomedical_concept_uid = ac.concept_uid
             AND bc.soa_id = ac.soa_id
            INNER JOIN alias_code a
              ON bc.code = a.alias_code_uid AND bc.soa_id = a.soa_id
            INNER JOIN code c
              ON a.standard_code = c.code_uid AND a.soa_id = c.soa_id
            WHERE ac.concept_code IS NOT NULL
              AND ac.concept_code != c.code
            """
        )
        rows = cur.fetchall()
        if not rows:
            conn.close()
            return
        touched = set()  # (soa_id, concept_code)
        for bc_id, soa_id, bc_uid, old_alias, new_code, new_title in rows:
            # get-or-create the correct code row
            cur.execute(
                "SELECT code_uid FROM code WHERE soa_id=? AND code=?",
                (soa_id, new_code),
            )
            r = cur.fetchone()
            if r:
                new_code_uid = r[0]
            else:
                cur.execute(
                    "SELECT code_uid FROM code"
                    " WHERE soa_id=? AND code_uid LIKE 'Code_%'"
                    " UNION"
                    " SELECT code_uid FROM code_association"
                    " WHERE soa_id=? AND code_uid LIKE 'Code_%'",
                    (soa_id, soa_id),
                )
                existing = [x[0] for x in cur.fetchall() if x[0]]
                n = (
                    max(
                        (int(x.split("_")[1]) for x in existing),
                        default=0,
                    )
                    + 1
                )
                new_code_uid = f"Code_{n}"
                cur.execute(
                    "INSERT INTO code (soa_id, code_uid, code) VALUES (?,?,?)",
                    (soa_id, new_code_uid, new_code),
                )
            # get-or-create the alias_code row
            cur.execute(
                "SELECT alias_code_uid FROM alias_code"
                " WHERE soa_id=? AND standard_code=?",
                (soa_id, new_code_uid),
            )
            r = cur.fetchone()
            if r:
                new_alias_uid = r[0]
            else:
                cur.execute(
                    "SELECT alias_code_uid FROM alias_code"
                    " WHERE soa_id=? AND alias_code_uid LIKE 'AliasCode_%'",
                    (soa_id,),
                )
                existing = [x[0] for x in cur.fetchall() if x[0]]
                n = (
                    max(
                        (int(x.split("_")[1]) for x in existing),
                        default=0,
                    )
                    + 1
                )
                new_alias_uid = f"AliasCode_{n}"
                cur.execute(
                    "INSERT INTO alias_code"
                    " (soa_id, alias_code_uid, standard_code)"
                    " VALUES (?,?,?)",
                    (soa_id, new_alias_uid, new_code_uid),
                )
            # repoint the BC and refresh name to best-available title
            cur.execute(
                "UPDATE biomedical_concept SET code=?, name=? WHERE id=?",
                (new_alias_uid, new_title, bc_id),
            )
            touched.add((soa_id, new_code))
            # orphan-cleanup: old alias_code -> old code
            cur.execute(
                "SELECT 1 FROM biomedical_concept WHERE soa_id=? AND code=? LIMIT 1",
                (soa_id, old_alias),
            )
            if cur.fetchone():
                continue
            cur.execute(
                "SELECT standard_code FROM alias_code"
                " WHERE soa_id=? AND alias_code_uid=?",
                (soa_id, old_alias),
            )
            code_row = cur.fetchone()
            cur.execute(
                "DELETE FROM alias_code WHERE soa_id=? AND alias_code_uid=?",
                (soa_id, old_alias),
            )
            if code_row:
                old_code_uid = code_row[0]
                cur.execute(
                    "SELECT 1 FROM alias_code"
                    " WHERE soa_id=? AND standard_code=? LIMIT 1",
                    (soa_id, old_code_uid),
                )
                if not cur.fetchone():
                    cur.execute(
                        "DELETE FROM code WHERE soa_id=? AND code_uid=?",
                        (soa_id, old_code_uid),
                    )
        conn.commit()
        # synchronous enrichment for each touched (soa_id, concept_code)
        api_key = os.environ.get("CDISC_API_KEY") or os.environ.get(
            "CDISC_SUBSCRIPTION_KEY"
        )
        subscription_key = os.environ.get("CDISC_SUBSCRIPTION_KEY") or api_key
        headers = {"Accept": "application/json"}
        if subscription_key:
            headers["Ocp-Apim-Subscription-Key"] = subscription_key
        if api_key:
            headers["Authorization"] = f"Bearer {api_key}"
            headers["api-key"] = api_key
        for soa_id, concept_code in touched:
            try:
                url = (
                    "https://api.library.cdisc.org/api/cosmos/v2/"
                    "mdr/bc/biomedicalconcepts/" + concept_code
                )
                resp = requests.get(url, headers=headers, timeout=15)
                if resp.status_code != 200:
                    continue
                data = resp.json()
                href = (data.get("_links") or {}).get("parentPackage") or {}
                href = href.get("href", "") if isinstance(href, dict) else ""
                try:
                    code_system_version = href.split("/")[4]
                except Exception:
                    code_system_version = ""
                short_name = data.get("shortName")
                definition = data.get("definition")
                cur.execute(
                    "UPDATE code SET code_system=?, code_system_version=?,"
                    " decode=? WHERE code=? AND soa_id=?",
                    (
                        href,
                        code_system_version,
                        short_name,
                        concept_code,
                        soa_id,
                    ),
                )
                cur.execute(
                    """
                    UPDATE biomedical_concept
                       SET name=?, label=?, description=?
                     WHERE soa_id=?
                       AND biomedical_concept_uid IN (
                           SELECT concept_uid FROM activity_concept
                            WHERE soa_id=? AND concept_code=?
                              AND concept_uid IS NOT NULL
                       )
                    """,
                    (
                        short_name,
                        short_name,
                        definition,
                        soa_id,
                        soa_id,
                        concept_code,
                    ),
                )
                conn.commit()
            except Exception as e:
                logger.warning(
                    "_migrate_repoint_stale_bc_code_chains enrich soa=%s code=%s: %s",
                    soa_id,
                    concept_code,
                    e,
                )
        conn.close()
    except Exception as e:
        logger.warning("_migrate_repoint_stale_bc_code_chains: %s", e)


def _migrate_add_soa_id_indexes():
    """Add standalone soa_id indexes on high-traffic tables.

    The existing UNIQUE constraints cover (soa_id, uid) lookups, but bare
    WHERE soa_id=? list queries do full table scans without a leading index.
    These indexes cover the ~259 soa_id filter sites in the codebase.
    """
    try:
        conn = _connect()
        cur = conn.cursor()
        indexes = [
            ("idx_activity_soa", "activity", "soa_id"),
            ("idx_visit_soa", "visit", "soa_id"),
            ("idx_matrix_cells_soa", "matrix_cells", "soa_id"),
            ("idx_activity_concept_soa", "activity_concept", "soa_id"),
            ("idx_instances_soa", "instances", "soa_id"),
            ("idx_timing_soa", "timing", "soa_id"),
        ]
        created = []
        for idx_name, table, col in indexes:
            cur.execute(f"CREATE INDEX IF NOT EXISTS {idx_name} ON {table}({col})")
            created.append(idx_name)
        conn.commit()
        conn.close()
        logger.info("_migrate_add_soa_id_indexes: ensured indexes %s", created)
    except Exception as e:
        logger.warning("_migrate_add_soa_id_indexes: %s", e)


def _migrate_add_footnote_table():
    """Add the database table footnote"""
    try:
        conn = _connect()
        cur = conn.cursor()
        cur.execute(
            """CREATE TABLE IF NOT EXISTS footnote (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                soa_id INT,
                footnote_uid TEXT NOT NULL,
                name TEXT NOT NULL,
                label TEXT,
                description TEXT,
                text TEXT,
                dictionary_uid TEXT,
                UNIQUE(soa_id, footnote_uid)
            )"""
        )
        conn.commit()
        conn.close()
        logger.info("_migrate_add_footnote_table created footnote table")
    except Exception as e:
        logger.warning("_migrate_add_footnote_table failed: %s", e)


def _migrate_add_footnote_audit_table():
    """Create footnote_audit table for tracking create/update/delete operations."""
    try:
        conn = _connect()
        cur = conn.cursor()
        cur.execute(
            """CREATE TABLE IF NOT EXISTS footnote_audit (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                soa_id INTEGER NOT NULL,
                footnote_id INTEGER,
                action TEXT NOT NULL,
                before_json TEXT,
                after_json TEXT,
                performed_at TEXT NOT NULL
            )"""
        )
        conn.commit()
        conn.close()
        logger.info("_migrate_add_footnote_audit_table created footnote_audit table")
    except Exception as e:
        logger.warning("_migrate_add_footnote_audit_table failed: %s", e)


def _migrate_matrix_cells_add_superscript():
    """Add superscript TEXT column to matrix_cells if missing."""
    try:
        conn = _connect()
        cur = conn.cursor()
        cur.execute("PRAGMA table_info(matrix_cells)")
        if "superscript" not in {r[1] for r in cur.fetchall()}:
            cur.execute("ALTER TABLE matrix_cells ADD COLUMN superscript TEXT")
            conn.commit()
            logger.info("Added superscript column to matrix_cells")
        conn.close()
    except Exception as e:
        logger.warning("matrix_cells superscript migration failed: %s", e)


def _migrate_add_bc_surrogate_table():
    """Create biomedical_concept_surrogate table if missing."""
    try:
        conn = _connect()
        cur = conn.cursor()
        cur.execute(
            """CREATE TABLE IF NOT EXISTS biomedical_concept_surrogate (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                soa_id INT NOT NULL,
                surrogate_uid TEXT NOT NULL,
                name TEXT NOT NULL,
                label TEXT,
                description TEXT,
                reference TEXT,
                UNIQUE(surrogate_uid, soa_id)
            )"""
        )
        conn.commit()
        conn.close()
        logger.info(
            "_migrate_add_bc_surrogate_table: biomedical_concept_surrogate ready"
        )
    except Exception as e:
        logger.warning("_migrate_add_bc_surrogate_table failed: %s", e)


def _migrate_add_activity_surrogate_table():
    """Create activity_surrogate junction table if missing."""
    try:
        conn = _connect()
        cur = conn.cursor()
        cur.execute(
            """CREATE TABLE IF NOT EXISTS activity_surrogate (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                soa_id INT NOT NULL,
                activity_uid TEXT NOT NULL,
                surrogate_uid TEXT NOT NULL,
                UNIQUE(soa_id, activity_uid, surrogate_uid)
            )"""
        )
        conn.commit()
        conn.close()
        logger.info("_migrate_add_activity_surrogate_table: activity_surrogate ready")
    except Exception as e:
        logger.warning("_migrate_add_activity_surrogate_table failed: %s", e)


def _migrate_add_bc_surrogate_audit_table():
    """Create biomedical_concept_surrogate_audit table if missing."""
    try:
        conn = _connect()
        cur = conn.cursor()
        cur.execute(
            """CREATE TABLE IF NOT EXISTS biomedical_concept_surrogate_audit (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                soa_id INTEGER NOT NULL,
                surrogate_id INTEGER,
                action TEXT NOT NULL,
                before_json TEXT,
                after_json TEXT,
                performed_at TEXT NOT NULL
            )"""
        )
        conn.commit()
        conn.close()
        logger.info("_migrate_add_bc_surrogate_audit_table: audit table ready")
    except Exception as e:
        logger.warning("_migrate_add_bc_surrogate_audit_table failed: %s", e)


def _migrate_add_concept_group_table():
    """Create global concept_group and concept_group_concept tables if missing."""
    try:
        conn = _connect()
        cur = conn.cursor()
        cur.execute(
            """CREATE TABLE IF NOT EXISTS concept_group (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                concept_group_uid TEXT NOT NULL UNIQUE,
                name TEXT NOT NULL,
                label TEXT,
                description TEXT
            )"""
        )
        cur.execute(
            """CREATE TABLE IF NOT EXISTS concept_group_concept (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                concept_group_uid TEXT NOT NULL,
                concept_code TEXT NOT NULL,
                concept_title TEXT,
                UNIQUE(concept_group_uid, concept_code)
            )"""
        )
        conn.commit()
        conn.close()
        logger.info("_migrate_add_concept_group_table: concept_group tables ready")
    except Exception as e:
        logger.warning("_migrate_add_concept_group_table failed: %s", e)


def _migrate_activity_concept_add_concept_group_uid():
    """Add concept_group_uid column to activity_concept if missing."""
    try:
        conn = _connect()
        cur = conn.cursor()
        cur.execute("PRAGMA table_info(activity_concept)")
        cols = {r[1] for r in cur.fetchall()}
        if "concept_group_uid" not in cols:
            cur.execute(
                "ALTER TABLE activity_concept ADD COLUMN concept_group_uid TEXT"
            )
            conn.commit()
            logger.info("Added concept_group_uid column to activity_concept")
        conn.close()
    except Exception as e:
        logger.warning("_migrate_activity_concept_add_concept_group_uid failed: %s", e)


def _migrate_surrogate_add_concept_group_uid():
    """Add concept_group_uid column to biomedical_concept_surrogate if missing."""
    try:
        conn = _connect()
        cur = conn.cursor()
        cur.execute("PRAGMA table_info(biomedical_concept_surrogate)")
        cols = {r[1] for r in cur.fetchall()}
        if "concept_group_uid" not in cols:
            cur.execute(
                "ALTER TABLE biomedical_concept_surrogate "
                "ADD COLUMN concept_group_uid TEXT"
            )
            conn.commit()
            logger.info(
                "Added concept_group_uid column to biomedical_concept_surrogate"
            )
        conn.close()
    except Exception as e:
        logger.warning("_migrate_surrogate_add_concept_group_uid failed: %s", e)


def _migrate_activity_surrogate_add_concept_group_uid():
    """Add concept_group_uid column to activity_surrogate if missing."""
    try:
        conn = _connect()
        cur = conn.cursor()
        cur.execute("PRAGMA table_info(activity_surrogate)")
        cols = {r[1] for r in cur.fetchall()}
        if "concept_group_uid" not in cols:
            cur.execute(
                "ALTER TABLE activity_surrogate ADD COLUMN concept_group_uid TEXT"
            )
            conn.commit()
            logger.info("Added concept_group_uid column to activity_surrogate")
        conn.close()
    except Exception as e:
        logger.warning(
            "_migrate_activity_surrogate_add_concept_group_uid failed: %s",
            e,
        )


def _migrate_add_activity_concept_dss_table():
    """Create activity_concept_dss table for one-to-many DSS assignments.

    Migrates any existing single-row assignments from the
    activity_concept.dss_title / dss_href columns into the new table.
    """
    try:
        conn = _connect()
        cur = conn.cursor()
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS activity_concept_dss (
                id          INTEGER PRIMARY KEY AUTOINCREMENT,
                soa_id      INTEGER NOT NULL,
                activity_id INTEGER NOT NULL,
                concept_code TEXT NOT NULL,
                dss_title   TEXT NOT NULL,
                dss_href    TEXT NOT NULL,
                dss_domain  TEXT
            )
            """
        )
        conn.commit()

        # Migrate existing single-row assignments from activity_concept
        cur.execute("PRAGMA table_info(activity_concept)")
        cols = {r[1] for r in cur.fetchall()}
        if "dss_title" in cols and "dss_href" in cols:
            # Only insert rows that are not already present
            cur.execute("SELECT COUNT(*) FROM activity_concept_dss")
            if cur.fetchone()[0] == 0:
                dss_domain_col = "dss_domain" if "dss_domain" in cols else "NULL"
                cur.execute(
                    f"""
                    INSERT INTO activity_concept_dss
                        (soa_id, activity_id, concept_code,
                         dss_title, dss_href, dss_domain)
                    SELECT soa_id, activity_id, concept_code,
                           dss_title, dss_href, {dss_domain_col}
                    FROM activity_concept
                    WHERE dss_title IS NOT NULL AND dss_title != ''
                    """
                )
                conn.commit()
                logger.info(
                    "Migrated existing DSS assignments into activity_concept_dss"
                )
        conn.close()
    except Exception as e:
        logger.warning("_migrate_add_activity_concept_dss_table failed: %s", e)


def _migrate_activity_concept_dss_add_display():
    """Add dss_display column to activity_concept_dss for human-readable title."""
    try:
        conn = _connect()
        cur = conn.cursor()
        cur.execute("PRAGMA table_info(activity_concept_dss)")
        cols = {r[1] for r in cur.fetchall()}
        if "dss_display" not in cols:
            cur.execute("ALTER TABLE activity_concept_dss ADD COLUMN dss_display TEXT")
            conn.commit()
            logger.info("Added dss_display column to activity_concept_dss")
        conn.close()
    except Exception as e:
        logger.warning("_migrate_activity_concept_dss_add_display failed: %s", e)
