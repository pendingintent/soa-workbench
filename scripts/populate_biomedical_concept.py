"""One-time script: populate biomedical_concept from activity_concept.

Usage:
    python scripts/populate_biomedical_concept.py

Deduplicates by (soa_id, concept_uid). Skips rows where concept_uid is NULL.
Aborts without writing if biomedical_concept is already populated.
"""

import sqlite3
import sys
from pathlib import Path

DB = Path("soa_builder_web.db")

conn = sqlite3.connect(DB)
cur = conn.cursor()

cur.execute("SELECT COUNT(*) FROM biomedical_concept")
existing = cur.fetchone()[0]
if existing:
    print(f"biomedical_concept already has {existing} rows — aborting.")
    conn.close()
    sys.exit(0)

cur.execute(
    "SELECT soa_id, concept_uid, MIN(concept_title), concept_code "
    "FROM activity_concept "
    "WHERE concept_uid IS NOT NULL "
    "GROUP BY soa_id, concept_uid "
    "ORDER BY soa_id, concept_title"
)
rows = cur.fetchall()

inserted = 0
for soa_id, concept_uid, concept_title, concept_code in rows:
    cur.execute(
        "INSERT INTO biomedical_concept (soa_id, biomedical_concept_uid, name, code) "
        "VALUES (?, ?, ?, ?)",
        (soa_id, concept_uid, concept_title, concept_code),
    )
    inserted += 1

conn.commit()
conn.close()
print(f"Inserted {inserted} rows into biomedical_concept.")
