"""Add the study population planned sex and planned age columns.

USDM v4.0 StudyDesignPopulation.plannedSex holds up to two codes from
the Sex of Participants codelist (C66732, CORE DDF00098) and
plannedAge is a Range of two age Quantities (DDF00097). The workbench
has one population per study design, so both are stored on ``soa``.

Either age bound may be absent ("aged 18 years or over"). USDM v4.0
requires both bounds; v4.1 is expected to allow a missing one.
"""

from ..db import _connect

SOA_COLUMNS = (
    ("planned_sex", "TEXT"),  # JSON list of {code, decode, version}
    ("planned_age_min", "REAL"),
    ("planned_age_max", "REAL"),
    ("planned_age_unit_code", "TEXT"),
    ("planned_age_unit_decode", "TEXT"),
    ("planned_age_unit_version", "TEXT"),
    ("planned_age_is_approximate", "INTEGER NOT NULL DEFAULT 0"),
)


def m0117_population_age_sex() -> None:
    conn = _connect()
    try:
        cur = conn.cursor()
        existing = {row[1] for row in cur.execute("PRAGMA table_info(soa)")}
        for name, decl in SOA_COLUMNS:
            if name not in existing:
                cur.execute(f"ALTER TABLE soa ADD COLUMN {name} {decl}")
        conn.commit()
    finally:
        conn.close()
