"""Add embedded_copy_uid: the uid of each extra embedded copy.

One stored TransitionRule can serve several elements and encounters,
and one GeographicScope an amendment, its governance dates and its
enrollments. USDM embeds the object under every owner and requires
unique ids, so every copy but one needs its own {Entity}_N uid (with
its nested Code/AliasCode uids). Rows map (owner_uid, slot, source_uid)
to the copy's uid; a row whose copy_uid equals source_uid records the
owner that keeps the stored uid. Rows are allocated on first export
and keyed on immutable uids, so ids stay the same across exports.
"""

from ..db import _connect


def m0120_embedded_copy_uid() -> None:
    conn = _connect()
    try:
        conn.execute(
            """CREATE TABLE IF NOT EXISTS embedded_copy_uid (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                soa_id INTEGER NOT NULL REFERENCES soa(id) ON DELETE CASCADE,
                owner_uid TEXT NOT NULL,
                slot TEXT NOT NULL,
                source_uid TEXT NOT NULL,
                copy_uid TEXT NOT NULL,
                UNIQUE(soa_id, owner_uid, slot, source_uid),
                UNIQUE(soa_id, copy_uid)
            )"""
        )
        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_embedded_copy_uid_soa"
            " ON embedded_copy_uid(soa_id)"
        )
        conn.commit()
    finally:
        conn.close()
