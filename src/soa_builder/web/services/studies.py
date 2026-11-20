"""Create and update the root ``soa`` row (issue #382).

Shared by the index-page form (``POST /ui/soa/create``), the JSON API
(``POST /soa``), the edit-page metadata form and the setup wizard, so
the study_id uniqueness rule is enforced in one place.
"""

import logging

from datetime import datetime
from datetime import timezone
from typing import Optional

from ..db import _connect

logger = logging.getLogger(__name__)


class DuplicateStudyIdError(ValueError):
    """Another study already uses this study_id."""


class MissingStudyIdError(ValueError):
    """A study_id is required but none is set or supplied."""


def _clean(value: Optional[str]) -> Optional[str]:
    return (value or "").strip() or None


def create_soa_record(
    name: str,
    study_id: Optional[str] = None,
    study_label: Optional[str] = None,
    study_description: Optional[str] = None,
) -> int:
    """Insert a study and return its id.

    Raises DuplicateStudyIdError when a non-blank study_id is taken.
    """
    clean_study_id = _clean(study_id)
    conn = _connect()
    try:
        cur = conn.cursor()
        if clean_study_id:
            cur.execute("SELECT 1 FROM soa WHERE study_id=?", (clean_study_id,))
            if cur.fetchone():
                raise DuplicateStudyIdError("study_id already exists")
        cur.execute(
            "INSERT INTO soa (name, created_at, study_id, study_label,"
            " study_description) VALUES (?,?,?,?,?)",
            (
                name,
                datetime.now(timezone.utc).isoformat(),
                clean_study_id,
                _clean(study_label),
                _clean(study_description),
            ),
        )
        conn.commit()
        return cur.lastrowid
    finally:
        conn.close()


def update_soa_record(
    soa_id: int,
    name: Optional[str],
    study_id: Optional[str],
    study_label: Optional[str],
    study_description: Optional[str],
) -> None:
    """Update a study's metadata.

    A blank study_id keeps the current one. Raises
    DuplicateStudyIdError when the study_id belongs to another study
    and MissingStudyIdError when the study would be left without one.
    """
    conn = _connect()
    try:
        cur = conn.cursor()
        cur.execute("SELECT study_id FROM soa WHERE id=?", (soa_id,))
        row = cur.fetchone()
        current_study_id = row[0] if row else None
        new_study_id = _clean(study_id) or current_study_id
        if not new_study_id:
            raise MissingStudyIdError("study_id is required")
        cur.execute(
            "SELECT 1 FROM soa WHERE study_id=? AND id<>?",
            (new_study_id, soa_id),
        )
        if cur.fetchone():
            raise DuplicateStudyIdError("study_id already exists")
        cur.execute(
            "UPDATE soa SET name=?, study_id=?, study_label=?,"
            " study_description=? WHERE id=?",
            (
                _clean(name),
                new_study_id,
                _clean(study_label),
                _clean(study_description),
                soa_id,
            ),
        )
        conn.commit()
    finally:
        conn.close()
