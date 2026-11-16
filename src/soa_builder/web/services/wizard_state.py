"""Persisted progress of the guided study setup wizard (issue #382).

Backed by the ``wizard_state`` table (migration m0115): one row per
study, holding the current step, the completed steps (a JSON list)
and a ``draft``/``complete`` status.
"""

import json
import logging

from datetime import datetime
from datetime import timezone
from typing import Optional

from ..db import _connect
from ..errors import MALFORMED_JSON_ERRORS

logger = logging.getLogger(__name__)

STATUS_DRAFT = "draft"
STATUS_COMPLETE = "complete"


def _now() -> str:
    return datetime.now(timezone.utc).isoformat()


def get_wizard_state(soa_id: int) -> Optional[dict]:
    """Return the study's wizard state, or None if it has none."""
    conn = _connect()
    try:
        row = conn.execute(
            "SELECT current_step, completed_steps, status FROM wizard_state"
            " WHERE soa_id=?",
            (soa_id,),
        ).fetchone()
    finally:
        conn.close()
    if not row:
        return None
    try:
        completed = json.loads(row[1] or "[]")
    except MALFORMED_JSON_ERRORS:
        logger.warning("wizard_state %s: unreadable completed_steps", soa_id)
        completed = []
    return {
        "soa_id": soa_id,
        "current_step": row[0],
        "completed_steps": [s for s in completed if isinstance(s, str)],
        "status": row[2],
    }


def save_wizard_state(
    soa_id: int,
    current_step: str,
    completed_steps: list[str],
    status: str = STATUS_DRAFT,
) -> None:
    """Insert or replace the study's wizard state."""
    now = _now()
    conn = _connect()
    try:
        conn.execute(
            "INSERT INTO wizard_state (soa_id, current_step,"
            " completed_steps, status, created_at, updated_at)"
            " VALUES (?,?,?,?,?,?)"
            " ON CONFLICT(soa_id) DO UPDATE SET"
            " current_step=excluded.current_step,"
            " completed_steps=excluded.completed_steps,"
            " status=excluded.status,"
            " updated_at=excluded.updated_at",
            (
                soa_id,
                current_step,
                json.dumps(list(dict.fromkeys(completed_steps))),
                status,
                now,
                now,
            ),
        )
        conn.commit()
    finally:
        conn.close()
