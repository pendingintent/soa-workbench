"""Getting-started progress for a study (issue #382).

Each setup step's state is derived from data the study already holds,
so the edit-page checklist (and the setup wizard) need no stored
state of their own.
"""

import logging

from ..db import _connect

logger = logging.getLogger(__name__)

# (key, label, count SQL, link path relative to /ui/soa/{id})
# Every query takes the study id as its only parameter.
_STEPS = (
    (
        "study",
        "Study details (name and study ID)",
        "SELECT COUNT(*) FROM soa WHERE id=? AND TRIM(COALESCE(study_id, '')) != ''",
        "/edit#study-meta",
    ),
    (
        "titles",
        "Study titles",
        "SELECT COUNT(*) FROM study_title WHERE soa_id=?",
        "/edit#study-meta",
    ),
    (
        "epochs",
        "Epochs",
        "SELECT COUNT(*) FROM epoch WHERE soa_id=?",
        "/epochs",
    ),
    (
        "arms",
        "Arms",
        "SELECT COUNT(*) FROM arm WHERE soa_id=?",
        "/arms",
    ),
    (
        "encounters",
        "Encounters",
        "SELECT COUNT(*) FROM visit WHERE soa_id=?",
        "/visits",
    ),
    (
        "activities",
        "Activities",
        "SELECT COUNT(*) FROM activity WHERE soa_id=?",
        "/activities",
    ),
    (
        "instances",
        "Scheduled instances (matrix columns)",
        "SELECT COUNT(*) FROM instances WHERE soa_id=?",
        "/instances",
    ),
    (
        "matrix",
        "Activities scheduled in the matrix",
        "SELECT COUNT(*) FROM matrix_cells WHERE soa_id=?",
        "/edit#matrix",
    ),
    (
        "concepts",
        "Activities with biomedical concepts",
        "SELECT COUNT(DISTINCT activity_id) FROM activity_concept WHERE soa_id=?",
        "/activities",
    ),
)


def get_setup_progress(soa_id: int) -> list[dict]:
    """Return the setup checklist for one study.

    Each item is ``{key, label, count, done, href}``. A step is done
    when its count is at least one.
    """
    conn = _connect()
    try:
        cur = conn.cursor()
        items = []
        for key, label, sql, path in _STEPS:
            cur.execute(sql, (soa_id,))
            count = (cur.fetchone() or [0])[0] or 0
            items.append(
                {
                    "key": key,
                    "label": label,
                    "count": count,
                    "done": count > 0,
                    "href": f"/ui/soa/{soa_id}{path}",
                }
            )
        return items
    finally:
        conn.close()
