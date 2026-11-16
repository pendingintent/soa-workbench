"""Helpers for checked-in JSON snapshot tests.

Run ``UPDATE_SNAPSHOTS=1 pytest <test file>`` to (re)write a snapshot
after an intended change, then review the diff before committing.
"""

import json
import os
from pathlib import Path

SNAPSHOT_DIR = Path(__file__).parent / "snapshots"


def assert_matches_snapshot(name: str, data) -> None:
    path = SNAPSHOT_DIR / name
    rendered = json.dumps(data, indent=1, sort_keys=True) + "\n"
    if os.environ.get("UPDATE_SNAPSHOTS") == "1":
        path.write_text(rendered)
        return
    assert path.exists(), f"Missing snapshot {path}; run with UPDATE_SNAPSHOTS=1"
    expected = json.loads(path.read_text())
    assert data == expected, (
        f"{name} changed. If intended, regenerate with UPDATE_SNAPSHOTS=1 "
        "and review the diff."
    )
