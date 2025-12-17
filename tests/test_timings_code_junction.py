from fastapi.testclient import TestClient

from soa_builder.web.app import app
from soa_builder.web.db import _connect

client = TestClient(app)


def _ensure_soa_clean(soa_id: int) -> int:
    conn = _connect()
    cur = conn.cursor()
    # Ensure SOA exists
    cur.execute(
        "INSERT OR IGNORE INTO soa (id, name) VALUES (?, ?)",
        (soa_id, f"Test SOA {soa_id}"),
    )
    # Clean related tables for isolation
    cur.execute("DELETE FROM timing WHERE soa_id=?", (soa_id,))
    cur.execute("DELETE FROM code WHERE soa_id=?", (soa_id,))
    conn.commit()
    # Seed minimal ddf_terminology if missing
    cur.execute(
        "SELECT name FROM sqlite_master WHERE type='table' AND name='ddf_terminology'"
    )
    if cur.fetchone() is None:
        cur.execute(
            """
            CREATE TABLE ddf_terminology (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                codelist_code TEXT,
                cdisc_submission_value TEXT,
                code TEXT
            )
            """
        )
        conn.commit()
    # Upsert test codelist entries for C201264 (type) and C201265 (relativeToFrom)
    # Clear any existing test entries to avoid duplicates
    cur.execute(
        "DELETE FROM ddf_terminology WHERE codelist_code IN ('C201264','C201265')"
    )
    cur.executemany(
        "INSERT INTO ddf_terminology (codelist_code, cdisc_submission_value, code) VALUES (?,?,?)",
        [
            ("C201264", "TYPE_A", "C201264_A_CODE"),
            ("C201264", "TYPE_B", "C201264_B_CODE"),
            ("C201265", "from", "C201265_FROM_CODE"),
            ("C201265", "to", "C201265_TO_CODE"),
        ],
    )
    conn.commit()
    conn.close()
    return soa_id


def _list_timings(soa_id: int):
    r = client.get(f"/soa/{soa_id}/timings")
    assert r.status_code == 200, r.text
    return r.json()


def _code_rows(soa_id: int, codelist_code: str):
    conn = _connect()
    cur = conn.cursor()
    cur.execute(
        "SELECT code_uid, code FROM code WHERE soa_id=? AND codelist_code=? ORDER BY id",
        (soa_id, codelist_code),
    )
    rows = cur.fetchall() or []
    conn.close()
    return [(r[0], r[1]) for r in rows]


def test_relative_to_from_update_creates_new_code_and_unchanged_does_not():
    soa_id = _ensure_soa_clean(17001)

    # Create T1 with relative_to_from = 'from'; T2 with 'to'.
    r1 = client.post(
        f"/ui/soa/{soa_id}/timings/create",
        data={
            "name": "T1",
            "type_submission_value": "TYPE_A",
            "relative_to_from_submission_value": "from",
        },
        follow_redirects=True,
    )
    assert r1.status_code in (200, 303)
    r2 = client.post(
        f"/ui/soa/{soa_id}/timings/create",
        data={
            "name": "T2",
            "type_submission_value": "TYPE_B",
            "relative_to_from_submission_value": "to",
        },
        follow_redirects=True,
    )
    assert r2.status_code in (200, 303)

    timings = _list_timings(soa_id)
    assert len(timings) == 2
    t1_id = timings[0]["id"]
    t2_id = timings[1]["id"]

    code_rtf_before = _code_rows(soa_id, "C201265")
    assert len(code_rtf_before) == 2

    # Update T1 relative_to_from to 'to' (same code exists from T2) -> must create NEW Code_N
    u1 = client.post(
        f"/ui/soa/{soa_id}/timings/{t1_id}/update",
        data={"name": timings[0]["name"], "relative_to_from_submission_value": "to"},
        follow_redirects=True,
    )
    assert u1.status_code in (200, 303)

    code_rtf_after_change = _code_rows(soa_id, "C201265")
    assert len(code_rtf_after_change) == 3  # new code row created

    timings_after = _list_timings(soa_id)
    t1_after = [t for t in timings_after if t["id"] == t1_id][0]
    t2_after = [t for t in timings_after if t["id"] == t2_id][0]
    assert (
        t1_after["relative_to_from"] != t2_after["relative_to_from"]
    )  # distinct Code_Ns

    # Update T1 relative_to_from to 'to' again (unchanged) -> must NOT create a new code
    u2 = client.post(
        f"/ui/soa/{soa_id}/timings/{t1_id}/update",
        data={"name": t1_after["name"], "relative_to_from_submission_value": "to"},
        follow_redirects=True,
    )
    assert u2.status_code in (200, 303)

    code_rtf_after_unchanged = _code_rows(soa_id, "C201265")
    assert len(code_rtf_after_unchanged) == 3  # unchanged selection does not add


def test_type_update_creates_new_code_and_unchanged_does_not():
    soa_id = _ensure_soa_clean(17002)

    # Create T1 with type = 'TYPE_A'; T2 with 'TYPE_B'.
    r1 = client.post(
        f"/ui/soa/{soa_id}/timings/create",
        data={
            "name": "T1",
            "type_submission_value": "TYPE_A",
            "relative_to_from_submission_value": "from",
        },
        follow_redirects=True,
    )
    assert r1.status_code in (200, 303)
    r2 = client.post(
        f"/ui/soa/{soa_id}/timings/create",
        data={
            "name": "T2",
            "type_submission_value": "TYPE_B",
            "relative_to_from_submission_value": "to",
        },
        follow_redirects=True,
    )
    assert r2.status_code in (200, 303)

    timings = _list_timings(soa_id)
    assert len(timings) == 2
    t1_id = timings[0]["id"]
    t2_id = timings[1]["id"]

    code_type_before = _code_rows(soa_id, "C201264")
    assert len(code_type_before) == 2

    # Update T1 type to 'TYPE_B' (same code exists from T2) -> must create NEW Code_N
    u1 = client.post(
        f"/ui/soa/{soa_id}/timings/{t1_id}/update",
        data={"name": timings[0]["name"], "type_submission_value": "TYPE_B"},
        follow_redirects=True,
    )
    assert u1.status_code in (200, 303)

    code_type_after_change = _code_rows(soa_id, "C201264")
    assert len(code_type_after_change) == 3  # new code row created

    timings_after = _list_timings(soa_id)
    t1_after = [t for t in timings_after if t["id"] == t1_id][0]
    t2_after = [t for t in timings_after if t["id"] == t2_id][0]
    assert t1_after["type"] != t2_after["type"]  # distinct Code_Ns

    # Update T1 type to 'TYPE_B' again (unchanged) -> must NOT create a new code
    u2 = client.post(
        f"/ui/soa/{soa_id}/timings/{t1_id}/update",
        data={"name": t1_after["name"], "type_submission_value": "TYPE_B"},
        follow_redirects=True,
    )
    assert u2.status_code in (200, 303)

    code_type_after_unchanged = _code_rows(soa_id, "C201264")
    assert len(code_type_after_unchanged) == 3  # unchanged selection does not add
