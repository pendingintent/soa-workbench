from soa_builder.web.db import _connect
from soa_builder.web.initialize_database import _init_db
from soa_builder.web.app import _get_next_code_uid


def setup_module(module):
    # Ensure test DB is initialized
    _init_db()


def test_get_next_code_uid_empty_soa():
    conn = _connect()
    cur = conn.cursor()
    # create a dummy SOA
    cur.execute(
        "INSERT INTO soa (name, created_at) VALUES (?, datetime('now'))", ("TestStudy",)
    )
    soa_id = cur.lastrowid
    conn.commit()
    # No existing codes -> should return Code_1
    code_uid = _get_next_code_uid(cur, soa_id)
    assert code_uid == "Code_1"
    conn.close()


def test_get_next_code_uid_mixed_existing():
    conn = _connect()
    cur = conn.cursor()
    # create a new SOA
    cur.execute(
        "INSERT INTO soa (name, created_at) VALUES (?, datetime('now'))",
        ("TestStudy2",),
    )
    soa_id = cur.lastrowid
    conn.commit()
    # Insert mixed existing code_uids
    cur.execute(
        "INSERT INTO code_association (soa_id, code_uid, codelist_table, codelist_code, code) VALUES (?,?,?,?,?)",
        (soa_id, "Code_1", "protocol_terminology", "C174222", "X"),
    )
    cur.execute(
        "INSERT INTO code_association (soa_id, code_uid, codelist_table, codelist_code, code) VALUES (?,?,?,?,?)",
        (soa_id, "Code_3", "http://www.cdisc.org", "C188727", "Y"),
    )
    # Malformed tail should be ignored in max() and trigger fallback only if parsing fails for all
    cur.execute(
        "INSERT INTO code_association (soa_id, code_uid, codelist_table, codelist_code, code) VALUES (?,?,?,?,?)",
        (soa_id, "Code_X", "protocol_terminology", "C174222", "Z"),
    )
    conn.commit()
    # Expect next to be max numeric + 1 -> Code_4
    next_uid = _get_next_code_uid(cur, soa_id)
    assert next_uid == "Code_4"
    conn.close()


def test_get_next_code_uid_all_invalid_tails():
    conn = _connect()
    cur = conn.cursor()
    # create a new SOA
    cur.execute(
        "INSERT INTO soa (name, created_at) VALUES (?, datetime('now'))",
        ("TestStudy3",),
    )
    soa_id = cur.lastrowid
    conn.commit()
    # Insert only invalid tails that cannot be parsed as integers
    cur.execute(
        "INSERT INTO code_association (soa_id, code_uid, codelist_table, codelist_code, code) VALUES (?,?,?,?,?)",
        (soa_id, "Code_A", "protocol_terminology", "C174222", "X"),
    )
    cur.execute(
        "INSERT INTO code_association (soa_id, code_uid, codelist_table, codelist_code, code) VALUES (?,?,?,?,?)",
        (soa_id, "Code_B", "http://www.cdisc.org", "C188727", "Y"),
    )
    conn.commit()
    # Fallback should use len(existing)+1 -> 3
    next_uid = _get_next_code_uid(cur, soa_id)
    assert next_uid == "Code_3"
    conn.close()
