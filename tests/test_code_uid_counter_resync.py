"""Code_N / AliasCode_N allocation must not collide with existing rows.

Several writers create `code` and `alias_code` rows. Some draw from
the durable uid_counter (get_next_code_uid, get_next_alias_code_uid)
and some historically numbered rows by scanning for the highest
existing suffix, without advancing the counter. A study that mixed
the two ended up with a counter behind its own rows, and every later
counter allocation (e.g. BiomedicalConceptProperty population)
failed with ``UNIQUE constraint failed: code.code_uid, code.soa_id``.
"""

from unittest.mock import patch

from soa_builder.web.app import app  # noqa: F401  (triggers DB init)
from soa_builder.web.db import _connect
from soa_builder.web.services.concepts import _upsert_biomedical_concept
from soa_builder.web.utils import get_next_alias_code_uid, get_next_code_uid
from usdm.generate_biomedical_concept_properties import (
    populate_biomedical_concept_properties_for_bc,
)

MOCK_BC_API = {
    "_links": {
        "parentPackage": {"href": "/mdr/bc/packages/2024-09-27/biomedicalconcepts"}
    },
    "dataElementConcepts": [
        {"conceptId": "C25347", "shortName": "Height", "dataType": "decimal"},
        {"conceptId": "C25208", "shortName": "Weight", "dataType": "decimal"},
    ],
}


def _new_soa(cur, name):
    cur.execute(
        "INSERT INTO soa (name, created_at) VALUES (?, datetime('now'))", (name,)
    )
    return cur.lastrowid


def _insert_code(cur, soa_id, code_uid, code="C0"):
    cur.execute(
        "INSERT INTO code (soa_id, code_uid, code) VALUES (?,?,?)",
        (soa_id, code_uid, code),
    )


def _insert_alias(cur, soa_id, alias_uid, code_uid):
    cur.execute(
        "INSERT INTO alias_code (soa_id, alias_code_uid, standard_code) VALUES (?,?,?)",
        (soa_id, alias_uid, code_uid),
    )


def test_code_counter_skips_numbers_taken_outside_it():
    conn = _connect()
    cur = conn.cursor()
    try:
        soa_id = _new_soa(cur, "resync-code")
        first = get_next_code_uid(cur, soa_id)
        assert first == "Code_1"
        _insert_code(cur, soa_id, first)
        # Rows numbered by a max+1 scan that never touched the counter.
        for n in range(2, 6):
            _insert_code(cur, soa_id, f"Code_{n}")
        conn.commit()

        assert get_next_code_uid(cur, soa_id) == "Code_6"
    finally:
        conn.close()


def test_alias_counter_skips_numbers_taken_outside_it():
    conn = _connect()
    cur = conn.cursor()
    try:
        soa_id = _new_soa(cur, "resync-alias")
        _insert_code(cur, soa_id, "Code_1")
        first = get_next_alias_code_uid(cur, soa_id)
        assert first == "AliasCode_1"
        _insert_alias(cur, soa_id, first, "Code_1")
        for n in range(2, 4):
            _insert_alias(cur, soa_id, f"AliasCode_{n}", "Code_1")
        conn.commit()

        assert get_next_alias_code_uid(cur, soa_id) == "AliasCode_4"
    finally:
        conn.close()


def test_code_counter_still_never_reuses_a_freed_number():
    conn = _connect()
    cur = conn.cursor()
    try:
        soa_id = _new_soa(cur, "resync-freed")
        for _ in range(2):
            _insert_code(cur, soa_id, get_next_code_uid(cur, soa_id))
        cur.execute("DELETE FROM code WHERE soa_id=? AND code_uid='Code_2'", (soa_id,))
        conn.commit()

        assert get_next_code_uid(cur, soa_id) == "Code_3"
    finally:
        conn.close()


def test_assigning_a_concept_advances_the_code_counters():
    conn = _connect()
    cur = conn.cursor()
    try:
        soa_id = _new_soa(cur, "resync-bc")
        # Something else (a study title, say) seeds the counters first.
        code_uid = get_next_code_uid(cur, soa_id)
        _insert_code(cur, soa_id, code_uid)
        _insert_alias(cur, soa_id, get_next_alias_code_uid(cur, soa_id), code_uid)

        _upsert_biomedical_concept(
            cur, soa_id, "BiomedicalConcept_1", "Body Height", "C25347"
        )
        conn.commit()

        cur.execute(
            "SELECT next_n FROM uid_counter WHERE soa_id=? AND prefix=?",
            (soa_id, "Code_"),
        )
        assert cur.fetchone()[0] == 3
        cur.execute(
            "SELECT next_n FROM uid_counter WHERE soa_id=? AND prefix=?",
            (soa_id, "AliasCode_"),
        )
        assert cur.fetchone()[0] == 3
    finally:
        conn.close()


def test_properties_populate_after_concepts_are_assigned():
    """The reported failure: a seeded counter, then concepts, then BCPs."""
    conn = _connect()
    cur = conn.cursor()
    try:
        soa_id = _new_soa(cur, "resync-bcp")
        code_uid = get_next_code_uid(cur, soa_id)
        _insert_code(cur, soa_id, code_uid)
        for n in (1, 2):
            _upsert_biomedical_concept(
                cur, soa_id, f"BiomedicalConcept_{n}", f"BC {n}", f"C9000{n}"
            )
        conn.commit()
    finally:
        conn.close()

    with (
        patch(
            "usdm.generate_biomedical_concept_properties._get_biomedical_concept_data",
            return_value=MOCK_BC_API,
        ),
        patch(
            "usdm.generate_biomedical_concept_properties._get_dss_href_for_bc",
            return_value="",
        ),
    ):
        populate_biomedical_concept_properties_for_bc(
            soa_id, "BiomedicalConcept_2", "C90002"
        )

    conn = _connect()
    try:
        rows = conn.execute(
            "SELECT name FROM biomedical_concept_property"
            " WHERE soa_id=? AND biomedical_concept_uid=? ORDER BY id",
            (soa_id, "BiomedicalConcept_2"),
        ).fetchall()
    finally:
        conn.close()
    assert [r[0] for r in rows] == ["Height", "Weight"]
