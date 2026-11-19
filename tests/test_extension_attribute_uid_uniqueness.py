"""Every ExtensionAttribute_N uid must be unique within a SoA.

Four writers hand them out: the DSS and CRF backfills, the BC-grouping
backfill and the tool extension. USDM requires unique ids across the
whole document, so whatever order these run in (and whatever uids a
rollback or legacy data already left behind), no two may share a uid.
"""

import itertools
from collections import Counter

import pytest

from soa_builder.web.app import app  # noqa: F401  (triggers DB init)
from soa_builder.web.db import _connect
from soa_builder.web.migrations.m0119_dedupe_extension_attribute_uids import (
    m0119_dedupe_extension_attribute_uids,
)
from usdm.generate_activity_grouping_extensions import (
    populate_activity_grouping_extensions,
)
from usdm.generate_extension_attributes import (
    populate_crf_extension_attributes,
    populate_extension_attributes,
)
from usdm.generate_usdm import _get_or_create_tool_uids

SOA_ID = 9311
GROUP_UID = "cdisc:concept_group:cg_ea_uid_test"

EA_COLUMNS = (
    ("activity_concept_dss", "extension_attribute_uid"),
    ("activity_concept_crf", "extension_attribute_uid"),
    ("activity_grouping_extension", "ea_outer_uid"),
    ("activity_grouping_extension", "ea_scheme_uid"),
    ("activity_grouping_extension", "ea_value_uid"),
    ("soa_tool_extension", "ea_outer_uid"),
    ("soa_tool_extension", "ea_name_uid"),
    ("soa_tool_extension", "ea_version_uid"),
    ("soa_tool_extension", "ea_date_uid"),
)
TABLES = (
    "activity_concept_dss",
    "activity_concept_crf",
    "activity_grouping_extension",
    "soa_tool_extension",
    "activity_concept",
    "uid_counter",
)


@pytest.fixture(autouse=True)
def _clean():
    def wipe():
        conn = _connect()
        for table in TABLES:
            conn.execute(f"DELETE FROM {table} WHERE soa_id=?", (SOA_ID,))
        conn.execute(
            "DELETE FROM concept_group WHERE concept_group_uid=?", (GROUP_UID,)
        )
        conn.commit()
        conn.close()

    wipe()
    conn = _connect()
    conn.execute(
        "INSERT INTO concept_group"
        " (concept_group_uid, name, source, cdisc_scheme_id,"
        "  cdisc_scheme_name, cdisc_value_id)"
        " VALUES (?, 'g', 'cdisc', 'concept_group', 'Concept Group', 'g')",
        (GROUP_UID,),
    )
    conn.commit()
    conn.close()
    yield
    wipe()


def _add_dss(n: int, uid=None) -> None:
    conn = _connect()
    for i in range(n):
        conn.execute(
            "INSERT INTO activity_concept_dss (soa_id, activity_id,"
            " concept_code, dss_title, dss_href, extension_attribute_uid)"
            " VALUES (?, 1, 'C1', ?, 'h', ?)",
            (SOA_ID, f"DSS{i}", uid),
        )
    conn.commit()
    conn.close()


def _add_crf(n: int, uid=None) -> None:
    conn = _connect()
    for i in range(n):
        conn.execute(
            "INSERT INTO activity_concept_crf (soa_id, activity_id,"
            " concept_code, crf_title, crf_href, extension_attribute_uid)"
            " VALUES (?, 1, 'C1', ?, 'h', ?)",
            (SOA_ID, f"CRF{i}", uid),
        )
    conn.commit()
    conn.close()


_next_activity = itertools.count(1)


def _add_group_assignment() -> None:
    n = next(_next_activity)
    conn = _connect()
    conn.execute(
        "INSERT INTO activity_concept (soa_id, activity_id, activity_uid,"
        " concept_uid, concept_code, concept_title, concept_group_uid)"
        " VALUES (?, ?, ?, 'BiomedicalConcept_1', 'C1', 't', ?)",
        (SOA_ID, n, f"Activity_{n}", GROUP_UID),
    )
    conn.commit()
    conn.close()


def _ea_uses() -> Counter:
    conn = _connect()
    uses: Counter = Counter()
    for table, col in EA_COLUMNS:
        for (uid,) in conn.execute(
            f"SELECT {col} FROM {table} WHERE soa_id=? AND {col} IS NOT NULL",
            (SOA_ID,),
        ):
            uses[uid] += 1
    conn.close()
    return uses


def _duplicates() -> dict:
    return {uid: n for uid, n in _ea_uses().items() if n > 1}


def _tool():
    _get_or_create_tool_uids(SOA_ID)


def _dss():
    _add_dss(2)
    populate_extension_attributes(SOA_ID)


def _crf():
    _add_crf(2)
    populate_crf_extension_attributes(SOA_ID)


def _grouping():
    _add_group_assignment()
    _add_group_assignment()
    populate_activity_grouping_extensions(SOA_ID)


WRITERS = {"tool": _tool, "dss": _dss, "crf": _crf, "grouping": _grouping}


@pytest.mark.parametrize(
    "order",
    list(itertools.permutations(WRITERS)),
    ids=lambda o: "-".join(o),
)
def test_writers_never_hand_out_the_same_uid(order):
    for name in order:
        WRITERS[name]()
    # A second round of DSS, CRF and grouping rows after the others.
    _dss()
    _crf()
    _grouping()
    uses = _ea_uses()
    assert sum(uses.values()) == 4 + 4 + 4 + 4 * 3
    assert _duplicates() == {}


def test_writers_skip_uids_already_held_outside_the_counter():
    """Rollback and legacy data write uids without the counter. A uid
    already in a table must never be handed out again."""
    _tool()
    _add_dss(1, uid="ExtensionAttribute_50")
    _grouping()
    _dss()
    _crf()
    assert _duplicates() == {}


def _held(table: str, col: str) -> list:
    conn = _connect()
    rows = conn.execute(
        f"SELECT {col} FROM {table} WHERE soa_id=? ORDER BY id", (SOA_ID,)
    ).fetchall()
    conn.close()
    return [r[0] for r in rows]


def test_migration_reassigns_duplicates_held_by_generated_extensions():
    """The SoA 60 pattern: DSS/CRF rows hold uids that the tool and
    grouping extensions also hold. DSS/CRF keep theirs (freeze diffs
    compare them); the generated extensions get fresh uids."""
    conn = _connect()
    conn.execute(
        "INSERT INTO soa_tool_extension (soa_id, ea_outer_uid, ec_uid,"
        " ea_name_uid, ea_version_uid, ea_date_uid) VALUES (?,"
        " 'ExtensionAttribute_1', 'ExtensionClass_1', 'ExtensionAttribute_2',"
        " 'ExtensionAttribute_3', 'ExtensionAttribute_4')",
        (SOA_ID,),
    )
    conn.execute(
        "INSERT INTO activity_grouping_extension (soa_id, activity_uid,"
        " concept_group_uid, ea_outer_uid, ec_uid, ea_scheme_uid,"
        " ea_value_uid) VALUES (?, 'Activity_1', ?, 'ExtensionAttribute_5',"
        " 'ExtensionClass_2', 'ExtensionAttribute_6', 'ExtensionAttribute_7')",
        (SOA_ID, GROUP_UID),
    )
    conn.commit()
    conn.close()
    for n in (1, 2, 3, 4):
        _add_dss(1, uid=f"ExtensionAttribute_{n}")
    for n in (5, 6):
        _add_crf(1, uid=f"ExtensionAttribute_{n}")
    _add_dss(1, uid="ExtensionAttribute_7")
    assert len(_duplicates()) == 7

    m0119_dedupe_extension_attribute_uids()

    assert _duplicates() == {}
    assert _held("activity_concept_dss", "extension_attribute_uid") == [
        f"ExtensionAttribute_{n}" for n in (1, 2, 3, 4, 7)
    ]
    assert _held("activity_concept_crf", "extension_attribute_uid") == [
        "ExtensionAttribute_5",
        "ExtensionAttribute_6",
    ]
    # Reassigned uids are new numbers above every uid in use.
    reassigned = _held("soa_tool_extension", "ea_outer_uid") + _held(
        "activity_grouping_extension", "ea_outer_uid"
    )
    assert all(int(u.split("_")[1]) > 7 for u in reassigned)
    # The class uids were not duplicated and are left alone.
    assert _held("soa_tool_extension", "ec_uid") == ["ExtensionClass_1"]

    # Later allocations do not reissue a reassigned uid.
    _dss()
    assert _duplicates() == {}


def test_migration_leaves_unique_uids_alone():
    _tool()
    _dss()
    before = {(t, c): _held(t, c) for t, c in EA_COLUMNS}
    m0119_dedupe_extension_attribute_uids()
    assert {(t, c): _held(t, c) for t, c in EA_COLUMNS} == before
