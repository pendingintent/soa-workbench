"""Tests for the crf_href latest-version backfill migration."""

from soa_builder.web.db import _connect
from soa_builder.web.migrate_database import (
    _migrate_backfill_crf_href_latest_version,
)

SOA_ID = 9301
ACTIVITY_ID = 8101
CONCEPT_CODE = "C25564"

OLD_HREF = (
    "https://api.library.cdisc.org/api/cosmos/v2/mdr/specializations/"
    "crf/packages/2026-06-30/specializations/VSTEST"
)
NEW_HREF = (
    "https://api.library.cdisc.org/api/cosmos/v2/mdr/specializations/"
    "crf/specializations/VSTEST"
)
ALREADY_NEW_HREF = (
    "https://api.library.cdisc.org/api/cosmos/v2/mdr/specializations/"
    "crf/specializations/VSPOS"
)


def _clean():
    conn = _connect()
    cur = conn.cursor()
    cur.execute("DELETE FROM activity_concept_crf WHERE soa_id=?", (SOA_ID,))
    conn.commit()
    conn.close()


def _insert_crf(href, title="Vital Signs"):
    conn = _connect()
    cur = conn.cursor()
    cur.execute(
        "INSERT INTO activity_concept_crf"
        " (soa_id, activity_id, concept_code, crf_title, crf_href)"
        " VALUES (?, ?, ?, ?, ?)",
        (SOA_ID, ACTIVITY_ID, CONCEPT_CODE, title, href),
    )
    conn.commit()
    row_id = cur.lastrowid
    conn.close()
    return row_id


def _get_href(row_id):
    conn = _connect()
    cur = conn.cursor()
    cur.execute("SELECT crf_href FROM activity_concept_crf WHERE id=?", (row_id,))
    row = cur.fetchone()
    conn.close()
    return row[0]


def test_backfill_rewrites_dated_package_href():
    _clean()
    row_id = _insert_crf(OLD_HREF)

    _migrate_backfill_crf_href_latest_version()

    assert _get_href(row_id) == NEW_HREF
    _clean()


def test_backfill_leaves_new_format_untouched():
    _clean()
    row_id = _insert_crf(ALREADY_NEW_HREF)

    _migrate_backfill_crf_href_latest_version()

    assert _get_href(row_id) == ALREADY_NEW_HREF
    _clean()


def test_backfill_is_idempotent():
    _clean()
    row_id = _insert_crf(OLD_HREF)

    _migrate_backfill_crf_href_latest_version()
    _migrate_backfill_crf_href_latest_version()

    assert _get_href(row_id) == NEW_HREF
    _clean()
