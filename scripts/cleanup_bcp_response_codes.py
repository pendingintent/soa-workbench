"""Clean up stale and mismatched BiomedicalConceptProperty response codes.

Removes orphaned ``bcp_response_code`` / ``biomedical_concept_property``
rows (parent gone) and re-populates BCs whose live response codes are
anomalous (duplicate RCs on a property, or mixed code-system versions
within a BC) so the RCs are regenerated from the CDISC source.

Usage:
    # Report only (default), against SOA_BUILDER_DB (default
    # soa_builder_web.db):
    python scripts/cleanup_bcp_response_codes.py --dry-run

    # Apply changes (back up the database first!):
    python scripts/cleanup_bcp_response_codes.py --apply

    # Restrict to specific SOAs:
    python scripts/cleanup_bcp_response_codes.py --apply --soa-id 60

Re-population (step 2) calls the CDISC Library API, so
``CDISC_API_KEY``/``CDISC_SUBSCRIPTION_KEY`` must be set for it to work.
The orphan sweep (step 1) is offline and always safe.
"""

import argparse
import logging
import os
import sys
from typing import List, Optional

from soa_builder.web.db import _connect
from usdm.generate_biomedical_concept_properties import (
    populate_biomedical_concept_properties_for_bc,
    sweep_orphaned_bcp_rows,
)

logger = logging.getLogger("cleanup_bcp_response_codes")


def _all_soas_with_response_codes(cur) -> List[int]:
    """Return every SOA that has any response-code rows.

    Used as the default scope so anomalies are never missed because a SOA
    happened to lack orphans (orphan-free SOAs can still hold mismatched
    or duplicate live response codes).
    """
    cur.execute("SELECT DISTINCT soa_id FROM bcp_response_code ORDER BY soa_id")
    return [r[0] for r in cur.fetchall()]


def _count_orphans(cur, soa_id: int) -> int:
    """Count RC rows in a SOA whose backing BCP+BC pair is absent."""
    cur.execute(
        "SELECT COUNT(*) FROM bcp_response_code rc"
        " WHERE rc.soa_id=? AND NOT EXISTS ("
        "   SELECT 1 FROM biomedical_concept_property bcp"
        "   JOIN biomedical_concept bc"
        "     ON bc.soa_id=bcp.soa_id"
        "     AND bc.biomedical_concept_uid=bcp.biomedical_concept_uid"
        "   WHERE bcp.soa_id=rc.soa_id"
        "     AND bcp.biomedical_concept_property_uid"
        "       =rc.biomedical_concept_property_uid"
        " )",
        (soa_id,),
    )
    return cur.fetchone()[0]


def _anomalous_bcs(cur, soa_id: int) -> List[str]:
    """Return BC UIDs whose live response codes look corrupt.

    A BC is flagged when any live property carries more than one response
    code, or when the BC's response codes span more than one code-system
    version (accretion across populate runs).
    """
    flagged = set()

    cur.execute(
        "SELECT bcp.biomedical_concept_uid"
        " FROM bcp_response_code rc"
        " JOIN biomedical_concept_property bcp"
        "   ON bcp.soa_id=rc.soa_id"
        "   AND bcp.biomedical_concept_property_uid"
        "     =rc.biomedical_concept_property_uid"
        " WHERE rc.soa_id=?"
        " GROUP BY rc.biomedical_concept_property_uid,"
        "          bcp.biomedical_concept_uid"
        " HAVING COUNT(*) > 1",
        (soa_id,),
    )
    flagged.update(r[0] for r in cur.fetchall())

    cur.execute(
        "SELECT bcp.biomedical_concept_uid"
        " FROM bcp_response_code rc"
        " JOIN biomedical_concept_property bcp"
        "   ON bcp.soa_id=rc.soa_id"
        "   AND bcp.biomedical_concept_property_uid"
        "     =rc.biomedical_concept_property_uid"
        " JOIN alias_code ac ON rc.code=ac.alias_code_uid"
        "   AND rc.soa_id=ac.soa_id"
        " JOIN code c ON ac.standard_code=c.code_uid"
        "   AND ac.soa_id=c.soa_id"
        " WHERE rc.soa_id=?"
        " GROUP BY bcp.biomedical_concept_uid"
        " HAVING COUNT(DISTINCT c.code_system_version) > 1",
        (soa_id,),
    )
    flagged.update(r[0] for r in cur.fetchall())

    return sorted(flagged)


def _concept_code_for_bc(cur, soa_id: int, bc_uid: str) -> Optional[str]:
    """Look up the concept_code linked to a BC via activity_concept."""
    cur.execute(
        "SELECT concept_code FROM activity_concept"
        " WHERE soa_id=? AND concept_uid=? AND concept_code IS NOT NULL"
        " LIMIT 1",
        (soa_id, bc_uid),
    )
    row = cur.fetchone()
    return row[0] if row else None


def main(argv: Optional[List[str]] = None) -> int:
    logging.basicConfig(level=logging.INFO, format="%(message)s")
    parser = argparse.ArgumentParser(description=__doc__)
    group = parser.add_mutually_exclusive_group()
    group.add_argument(
        "--dry-run",
        action="store_true",
        help="Report what would change without writing (default).",
    )
    group.add_argument(
        "--apply",
        action="store_true",
        help="Apply the cleanup (mutates the database).",
    )
    parser.add_argument(
        "--soa-id",
        type=int,
        action="append",
        help="Restrict to this SOA (repeatable). Default: known affected SOAs.",
    )
    args = parser.parse_args(argv)
    apply = args.apply  # dry-run is the default when neither is given

    db_path = os.environ.get("SOA_BUILDER_DB", "soa_builder_web.db")
    print(f"Target database: {db_path}")
    if apply:
        print("Mode: APPLY (database will be modified) — ensure a backup exists.")
    else:
        print("Mode: DRY-RUN (no changes will be written).")

    conn = _connect()
    cur = conn.cursor()
    try:
        soa_ids = args.soa_id or _all_soas_with_response_codes(cur)
        print(f"Scope: SOAs {soa_ids}")
        print("\n== Before ==")
        for sid in soa_ids:
            orphans = _count_orphans(cur, sid)
            flagged = _anomalous_bcs(cur, sid)
            print(
                f"  soa {sid}: {orphans} orphaned RC, "
                f"{len(flagged)} anomalous BC(s): {flagged}"
            )
    finally:
        conn.close()

    if not apply:
        print("\nDry-run complete. Re-run with --apply to make changes.")
        return 0

    # Step 1: orphan sweep (offline, safe).
    print("\n== Step 1: sweeping orphaned rows ==")
    for sid in soa_ids:
        swept = sweep_orphaned_bcp_rows(sid)
        print(f"  soa {sid}: removed {swept}")

    # Step 2: re-populate anomalous BCs from the CDISC source.
    print("\n== Step 2: re-populating anomalous BCs from source ==")
    conn = _connect()
    cur = conn.cursor()
    try:
        targets = []
        for sid in soa_ids:
            for bc_uid in _anomalous_bcs(cur, sid):
                code = _concept_code_for_bc(cur, sid, bc_uid)
                targets.append((sid, bc_uid, code))
    finally:
        conn.close()

    for sid, bc_uid, code in targets:
        if not code:
            print(f"  soa {sid} {bc_uid}: no concept_code — skipped")
            continue
        try:
            populate_biomedical_concept_properties_for_bc(sid, bc_uid, code)
            print(f"  soa {sid} {bc_uid} ({code}): re-populated")
        except Exception as exc:  # noqa: BLE001 - report and continue
            print(f"  soa {sid} {bc_uid} ({code}): FAILED — {exc}")
            logger.exception("re-populate failed soa=%s bc=%s", sid, bc_uid)

    # Final report.
    print("\n== After ==")
    conn = _connect()
    cur = conn.cursor()
    try:
        for sid in soa_ids:
            orphans = _count_orphans(cur, sid)
            flagged = _anomalous_bcs(cur, sid)
            print(
                f"  soa {sid}: {orphans} orphaned RC, "
                f"{len(flagged)} anomalous BC(s): {flagged}"
            )
    finally:
        conn.close()
    return 0


if __name__ == "__main__":
    sys.exit(main())
