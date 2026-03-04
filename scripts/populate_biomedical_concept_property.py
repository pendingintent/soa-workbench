"""One-time script: populate biomedical_concept_property from DSS variables.

Usage:
    CDISC_SUBSCRIPTION_KEY=<key> python scripts/populate_biomedical_concept_property.py

For each biomedical_concept row with no biomedical_concept_property rows, fetches
DSS variables from the CDISC Library API and inserts property rows. Idempotent —
re-running skips concepts that already have property rows and never recreates UIDs.
"""

import os
import sqlite3
import sys
from pathlib import Path

import requests

DB = Path("soa_builder_web.db")

api_key = os.environ.get("CDISC_API_KEY") or os.environ.get("CDISC_SUBSCRIPTION_KEY")
subscription_key = os.environ.get("CDISC_SUBSCRIPTION_KEY") or api_key
headers = {"Accept": "application/json"}
if subscription_key:
    headers["Ocp-Apim-Subscription-Key"] = subscription_key
if api_key:
    headers["Authorization"] = f"Bearer {api_key}"
    headers["api-key"] = api_key

# Discover biomedical_concept rows with no property rows
conn = sqlite3.connect(DB)
cur = conn.cursor()
cur.execute(
    """
    SELECT bc.soa_id, bc.biomedical_concept_uid, ac.concept_code, ac.activity_id
    FROM biomedical_concept bc
    LEFT JOIN biomedical_concept_property bcp
           ON bcp.biomedical_concept_uid = bc.biomedical_concept_uid
          AND bcp.soa_id = bc.soa_id
    LEFT JOIN activity_concept ac
           ON ac.concept_uid = bc.biomedical_concept_uid
          AND ac.soa_id = bc.soa_id
    WHERE bcp.id IS NULL
      AND ac.concept_code IS NOT NULL
    GROUP BY bc.soa_id, bc.biomedical_concept_uid
    """
)
rows = cur.fetchall()
conn.close()

if not rows:
    print("No biomedical_concept rows need property population — done.")
    sys.exit(0)

print(f"Found {len(rows)} concepts to process.")
inserted_total = 0

for soa_id, bc_uid, concept_code, activity_id in rows:
    # Step 1: discover DSS href
    try:
        r1 = requests.get(
            "https://api.library.cdisc.org/api/cosmos/v2/mdr/specializations"
            "/datasetspecializations?biomedicalconcept=" + concept_code,
            headers=headers,
            timeout=15,
        )
    except Exception as e:
        print(f"  {concept_code}: network error step 1: {e}")
        continue
    if r1.status_code != 200:
        print(f"  {concept_code}: step 1 HTTP {r1.status_code} — skipping")
        continue
    try:
        sdtm_links = r1.json()["_links"]["datasetSpecializations"]["sdtm"]
    except (KeyError, TypeError):
        print(f"  {concept_code}: no sdtm links — skipping")
        continue
    if not sdtm_links:
        print(f"  {concept_code}: empty sdtm links — skipping")
        continue
    dss_href = sdtm_links[0]["href"]
    if dss_href.startswith("/"):
        dss_href = "https://api.library.cdisc.org/api/cosmos/v2" + dss_href

    # Step 2: fetch DSS detail
    try:
        r2 = requests.get(dss_href, headers=headers, timeout=15)
    except Exception as e:
        print(f"  {concept_code}: network error step 2: {e}")
        continue
    if r2.status_code != 200:
        print(f"  {concept_code}: step 2 HTTP {r2.status_code} — skipping")
        continue
    raw = r2.json()
    variables = raw.get("variables") or []
    if not variables:
        print(f"  {concept_code}: no variables — skipping")
        continue

    pkg_href = (raw.get("_links") or {}).get("parentPackage") or {}
    pkg_href = pkg_href.get("href", "") if isinstance(pkg_href, dict) else ""
    try:
        code_system_version = pkg_href.split("/")[5]
    except Exception:
        code_system_version = ""

    # Step 3: persist — one transaction per concept
    conn = sqlite3.connect(DB)
    cur = conn.cursor()
    inserted = 0

    for var in variables:
        var_concept_id = var.get("dataElementConceptId")
        if not var_concept_id:
            continue
        var_name = var.get("name")
        var_required = var.get("mandatoryVariable")
        var_datatype = var.get("dataType")

        # skip if this named property already exists for this BC — UIDs are immutable
        cur.execute(
            "SELECT id FROM biomedical_concept_property"
            " WHERE soa_id=? AND biomedical_concept_uid=? AND name=?",
            (soa_id, bc_uid, var_name),
        )
        if cur.fetchone():
            continue

        # always create a new code row for this property (never reuse)
        cur.execute(
            "SELECT code_uid FROM code WHERE soa_id=? AND code_uid LIKE 'Code_%'",
            (soa_id,),
        )
        existing = [x[0] for x in cur.fetchall() if x[0]]
        code_n = max((int(x.split("_")[1]) for x in existing), default=0) + 1
        code_uid = f"Code_{code_n}"
        cur.execute(
            "INSERT INTO code"
            " (soa_id, code_uid, code, code_system, code_system_version, decode)"
            " VALUES (?,?,?,?,?,?)",
            (soa_id, code_uid, var_concept_id, pkg_href, code_system_version, var_name),
        )

        # always create a new alias_code row for this property (never reuse)
        cur.execute(
            "SELECT alias_code_uid FROM alias_code"
            " WHERE soa_id=? AND alias_code_uid LIKE 'AliasCode_%'",
            (soa_id,),
        )
        existing = [x[0] for x in cur.fetchall() if x[0]]
        alias_n = max((int(x.split("_")[1]) for x in existing), default=0) + 1
        alias_uid = f"AliasCode_{alias_n}"
        cur.execute(
            "INSERT INTO alias_code (soa_id, alias_code_uid, standard_code) VALUES (?,?,?)",
            (soa_id, alias_uid, code_uid),
        )

        # generate monotonic BiomedicalConceptProperty_N uid
        cur.execute(
            "SELECT biomedical_concept_property_uid FROM biomedical_concept_property"
            " WHERE soa_id=? AND biomedical_concept_property_uid LIKE 'BiomedicalConceptProperty_%'",
            (soa_id,),
        )
        existing_uids = [r[0] for r in cur.fetchall() if r[0]]
        n = max((int(u.split("_")[1]) for u in existing_uids), default=0) + 1
        bcp_uid = f"BiomedicalConceptProperty_{n}"

        cur.execute(
            "INSERT INTO biomedical_concept_property"
            " (soa_id, biomedical_concept_uid, biomedical_concept_property_uid,"
            "  name, label, isRequired, datatype, code)"
            " VALUES (?,?,?,?,?,?,?,?)",
            (
                soa_id,
                bc_uid,
                bcp_uid,
                var_name,
                var_name,
                var_required,
                var_datatype,
                alias_uid,
            ),
        )
        inserted += 1

    conn.commit()
    conn.close()
    print(f"  {concept_code}: inserted {inserted} property rows")
    inserted_total += inserted

print(f"\nDone. Total property rows inserted: {inserted_total}")
