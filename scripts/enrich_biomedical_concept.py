"""One-time script: populate label and description in biomedical_concept from CDISC API.

Usage:
    CDISC_API_KEY=<key> python scripts/enrich_biomedical_concept.py

Processes only rows where label IS NULL or description IS NULL, so it is safe
to re-run after partial failures.
"""

import os
import sqlite3
import time
from pathlib import Path

import requests

DB = Path("soa_builder_web.db")
URL_PREFIX = "https://api.library.cdisc.org/api/cosmos/v2/mdr/bc/biomedicalconcepts/"

api_key = os.environ.get("CDISC_API_KEY") or os.environ.get("CDISC_SUBSCRIPTION_KEY")
subscription_key = os.environ.get("CDISC_SUBSCRIPTION_KEY") or api_key
headers: dict = {"Accept": "application/json"}
if subscription_key:
    headers["Ocp-Apim-Subscription-Key"] = subscription_key
if api_key:
    headers["Authorization"] = f"Bearer {api_key}"
    headers["api-key"] = api_key

conn = sqlite3.connect(DB)
cur = conn.cursor()

cur.execute(
    "SELECT id, code FROM biomedical_concept WHERE label IS NULL OR description IS NULL"
)
rows = cur.fetchall()
print(f"Rows to enrich: {len(rows)}")

updated = skipped = errors = 0
for row_id, code in rows:
    if not code:
        skipped += 1
        continue
    try:
        resp = requests.get(URL_PREFIX + code, headers=headers, timeout=15)
        if resp.status_code != 200:
            print(f"  SKIP {code}: HTTP {resp.status_code}")
            skipped += 1
            continue
        data = resp.json()
        label = data.get("shortName")
        description = data.get("definition")
        cur.execute(
            "UPDATE biomedical_concept SET label=?, description=? WHERE id=?",
            (label, description, row_id),
        )
        updated += 1
        print(f"  OK   {code}: {label}")
    except Exception as exc:
        print(f"  ERR  {code}: {exc}")
        errors += 1
    time.sleep(0.1)  # avoid hammering the API

conn.commit()
conn.close()
print(f"\nDone. updated={updated} skipped={skipped} errors={errors}")
