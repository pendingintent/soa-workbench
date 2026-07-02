"""Refresh the vendored define.yaml schema from CDISC DataExchange-DDS.

The workbench validates generated Define-JSON output against the
LinkML schema published at:

    https://github.com/cdisc-org/DataExchange-DDS/blob/main/model/define.yaml

That repository is the source of truth. This script re-downloads the
schema and overwrites the local copy at schema/define.yaml, along
with a schema/define.yaml.source file recording the commit it came
from so the vendored copy stays traceable.

Usage:
    python scripts/fetch_define_schema.py [--ref main]
"""

import argparse
import logging
import urllib.error
import urllib.request
from pathlib import Path

logger = logging.getLogger(__name__)

REPO = "cdisc-org/DataExchange-DDS"
SCHEMA_PATH_IN_REPO = "model/define.yaml"
DEST_PATH = Path(__file__).resolve().parent.parent / "schema" / "define.yaml"


def _resolve_commit_sha(ref):
    url = (
        f"https://api.github.com/repos/{REPO}/commits"
        f"?path={SCHEMA_PATH_IN_REPO}&sha={ref}&per_page=1"
    )
    with urllib.request.urlopen(url) as response:
        import json

        commits = json.load(response)
    return commits[0]["sha"]


def fetch_define_schema(ref="main"):
    """Download define.yaml from DataExchange-DDS to schema/define.yaml."""
    sha = _resolve_commit_sha(ref)
    raw_url = f"https://raw.githubusercontent.com/{REPO}/{sha}/{SCHEMA_PATH_IN_REPO}"
    logger.info("Fetching %s", raw_url)
    try:
        with urllib.request.urlopen(raw_url) as response:
            content = response.read()
    except urllib.error.HTTPError as exc:
        raise RuntimeError(f"Failed to download {raw_url}: {exc}") from exc

    DEST_PATH.parent.mkdir(parents=True, exist_ok=True)
    DEST_PATH.write_bytes(content)

    source_note = DEST_PATH.with_suffix(".yaml.source")
    source_note.write_text(
        f"repo: https://github.com/{REPO}\n"
        f"path: {SCHEMA_PATH_IN_REPO}\n"
        f"ref: {ref}\n"
        f"commit: {sha}\n"
    )
    logger.info("Wrote %s (commit %s)", DEST_PATH, sha[:12])


def main():
    logging.basicConfig(level=logging.INFO, format="%(message)s")
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--ref",
        default="main",
        help="Branch, tag, or commit to fetch define.yaml from (default: main)",
    )
    args = parser.parse_args()
    fetch_define_schema(ref=args.ref)


if __name__ == "__main__":
    main()
