"""Pytest configuration to isolate tests from the production SoA database.

Sets SOA_BUILDER_DB environment variable BEFORE importing the application module so
all test connections use a separate SQLite file, preserving existing studies.
"""

import os
from pathlib import Path

# Use a test-specific database file in the workspace root
TEST_DB_PATH = Path("soa_builder_web_tests.db").absolute()
# Only set if not already overridden externally
os.environ.setdefault("SOA_BUILDER_DB", str(TEST_DB_PATH))

# Ensure directory exists (for absolute paths inside nested structures)
TEST_DB_PATH.parent.mkdir(parents=True, exist_ok=True)

# (App module will be imported by test modules afterwards and will pick this path.)
