"""Shared setup for the legacy migrations split out of migrate_database.py.

Importing it loads ``.env`` and configures the ``soa_builder.concepts``
logger exactly as migrate_database.py did at import time.
"""

import logging

from dotenv import load_dotenv

load_dotenv()
logger = logging.getLogger("soa_builder.concepts")
if not logger.handlers:
    _h = logging.StreamHandler()
    _h.setFormatter(logging.Formatter("[%(asctime)s] %(levelname)s %(message)s"))
    logger.addHandler(_h)
logger.setLevel(logging.INFO)
