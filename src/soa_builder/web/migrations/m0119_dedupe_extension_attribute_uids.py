"""Give every duplicated ExtensionAttribute_N uid a single holder.

Before the fix the DSS and CRF backfills numbered their rows from the
DSS/CRF uids alone, and the tool and BC-grouping extensions took one
number from the counter but used three or four. Both handed out uids
already held elsewhere, which put duplicate ids in the USDM output.
See utils.dedupe_extension_attribute_uids for which holder keeps a uid.
"""

from ..db import _connect
from ..utils import dedupe_extension_attribute_uids


def m0119_dedupe_extension_attribute_uids() -> None:
    conn = _connect()
    try:
        dedupe_extension_attribute_uids(conn.cursor())
        conn.commit()
    finally:
        conn.close()
