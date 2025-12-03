from typing import Any


def get_next_code_uid(cur: Any, soa_id: int) -> str:
    """Compute next unique Code_N for the given SOA.

    Assumes `cur` is a sqlite cursor within an open transaction.
    """
    cur.execute(
        "SELECT code_uid FROM code WHERE soa_id=? AND code_uid LIKE 'Code_%'",
        (soa_id,),
    )
    existing = [x[0] for x in cur.fetchall() if x[0]]
    n = 1
    if existing:
        try:
            n = max(int(x.split("_")[1]) for x in existing) + 1
        except Exception:
            n = len(existing) + 1
    return f"Code_{n}"
