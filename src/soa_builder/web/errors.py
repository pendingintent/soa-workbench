"""Named exception groups for the handlers that recover from bad input.

Using a named group instead of ``except Exception`` keeps genuine bugs
(NameError, sqlite3 errors in unrelated code, ...) loud while
documenting what a handler expects to go wrong.
"""

import requests

# json.loads on stored audit/snapshot JSON, then .get()/int() on the
# result: bad JSON (ValueError), None (TypeError), non-dict (AttributeError).
MALFORMED_JSON_ERRORS = (AttributeError, TypeError, ValueError)

# A CDISC Library (or other remote) call plus navigation of its payload:
# network failures and malformed or unexpected response shapes.
REMOTE_PAYLOAD_ERRORS = (
    requests.RequestException,
    ValueError,
    TypeError,
    AttributeError,
    KeyError,
    IndexError,
)
