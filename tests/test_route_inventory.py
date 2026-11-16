"""Snapshot of every (method, path) the app serves.

Guards the app.py split and router package moves (plan Phases 3 and
5) and the URLs the MCP server calls: a refactor must not add, drop
or rename an endpoint by accident.
"""

import re

from fastapi.routing import APIRoute

from soa_builder.web.app import app

from _snapshot import assert_matches_snapshot


def _walk(routes, prefix: str = ""):
    """Yield (method, full path) for every API route, including those
    in routers that FastAPI keeps as nested included-router entries."""
    for route in routes:
        if isinstance(route, APIRoute):
            for method in sorted(route.methods):
                yield method, prefix + route.path
        elif hasattr(route, "original_router"):
            nested_prefix = prefix + (route.include_context.prefix or "")
            yield from _walk(route.original_router.routes, nested_prefix)


def _routes() -> list:
    return sorted({f"{method} {path}" for method, path in _walk(app.routes)})


def test_route_inventory_matches_snapshot():
    assert_matches_snapshot("routes.json", _routes())


def test_inventory_covers_openapi():
    """Sanity check that the walk sees the routers, not just app.py."""
    documented = {
        f"{method.upper()} {path}"
        for path, ops in app.openapi()["paths"].items()
        for method in ops
    }
    # OpenAPI drops path converters such as {concept_code:path}
    walked = {re.sub(r"\{(\w+):\w+\}", r"{\1}", r) for r in _routes()}
    assert documented <= walked


def test_no_duplicate_method_path_registrations():
    seen = set()
    dupes = []
    for key in _walk(app.routes):
        if key in seen:
            dupes.append(key)
        seen.add(key)
    assert not dupes, f"Routes registered twice: {sorted(dupes)}"
