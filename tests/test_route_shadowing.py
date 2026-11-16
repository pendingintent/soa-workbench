"""No route may be unreachable because an earlier route captures it.

Starlette matches in registration order, so ``GET /x/{id}`` registered
before ``GET /x/summary`` turns every request for the latter into a
lookup of id "summary". That silently broke /freeze/diff.json.
"""

from fastapi.routing import APIRoute

from soa_builder.web.app import app


def _walk(routes, prefix: str = ""):
    for route in routes:
        if isinstance(route, APIRoute):
            yield route, prefix + route.path
        elif hasattr(route, "original_router"):
            nested = prefix + (route.include_context.prefix or "")
            yield from _walk(route.original_router.routes, nested)


def _is_param(segment: str) -> bool:
    return segment.startswith("{") and segment.endswith("}")


def _shadows(earlier: str, later: str) -> bool:
    """True when every request for ``later`` would match ``earlier``.

    Compared segment by segment: ``earlier`` captures ``later`` when it
    has a parameter wherever ``later`` has a literal, and the same
    literal (or a parameter) everywhere else.
    """
    if earlier.endswith(":path}") and later.startswith(earlier.rsplit("/", 1)[0]):
        return earlier != later
    e_parts, l_parts = earlier.split("/"), later.split("/")
    if len(e_parts) != len(l_parts):
        return False
    captured = False
    for e_seg, l_seg in zip(e_parts, l_parts):
        if _is_param(e_seg):
            captured |= not _is_param(l_seg)
        elif e_seg != l_seg:
            return False
    return captured


def test_no_route_is_shadowed_by_an_earlier_one():
    ordered = list(_walk(app.routes))
    shadowed = []
    for i, (route, path) in enumerate(ordered):
        for earlier, earlier_path in ordered[:i]:
            if earlier.methods & route.methods and _shadows(earlier_path, path):
                shadowed.append(f"{sorted(route.methods)} {path} <- {earlier_path}")
                break
    assert not shadowed, "Unreachable routes:\n" + "\n".join(shadowed)
