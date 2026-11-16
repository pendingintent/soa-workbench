"""CachedResource freshness / store / fail semantics."""

from soa_builder.web.caching import CachedResource


def test_new_cache_is_stale_and_dict_compatible():
    cache = CachedResource(60, rows=None)
    assert not cache.is_fresh("rows")
    assert cache == {"fetched_at": 0, "last_error": None, "rows": None}
    cache.update(rows=[1], fetched_at=1000)
    assert cache["rows"] == [1]


def test_store_then_expire():
    cache = CachedResource(60, rows=None)
    cache.store(1000, rows=[1])
    assert cache.is_fresh("rows", now=1059)
    assert not cache.is_fresh("rows", now=1060)
    assert cache["last_error"] is None


def test_empty_values_are_stale_unless_allowed():
    cache = CachedResource(60, rows=None)
    cache.fail("boom", 1000, rows=[])
    assert cache["last_error"] == "boom"
    assert not cache.is_fresh("rows", now=1001)
    assert cache.is_fresh("rows", now=1001, allow_empty=True)


def test_ttl_override_and_invalidate():
    cache = CachedResource(60, rows=None)
    cache.store(1000, rows=[1])
    assert not cache.is_fresh("rows", now=1010, ttl=5)
    cache.invalidate()
    assert not cache.is_fresh("rows", now=1001)
