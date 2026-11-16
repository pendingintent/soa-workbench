"""Encounter environmental-setting and contact-mode option loaders."""

import pytest

from soa_builder.web import utils


class _Resp:
    def __init__(self, payload, status=200):
        self._payload = payload
        self.status_code = status

    def json(self):
        return self._payload


LOADERS = [
    (utils.load_environmental_setting_options, utils._env_setting_cache),
    (utils.load_contact_mode_options, utils._contact_mode_cache),
]
IDS = ["environmental_setting", "contact_mode"]


@pytest.fixture(params=LOADERS, ids=IDS)
def loader(request, monkeypatch):
    fn, cache = request.param
    cache.update(options=None, fetched_at=0, last_error=None)
    monkeypatch.setattr(utils, "get_latest_sdtm_ct_href", lambda: "sdtmct-2026-01-01")
    calls = []

    def install(responses):
        def fake_get(url, **_kwargs):
            calls.append(url)
            return responses(url)

        monkeypatch.setattr(utils.requests, "get", fake_get)

    yield fn, cache, install, calls
    cache.update(options=None, fetched_at=0, last_error=None)


TERMS = {
    "terms": [
        {"conceptId": "C2", "submissionValue": "ZETA"},
        {"conceptId": "C1", "submissionValue": "ALPHA"},
    ]
}


def test_options_are_sorted_with_package(loader):
    fn, _, install, _ = loader
    install(lambda url: _Resp(TERMS))
    assert fn() == [
        {"conceptId": "C1", "submissionValue": "ALPHA", "package": "sdtmct-2026-01-01"},
        {"conceptId": "C2", "submissionValue": "ZETA", "package": "sdtmct-2026-01-01"},
    ]


def test_second_call_is_served_from_cache(loader):
    fn, _, install, calls = loader
    install(lambda url: _Resp(TERMS))
    first = fn()
    second = fn()
    assert first == second
    assert len(calls) == 1


def test_force_refetches(loader):
    fn, _, install, calls = loader
    install(lambda url: _Resp(TERMS))
    fn()
    fn(force=True)
    assert len(calls) == 2


def test_falls_back_to_term_hrefs(loader):
    fn, _, install, _ = loader

    def responses(url):
        if url.endswith("/terms/C9"):
            return _Resp({"conceptId": "C9", "submissionValue": "VIA HREF"})
        return _Resp({"terms": [{"_links": {"self": {"href": "/mdr/x/terms/C9"}}}]})

    install(responses)
    assert [o["submissionValue"] for o in fn()] == ["VIA HREF"]


def test_http_error_returns_empty_and_records_error(loader):
    fn, cache, install, _ = loader
    install(lambda url: _Resp({}, status=503))
    assert fn() == []
    assert cache["last_error"] == "HTTP 503"


def test_missing_package_clears_stale_options(loader, monkeypatch):
    fn, cache, install, _ = loader
    install(lambda url: _Resp(TERMS))
    fn()
    monkeypatch.setattr(utils, "get_latest_sdtm_ct_href", lambda: None)
    assert fn(force=True) == []
    assert cache["options"] == []
    assert cache["last_error"] == "missing slug"
