import logging

from soa_builder.web import application  # noqa: F401 - configures logging


def test_watchfiles_change_messages_suppressed():
    # Check the logger's own level: under pytest, basicConfig is a
    # no-op, so the effective level would pass vacuously.
    assert logging.getLogger("watchfiles").level == logging.WARNING


def test_reload_watches_only_source_tree(monkeypatch):
    import pathlib

    import uvicorn

    captured = {}
    monkeypatch.setattr(uvicorn, "run", lambda *a, **kw: captured.update(kw))
    application.main()

    src_dir = pathlib.Path(application.__file__).resolve().parents[2]
    assert captured["reload"] is True
    assert captured["reload_dirs"] == [str(src_dir)]
    assert src_dir.name == "src"
