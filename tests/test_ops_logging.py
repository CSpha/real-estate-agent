import json

from app.ops.logging import get_logger, log_context


def test_text_format_includes_context_and_restores(monkeypatch, capsys):
    monkeypatch.delenv("LOG_FORMAT", raising=False)
    log = get_logger("app.test_text")
    with log_context(run_id=7, provider="rentcast"):
        log.info("Step: syncing.")
        with log_context(alert_id=3, source_listing_id=None):
            log.info("Inner.")
    log.info("Outside.")
    lines = capsys.readouterr().out.splitlines()
    assert lines[0] == "Step: syncing.  [run_id=7 provider=rentcast]"
    assert lines[1] == "Inner.  [run_id=7 provider=rentcast alert_id=3]"
    assert lines[2] == "Outside."


def test_warning_prefix_in_text_format(monkeypatch, capsys):
    monkeypatch.setenv("LOG_FORMAT", "text")
    get_logger("app.test_warn").warning("careful")
    assert capsys.readouterr().out.strip() == "WARNING: careful"


def test_json_format_one_object_per_line(monkeypatch, capsys):
    monkeypatch.setenv("LOG_FORMAT", "json")
    log = get_logger("app.test_json")
    with log_context(run_id=9, provider="rentcast"):
        log.info("hello %s", "world", extra={"request_count": 3})
    monkeypatch.setenv("LOG_FORMAT", "text")
    get_logger("app.test_json")
    out = capsys.readouterr().out.strip().splitlines()
    assert len(out) == 1
    payload = json.loads(out[0])
    assert payload["message"] == "hello world"
    assert payload["run_id"] == 9
    assert payload["provider"] == "rentcast"
    assert payload["request_count"] == 3
    assert payload["level"] == "INFO"


def test_urls_and_secret_fields_are_not_logged(monkeypatch, capsys):
    secret = "https://hooks.slack.com/services/T000/B000/SECRETVALUE"
    for fmt in ("text", "json"):
        monkeypatch.setenv("LOG_FORMAT", fmt)
        log = get_logger("app.test_secret")
        with log_context(webhook_url=secret):
            log.info("posting to %s", secret, extra={"api_key": "abc123"})
        try:
            raise RuntimeError(f"failed calling {secret}")
        except RuntimeError:
            log.exception("boom")
    monkeypatch.setenv("LOG_FORMAT", "text")
    get_logger("app.test_secret")
    out = capsys.readouterr().out
    assert "SECRETVALUE" not in out
    assert "abc123" not in out
    assert "<redacted>" in out
