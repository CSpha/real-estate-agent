from types import SimpleNamespace

import pytest

from app.alerts import send_test_alerts as module
from app.alerts.send_test_alerts import TEST_BANNER, send_test_alerts, resolve_test_webhook_url


TEST_URL = "https://hooks.slack.com/services/T000/B000/test-token"
PROD_URL = "https://hooks.slack.com/services/T000/B111/prod-token"


def _settings(monkeypatch, *, test=TEST_URL, prod=PROD_URL):
    monkeypatch.setattr(
        module,
        "get_settings",
        lambda: SimpleNamespace(slack_test_webhook_url=test, slack_webhook_url=prod),
    )


def _messages(monkeypatch, count=3):
    messages = [
        ("price_drop", f"id-{index}", {"text": TEST_BANNER + f"event {index}"})
        for index in range(count)
    ]
    monkeypatch.setattr(module, "build_test_messages", lambda **kwargs: messages)


def test_sends_only_to_the_test_webhook_with_pacing(monkeypatch):
    _settings(monkeypatch)
    _messages(monkeypatch)
    sent, sleeps = [], []

    count = send_test_alerts(
        send=lambda url, message: sent.append((url, message)),
        sleep=sleeps.append,
    )

    assert count == 3
    assert {url for url, _message in sent} == {TEST_URL}
    assert all(message["text"].startswith(TEST_BANNER) for _url, message in sent)
    assert sleeps == [1.0, 1.0]


def test_dry_run_sends_nothing_and_needs_no_webhook(monkeypatch):
    _settings(monkeypatch, test=None)
    _messages(monkeypatch)
    sent = []

    assert send_test_alerts(dry_run=True, send=lambda *args: sent.append(args)) == 0
    assert sent == []


def test_refuses_when_test_webhook_is_missing(monkeypatch):
    _settings(monkeypatch, test=None)

    with pytest.raises(ValueError, match="SLACK_TEST_WEBHOOK_URL"):
        resolve_test_webhook_url()


def test_refuses_when_test_webhook_is_the_production_webhook(monkeypatch):
    _settings(monkeypatch, test=PROD_URL + " ")

    with pytest.raises(ValueError, match="same as SLACK_WEBHOOK_URL"):
        resolve_test_webhook_url()


def test_limit_is_capped(monkeypatch):
    _settings(monkeypatch)

    with pytest.raises(ValueError, match="between 1 and 10"):
        send_test_alerts(limit=11, dry_run=True)
