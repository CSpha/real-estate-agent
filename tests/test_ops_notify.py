from datetime import datetime, timezone

import requests

from app.ops import notify
from app.ops.notify import OpsNotifier, post_ops_message


NOW = datetime(2026, 10, 31, 18, 30, tzinfo=timezone.utc)
WEBHOOK = "https://hooks.slack.com/services/T000/B000/secret-token"


def _notifier(tmp_path, outcomes=None):
    sent = []
    outcomes = list(outcomes or [])

    def send(message):
        sent.append(message)
        return outcomes.pop(0) if outcomes else True

    return OpsNotifier(state_path=tmp_path / "ops.json", send=send, now=lambda: NOW), sent


def test_problem_is_reported_once_until_resolved(tmp_path):
    notifier, sent = _notifier(tmp_path)

    assert notifier.problem("run failed", "RentCastAPIError: HTTP 503")
    assert not notifier.problem("run failed", "RentCastAPIError: HTTP 503")
    assert not notifier.problem("overdue", "No run finished")

    assert len(sent) == 1
    assert "run failed" in sent[0]
    assert "2026-10-31T18:30:00+00:00" in sent[0]
    assert "HTTP 503" in sent[0]


def test_recovery_is_reported_once_and_resets(tmp_path):
    notifier, sent = _notifier(tmp_path)
    notifier.problem("overdue", "No run finished")

    assert notifier.resolved("Run 7 succeeded.")
    assert not notifier.resolved("Run 8 succeeded.")
    assert notifier.problem("run failed", "boom")

    assert len(sent) == 3
    assert "recovered from overdue" in sent[1]
    assert "Run 7" in sent[1]


def test_state_survives_a_restart(tmp_path):
    first, sent = _notifier(tmp_path)
    first.problem("overdue", "No run finished")

    restarted = OpsNotifier(state_path=tmp_path / "ops.json", send=sent.append)

    assert not restarted.problem("overdue", "No run finished")
    assert len(sent) == 1


def test_failed_post_is_retried_on_the_next_report(tmp_path):
    notifier, sent = _notifier(tmp_path, outcomes=[False, True])

    assert not notifier.problem("run failed", "first")
    assert notifier.problem("run failed", "second")
    assert not notifier.problem("run failed", "third")

    assert len(sent) == 2


def test_recovery_is_silent_when_the_problem_was_never_delivered(tmp_path):
    notifier, sent = _notifier(tmp_path, outcomes=[False])
    notifier.problem("run failed", "first")

    notifier.resolved("Run 9 succeeded.")
    notifier.resolved("Run 10 succeeded.")

    assert len(sent) == 1


def test_long_details_are_shortened(tmp_path):
    notifier, sent = _notifier(tmp_path)

    notifier.problem("run failed", "x" * 1000)

    assert len(sent[0]) < 400
    assert sent[0].endswith("...")


def test_post_skips_when_no_webhook_is_configured(monkeypatch):
    monkeypatch.setattr(notify, "ops_webhook_url", lambda: None)
    calls = []

    assert not post_ops_message("hello", post=lambda *args, **kwargs: calls.append(1))
    assert calls == []


class _Response:
    def __init__(self, status_code):
        self.status_code = status_code

    def raise_for_status(self):
        if self.status_code >= 400:
            raise requests.HTTPError(response=self)


def test_post_sends_text_with_a_timeout():
    calls = []

    def fake_post(url, **kwargs):
        calls.append((url, kwargs))
        return _Response(200)

    assert post_ops_message("hello", webhook_url=WEBHOOK, post=fake_post)
    assert calls == [(WEBHOOK, {"json": {"text": "hello"}, "timeout": 10})]


def test_post_failure_never_raises_or_prints_the_webhook(capsys):
    def failing_post(url, **kwargs):
        return _Response(404)

    def timeout_post(url, **kwargs):
        raise requests.Timeout(f"timed out calling {url}")

    assert not post_ops_message("hello", webhook_url=WEBHOOK, post=failing_post)
    assert not post_ops_message("hello", webhook_url=WEBHOOK, post=timeout_post)

    output = capsys.readouterr().out
    assert "HTTP 404" in output
    assert "secret-token" not in output
