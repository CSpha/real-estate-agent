from unittest.mock import MagicMock

import pytest
import requests

from app.alerts import outbox
from app.alerts import process_alert_outbox as worker_module
from app.alerts.send_potential_deal_alerts import format_slack_message
from app.alerts.slack import SlackDeliveryError, send_to_slack


def response(status_code, body="", headers=None):
    result = requests.Response()
    result.status_code = status_code
    result._content = body.encode()
    result.headers.update(headers or {})
    return result


def send_with(result):
    post = MagicMock(return_value=result)
    send_to_slack("https://example.invalid/hook", {"text": "hello"}, post=post)
    return post


def classify(result) -> SlackDeliveryError:
    with pytest.raises(SlackDeliveryError) as raised:
        send_with(result)
    return raised.value


def test_slack_success_posts_once_with_timeout():
    post = send_with(response(200, "ok"))
    post.assert_called_once()
    assert post.call_args.kwargs["timeout"] == 30


def test_rate_limit_is_retryable_and_reads_retry_after():
    error = classify(response(429, "rate_limited", {"Retry-After": "30"}))
    assert error.retryable and error.rate_limited
    assert error.retry_after_seconds == 30
    assert not error.webhook_unusable


def test_rate_limit_without_retry_after_still_stops():
    error = classify(response(429))
    assert error.rate_limited and error.retry_after_seconds is None


def test_server_errors_and_timeouts_are_retryable():
    assert classify(response(503)).retryable
    post = MagicMock(side_effect=requests.Timeout("slow"))
    with pytest.raises(SlackDeliveryError) as raised:
        send_to_slack("https://example.invalid/hook", {"text": "x"}, post=post)
    assert raised.value.retryable


def test_malformed_payload_is_permanent_but_webhook_is_still_usable():
    error = classify(response(400, "invalid_payload"))
    assert not error.retryable
    assert not error.webhook_unusable


@pytest.mark.parametrize(
    "status,body",
    [(403, "invalid_token"), (404, "channel_not_found"), (410, "channel_is_archived")],
)
def test_revoked_or_missing_webhooks_are_unusable(status, body):
    error = classify(response(status, body))
    assert not error.retryable
    assert error.webhook_unusable


@pytest.mark.parametrize("payload", [{}, {"text": "  "}, {"text": "x" * 4001}])
def test_invalid_payloads_are_rejected_before_posting(payload):
    post = MagicMock()
    with pytest.raises(SlackDeliveryError) as raised:
        send_to_slack("https://example.invalid/hook", payload, post=post)
    assert not raised.value.retryable
    post.assert_not_called()


def test_retry_delay_uses_equal_jitter_and_caps():
    assert outbox.retry_delay_seconds(1, jitter=lambda: 0) == 30
    assert outbox.retry_delay_seconds(1, jitter=lambda: 1) == 60
    assert outbox.retry_delay_seconds(3, jitter=lambda: 0.5) == 180
    assert outbox.retry_delay_seconds(20, jitter=lambda: 1) == 3600


def test_retry_delay_never_precedes_retry_after():
    assert outbox.retry_delay_seconds(1, retry_after_seconds=120, jitter=lambda: 0) == 120
    assert outbox.retry_delay_seconds(4, retry_after_seconds=5, jitter=lambda: 0) == 240


def _mark_failed_params(**kwargs):
    engine = MagicMock()
    outbox.mark_alert_failed(
        {"id": 1, "attempt_count": outbox.MAX_ATTEMPTS},
        RuntimeError("boom"),
        engine=engine,
        **kwargs,
    )
    return engine.begin.return_value.__enter__.return_value.execute.call_args.args[1]


def test_rate_limits_never_exhaust_attempts():
    assert _mark_failed_params(retryable=True)["status"] == "permanent_failed"
    params = _mark_failed_params(retryable=True, rate_limited=True)
    assert params["status"] == "retryable_failed"


@pytest.fixture
def fake_outbox(monkeypatch):
    """Replace database calls so delivery control flow can be tested directly."""
    queue = []
    state = {"claimed": [], "sent": [], "failed": []}

    def claim(_engine):
        if not queue:
            return None
        alert = queue.pop(0)
        state["claimed"].append(alert["id"])
        return alert

    monkeypatch.setattr(outbox, "claim_next_alert", claim)
    monkeypatch.setattr(
        outbox, "mark_alert_sent", lambda alert, _engine: state["sent"].append(alert["id"])
    )
    monkeypatch.setattr(
        outbox,
        "mark_alert_failed",
        lambda alert, error, **kwargs: state["failed"].append((alert["id"], kwargs)),
    )

    def add(count):
        queue.extend(
            {"id": i, "alert_type": "price_drop", "attempt_count": 1, "payload_json": {"n": i}}
            for i in range(1, count + 1)
        )

    state["add"] = add
    return state


def _deliver(send, *, sleep=None, formatter=None):
    # A frozen clock makes every send after the first wait the full interval.
    return outbox.deliver_pending_alerts(
        "https://example.invalid/hook",
        {"price_drop": formatter or (lambda payload: {"text": str(payload["n"])})},
        send=send,
        engine=MagicMock(),
        sleep=sleep or MagicMock(),
        monotonic=lambda: 0.0,
    )


def test_delivery_paces_consecutive_sends(fake_outbox):
    fake_outbox["add"](3)
    sleep = MagicMock()
    assert _deliver(MagicMock(), sleep=sleep) == {"sent": 3, "failed": 0}
    assert [call.args[0] for call in sleep.call_args_list] == [1.0, 1.0]


def test_rate_limit_stops_the_batch_and_keeps_retry_after(fake_outbox):
    fake_outbox["add"](3)
    send = MagicMock(
        side_effect=[
            None,
            SlackDeliveryError(
                "429", retryable=True, rate_limited=True, retry_after_seconds=30
            ),
        ]
    )
    assert _deliver(send) == {"sent": 1, "failed": 1}
    assert fake_outbox["claimed"] == [1, 2]
    alert_id, kwargs = fake_outbox["failed"][0]
    assert alert_id == 2
    assert kwargs["retryable"] and kwargs["rate_limited"]
    assert kwargs["retry_after_seconds"] == 30


def test_unusable_webhook_stops_without_burning_remaining_alerts(fake_outbox):
    fake_outbox["add"](3)
    send = MagicMock(
        side_effect=SlackDeliveryError("404", retryable=False, webhook_unusable=True)
    )
    assert _deliver(send) == {"sent": 0, "failed": 1}
    assert fake_outbox["claimed"] == [1]
    assert fake_outbox["failed"][0][1]["retryable"] is False


def test_formatter_errors_are_permanent_and_skip_sending(fake_outbox):
    fake_outbox["add"](1)
    send = MagicMock()

    def broken(_payload):
        raise KeyError("address")

    assert _deliver(send, formatter=broken) == {"sent": 0, "failed": 1}
    send.assert_not_called()
    assert fake_outbox["failed"][0][1]["retryable"] is False


def test_potential_deal_formatter_reads_string_payloads():
    message = format_slack_message(
        {
            "address": "1 Main St",
            "city": "Wooster",
            "state": "OH",
            "county_name": "Wayne",
            "list_price": "150000",
            "county_median_sale_price": "200000",
            "price_vs_county_median": "-50000",
            "pct_below_or_above_median": "-0.25",
            "market_score": "80",
            "score_reason": "Below county median",
            "source_listing_id": "abc",
        }
    )
    assert "List price: $150,000" in message["text"]
    assert "Percent vs median: -25.00%" in message["text"]


def _preview_rows():
    return [
        ({"id": 1, "alert_type": "price_drop", "source_listing_id": "a"}, {"text": "one"}, None),
        ({"id": 2, "alert_type": "unknown", "source_listing_id": "b"}, None, "Unsupported"),
        ({"id": 3, "alert_type": "price_drop", "source_listing_id": "c"}, {"text": "three"}, None),
    ]


def _settings(**values):
    defaults = {"slack_webhook_url": None, "slack_test_webhook_url": None}
    defaults.update(values)
    return type("Settings", (), defaults)()


def test_dry_run_sends_nothing(monkeypatch):
    monkeypatch.setattr(worker_module, "preview_due_alerts", lambda *a, **k: _preview_rows())
    monkeypatch.setattr(worker_module, "get_settings", lambda: _settings())
    send = MagicMock()
    counts = worker_module.preview_alert_outbox(send=send)
    assert counts == {"previewed": 2, "invalid": 1, "test_sent": 0}
    send.assert_not_called()


def test_test_channel_uses_only_the_test_webhook(monkeypatch):
    monkeypatch.setattr(worker_module, "preview_due_alerts", lambda *a, **k: _preview_rows())
    monkeypatch.setattr(
        worker_module,
        "get_settings",
        lambda: _settings(
            slack_webhook_url="https://example.invalid/production",
            slack_test_webhook_url="https://example.invalid/test",
        ),
    )
    send = MagicMock()
    counts = worker_module.preview_alert_outbox(
        test_channel=True, send=send, sleep=MagicMock()
    )
    assert counts["test_sent"] == 2
    assert {call.args[0] for call in send.call_args_list} == {"https://example.invalid/test"}


def test_test_channel_requires_a_test_webhook(monkeypatch):
    monkeypatch.setattr(
        worker_module,
        "get_settings",
        lambda: _settings(slack_webhook_url="https://example.invalid/production"),
    )
    with pytest.raises(ValueError, match="SLACK_TEST_WEBHOOK_URL"):
        worker_module.preview_alert_outbox(test_channel=True, send=MagicMock())
