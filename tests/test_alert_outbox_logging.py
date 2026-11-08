import json
from unittest.mock import MagicMock

import pytest

import app.alerts.live_alert_worker as worker
from app.alerts import outbox
from app.alerts.process_alert_outbox import SUMMARIZERS
from app.alerts.send_price_drop_alerts import format_slack_message as format_drop
from app.alerts.slack import SlackDeliveryError
from app.ops.logging import get_logger

SECRET_URL = "https://hooks.slack.com/services/T000/B000/SECRETVALUE"
DROP = {
    "address": "3763 Bates Rd",
    "city": "Wooster",
    "state": "OH",
    "previous_price": "324900.00",
    "current_price": "299900.00",
    "price_change": "-25000.00",
    "price_change_pct": "-7.69",
    "source_listing_id": "abc",
}


@pytest.fixture
def json_logs(monkeypatch, capsys):
    """Switch logging to JSON and return a reader for the lines emitted so far."""
    monkeypatch.setenv("LOG_FORMAT", "json")
    get_logger("app.test")

    def read():
        return [
            json.loads(line)
            for line in capsys.readouterr().out.splitlines()
            if line.strip()
        ]

    yield read
    monkeypatch.setenv("LOG_FORMAT", "text")
    get_logger("app.test")


def _alerts(count):
    return [
        {"id": i, "alert_type": "price_drop", "attempt_count": 1, "payload_json": DROP}
        for i in range(1, count + 1)
    ]


def _patch_outbox(monkeypatch, alerts, *, digests=False):
    queue = list(alerts)
    if digests:
        monkeypatch.setattr(outbox, "count_due_alerts", lambda _engine: len(queue))

        def claim(_engine, size):
            batch = queue[:size]
            del queue[:size]
            return batch

        monkeypatch.setattr(outbox, "claim_alerts", claim)
    else:
        monkeypatch.setattr(
            outbox, "claim_next_alert", lambda _engine: queue.pop(0) if queue else None
        )
    monkeypatch.setattr(outbox, "mark_alert_sent", lambda alert, _engine: None)


def _deliver(send, *, summarizers=None):
    return outbox.deliver_pending_alerts(
        SECRET_URL,
        {"price_drop": format_drop},
        send=send,
        engine=MagicMock(),
        sleep=MagicMock(),
        monotonic=lambda: 0.0,
        summarizers=summarizers,
    )


def test_worker_cycle_logs_json_with_component_and_run_id(monkeypatch, json_logs):
    monkeypatch.setattr(
        worker, "latest_shadow_run", lambda _engine: {"id": 41, "status": "succeeded"}
    )
    monkeypatch.setattr(worker, "enqueue_price_drop_events", lambda _engine: 2)
    monkeypatch.setattr(worker, "enqueue_potential_deal_events", lambda _engine: 3)

    worker.run_live_alert_cycle(
        engine=object(), deliver=lambda **_kwargs: {"sent": 5, "failed": 0}
    )

    step1, step2 = json_logs()
    assert step1["message"].startswith("Step 1/2: After shadow run 41, queued 2 price")
    assert step1["component"] == "alert_worker"
    assert step1["run_id"] == 41
    assert step1["queued_price_drops"] == 2
    assert step2["message"].startswith("Step 2/2:")
    assert step2["component"] == "alert_worker"
    assert "run_id" not in step2


def test_run_forever_logs_cycle_number_and_failures(json_logs):
    results = iter([RuntimeError("database unavailable"), {"sent": 2, "failed": 0}])

    def cycle():
        result = next(results)
        if isinstance(result, Exception):
            raise result
        return result

    worker.run_forever(
        interval_seconds=1, cycle=cycle, sleep=lambda _s: None, max_cycles=2
    )

    lines = json_logs()
    failed = next(line for line in lines if "cycle failed" in line["message"])
    assert failed["level"] == "ERROR"
    assert failed["component"] == "alert_worker"
    assert failed["cycle"] == 1
    assert "database unavailable" in failed["message"]
    complete = next(line for line in lines if "cycle complete" in line["message"])
    assert (complete["cycle"], complete["sent"], complete["failed"]) == (2, 2, 0)


def test_worker_text_output_keeps_step_labels(monkeypatch, capsys):
    monkeypatch.setenv("LOG_FORMAT", "text")
    get_logger("app.test")
    monkeypatch.setattr(worker, "latest_shadow_run", lambda _engine: None)

    worker.run_live_alert_cycle(engine=object(), deliver=lambda **_k: {})

    lines = capsys.readouterr().out.splitlines()
    assert lines == [
        "Step 1/2: No shadow run yet; nothing to queue.  [component=alert_worker]",
        "Step 2/2: Delivering due alerts to SLACK_WEBHOOK_URL.  [component=alert_worker]",
    ]


def test_individual_delivery_logs_outcomes_without_secrets(monkeypatch, json_logs):
    _patch_outbox(monkeypatch, _alerts(2))
    send = MagicMock(
        side_effect=[
            None,
            SlackDeliveryError(
                f"Slack request failed: {SECRET_URL} SECRETVALUE", retryable=True
            ),
        ]
    )

    assert _deliver(send) == {"sent": 1, "failed": 1}

    lines = json_logs()
    sent, failed = lines
    assert (sent["alert_id"], sent["event_type"], sent["outcome"]) == (
        1,
        "price_drop",
        "sent",
    )
    assert failed["level"] == "WARNING"
    assert failed["alert_id"] == 2
    assert failed["event_type"] == "price_drop"
    assert failed["outcome"] == "retryable_failed"
    assert failed["error_type"] == "SlackDeliveryError"
    assert "SECRETVALUE" not in json.dumps(lines)


def test_webhook_and_rate_limit_stops_never_log_the_error_text(monkeypatch, json_logs):
    for error in (
        SlackDeliveryError(
            f"Slack returned HTTP 404: {SECRET_URL}",
            retryable=False,
            webhook_unusable=True,
        ),
        SlackDeliveryError(
            f"Slack returned HTTP 429: {SECRET_URL}",
            retryable=True,
            rate_limited=True,
        ),
    ):
        _patch_outbox(monkeypatch, _alerts(1))
        _deliver(MagicMock(side_effect=error))
    raw = json.dumps(json_logs())
    assert "Stopping delivery: Slack webhook is unusable." in raw
    assert "Stopping delivery: Slack rate limited this webhook." in raw
    assert "SECRETVALUE" not in raw
    assert "hooks.slack.com" not in raw


def test_digest_delivery_logs_one_outcome_per_digest(monkeypatch, json_logs):
    _patch_outbox(monkeypatch, _alerts(11), digests=True)

    assert _deliver(MagicMock(), summarizers=SUMMARIZERS) == {"sent": 11, "failed": 0}

    lines = json_logs()
    assert lines[0]["message"] == "Several alerts are due; sending them as digests."
    assert lines[0]["due"] == 11
    digests = lines[1:]
    assert [line["message"] for line in digests] == [
        "Digest of 6 alert(s) processed.",
        "Digest of 5 alert(s) processed.",
    ]
    assert digests[0]["outcome"] == "sent"
    assert digests[0]["alert_ids"] == [1, 2, 3, 4, 5, 6]
    assert digests[1]["alert_ids"] == [7, 8, 9, 10, 11]


def test_failed_digest_logs_each_alert_and_the_digest(monkeypatch, json_logs):
    _patch_outbox(monkeypatch, _alerts(4), digests=True)
    send = MagicMock(side_effect=SlackDeliveryError("503", retryable=True))

    assert _deliver(send, summarizers=SUMMARIZERS) == {"sent": 0, "failed": 4}

    lines = json_logs()
    alert_lines = [line for line in lines if line["message"] == "Alert delivery failed."]
    assert [line["alert_id"] for line in alert_lines] == [1, 2, 3, 4]
    assert {line["outcome"] for line in alert_lines} == {"retryable_failed"}
    assert lines[-1]["outcome"] == "failed"
