from __future__ import annotations

import pytest

import app.alerts.live_alert_worker as worker


def _patch_queueing(monkeypatch, run):
    calls = []
    monkeypatch.setattr(worker, "latest_shadow_run", lambda _engine: run)
    monkeypatch.setattr(
        worker, "enqueue_price_drop_events", lambda _engine: calls.append("drops") or 2
    )
    monkeypatch.setattr(
        worker,
        "enqueue_potential_deal_events",
        lambda _engine: calls.append("deals") or 3,
    )
    return calls


def _deliver(**_kwargs):
    return {"sent": 5, "failed": 0}


def test_cycle_queues_after_successful_run_then_delivers(monkeypatch):
    calls = _patch_queueing(monkeypatch, {"id": 41, "status": "succeeded"})

    result = worker.run_live_alert_cycle(engine=object(), deliver=_deliver)

    assert calls == ["drops", "deals"]
    assert result == {
        "run_id": 41,
        "queued_price_drops": 2,
        "queued_potential_deals": 3,
        "sent": 5,
        "failed": 0,
    }


@pytest.mark.parametrize("run", [None, {"id": 42, "status": "running"}, {"id": 43, "status": "failed"}])
def test_cycle_does_not_queue_without_a_finished_successful_run(monkeypatch, run):
    calls = _patch_queueing(monkeypatch, run)
    delivered = []

    result = worker.run_live_alert_cycle(
        engine=object(), deliver=lambda **kwargs: delivered.append(kwargs) or {"sent": 0}
    )

    assert calls == []
    assert len(delivered) == 1
    assert result["queued_price_drops"] == result["queued_potential_deals"] == 0


def test_worker_refuses_to_start_unless_live_alerts_enabled(monkeypatch):
    class Off:
        live_alerts_enabled = False
        slack_webhook_url = "https://example.invalid/hook"

    monkeypatch.setattr(worker, "get_settings", lambda: Off())
    with pytest.raises(SystemExit, match="LIVE_ALERTS_ENABLED"):
        worker.require_live_alerts()

    class NoWebhook:
        live_alerts_enabled = True
        slack_webhook_url = None

    monkeypatch.setattr(worker, "get_settings", lambda: NoWebhook())
    with pytest.raises(ValueError, match="SLACK_WEBHOOK_URL"):
        worker.require_live_alerts()


class FakeNotifier:
    def __init__(self):
        self.events = []

    def problem(self, kind, detail):
        self.events.append(("problem", kind))

    def resolved(self, detail):
        self.events.append(("resolved",))


def test_run_forever_warns_ops_on_failures_and_keeps_going():
    results = iter(
        [
            RuntimeError("database unavailable"),
            {"failed": 1},
            {"failed": 0},
        ]
    )

    def cycle():
        result = next(results)
        if isinstance(result, Exception):
            raise result
        return result

    sleeps = []
    notifier = FakeNotifier()
    worker.run_forever(
        interval_seconds=900,
        cycle=cycle,
        notifier=notifier,
        sleep=sleeps.append,
        max_cycles=3,
    )

    assert notifier.events == [
        ("problem", "cycle failed"),
        ("problem", "delivery failed"),
        ("resolved",),
    ]
    assert sleeps == [900, 900]


def test_interval_minutes_validates(monkeypatch):
    monkeypatch.setenv("ALERT_WORKER_INTERVAL_MINUTES", "0")
    with pytest.raises(ValueError):
        worker.interval_minutes()
    monkeypatch.setenv("ALERT_WORKER_INTERVAL_MINUTES", "30")
    assert worker.interval_minutes() == 30
