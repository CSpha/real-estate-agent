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


def test_disabled_worker_idles_instead_of_exiting(monkeypatch, caplog):
    class Off:
        live_alerts_enabled = False
        slack_webhook_url = None

    monkeypatch.setattr(worker, "get_settings", lambda: Off())
    monkeypatch.setattr("sys.argv", ["live_alert_worker"])
    idled = []
    monkeypatch.setattr(
        worker,
        "idle_while_disabled",
        lambda: idled.append(True),
    )
    cycles = []
    monkeypatch.setattr(worker, "run_live_alert_cycle", lambda: cycles.append(1))

    worker.main()

    assert idled == [True]
    assert cycles == []


def test_disabled_worker_once_still_exits_with_error(monkeypatch):
    class Off:
        live_alerts_enabled = False
        slack_webhook_url = None

    monkeypatch.setattr(worker, "get_settings", lambda: Off())
    monkeypatch.setattr("sys.argv", ["live_alert_worker", "--once"])

    with pytest.raises(SystemExit, match="LIVE_ALERTS_ENABLED"):
        worker.main()


def test_enabled_worker_without_webhook_is_a_hard_exit(monkeypatch):
    class NoWebhook:
        live_alerts_enabled = True
        slack_webhook_url = None

    monkeypatch.setattr(worker, "get_settings", lambda: NoWebhook())
    monkeypatch.setattr("sys.argv", ["live_alert_worker"])

    with pytest.raises(ValueError, match="SLACK_WEBHOOK_URL"):
        worker.main()


def test_idle_while_disabled_logs_once_and_sleeps_hourly(monkeypatch, capsys):
    # App loggers do not propagate to the root logger, so caplog never sees
    # them; read the stdout handler's output instead.
    monkeypatch.setenv("LOG_FORMAT", "text")
    sleeps = []

    worker.idle_while_disabled(sleep=sleeps.append, max_idles=3)

    assert sleeps == [worker.IDLE_SLEEP_SECONDS] * 3
    lines = capsys.readouterr().out.splitlines()
    messages = [line for line in lines if "Live alerts are off" in line]
    assert len(messages) == 1
    assert "recreate the container" in messages[0]
