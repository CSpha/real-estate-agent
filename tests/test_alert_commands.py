import pytest

from app.alerts import process_alert_outbox as worker_module
from app.alerts import queue_price_drop_alerts as queue_module
from app.alerts import send_potential_deal_alerts as potential_deal_module


class FakeSettings:
    slack_webhook_url = "https://example.test"
    slack_test_webhook_url = "https://example.test/test"

    def __init__(self, *, live_alerts_enabled):
        self.live_alerts_enabled = live_alerts_enabled


def test_potential_deal_command_only_enqueues(monkeypatch):
    monkeypatch.setattr(
        potential_deal_module,
        "enqueue_potential_deal_events",
        lambda engine=None: 3,
    )
    sent = []
    monkeypatch.setattr("app.alerts.slack.send_to_slack", lambda *a, **k: sent.append(a))

    potential_deal_module.main()
    assert sent == []


def test_queue_command_only_enqueues(monkeypatch):
    monkeypatch.setattr(
        queue_module,
        "enqueue_price_drop_events",
        lambda _engine=None: 2,
    )

    assert queue_module.queue_price_drop_alerts() == 2


def test_worker_only_delivers_existing_outbox_rows(monkeypatch):
    monkeypatch.setattr(
        worker_module,
        "get_settings",
        lambda: FakeSettings(live_alerts_enabled=True),
    )
    calls = []

    def fake_deliver(webhook_url, formatters, *, send, engine, limit, summarizers):
        calls.append((webhook_url, set(formatters), send, engine, limit))
        assert set(summarizers) == set(formatters)
        return {"sent": 2, "failed": 1}

    monkeypatch.setattr(worker_module, "deliver_pending_alerts", fake_deliver)

    assert worker_module.process_alert_outbox(limit=25) == {
        "sent": 2,
        "failed": 1,
    }
    assert calls[0][0] == "https://example.test"
    assert calls[0][1] == {"price_drop", "potential_deal"}
    assert calls[0][4] == 25


def test_production_send_refuses_when_live_alerts_are_off(monkeypatch):
    monkeypatch.setattr(
        worker_module, "get_settings", lambda: FakeSettings(live_alerts_enabled=False)
    )
    delivered = []
    monkeypatch.setattr(
        worker_module,
        "deliver_pending_alerts",
        lambda *a, **k: delivered.append(a) or {"sent": 0, "failed": 0},
    )

    with pytest.raises(SystemExit, match="LIVE_ALERTS_ENABLED"):
        worker_module.process_alert_outbox()
    assert delivered == []


def test_main_refuses_production_send_when_live_alerts_are_off(monkeypatch):
    monkeypatch.setattr("sys.argv", ["process_alert_outbox"])
    monkeypatch.setattr(
        worker_module, "get_settings", lambda: FakeSettings(live_alerts_enabled=False)
    )
    delivered = []
    monkeypatch.setattr(
        worker_module,
        "deliver_pending_alerts",
        lambda *a, **k: delivered.append(a) or {"sent": 0, "failed": 0},
    )

    with pytest.raises(SystemExit, match="LIVE_ALERTS_ENABLED"):
        worker_module.main()
    assert delivered == []


@pytest.mark.parametrize("flag", ["--dry-run", "--test-channel"])
def test_main_previews_still_work_when_live_alerts_are_off(monkeypatch, flag):
    monkeypatch.setattr("sys.argv", ["process_alert_outbox", flag])
    monkeypatch.setattr(
        worker_module, "get_settings", lambda: FakeSettings(live_alerts_enabled=False)
    )
    monkeypatch.setattr(
        worker_module, "preview_due_alerts", lambda *a, **k: []
    )

    worker_module.main()
