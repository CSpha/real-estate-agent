from app.alerts import process_alert_outbox as worker_module
from app.alerts import queue_price_drop_alerts as queue_module


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
        lambda: type("Settings", (), {"slack_webhook_url": "https://example.test"})(),
    )
    calls = []

    def fake_deliver(webhook_url, formatters, *, send, engine, limit):
        calls.append((webhook_url, set(formatters), send, engine, limit))
        return {"sent": 2, "failed": 1}

    monkeypatch.setattr(worker_module, "deliver_pending_alerts", fake_deliver)

    assert worker_module.process_alert_outbox(limit=25) == {
        "sent": 2,
        "failed": 1,
    }
    assert calls[0][0] == "https://example.test"
    assert calls[0][1] == {"price_drop"}
    assert calls[0][4] == 25
