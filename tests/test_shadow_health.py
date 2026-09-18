from datetime import datetime, timedelta, timezone
from unittest.mock import MagicMock

from app.shadow.health import scheduler_is_healthy


def _engine(row):
    engine = MagicMock()
    engine.connect.return_value.__enter__.return_value.execute.return_value.mappings.return_value.first.return_value = row
    return engine


def test_scheduler_health_allows_a_current_run(monkeypatch):
    monkeypatch.setenv("SHADOW_INTERVAL_HOURS", "72")
    now = datetime(2026, 9, 18, tzinfo=timezone.utc)
    assert scheduler_is_healthy(
        engine=_engine({"status": "succeeded", "started_at": now, "finished_at": now}),
        now=now + timedelta(hours=72, minutes=15),
    )


def test_scheduler_health_rejects_a_missed_deadline(monkeypatch):
    monkeypatch.setenv("SHADOW_OVERDUE_GRACE_MINUTES", "15")
    now = datetime(2026, 9, 18, tzinfo=timezone.utc)
    assert not scheduler_is_healthy(
        engine=_engine({"status": "succeeded", "started_at": now, "finished_at": now}),
        now=now + timedelta(hours=72, minutes=16),
    )
