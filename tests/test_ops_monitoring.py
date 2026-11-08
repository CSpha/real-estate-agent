from datetime import datetime, timedelta, timezone
from unittest.mock import MagicMock

import pytest

import app.ops.monitoring as monitoring
import app.shadow.scheduler as scheduler
from app.ops.monitoring import (
    MonitorNotifiers,
    budget_detail,
    check_database_capacity,
    check_provider_health,
    database_size_detail,
    repeated_failure_detail,
    unexpected_skip_detail,
)
from app.ops.notify import OpsNotifier

NOW = datetime(2026, 11, 6, 12, 0, tzinfo=timezone.utc)
MB = 1024 * 1024


def run(id_, status, requested=True, error=None):
    return dict(id=id_, status=status, requested=requested, synced=status == "succeeded",
                error_message=error, started_at=NOW)


def test_two_consecutive_failures_warn():
    runs = [run(3, "failed", error="RentCastAPIError: 503"), run(2, "failed"), run(1, "succeeded")]
    assert "last 2 RentCast syncs failed" in repeated_failure_detail(runs, 2)
    assert "503" in repeated_failure_detail(runs, 2)


def test_success_between_failures_or_too_few_attempts_does_not_warn():
    assert repeated_failure_detail([run(3, "failed"), run(2, "succeeded")], 2) is None
    assert repeated_failure_detail([run(1, "failed")], 2) is None


def test_runs_that_skipped_rentcast_do_not_break_a_failure_streak():
    runs = [run(3, "failed"), run(2, "succeeded", requested=False), run(1, "failed")]
    assert repeated_failure_detail(runs, 2) is not None


def skip_kwargs(**over):
    base = dict(calls_enabled=True, budget_blocked=False, last_attempt_at=None, min_hours=60, now=NOW)
    return {**base, **over}


def test_unexpected_skips_warn_only_without_an_explanation():
    skipped = [run(2, "succeeded", requested=False), run(1, "succeeded", requested=False)]
    assert unexpected_skip_detail(skipped, 2, **skip_kwargs()) is not None
    assert unexpected_skip_detail(skipped, 2, **skip_kwargs(calls_enabled=False)) is None
    assert unexpected_skip_detail(skipped, 2, **skip_kwargs(budget_blocked=True)) is None
    recent = skip_kwargs(last_attempt_at=NOW - timedelta(hours=10))
    assert unexpected_skip_detail(skipped, 2, **recent) is None
    assert unexpected_skip_detail(skipped[:1], 2, **skip_kwargs()) is None
    assert unexpected_skip_detail([run(2, "failed"), *skipped], 2, **skip_kwargs()) is None


@pytest.mark.parametrize("used,warn", [(31, False), (32, True), (40, True)])
def test_budget_warns_at_eighty_percent(used, warn):
    assert (budget_detail(used, 40, 80) is not None) is warn


def test_zero_limit_never_warns_about_budget():
    assert budget_detail(0, 0, 80) is None


def test_database_size_threshold():
    assert database_size_detail(99 * MB, 100, []) is None
    detail = database_size_detail(150 * MB, 100, [("listing_history", 90 * MB)])
    assert "150 MB" in detail and "listing_history 90 MB" in detail
    assert database_size_detail(10**12, 0, []) is None


def make_notifiers(tmp_path):
    sent = []
    notifiers = MonitorNotifiers(
        tmp_path,
        factory=lambda state_path: OpsNotifier(
            state_path=state_path, send=lambda m: sent.append(m) or True, now=lambda: NOW
        ),
    )
    return notifiers, sent


HEALTHY = dict(requests_used=0, request_limit=40, last_attempt_at=NOW, calls_enabled=True,
               budget_blocked=False, min_hours=60, now=NOW)


def test_provider_check_warns_once_then_resolves(tmp_path, monkeypatch):
    notifiers, sent = make_notifiers(tmp_path)
    history = [run(2, "failed", error="boom"), run(1, "failed")]
    monkeypatch.setattr(monitoring, "fetch_recent_rentcast_runs", lambda e, n: history)

    check_provider_health(None, notifiers, **HEALTHY)
    check_provider_health(None, notifiers, **HEALTHY)
    assert len(sent) == 1 and "repeated RentCast failures" in sent[0]

    history.insert(0, run(3, "succeeded"))
    check_provider_health(None, notifiers, **HEALTHY)
    check_provider_health(None, notifiers, **HEALTHY)
    assert len(sent) == 2 and "recovered" in sent[1]


def test_budget_and_failures_are_separate_problem_states(tmp_path, monkeypatch):
    notifiers, sent = make_notifiers(tmp_path)
    monkeypatch.setattr(
        monitoring, "fetch_recent_rentcast_runs", lambda e, n: [run(2, "failed"), run(1, "failed")]
    )
    check_provider_health(None, notifiers, **{**HEALTHY, "requests_used": 36})
    assert len(sent) == 2


def test_invalid_env_values_fall_back(monkeypatch):
    monkeypatch.setenv("PROVIDER_FAILURE_THRESHOLD", "abc")
    assert monitoring.env_int("PROVIDER_FAILURE_THRESHOLD", 2, 1) == 2
    monkeypatch.setenv("PROVIDER_FAILURE_THRESHOLD", "0")
    assert monitoring.env_int("PROVIDER_FAILURE_THRESHOLD", 2, 1) == 2


def test_capacity_check_warns_over_threshold_and_resolves(tmp_path, monkeypatch):
    notifiers, sent = make_notifiers(tmp_path)
    size = {"value": 200 * MB}
    monkeypatch.setenv("DB_SIZE_WARN_MB", "100")
    monkeypatch.setattr(monitoring, "fetch_database_size", lambda e: size["value"])
    monkeypatch.setattr(
        monitoring, "fetch_largest_tables", lambda e, limit=5: [("listing_history", 150 * MB)]
    )

    check_database_capacity(None, notifiers)
    check_database_capacity(None, notifiers)
    assert len(sent) == 1 and "database size over threshold" in sent[0]

    size["value"] = 10 * MB
    check_database_capacity(None, notifiers)
    assert len(sent) == 2 and "recovered" in sent[1]


def test_monitor_after_run_never_raises(tmp_path, capsys):
    engine = MagicMock()
    engine.connect.side_effect = RuntimeError("db down")
    notifiers, sent = make_notifiers(tmp_path)
    scheduler.monitor_after_run(engine=engine, notifiers=notifiers)
    out = capsys.readouterr().out
    assert "Provider monitoring failed" in out and "Capacity monitoring failed" in out
    assert sent == []
