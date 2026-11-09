from datetime import date, datetime, timedelta, timezone
from unittest.mock import MagicMock

import pytest

import app.market.mortgage_rates as mortgage_rates
import app.shadow.scheduler as scheduler
from app.market.mortgage_rates import refresh_pmms_if_stale

NOW = datetime(2026, 11, 8, 12, 0, tzinfo=timezone.utc)


def engine_with(*rows):
    """Fake engine whose successive MAX(...) queries return the given rows."""
    engine = MagicMock()
    engine.connect.return_value.__enter__.return_value.execute.return_value.one.side_effect = list(rows)
    return engine


class FakeFetch:
    def __init__(self, result="csv", error=None):
        self.calls = 0
        self.result = result
        self.error = error

    def __call__(self):
        self.calls += 1
        if self.error:
            raise self.error
        return self.result


@pytest.fixture
def fake_load(monkeypatch):
    loaded = []

    def load(csv_text, engine=None):
        loaded.append(csv_text)
        return {"received": 10, "inserted_or_changed": 2}

    monkeypatch.setattr(mortgage_rates, "load_pmms_csv", load)
    return loaded


def test_fresh_data_is_skipped_without_downloading(fake_load, capsys):
    engine = engine_with((NOW - timedelta(days=2), date(2026, 11, 5)))
    fetch = FakeFetch()

    result = refresh_pmms_if_stale(engine=engine, now=NOW, fetch=fetch)

    assert result["status"] == "skipped"
    assert fetch.calls == 0 and fake_load == []
    assert "PMMS refresh skipped" in capsys.readouterr().out


def test_stale_data_is_downloaded_and_loaded(fake_load, capsys):
    engine = engine_with(
        (NOW - timedelta(days=7), date(2026, 9, 10)),
        (NOW, date(2026, 11, 5)),
    )
    fetch = FakeFetch("new csv")

    result = refresh_pmms_if_stale(engine=engine, now=NOW, fetch=fetch)

    assert result == {
        "status": "refreshed",
        "received": 10,
        "inserted_or_changed": 2,
        "newest_observation_date": date(2026, 11, 5),
    }
    assert fake_load == ["new csv"]
    assert "newest observation 2026-11-05" in capsys.readouterr().out


def test_exactly_min_age_is_refreshed(fake_load):
    engine = engine_with((NOW - timedelta(days=6), date(2026, 11, 5)), (NOW, date(2026, 11, 5)))

    assert refresh_pmms_if_stale(engine=engine, now=NOW, fetch=FakeFetch())["status"] == "refreshed"


def test_empty_table_is_refreshed(fake_load):
    engine = engine_with((None, None), (NOW, date(2026, 11, 5)))
    fetch = FakeFetch()

    result = refresh_pmms_if_stale(engine=engine, now=NOW, fetch=fetch)

    assert result["status"] == "refreshed"
    assert fetch.calls == 1


def test_download_error_propagates_and_loads_nothing(fake_load):
    engine = engine_with((None, None))

    with pytest.raises(RuntimeError, match="boom"):
        refresh_pmms_if_stale(engine=engine, now=NOW, fetch=FakeFetch(error=RuntimeError("boom")))
    assert fake_load == []


def test_csv_with_no_rows_changes_nothing_and_warns(monkeypatch, capsys):
    monkeypatch.setattr(
        mortgage_rates,
        "load_pmms_csv",
        lambda csv_text, engine=None: {"received": 0, "inserted_or_changed": 0},
    )
    engine = engine_with((NOW - timedelta(days=30), date(2026, 9, 10)))

    result = refresh_pmms_if_stale(engine=engine, now=NOW, fetch=FakeFetch("date,pmms30,pmms15\n"))

    assert result["status"] == "no_rows"
    assert result["newest_observation_date"] == date(2026, 9, 10)
    assert "no usable rows" in capsys.readouterr().out


def test_old_newest_observation_logs_stale_warning(fake_load, capsys):
    engine = engine_with((None, None), (NOW, date(2026, 9, 10)))

    refresh_pmms_if_stale(engine=engine, now=NOW, fetch=FakeFetch())

    assert "PMMS data is stale" in capsys.readouterr().out


def test_scheduler_swallows_refresh_errors(monkeypatch, capsys):
    monkeypatch.delenv("SHADOW_PMMS_REFRESH_ENABLED", raising=False)
    engine = engine_with((None, None))

    def refresh(**kwargs):
        return refresh_pmms_if_stale(fetch=FakeFetch(error=OSError("offline")), **kwargs)

    scheduler.refresh_pmms_before_run(engine=engine, refresh=refresh)  # must not raise

    assert "PMMS refresh failed: OSError: offline" in capsys.readouterr().out


def test_scheduler_swallows_engine_errors(monkeypatch):
    monkeypatch.delenv("SHADOW_PMMS_REFRESH_ENABLED", raising=False)
    monkeypatch.setattr(scheduler, "get_engine", MagicMock(side_effect=RuntimeError("db down")))

    scheduler.refresh_pmms_before_run()  # must not raise


def test_scheduler_runs_refresh_when_enabled_by_default(monkeypatch):
    monkeypatch.delenv("SHADOW_PMMS_REFRESH_ENABLED", raising=False)
    refresh = MagicMock()

    scheduler.refresh_pmms_before_run(engine="engine", refresh=refresh)

    refresh.assert_called_once_with(engine="engine")


@pytest.mark.parametrize("value", ["false", "FALSE", "0", "no", "off"])
def test_scheduler_skips_refresh_when_disabled(monkeypatch, value):
    monkeypatch.setenv("SHADOW_PMMS_REFRESH_ENABLED", value)
    refresh = MagicMock()

    scheduler.refresh_pmms_before_run(engine="engine", refresh=refresh)

    refresh.assert_not_called()
