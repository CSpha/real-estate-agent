from datetime import date, datetime, timedelta, timezone

from app.market.analysis_dates import analysis_today, utc_date


def test_evening_local_time_uses_next_utc_analysis_day(monkeypatch):
    instant = datetime(2026, 9, 8, 21, 30, tzinfo=timezone(timedelta(hours=-4)))

    class Clock(datetime):
        @classmethod
        def now(cls, tz):
            return cls.fromtimestamp(instant.timestamp(), tz)

    monkeypatch.setattr("app.market.analysis_dates.datetime", Clock)
    assert analysis_today() == date(2026, 9, 9)


def test_timestamp_conversion_is_independent_of_offset():
    assert utc_date(
        datetime(2026, 9, 8, 21, tzinfo=timezone(timedelta(hours=-4)))
    ) == date(2026, 9, 9)
    assert utc_date(datetime(2026, 9, 9, 1, tzinfo=timezone.utc)) == date(2026, 9, 9)
    assert utc_date(datetime(2026, 9, 9, 1)) == date(2026, 9, 9)
    assert utc_date(date(2026, 9, 9)) == date(2026, 9, 9)
