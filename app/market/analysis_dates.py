"""UTC calendar days for comparable valuation and listing-history evidence."""

from datetime import date, datetime, timezone


def utc_date(value: date | datetime) -> date:
    if isinstance(value, datetime):
        # Legacy naive timestamps were written in the database's UTC timezone.
        return (
            value.replace(tzinfo=value.tzinfo or timezone.utc)
            .astimezone(timezone.utc)
            .date()
        )
    return value


def analysis_today() -> date:
    return utc_date(datetime.now(timezone.utc))
