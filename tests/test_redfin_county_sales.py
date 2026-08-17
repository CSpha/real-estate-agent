from __future__ import annotations

import io
from datetime import date
from decimal import Decimal

import pytest

import app.ingest.load_redfin_county_sales as redfin_module


CSV_HEADER = (
    '"FREQUENCY","PERIOD END","REGION TYPE","REGION NAME",'
    '"HOMES SOLD","MEDIAN SALE PRICE NSA ($)",'
    '"MEDIAN DAYS ON MARKET (DAYS)","NEW LISTINGS","ACTIVE LISTINGS"'
)


def _csv(*rows: str) -> str:
    return "\n".join((CSV_HEADER, *rows))


def test_parser_keeps_only_wayne_county_monthly_rows_and_maps_na():
    parsed = redfin_module.parse_redfin_county_sales(
        io.StringIO(
            _csv(
                '"Monthly","2026-06-30","County","Wayne County, OH",'
                '"95","259269","30","120","180"',
                '"Monthly","2026-07-31","County","Wayne County, OH",'
                '"NA","260000","NA","125","185"',
                '"Monthly","2026-07-31","County","Stark County, OH",'
                '"200","250000","25","220","300"',
                '"Weekly","2026-07-31","County","Wayne County, OH",'
                '"20","250000","25","30","100"',
            )
        )
    )

    assert parsed.rows_scanned == 4
    assert parsed.records == [
        {
            "county_name": "Wayne",
            "state": "OH",
            "period_date": date(2026, 6, 30),
            "median_sale_price": Decimal("259269"),
            "homes_sold": 95,
            "new_listings": 120,
            "active_listings": 180,
            "median_days_on_market": 30,
            "source_file": redfin_module.REDFIN_SOURCE_FILE,
        },
        {
            "county_name": "Wayne",
            "state": "OH",
            "period_date": date(2026, 7, 31),
            "median_sale_price": Decimal("260000"),
            "homes_sold": None,
            "new_listings": 125,
            "active_listings": 185,
            "median_days_on_market": None,
            "source_file": redfin_module.REDFIN_SOURCE_FILE,
        },
    ]


def test_parser_rejects_missing_columns():
    with pytest.raises(ValueError, match="missing columns"):
        redfin_module.parse_redfin_county_sales(
            io.StringIO('"FREQUENCY","REGION NAME"\n"Monthly","Wayne County, OH"')
        )


def test_parser_rejects_conflicting_periods():
    with pytest.raises(ValueError, match="Conflicting Wayne County rows"):
        redfin_module.parse_redfin_county_sales(
            io.StringIO(
                _csv(
                    '"Monthly","2026-07-31","County","Wayne County, OH",'
                    '"95","259269","30","120","180"',
                    '"Monthly","2026-07-31","County","Wayne County, OH",'
                    '"96","259269","30","120","180"',
                )
            )
        )


class FakeResponse:
    def __init__(self, content: str):
        self.content = content
        self.encoding = None

    def __enter__(self):
        return self

    def __exit__(self, *_args):
        return None

    def raise_for_status(self):
        return None

    def iter_lines(self, *, decode_unicode=False):
        assert decode_unicode is True
        return iter(self.content.splitlines())


class FakeSession:
    def __init__(self, content: str):
        self.content = content
        self.calls = []

    def get(self, url, **kwargs):
        self.calls.append((url, kwargs))
        return FakeResponse(self.content)


def test_loader_streams_and_reports_the_loaded_period(monkeypatch):
    session = FakeSession(
        _csv(
            '"Monthly","2026-07-31","County","Wayne County, OH",'
            '"95","259269","30","120","180"'
        )
    )
    captured = {}

    def fake_upsert(records, *, engine=None):
        captured["records"] = records
        captured["engine"] = engine
        return 1

    monkeypatch.setattr(redfin_module, "upsert_redfin_county_sales", fake_upsert)

    result = redfin_module.load_redfin_county_sales(
        engine=object(),
        url="https://example.invalid/redfin.csv",
        session=session,
    )

    assert result.matched_rows == 1
    assert result.inserted_or_changed == 1
    assert result.earliest_period == date(2026, 7, 31)
    assert result.latest_period == date(2026, 7, 31)
    assert captured["records"][0]["county_name"] == "Wayne"
    assert session.calls[0][1]["stream"] is True
