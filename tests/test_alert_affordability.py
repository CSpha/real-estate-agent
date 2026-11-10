from datetime import date
from decimal import Decimal

from app.alerts.affordability import attach_affordability, summarize_affordability
from app.alerts.formatting import (
    affordability_block,
    affordability_brief,
    affordability_lines,
)
from app.alerts.send_potential_deal_alerts import (
    format_slack_message as format_deal,
    summarize_event as summarize_deal,
)
from app.alerts.send_price_drop_alerts import (
    format_slack_message as format_drop,
    summarize_event as summarize_drop,
)
from app.market.affordability import BenchmarkRate


BENCHMARK = BenchmarkRate(
    rate_pct=Decimal("7.180"),
    as_of_date=date(2026, 11, 5),
    source="freddie_mac_pmms",
    source_url="https://example.test/pmms.csv",
)
DROP = {
    "source": "rentcast",
    "source_listing_id": "MLSNOW:1",
    "address": "1351 Venture Blvd",
    "city": "Wooster",
    "state": "OH",
    "previous_price": "309000.00",
    "current_price": "299000.00",
    "price_change": "-10000.00",
    "price_change_pct": "-3.24",
}
DEAL = {
    "source": "rentcast",
    "source_listing_id": "MLSNOW:2",
    "address": "211 S Elm St",
    "city": "Orrville",
    "state": "OH",
    "county_name": "Wayne",
    "list_price": "139900",
    "county_median_sale_price": "249342",
    "price_vs_county_median": "-109442",
    "pct_below_or_above_median": "-0.4389",
    "market_score": "90",
    "score_reason": "Price is at least 30% below county median",
}


def _with_rent():
    return {**DROP, "affordability": summarize_affordability("299000", BENCHMARK, "1450")}


def test_summary_compares_down_payments_and_dscr_loan():
    info = summarize_affordability("299000", BENCHMARK, "1450")

    assert info["conventional_10_down"] == "2433.44"  # includes PMI
    assert info["conventional_20_down"] == "2118.75"
    assert info["dscr_rate_pct"] == "8.180"  # PMMS + 1.00 DSCR premium
    assert info["dscr_20_down"] == "2283.60"
    assert info["property_tax_rate_pct"] == "1.50"
    # Coverage is measured on the DSCR loan, as a DSCR lender would.
    assert info["dscr"] == "0.63"
    assert info["rent_for_target"] == "2854.50"


def test_summary_without_rent_still_reports_rent_needed():
    info = summarize_affordability("299000", BENCHMARK)

    assert info["monthly_rent"] is None
    assert info["dscr"] is None
    assert info["rent_for_target"] == "2854.50"


def test_summary_skips_unusable_prices():
    assert summarize_affordability(None, BENCHMARK) is None
    assert summarize_affordability("0", BENCHMARK) is None


def test_lines_show_rate_as_of_date_and_assumptions():
    lines = affordability_lines(_with_rent())

    assert lines == [
        "Est. monthly cost at the 7.18% 30-year PMMS rate "
        "(as of Nov 5; 1.50% tax, 0.50% insurance):",
        "Conventional: $2,433 with 10% down (incl. PMI), $2,119 with 20% down",
        "DSCR loan, 20% down at 8.18%: $2,284",
        "Rent est. $1,450/mo, DSCR 0.63; $2,854/mo rent needed for 1.25 DSCR",
    ]


def test_lines_without_rent_estimate():
    event = {**DROP, "affordability": summarize_affordability("299000", BENCHMARK)}

    assert affordability_lines(event)[-1] == (
        "No rent estimate on file; $2,854/mo rent needed for 1.25 DSCR"
    )
    assert affordability_brief(event) == ", est. $2,119/mo with 20% down"


def test_price_drop_message_includes_affordability_section():
    message = format_drop(_with_rent())

    section = message["blocks"][3]["text"]["text"]
    assert section.startswith("*Est. monthly cost at the 7.18% 30-year PMMS rate")
    assert "DSCR loan, 20% down at 8.18%: $2,284" in section
    assert message["blocks"][-1]["type"] == "context"
    assert message["text"].endswith(
        "Rent est. $1,450/mo, DSCR 0.63; $2,854/mo rent needed for 1.25 DSCR"
    )


def test_deal_message_and_digest_line_include_affordability():
    event = {**DEAL, "affordability": summarize_affordability("139900", BENCHMARK)}

    message = format_deal(event)
    rich, plain = summarize_deal(event)

    assert any(
        "Conventional:" in block.get("text", {}).get("text", "")
        for block in message["blocks"]
    )
    assert "Conventional: $1,139 with 10% down" in message["text"]
    assert plain.endswith(", est. $991/mo with 20% down")
    assert rich.endswith(", est. $991/mo with 20% down")


def test_older_payloads_without_affordability_render_as_before():
    # Alerts queued before this change, or with no stored PMMS rate.
    for event in (DROP, {**DROP, "affordability": None}):
        assert affordability_block(event) is None
        assert affordability_brief(event) == ""
        assert len(format_drop(event)["blocks"]) == 4
        assert summarize_drop(event)[1].endswith("from $309,000")


def test_attach_affordability_without_benchmark_leaves_section_out(monkeypatch):
    monkeypatch.setattr(
        "app.alerts.affordability.get_latest_benchmark_rate", lambda engine: None
    )

    events = attach_affordability([DEAL], "list_price", engine=object())

    assert events == [{**DEAL, "affordability": None}]


def test_attach_affordability_matches_rents_by_source_and_listing(monkeypatch):
    monkeypatch.setattr(
        "app.alerts.affordability.get_latest_benchmark_rate", lambda engine: BENCHMARK
    )
    monkeypatch.setattr(
        "app.alerts.affordability.get_latest_rents",
        lambda events, engine: {("rentcast", "MLSNOW:1"): Decimal("1450.00")},
    )

    rented, unrented = attach_affordability(
        [DROP, {**DROP, "source": "fixture"}], "current_price", engine=object()
    )

    assert rented["affordability"]["dscr"] == "0.63"
    assert unrented["affordability"]["dscr"] is None
    assert "affordability" not in DROP  # inputs are copied, not mutated
