from decimal import Decimal

import pytest

from app.market import affordability
from app.market.affordability import (
    AffordabilityAssumptions,
    calculate_affordability,
    get_latest_benchmark_rate,
)
from app.market.rental_underwriting import UnderwritingAssumptions, payment_factor


def _no_down(**overrides):
    return AffordabilityAssumptions(down_payment_pct=Decimal("0"), **overrides)


def test_defaults_match_documented_values_and_underwriting():
    defaults = AffordabilityAssumptions()
    underwriting = UnderwritingAssumptions()

    assert defaults.down_payment_pct == Decimal("20")
    assert defaults.term_years == 30
    assert defaults.rate_spread_pct == Decimal("0.50")
    assert defaults.property_tax_rate_pct == underwriting.property_tax_rate_pct
    assert defaults.insurance_rate_pct == underwriting.insurance_rate_pct
    assert defaults.monthly_hoa == underwriting.monthly_hoa


# Amortization numbers below were checked independently of the closed-form
# formula by bisecting on the payment until a month-by-month balance reached 0
# (1199.1011 and 1995.9075 before rounding).
def test_amortization_200k_at_6_percent_over_30_years():
    result = calculate_affordability(
        Decimal("200000"), Decimal("6.00") + Decimal("0.50"), _no_down()
    )

    assert result.loan_amount == Decimal("200000.00")
    assert result.scenarios[0].rate_pct == Decimal("6.00")
    assert result.scenarios[0].principal_interest == Decimal("1199.10")


def test_amortization_300k_at_7_percent_over_30_years():
    result = calculate_affordability(Decimal("300000"), Decimal("7.00"), _no_down())

    assert result.scenarios[1].principal_interest == Decimal("1995.91")


def test_amortization_uses_loan_after_down_payment():
    # $250,000 at 20% down is a $200,000 loan.
    result = calculate_affordability(
        Decimal("250000"), Decimal("6.50"), AffordabilityAssumptions()
    )

    assert result.down_payment == Decimal("50000.00")
    assert result.loan_amount == Decimal("200000.00")
    assert result.scenarios[0].principal_interest == Decimal("1199.10")


def test_breakdown_and_total_for_default_assumptions():
    result = calculate_affordability(Decimal("250000"), Decimal("6.50"))
    benchmark = result.scenarios[1]

    # 1.50% and 0.50% of $250,000 per year, divided by 12.
    assert benchmark.property_tax == Decimal("312.50")
    assert benchmark.insurance == Decimal("104.17")
    assert benchmark.hoa == Decimal("0.00")
    assert benchmark.total == (
        benchmark.principal_interest
        + benchmark.property_tax
        + benchmark.insurance
        + benchmark.hoa
    )


def test_hoa_is_added_to_every_scenario_total():
    base = calculate_affordability(Decimal("250000"), Decimal("6.50"))
    with_hoa = calculate_affordability(
        Decimal("250000"),
        Decimal("6.50"),
        AffordabilityAssumptions(monthly_hoa=Decimal("75")),
    )

    for plain, hoa in zip(base.scenarios, with_hoa.scenarios):
        assert hoa.hoa == Decimal("75.00")
        assert hoa.total - plain.total == Decimal("75.00")


def test_scenarios_are_exactly_half_a_point_apart_and_ordered():
    result = calculate_affordability(Decimal("250000"), Decimal("6.43"))
    low, mid, high = result.scenarios

    assert (low.rate_pct, mid.rate_pct, high.rate_pct) == (
        Decimal("5.93"),
        Decimal("6.43"),
        Decimal("6.93"),
    )
    assert mid.rate_pct - low.rate_pct == Decimal("0.50")
    assert high.rate_pct - mid.rate_pct == Decimal("0.50")
    assert low.principal_interest < mid.principal_interest < high.principal_interest
    assert low.total < mid.total < high.total
    assert result.benchmark_rate_pct == Decimal("6.43")


def test_custom_spread_is_used():
    result = calculate_affordability(
        Decimal("250000"),
        Decimal("6.00"),
        AffordabilityAssumptions(rate_spread_pct=Decimal("1.00")),
    )

    assert [s.rate_pct for s in result.scenarios] == [
        Decimal("5.00"),
        Decimal("6.00"),
        Decimal("7.00"),
    ]


def test_zero_percent_down_finances_the_whole_price():
    result = calculate_affordability(Decimal("200000"), Decimal("6.50"), _no_down())

    assert result.down_payment == Decimal("0.00")
    assert result.loan_amount == Decimal("200000.00")


def test_one_hundred_percent_down_means_no_loan_but_still_tax_and_insurance():
    result = calculate_affordability(
        Decimal("200000"),
        Decimal("6.50"),
        AffordabilityAssumptions(down_payment_pct=Decimal("100")),
    )

    assert result.loan_amount == Decimal("0.00")
    for scenario in result.scenarios:
        assert scenario.principal_interest == Decimal("0.00")
        assert scenario.total == Decimal("250.00") + Decimal("83.33")


def test_smallest_rate_gives_zero_interest_lower_scenario():
    # Benchmark 0.50 makes the lower scenario exactly 0%: loan / 360 months.
    result = calculate_affordability(Decimal("360000"), Decimal("0.50"), _no_down())

    assert result.scenarios[0].rate_pct == Decimal("0.00")
    assert result.scenarios[0].principal_interest == Decimal("1000.00")
    assert result.scenarios[1].principal_interest > Decimal("1000.00")


def test_benchmark_too_low_for_the_lower_scenario_is_rejected():
    with pytest.raises(ValueError, match="negative"):
        calculate_affordability(Decimal("200000"), Decimal("0.49"))


def test_largest_rate_is_accepted_and_above_it_is_rejected():
    result = calculate_affordability(Decimal("200000"), Decimal("30"))

    assert result.scenarios[2].rate_pct == Decimal("30.50")
    assert result.scenarios[2].principal_interest > result.scenarios[1].principal_interest
    with pytest.raises(ValueError, match="above"):
        calculate_affordability(Decimal("200000"), Decimal("30.01"))


@pytest.mark.parametrize("price", [None, 0, Decimal("0"), -1, Decimal("-250000")])
def test_missing_or_non_positive_price_raises_a_clear_error(price):
    with pytest.raises(ValueError, match="price"):
        calculate_affordability(price, Decimal("6.50"))


@pytest.mark.parametrize("down", [Decimal("-1"), Decimal("100.01")])
def test_down_payment_outside_zero_to_hundred_is_rejected(down):
    with pytest.raises(ValueError, match="down_payment_pct"):
        calculate_affordability(
            Decimal("200000"),
            Decimal("6.50"),
            AffordabilityAssumptions(down_payment_pct=down),
        )


def test_price_can_be_a_string_or_int_and_money_is_rounded_to_cents():
    result = calculate_affordability("199999.999", 6, _no_down())

    assert result.price == Decimal("200000.00")
    for scenario in result.scenarios:
        for value in (
            scenario.principal_interest,
            scenario.property_tax,
            scenario.insurance,
            scenario.hoa,
            scenario.total,
        ):
            assert value == value.quantize(Decimal("0.01"))


def test_payment_factor_is_shared_with_rental_underwriting():
    # Same function object under both names, so existing results cannot change.
    from app.market import rental_underwriting

    assert payment_factor is rental_underwriting._payment_factor


class _FakeResult:
    def __init__(self, row):
        self._row = row

    def mappings(self):
        return self

    def first(self):
        return self._row


class _FakeConnection:
    def __init__(self, row):
        self._row = row

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        return False

    def execute(self, *_args, **_kwargs):
        return _FakeResult(self._row)


class _FakeEngine:
    def __init__(self, row):
        self._row = row

    def connect(self):
        return _FakeConnection(self._row)


def test_missing_benchmark_is_reported_not_defaulted():
    assert get_latest_benchmark_rate(_FakeEngine(None)) is None


def test_cli_stops_with_a_clear_message_when_no_benchmark_is_stored(
    monkeypatch, capsys
):
    monkeypatch.setattr(affordability, "get_engine", lambda: object())
    monkeypatch.setattr(affordability, "get_latest_benchmark_rate", lambda _e: None)

    def fail_if_listings_are_read(*_args, **_kwargs):
        raise AssertionError("listings must not be read without a benchmark")

    monkeypatch.setattr(affordability, "get_listings", fail_if_listings_are_read)

    assert affordability.main([]) == 1
    output = capsys.readouterr().out
    assert "No benchmark rate stored" in output
    assert "no default rate is used" in output


def test_stored_benchmark_is_returned_with_date_and_source():
    from datetime import date

    row = {
        "rate_percent": Decimal("6.580"),
        "observation_date": date(2026, 7, 23),
        "source": "freddie_mac_pmms",
        "source_url": "https://example.test/pmms.csv",
        "product_type": "30_year_fixed",
    }
    benchmark = get_latest_benchmark_rate(_FakeEngine(row))

    assert benchmark.rate_pct == Decimal("6.580")
    assert benchmark.as_of_date == date(2026, 7, 23)
    assert benchmark.source == "freddie_mac_pmms"
