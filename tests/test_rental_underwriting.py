from decimal import Decimal

import pytest

from app.market.rental_underwriting import (
    UnderwritingAssumptions,
    calculate_rental_underwriting,
)


def test_dscr_target_caps_loan_before_ltv_when_rent_is_constrained():
    result = calculate_rental_underwriting(
        purchase_price=Decimal("300000"),
        monthly_rent=Decimal("1800"),
    )

    assert result["constraint"] == "dscr"
    assert result["target_loan_amount"] < result["ltv_cap_loan"]
    assert result["underwritten_dscr"] == Decimal("1.250")
    assert result["minimum_rent_for_target_dscr"] == Decimal("1800.00")


def test_ltv_cap_preserves_dscr_cushion_when_rent_supports_more_debt():
    result = calculate_rental_underwriting(
        purchase_price=Decimal("100000"),
        monthly_rent=Decimal("2000"),
    )

    assert result["constraint"] == "ltv"
    assert result["target_loan_amount"] == Decimal("75000.00")
    assert result["target_ltv_pct"] == Decimal("75.0000")
    assert result["underwritten_dscr"] > Decimal("1.25")


def test_zero_interest_uses_straight_line_amortization():
    result = calculate_rental_underwriting(
        purchase_price=Decimal("120000"),
        monthly_rent=Decimal("1500"),
        assumptions=UnderwritingAssumptions(interest_rate_pct=Decimal("0")),
    )

    assert result["monthly_principal_interest"] == Decimal("250.00")


def test_invalid_assumptions_are_rejected():
    with pytest.raises(ValueError, match="max_ltv_pct"):
        calculate_rental_underwriting(
            purchase_price=Decimal("100000"),
            monthly_rent=Decimal("1000"),
            assumptions=UnderwritingAssumptions(max_ltv_pct=Decimal("110")),
        )
