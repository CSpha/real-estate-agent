from fastapi.testclient import TestClient

import app.api as api_module
from app.api import app
from app.dashboard.data import summarize


def test_dashboard_serves_the_review_workspace():
    response = TestClient(app).get("/dashboard")

    assert response.status_code == 200
    assert "Property Signal" in response.text
    assert "Scored opportunities" in response.text


def test_dashboard_data_endpoint_uses_read_only_builder(monkeypatch):
    expected = {
        "summary": {"listing_count": 2},
        "listings": [],
        "runs": [],
        "readiness": {},
    }
    engine = object()
    monkeypatch.setattr(api_module, "get_engine", lambda: engine)
    monkeypatch.setattr(
        api_module,
        "build_dashboard_data",
        lambda *, engine: expected,
    )

    response = TestClient(app).get("/dashboard/data")

    assert response.status_code == 200
    assert response.json() == expected


def test_dashboard_summary_counts_review_and_score_availability():
    listings = [
        {
            "scoring_eligible": True,
            "sqft": 1500,
            "deal_score_status": "complete",
            "valuation_status": "valued",
            "confidence_label": "medium",
            "review_flags": [],
            "alert_eligible": False,
            "monthly_rent": 1800,
            "monthly_cash_flow": 125,
            "rental_loan_constraint": "ltv",
        },
        {
            "scoring_eligible": True,
            "sqft": None,
            "deal_score_status": None,
            "valuation_status": None,
            "confidence_label": None,
            "review_flags": ["Missing square footage"],
            "alert_eligible": False,
            "monthly_rent": None,
            "monthly_cash_flow": None,
            "rental_loan_constraint": None,
        },
    ]

    result = summarize(listings)

    assert result == {
        "listing_count": 2,
        "scoring_eligible_count": 2,
        "complete_score_count": 1,
        "valued_count": 1,
        "low_confidence_count": 0,
        "missing_sqft_count": 1,
        "review_count": 1,
        "alert_eligible_count": 0,
        "rent_estimate_count": 1,
        "positive_cash_flow_count": 1,
        "dscr_constrained_count": 0,
    }
