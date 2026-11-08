from datetime import datetime
from types import SimpleNamespace

import pytest
from fastapi.testclient import TestClient

import app.api as api_module
from app.api import app
from app.api_limits import paginate, statement_timeout_ms
from app.api_security import rate_limiter


class FakeResult:
    def __init__(self, rows):
        self.rows = rows

    def all(self):
        return self.rows


class FakeConnection:
    def __init__(self, rows, calls):
        self.rows = rows
        self.calls = calls

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        return False

    def execute(self, query, params=None):
        self.calls.append((str(query), params))
        return FakeResult(self.rows)


class FakeEngine:
    def __init__(self, rows):
        self.rows = rows
        self.calls = []

    def connect(self):
        return FakeConnection(self.rows, self.calls)


@pytest.fixture(autouse=True)
def _reset_rate_limit():
    rate_limiter.clear()


def _alert_row(i):
    values = {
        "id": i,
        "alert_type": "price_drop",
        "source": "s",
        "source_listing_id": str(i),
        "event_timestamp": datetime(2026, 1, 1),
        "payload_json": "{}",
        "sent_at": datetime(2026, 1, 1),
    }
    return SimpleNamespace(_mapping=values, **values)


@pytest.mark.parametrize(
    "path",
    [
        "/alerts",
        "/listings",
        "/saved-searches",
        "/saved-searches/1/evaluations",
        "/analyses/1/reviews",
        "/listings/s/1/analyses",
    ],
)
@pytest.mark.parametrize(
    "query", ["limit=0", "limit=-1", "limit=201", "limit=abc", "offset=-1"]
)
def test_list_endpoints_reject_invalid_paging(path, query, monkeypatch):
    monkeypatch.setenv("ANALYST_FEATURES_ENABLED", "true")
    assert TestClient(app).get(f"{path}?{query}").status_code == 422


def test_score_rejects_oversized_limit_and_negative_offset():
    client = TestClient(app)
    assert client.post("/score?limit=201").status_code == 422
    assert client.post("/score?offset=-5").status_code == 422


def test_alerts_use_default_limit_and_stable_order(monkeypatch):
    engine = FakeEngine([_alert_row(i) for i in range(3)])
    monkeypatch.setattr(api_module, "get_engine", lambda: engine)

    response = TestClient(app).get("/alerts")

    assert response.status_code == 200
    query, params = engine.calls[0]
    assert "ORDER BY sent_at DESC, id DESC" in query
    assert params == {"limit": 51, "offset": 0}
    assert response.headers["x-page-limit"] == "50"
    assert response.headers["x-has-more"] == "false"
    assert "x-next-offset" not in response.headers
    assert len(response.json()) == 3


def test_alerts_report_more_rows_and_trim_the_extra_one(monkeypatch):
    engine = FakeEngine([_alert_row(i) for i in range(3)])
    monkeypatch.setattr(api_module, "get_engine", lambda: engine)

    response = TestClient(app).get("/alerts?limit=2&offset=4")

    assert [row["id"] for row in response.json()] == [0, 1]
    assert response.headers["x-has-more"] == "true"
    assert response.headers["x-next-offset"] == "6"
    assert engine.calls[0][1] == {"limit": 3, "offset": 4}


def test_paginate_helper():
    assert paginate([1, 2, 3], 2) == ([1, 2], True)
    assert paginate([1, 2], 2) == ([1, 2], False)


def test_statement_timeout_defaults_and_overrides(monkeypatch):
    monkeypatch.delenv("API_STATEMENT_TIMEOUT_MS", raising=False)
    assert statement_timeout_ms() == 5000
    monkeypatch.setenv("API_STATEMENT_TIMEOUT_MS", "1500")
    assert statement_timeout_ms() == 1500
    monkeypatch.setenv("API_STATEMENT_TIMEOUT_MS", "nope")
    assert statement_timeout_ms() == 5000
    monkeypatch.setenv("API_STATEMENT_TIMEOUT_MS", "-1")
    assert statement_timeout_ms() == 5000


def test_dashboard_listing_query_is_bounded():
    from app.dashboard.data import DASHBOARD_MAX_LISTINGS, LISTINGS_SQL

    assert "LIMIT :row_limit" in str(LISTINGS_SQL)
    assert DASHBOARD_MAX_LISTINGS <= 5000
