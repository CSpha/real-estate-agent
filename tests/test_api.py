from fastapi.testclient import TestClient
from sqlalchemy.exc import OperationalError

import app.api as api_module
from app.api import app


def test_root_does_not_require_database():
    response = TestClient(app).get("/")

    assert response.status_code == 200
    assert response.json() == {"message": "Real Estate Agent API is running"}


def test_analyst_endpoints_are_disabled_by_default():
    response = TestClient(app).post(
        "/listings/test-source/1/analyses",
        json={"model": "gpt-5.4-mini"},
    )

    assert response.status_code == 404
    assert "disabled" in response.json()["detail"].lower()


def test_saved_search_routes_are_exposed():
    paths = TestClient(app).get("/openapi.json").json()["paths"]

    assert "/saved-searches" in paths
    assert "/saved-searches/{saved_search_id}" in paths
    assert "/saved-searches/{saved_search_id}/evaluate" in paths
    assert "/saved-searches/{saved_search_id}/evaluations" in paths


def test_invalid_saved_search_is_rejected_before_database_access():
    response = TestClient(app).post(
        "/saved-searches",
        json={
            "name": "Invalid empty search",
            "criteria": {},
        },
    )

    assert response.status_code == 422
    assert "At least one search criterion is required" in response.text


def test_health_reports_database_failure_without_details(monkeypatch):
    class UnavailableEngine:
        def connect(self):
            raise OperationalError("SELECT 1", {}, Exception("secret details"))

    monkeypatch.setattr(api_module, "get_engine", lambda: UnavailableEngine())
    response = TestClient(app).get("/health")

    assert response.status_code == 503
    assert response.json() == {"detail": "Database is unavailable"}
