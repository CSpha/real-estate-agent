from fastapi.testclient import TestClient
from datetime import datetime, timedelta, timezone

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


def test_health_reports_shadow_freshness_schedule_and_sanitized_failures(monkeypatch):
    now = datetime.now(timezone.utc)
    success = now - timedelta(hours=2)
    failure = now - timedelta(hours=1)

    class Result:
        def __init__(self, value):
            self.value = value

        def mappings(self):
            return self

        def one(self):
            return self.value

        def all(self):
            return self.value

    class Connection:
        def execute(self, query, *args, **kwargs):
            sql = str(query)
            if "SELECT 1" in sql:
                return Result(None)
            if "recent_failure_count" in sql:
                return Result(
                    {
                        "last_successful_run_at": success,
                        "latest_status": "succeeded",
                        "latest_run_at": success,
                        "recent_failure_count": 1,
                    }
                )
            return Result(
                [
                    {
                        "id": 7,
                        "started_at": failure,
                        "finished_at": failure,
                    }
                ]
            )

        def __enter__(self):
            return self

        def __exit__(self, *args):
            return False

    monkeypatch.setattr(
        api_module,
        "get_engine",
        lambda: type("Engine", (), {"connect": lambda self: Connection()})(),
    )
    monkeypatch.setenv("SHADOW_INTERVAL_HOURS", "72")

    response = TestClient(app).get("/health")

    assert response.status_code == 200
    body = response.json()
    assert body["status"] == "ok"
    shadow = body["shadow_pipeline"]
    assert shadow["freshness"] == "fresh"
    assert shadow["last_successful_run_age_seconds"] >= 7190
    assert shadow["recent_failure_count"] == 1
    assert shadow["recent_failures"] == [
        {
            "id": 7,
            "started_at": failure.isoformat(),
            "finished_at": failure.isoformat(),
        }
    ]
    assert shadow["next_run_at"] == (success + timedelta(hours=72)).isoformat()


def test_health_marks_missing_shadow_success_as_degraded(monkeypatch):
    class Result:
        def mappings(self):
            return self

        def one(self):
            return {
                "last_successful_run_at": None,
                "latest_status": None,
                "latest_run_at": None,
                "recent_failure_count": 0,
            }

        def all(self):
            return []

    class Connection:
        def execute(self, query, *args, **kwargs):
            return Result()

        def __enter__(self):
            return self

        def __exit__(self, *args):
            return False

    monkeypatch.setattr(
        api_module,
        "get_engine",
        lambda: type("Engine", (), {"connect": lambda self: Connection()})(),
    )
    response = TestClient(app).get("/health")
    assert response.status_code == 200
    assert response.json()["status"] == "degraded"
    assert response.json()["shadow_pipeline"]["freshness"] == "unknown"
