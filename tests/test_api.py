from fastapi.testclient import TestClient
from sqlalchemy.exc import OperationalError

import app.api as api_module
from app.api import app


def test_root_does_not_require_database():
    response = TestClient(app).get("/")

    assert response.status_code == 200
    assert response.json() == {"message": "Real Estate Agent API is running"}


def test_health_reports_database_failure_without_details(monkeypatch):
    class UnavailableEngine:
        def connect(self):
            raise OperationalError("SELECT 1", {}, Exception("secret details"))

    monkeypatch.setattr(api_module, "get_engine", lambda: UnavailableEngine())
    response = TestClient(app).get("/health")

    assert response.status_code == 503
    assert response.json() == {"detail": "Database is unavailable"}
