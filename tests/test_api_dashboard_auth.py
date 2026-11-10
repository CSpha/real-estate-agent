import pytest
from fastapi.testclient import TestClient

import app.api as api_module
from app.api import app
from app.api_security import SESSION_COOKIE, rate_limiter

TOKEN = "t" * 40


@pytest.fixture
def auth_on(monkeypatch):
    rate_limiter.clear()
    monkeypatch.setenv("API_AUTH_REQUIRED", "true")
    monkeypatch.setenv("API_AUTH_TOKEN", TOKEN)
    monkeypatch.delenv("API_COOKIE_SECURE", raising=False)
    monkeypatch.delenv("API_RATE_LIMIT_PER_MINUTE", raising=False)
    yield
    rate_limiter.clear()


def _client():
    return TestClient(app, follow_redirects=False)


def _sign_in(client):
    return client.post("/login", data={"token": TOKEN})


def test_dashboard_redirects_browsers_to_sign_in(auth_on):
    response = _client().get("/dashboard")

    assert response.status_code == 303
    assert response.headers["location"] == "/login"


def test_login_page_and_assets_are_public(auth_on):
    client = _client()

    page = client.get("/login")
    assert page.status_code == 200
    assert 'name="token"' in page.text
    assert page.headers["cache-control"] == "no-store"
    assert client.get("/dashboard-assets/dashboard.css").status_code == 200


def test_wrong_token_shows_an_error_and_sets_no_cookie(auth_on):
    response = _client().post("/login", data={"token": "wrong"})

    assert response.status_code == 401
    assert "not correct" in response.text
    assert SESSION_COOKIE not in response.cookies


def test_sign_in_sets_a_read_only_session(auth_on, monkeypatch):
    monkeypatch.setattr(
        api_module, "build_dashboard_data", lambda engine: {"listings": [], "runs": []}
    )
    monkeypatch.setattr(api_module, "get_engine", lambda: None)
    client = _client()

    response = _sign_in(client)
    assert response.status_code == 303
    assert response.headers["location"] == "/dashboard"
    set_cookie = response.headers["set-cookie"].lower()
    assert "httponly" in set_cookie and "samesite=lax" in set_cookie
    assert TOKEN not in response.headers["set-cookie"]

    assert client.get("/dashboard").status_code == 200
    assert client.get("/dashboard/data").status_code == 200
    # The cookie never authorizes writes; those still need the bearer header.
    write = client.post("/saved-searches", json={})
    assert write.status_code == 401


def test_rotating_the_token_invalidates_sessions(auth_on, monkeypatch):
    client = _client()
    _sign_in(client)

    monkeypatch.setenv("API_AUTH_TOKEN", "r" * 40)

    assert client.get("/dashboard").status_code == 303


def test_logout_clears_the_session(auth_on):
    client = _client()
    _sign_in(client)

    response = client.post("/logout")

    assert response.status_code == 303
    assert client.get("/dashboard").status_code == 303


def test_dashboard_data_returns_401_json_not_a_redirect(auth_on):
    response = _client().get("/dashboard/data")

    assert response.status_code == 401
    assert response.json() == {"detail": "Authentication required"}


def test_cookie_is_secure_behind_an_https_proxy(auth_on, monkeypatch):
    plain = _sign_in(_client())
    assert "secure" not in plain.headers["set-cookie"].lower()

    proxied = _client().post(
        "/login", data={"token": TOKEN}, headers={"X-Forwarded-Proto": "https"}
    )
    assert "secure" in proxied.headers["set-cookie"].lower()

    monkeypatch.setenv("API_COOKIE_SECURE", "true")
    forced = _sign_in(_client())
    assert "secure" in forced.headers["set-cookie"].lower()


def test_login_redirects_to_dashboard_when_auth_is_off(monkeypatch):
    rate_limiter.clear()
    monkeypatch.delenv("API_AUTH_REQUIRED", raising=False)
    client = _client()

    assert client.get("/login").headers["location"] == "/dashboard"
    assert client.post("/login", data={"token": "x"}).headers["location"] == "/dashboard"


def test_oversized_login_form_is_rejected(auth_on):
    response = _client().post("/login", data={"token": "x" * 5000})

    assert response.status_code == 413


def test_bearer_token_requires_the_bearer_scheme(auth_on):
    client = _client()

    assert client.get("/", headers={"Authorization": TOKEN}).status_code == 401
    assert client.get("/", headers={"Authorization": f"Token {TOKEN}"}).status_code == 401
    assert client.get("/", headers={"Authorization": f"bearer {TOKEN}"}).status_code == 200


def test_failed_logins_count_against_the_rate_limit(auth_on, monkeypatch):
    monkeypatch.setenv("API_RATE_LIMIT_PER_MINUTE", "2")
    client = _client()

    codes = [client.post("/login", data={"token": "wrong"}).status_code for _ in range(3)]

    assert codes == [401, 401, 429]


def test_dashboard_assets_are_exempt_from_the_rate_limit(monkeypatch):
    rate_limiter.clear()
    monkeypatch.setenv("API_RATE_LIMIT_PER_MINUTE", "1")
    client = _client()

    codes = [client.get("/dashboard-assets/dashboard.css").status_code for _ in range(3)]

    assert codes == [200, 200, 200]
    rate_limiter.clear()


@pytest.mark.parametrize(
    ("env", "message"),
    [
        ({"API_RATE_LIMIT_PER_MINUTE": "abc"}, "whole number"),
        ({"API_RATE_LIMIT_PER_MINUTE": "0"}, "at least 1"),
        ({"API_AUTH_REQUIRED": "true", "API_AUTH_TOKEN": ""}, "at least 32"),
        ({"API_AUTH_REQUIRED": "true", "API_AUTH_TOKEN": "short"}, "at least 32"),
    ],
)
def test_bad_settings_stop_startup_with_a_clear_message(monkeypatch, env, message):
    for name in ("API_RATE_LIMIT_PER_MINUTE", "API_AUTH_REQUIRED", "API_AUTH_TOKEN"):
        monkeypatch.delenv(name, raising=False)
    for name, value in env.items():
        monkeypatch.setenv(name, value)

    with pytest.raises(ValueError, match=message):
        with TestClient(app):
            pass


def test_valid_settings_start_cleanly(auth_on):
    with TestClient(app) as client:
        assert client.get("/health").status_code in {200, 503}
