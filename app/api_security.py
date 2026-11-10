"""Small, dependency-free controls for the local/public API boundary."""
from __future__ import annotations

import hashlib
import hmac
import os
import time
from collections import defaultdict, deque
from threading import Lock

from fastapi import HTTPException, Request


# Reachable without a token. The dashboard's CSS, JS, and favicon are static
# code with no data, and the login page needs them.
PUBLIC_PATHS = {"/health", "/login", "/logout"}
PUBLIC_PREFIXES = ("/dashboard-assets/",)
RATE_LIMIT_EXEMPT_PATHS = {"/health"}

SESSION_COOKIE = "rea_dashboard_session"
SESSION_MAX_AGE_SECONDS = 30 * 24 * 3600
# The browser session cookie authorizes only reads. Writes (saved searches,
# analyses, reviews, scoring) still need the bearer header, so a cross-site
# form cannot use a signed-in browser to change anything.
SESSION_METHODS = {"GET", "HEAD"}
MIN_TOKEN_LENGTH = 32
DEFAULT_RATE_LIMIT_PER_MINUTE = 120


def _enabled(name: str) -> bool:
    return os.getenv(name, "false").strip().lower() in {"1", "true", "yes", "on"}


def auth_required() -> bool:
    return _enabled("API_AUTH_REQUIRED")


def is_public_path(path: str) -> bool:
    return path in PUBLIC_PATHS or path.startswith(PUBLIC_PREFIXES)


def is_rate_limit_exempt(path: str) -> bool:
    return path in RATE_LIMIT_EXEMPT_PATHS or path.startswith(PUBLIC_PREFIXES)


def rate_limit_per_minute() -> int:
    raw = os.getenv("API_RATE_LIMIT_PER_MINUTE", str(DEFAULT_RATE_LIMIT_PER_MINUTE))
    try:
        limit = int(raw.strip())
    except ValueError:
        raise ValueError(
            f"API_RATE_LIMIT_PER_MINUTE must be a whole number, got {raw!r}."
        ) from None
    if limit < 1:
        raise ValueError(f"API_RATE_LIMIT_PER_MINUTE must be at least 1, got {limit}.")
    return limit


def validate_api_security_settings() -> None:
    """Fail at startup instead of on every request when settings are wrong."""
    rate_limit_per_minute()
    if auth_required():
        token = os.getenv("API_AUTH_TOKEN") or ""
        if len(token) < MIN_TOKEN_LENGTH:
            raise ValueError(
                "API_AUTH_REQUIRED=true needs an API_AUTH_TOKEN of at least "
                f"{MIN_TOKEN_LENGTH} characters (got {len(token)})."
            )


def token_matches(supplied: str) -> bool:
    expected = os.getenv("API_AUTH_TOKEN")
    if not expected or not supplied:
        return False
    return hmac.compare_digest(supplied.encode(), expected.encode())


def _bearer_token(request: Request) -> str:
    scheme, _, value = request.headers.get("authorization", "").partition(" ")
    return value.strip() if scheme.lower() == "bearer" else ""


def session_value() -> str:
    """Derive the cookie from the token, so rotating the token signs everyone out."""
    token = os.getenv("API_AUTH_TOKEN") or ""
    return hmac.new(token.encode(), b"dashboard-session-v1", hashlib.sha256).hexdigest()


def has_valid_session(request: Request) -> bool:
    if request.method not in SESSION_METHODS or not os.getenv("API_AUTH_TOKEN"):
        return False
    cookie = request.cookies.get(SESSION_COOKIE, "")
    return bool(cookie) and hmac.compare_digest(cookie, session_value())


def cookie_secure(request: Request) -> bool:
    """Mark the cookie Secure when the browser reached us over HTTPS.

    `API_COOKIE_SECURE=true` forces it, for HTTPS proxies that do not send
    X-Forwarded-Proto.
    """
    if _enabled("API_COOKIE_SECURE"):
        return True
    forwarded = request.headers.get("x-forwarded-proto", "").split(",")[0].strip()
    return request.url.scheme == "https" or forwarded.lower() == "https"


def require_api_token(request: Request) -> None:
    """Require a bearer token (or a dashboard session for reads) when auth is on."""
    if is_public_path(request.url.path) or not auth_required():
        return
    if token_matches(_bearer_token(request)) or has_valid_session(request):
        return
    raise HTTPException(status_code=401, detail="Authentication required")


class RequestRateLimiter:
    def __init__(self) -> None:
        self._requests: dict[str, deque[float]] = defaultdict(deque)
        self._lock = Lock()

    def allow(self, client: str, now: float | None = None) -> bool:
        limit = rate_limit_per_minute()
        now = time.monotonic() if now is None else now
        with self._lock:
            recent = self._requests[client]
            while recent and recent[0] <= now - 60:
                recent.popleft()
            if len(recent) >= limit:
                return False
            recent.append(now)
            return True

    def clear(self) -> None:
        with self._lock:
            self._requests.clear()


rate_limiter = RequestRateLimiter()
