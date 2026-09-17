"""Small, dependency-free controls for the local/public API boundary."""
from __future__ import annotations

import hmac
import os
import time
from collections import defaultdict, deque
from threading import Lock

from fastapi import HTTPException, Request


PUBLIC_PATHS = {"/health"}


def _enabled(name: str) -> bool:
    return os.getenv(name, "false").strip().lower() in {"1", "true", "yes", "on"}


def require_api_token(request: Request) -> None:
    """Require a bearer token only when public-access mode is explicitly enabled."""
    if request.url.path in PUBLIC_PATHS or not _enabled("API_AUTH_REQUIRED"):
        return
    expected = os.getenv("API_AUTH_TOKEN")
    supplied = request.headers.get("authorization", "").removeprefix("Bearer ")
    if not expected or not hmac.compare_digest(supplied, expected):
        raise HTTPException(status_code=401, detail="Authentication required")


class RequestRateLimiter:
    def __init__(self) -> None:
        self._requests: dict[str, deque[float]] = defaultdict(deque)
        self._lock = Lock()

    def allow(self, client: str, now: float | None = None) -> bool:
        limit = int(os.getenv("API_RATE_LIMIT_PER_MINUTE", "120"))
        if limit < 1:
            return False
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
