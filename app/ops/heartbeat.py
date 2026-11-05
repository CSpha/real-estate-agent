"""External heartbeat for the shadow scheduler.

After each successful run the scheduler pings `SHADOW_HEARTBEAT_URL` (for
example a Healthchecks.io check). If pings stop because the computer, Docker,
or the container is down, the external service raises the alarm, which the
in-process Slack notifier cannot do.
"""
from __future__ import annotations

import argparse
from typing import Any, Callable

import requests

from app.config import get_settings


REQUEST_TIMEOUT_SECONDS = 10


def heartbeat_url() -> str | None:
    return (get_settings().shadow_heartbeat_url or "").strip() or None


def ping_heartbeat(
    *,
    url: str | None = None,
    get: Callable[..., Any] = requests.get,
) -> bool:
    """Ping the heartbeat URL; never raise, never print the URL."""

    url = url or heartbeat_url()
    if url is None:
        print("Heartbeat not sent: SHADOW_HEARTBEAT_URL is not set.", flush=True)
        return False
    try:
        response = get(url, timeout=REQUEST_TIMEOUT_SECONDS)
        response.raise_for_status()
    except requests.RequestException as exc:
        status = getattr(getattr(exc, "response", None), "status_code", None)
        print(
            f"Heartbeat failed: {type(exc).__name__}"
            f"{f' (HTTP {status})' if status else ''}.",
            flush=True,
        )
        return False
    print("Heartbeat sent.", flush=True)
    return True


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Ping SHADOW_HEARTBEAT_URL the way a successful run does."
    )
    parser.add_argument(
        "--test",
        action="store_true",
        help="Send one ping to confirm the heartbeat URL works.",
    )
    args = parser.parse_args()
    if not args.test:
        parser.error("nothing to do; pass --test")
    raise SystemExit(0 if ping_heartbeat() else 1)


if __name__ == "__main__":
    main()
