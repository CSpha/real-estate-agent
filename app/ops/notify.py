"""Operational Slack notifications for the shadow scheduler.

This is deliberately separate from `app.alerts`: the shadow runner must not
import listing-alert code, and it only ever posts to `SLACK_OPS_WEBHOOK_URL`.
"""
from __future__ import annotations

import argparse
import json
import threading
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable

import requests

from app.config import get_settings


DEFAULT_STATE_PATH = Path("data/ops_notification_state.json")
MAX_DETAIL_LENGTH = 300
REQUEST_TIMEOUT_SECONDS = 10


def ops_webhook_url() -> str | None:
    return (get_settings().slack_ops_webhook_url or "").strip() or None


def post_ops_message(
    message: str,
    *,
    webhook_url: str | None = None,
    post: Callable[..., Any] = requests.post,
) -> bool:
    """Post one plain-text message; never raise, never print the webhook URL."""

    url = webhook_url or ops_webhook_url()
    if url is None:
        print("Ops notification not sent: SLACK_OPS_WEBHOOK_URL is not set.", flush=True)
        return False
    try:
        response = post(url, json={"text": message}, timeout=REQUEST_TIMEOUT_SECONDS)
        response.raise_for_status()
    except requests.RequestException as exc:
        status = getattr(getattr(exc, "response", None), "status_code", None)
        print(
            f"Ops notification failed: {type(exc).__name__}"
            f"{f' (HTTP {status})' if status else ''}.",
            flush=True,
        )
        return False
    print("Ops notification sent.", flush=True)
    return True


class OpsNotifier:
    """Notify once when a problem starts and once when it clears.

    State is kept in a small JSON file so a scheduler restart does not repeat
    a message. If posting fails, the next report of the same problem retries.
    """

    def __init__(
        self,
        *,
        state_path: Path = DEFAULT_STATE_PATH,
        send: Callable[[str], bool] = post_ops_message,
        now: Callable[[], datetime] = lambda: datetime.now(timezone.utc),
    ) -> None:
        self.state_path = state_path
        self._send = send
        self._now = now
        self._lock = threading.Lock()

    def problem(self, kind: str, detail: str) -> bool:
        with self._lock:
            state = self._load()
            if state.get("problem") and state.get("notified"):
                return False
            since = state.get("since") or self._now().isoformat()
            kind = state.get("problem") or kind
            sent = self._send(
                f":warning: Shadow pipeline {kind} (since {since}). "
                f"{_shorten(detail)}"
            )
            self._save({"problem": kind, "since": since, "notified": sent})
            return sent

    def resolved(self, detail: str) -> bool:
        with self._lock:
            state = self._load()
            if not state.get("problem"):
                return False
            sent = True
            if state.get("notified"):
                sent = self._send(
                    f":white_check_mark: Shadow pipeline recovered from "
                    f"{state['problem']} (since {state['since']}). {_shorten(detail)}"
                )
            self._save({})
            return sent

    def _load(self) -> dict[str, Any]:
        try:
            return json.loads(self.state_path.read_text())
        except (OSError, ValueError):
            return {}

    def _save(self, state: dict[str, Any]) -> None:
        self.state_path.parent.mkdir(parents=True, exist_ok=True)
        temporary = self.state_path.with_suffix(".tmp")
        temporary.write_text(json.dumps(state))
        temporary.replace(self.state_path)


def _shorten(detail: str) -> str:
    detail = " ".join(detail.split())
    if len(detail) <= MAX_DETAIL_LENGTH:
        return detail
    return detail[: MAX_DETAIL_LENGTH - 3] + "..."


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Send operational notifications to SLACK_OPS_WEBHOOK_URL."
    )
    parser.add_argument(
        "--test",
        action="store_true",
        help="Send one test message to confirm the ops webhook works.",
    )
    args = parser.parse_args()
    if not args.test:
        parser.error("nothing to do; pass --test")
    sent = post_ops_message(
        ":wrench: Test message from the real-estate shadow pipeline ops notifier."
    )
    raise SystemExit(0 if sent else 1)


if __name__ == "__main__":
    main()
