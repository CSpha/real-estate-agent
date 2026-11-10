"""Slack incoming-webhook delivery with explicit failure classification."""
from __future__ import annotations

import re
from typing import Any
from urllib.parse import urlsplit

import requests


# Slack truncates message text longer than 4,000 characters.
MAX_TEXT_LENGTH = 4000
# Block Kit messages allow at most 50 blocks.
MAX_BLOCKS = 50
REQUEST_TIMEOUT_SECONDS = 30

# Webhook-level errors mean every later message would fail the same way.
# https://docs.slack.dev/messaging/sending-messages-using-incoming-webhooks
WEBHOOK_ERROR_CODES = {
    "invalid_token",
    "no_service",
    "no_service_id",
    "no_team",
    "team_disabled",
    "action_prohibited",
    "channel_not_found",
    "channel_is_archived",
    "posting_to_general_channel_denied",
}


# requests puts the full URL in connection-error text, and a Slack webhook URL
# is a secret, so error text is scrubbed before it is stored, logged, or posted.
MAX_ERROR_DETAIL_LENGTH = 300
_URL_PATTERN = re.compile(r"https?://\S+")
_URL_PATH_PATTERN = re.compile(r"(?:url:\s*)?/services/\S*|url:\s*/\S*")


def redact_url_text(text: str, url: str | None = None) -> str:
    """Remove a URL, its path, and any URL- or /services/-style path from text."""
    if url:
        text = text.replace(url, "<url>")
        path = urlsplit(url).path
        if len(path) > 1:
            text = text.replace(path, "<path>")
    text = _URL_PATTERN.sub("<url>", text)
    text = _URL_PATH_PATTERN.sub("<path>", text)
    return " ".join(text.split())[:MAX_ERROR_DETAIL_LENGTH]


class SlackDeliveryError(Exception):
    """A classified Slack delivery failure."""

    def __init__(
        self,
        message: str,
        *,
        retryable: bool,
        webhook_unusable: bool = False,
        rate_limited: bool = False,
        retry_after_seconds: float | None = None,
    ) -> None:
        super().__init__(message)
        self.retryable = retryable
        self.webhook_unusable = webhook_unusable
        self.rate_limited = rate_limited
        self.retry_after_seconds = retry_after_seconds


def validate_payload(payload: dict[str, Any]) -> None:
    """Reject payloads Slack would refuse, without retrying them."""
    text = payload.get("text") if isinstance(payload, dict) else None
    if not isinstance(text, str) or not text.strip():
        raise SlackDeliveryError("Slack payload requires non-empty text", retryable=False)
    if len(text) > MAX_TEXT_LENGTH:
        raise SlackDeliveryError(
            f"Slack payload text exceeds {MAX_TEXT_LENGTH} characters",
            retryable=False,
        )
    blocks = payload.get("blocks")
    if blocks is not None and (
        not isinstance(blocks, list) or not 0 < len(blocks) <= MAX_BLOCKS
    ):
        raise SlackDeliveryError(
            f"Slack payload blocks must be a list of 1-{MAX_BLOCKS} blocks",
            retryable=False,
        )


def _retry_after(response: requests.Response) -> float | None:
    raw = response.headers.get("Retry-After")
    try:
        return max(float(raw), 0.0) if raw is not None else None
    except ValueError:
        return None


def send_to_slack(
    webhook_url: str,
    payload: dict[str, Any],
    *,
    post=requests.post,
) -> None:
    validate_payload(payload)
    try:
        response = post(webhook_url, json=payload, timeout=REQUEST_TIMEOUT_SECONDS)
    except requests.RequestException as exc:
        detail = redact_url_text(str(exc), webhook_url)
        raise SlackDeliveryError(
            f"Slack request failed: {type(exc).__name__}: {detail}", retryable=True
        ) from None

    if 200 <= response.status_code < 300:
        return

    body = (response.text or "").strip()[:200]
    message = f"Slack returned HTTP {response.status_code}: {body or 'no body'}"
    if response.status_code == 429:
        raise SlackDeliveryError(
            message,
            retryable=True,
            rate_limited=True,
            retry_after_seconds=_retry_after(response),
        )
    if response.status_code >= 500:
        raise SlackDeliveryError(message, retryable=True)
    raise SlackDeliveryError(
        message,
        retryable=False,
        webhook_unusable=body in WEBHOOK_ERROR_CODES
        or response.status_code in {401, 403, 404, 410},
    )
