"""Shared Slack formatting for listing alerts.

Messages carry Block Kit `blocks` for display plus a plain `text` fallback that
Slack uses for notifications and clients that cannot render blocks.
"""
from __future__ import annotations

from decimal import Decimal, InvalidOperation
from typing import Any, Callable, Mapping, Sequence
from urllib.parse import quote


# Zillow's address search opens the home's page when the address matches.
ZILLOW_SEARCH_URL = "https://www.zillow.com/homes/{}_rb/"
HEADER_MAX_LENGTH = 150
DIGEST_HEADER = ":house: {count} listing alerts"


def to_decimal(value: Any) -> Decimal | None:
    # Outbox payloads store numbers as strings; direct events use Decimal.
    if value is None or value == "None" or value == "":
        return None
    try:
        return Decimal(str(value))
    except InvalidOperation:
        return None


def money(value: Any) -> str:
    """Format whole dollars with a leading sign: $324,900 or -$29,900."""
    amount = to_decimal(value)
    if amount is None:
        return "N/A"
    sign = "-" if amount < 0 else ""
    return f"{sign}${abs(amount):,.0f}"


def percent(value: Any, *, fraction: bool = False) -> str:
    """Format a percent with two decimals; `fraction` means 0.25 is 25%."""
    amount = to_decimal(value)
    if amount is None:
        return "N/A"
    if fraction:
        amount *= 100
    return f"{amount:.2f}%"


def full_address(event: Mapping[str, Any]) -> str:
    parts = [event.get("address"), event.get("city"), event.get("state")]
    return ", ".join(str(part) for part in parts if part) or "Unknown address"


def zillow_url(event: Mapping[str, Any]) -> str:
    """Zillow address-search link: 3763-Bates-Rd,-Wooster,-OH_rb."""
    slug = "-".join(full_address(event).split())
    return ZILLOW_SEARCH_URL.format(quote(slug, safe=",-"))


def escape(value: Any) -> str:
    """Escape the three characters Slack mrkdwn treats as control characters."""
    return (
        str(value).replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")
    )


def address_link(event: Mapping[str, Any]) -> str:
    return f"<{zillow_url(event)}|{escape(full_address(event))}>"


def header_block(text: str) -> dict[str, Any]:
    if len(text) > HEADER_MAX_LENGTH:
        text = text[: HEADER_MAX_LENGTH - 3] + "..."
    return {"type": "header", "text": {"type": "plain_text", "text": text, "emoji": True}}


def section_block(text: str) -> dict[str, Any]:
    return {"type": "section", "text": {"type": "mrkdwn", "text": text}}


def fields_block(fields: Sequence[tuple[str, str]]) -> dict[str, Any]:
    return {
        "type": "section",
        "fields": [
            {"type": "mrkdwn", "text": f"*{label}*\n{value}"} for label, value in fields
        ],
    }


def context_block(text: str) -> dict[str, Any]:
    return {"type": "context", "elements": [{"type": "mrkdwn", "text": text}]}


def build_digest_message(
    items: Sequence[tuple[str, Mapping[str, Any]]],
    summarizers: Mapping[str, Callable[[Mapping[str, Any]], tuple[str, str]]],
) -> dict[str, Any]:
    """Combine several alerts into one message, one line per alert.

    Each summarizer returns (mrkdwn line, plain-text line) for its alert type.
    """
    lines = [summarizers[alert_type](payload) for alert_type, payload in items]
    title = DIGEST_HEADER.format(count=len(items))
    return {
        "text": "\n".join([title.replace(":house: ", "")] + [plain for _, plain in lines]),
        "blocks": [
            header_block(title),
            *[section_block(rich) for rich, _ in lines],
            context_block("Sent as one digest because several alerts were due at once."),
        ],
    }
