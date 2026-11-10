"""Shared Slack formatting for listing alerts.

Messages carry Block Kit `blocks` for display plus a plain `text` fallback that
Slack uses for notifications and clients that cannot render blocks.
"""
from __future__ import annotations

from datetime import date
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


def _rate(value: Any) -> str:
    amount = to_decimal(value)
    return "N/A" if amount is None else f"{amount:.2f}%"


def _as_of(value: Any) -> str:
    # "2026-11-05" -> "Nov 5"; anything unparseable is shown as stored.
    try:
        return date.fromisoformat(str(value)).strftime("%b %d").replace(" 0", " ")
    except ValueError:
        return str(value)


def affordability_lines(event: Mapping[str, Any]) -> list[str]:
    """Plain-text monthly cost and rent coverage, or [] when not computed."""
    info = event.get("affordability")
    if not info:
        return []
    lines = [
        f"Est. monthly cost at the {_rate(info['rate_pct'])} 30-year PMMS rate "
        f"(as of {_as_of(info['rate_as_of'])}; {_rate(info['property_tax_rate_pct'])} "
        f"tax, {_rate(info['insurance_rate_pct'])} insurance):",
        f"Conventional: {money(info['conventional_10_down'])} with 10% down "
        f"(incl. PMI), {money(info['conventional_20_down'])} with 20% down",
        f"DSCR loan, 20% down at {_rate(info['dscr_rate_pct'])}: "
        f"{money(info['dscr_20_down'])}",
    ]
    target = to_decimal(info["dscr_target"])
    needed = f"{money(info['rent_for_target'])}/mo rent needed for {target} DSCR"
    if info.get("monthly_rent") is None:
        lines.append(f"No rent estimate on file; {needed}")
    else:
        lines.append(
            f"Rent est. {money(info['monthly_rent'])}/mo, DSCR {info['dscr']}; {needed}"
        )
    return lines


def affordability_block(event: Mapping[str, Any]) -> dict[str, Any] | None:
    lines = affordability_lines(event)
    if not lines:
        return None
    title, *rest = lines
    return section_block("\n".join([f"*{escape(title)}*", *(escape(line) for line in rest)]))


def affordability_brief(event: Mapping[str, Any]) -> str:
    """Digest suffix such as ", est. $2,119/mo with 20% down, DSCR 0.63"."""
    info = event.get("affordability")
    if not info:
        return ""
    brief = f", est. {money(info['conventional_20_down'])}/mo with 20% down"
    if info.get("dscr") is not None:
        brief += f", DSCR {info['dscr']}"
    return brief
