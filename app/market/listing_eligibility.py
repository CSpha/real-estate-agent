from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal
from typing import Any

from app.normalization import normalize_property_type, normalize_status


AUTOMATED_PROPERTY_TYPES = frozenset(
    {"SingleFamilyResidence", "Condominium", "Townhouse"}
)
REVIEW_ONLY_PROPERTY_TYPES = frozenset({"Manufactured", "MultiFamily"})


@dataclass(frozen=True)
class ListingEligibility:
    status: str
    scoring_eligible: bool
    comparable_ready: bool
    reasons: tuple[str, ...]


def evaluate_listing_eligibility(record: dict[str, Any]) -> ListingEligibility:
    """Classify a listing for automated scoring without hiding reviewable data."""

    reasons: list[str] = []
    property_type = normalize_property_type(record.get("property_type"))
    status = normalize_status(record.get("status"))

    if property_type not in AUTOMATED_PROPERTY_TYPES:
        if property_type in REVIEW_ONLY_PROPERTY_TYPES:
            reasons.append(f"{property_type} needs a type-specific valuation")
            eligibility_status = "review"
        else:
            reasons.append(f"Unsupported property type: {property_type or 'missing'}")
            eligibility_status = "excluded"
        return ListingEligibility(
            status=eligibility_status,
            scoring_eligible=False,
            comparable_ready=False,
            reasons=tuple(reasons),
        )

    required = ("address", "city", "state", "zip", "list_price")
    missing = [field for field in required if record.get(field) in (None, "")]
    if missing:
        reasons.append("Missing required fields: " + ", ".join(missing))
        return ListingEligibility(
            status="excluded",
            scoring_eligible=False,
            comparable_ready=False,
            reasons=tuple(reasons),
        )

    if status != "Active":
        reasons.append(f"Listing status is {status or 'missing'}, not Active")
        return ListingEligibility(
            status="excluded",
            scoring_eligible=False,
            comparable_ready=False,
            reasons=tuple(reasons),
        )

    try:
        price = Decimal(str(record["list_price"]))
    except Exception:
        price = Decimal("0")
    if price <= 0:
        reasons.append("List price must be positive")
        return ListingEligibility(
            status="excluded",
            scoring_eligible=False,
            comparable_ready=False,
            reasons=tuple(reasons),
        )

    comparable_ready = record.get("sqft") not in (None, "")
    if not comparable_ready:
        reasons.append("Square footage is missing; county context only")
        return ListingEligibility(
            status="review",
            scoring_eligible=True,
            comparable_ready=False,
            reasons=tuple(reasons),
        )

    return ListingEligibility(
        status="eligible",
        scoring_eligible=True,
        comparable_ready=True,
        reasons=(),
    )
