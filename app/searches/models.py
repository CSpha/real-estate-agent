from __future__ import annotations

from datetime import datetime
from decimal import Decimal
from typing import Any, Literal

from pydantic import (
    BaseModel,
    ConfigDict,
    Field,
    field_validator,
    model_validator,
)

from app.normalization import (
    normalize_property_type,
    normalize_state,
    normalize_status,
    normalize_zip,
)


NotificationMode = Literal["immediate", "digest"]


class StrictModel(BaseModel):
    model_config = ConfigDict(extra="forbid")


class NumericRange(StrictModel):
    min: Decimal | None = Field(default=None, ge=0)
    max: Decimal | None = Field(default=None, ge=0)

    @model_validator(mode="after")
    def validate_bounds(self) -> NumericRange:
        if self.min is None and self.max is None:
            raise ValueError("At least one of min or max is required")
        if self.min is not None and self.max is not None and self.min > self.max:
            raise ValueError("min cannot be greater than max")
        return self


class LocationCriteria(StrictModel):
    state: str
    counties: list[str] = Field(default_factory=list)
    cities: list[str] = Field(default_factory=list)
    zips: list[str] = Field(default_factory=list)

    @field_validator("state", mode="before")
    @classmethod
    def validate_state(cls, value: Any) -> str:
        normalized = normalize_state(value)
        if normalized is None or len(normalized) != 2:
            raise ValueError("state must be a two-letter code")
        return normalized

    @field_validator("counties", "cities", mode="before")
    @classmethod
    def normalize_names(cls, value: Any) -> list[str]:
        if value is None:
            return []
        normalized = []
        for item in value:
            cleaned = " ".join(str(item).strip().split())
            if not cleaned:
                raise ValueError("location names cannot be empty")
            if cleaned.casefold() not in {entry.casefold() for entry in normalized}:
                normalized.append(cleaned)
        return normalized

    @field_validator("zips", mode="before")
    @classmethod
    def normalize_zips(cls, value: Any) -> list[str]:
        if value is None:
            return []
        normalized = []
        for item in value:
            cleaned = normalize_zip(item)
            if cleaned is None:
                raise ValueError("ZIP codes cannot be empty")
            if cleaned not in normalized:
                normalized.append(cleaned)
        return normalized


class PriceDropCriteria(StrictModel):
    min_amount: Decimal | None = Field(default=None, gt=0)
    min_percent: Decimal | None = Field(default=None, gt=0, le=100)

    @model_validator(mode="after")
    def require_threshold(self) -> PriceDropCriteria:
        if self.min_amount is None and self.min_percent is None:
            raise ValueError("At least one price-drop threshold is required")
        return self


class DealScoreCriteria(StrictModel):
    min: int = Field(ge=0, le=100)


class SearchCriteria(StrictModel):
    locations: list[LocationCriteria] = Field(default_factory=list)
    price: NumericRange | None = None
    beds: NumericRange | None = None
    baths: NumericRange | None = None
    sqft: NumericRange | None = None
    lot_size_acres: NumericRange | None = None
    property_types: list[str] = Field(default_factory=list)
    statuses: list[str] = Field(default_factory=list)
    max_days_on_market: int | None = Field(default=None, ge=0)
    price_drop: PriceDropCriteria | None = None
    deal_score: DealScoreCriteria | None = None

    @field_validator("property_types", mode="before")
    @classmethod
    def normalize_property_types(cls, value: Any) -> list[str]:
        if value is None:
            return []
        normalized = []
        for item in value:
            cleaned = normalize_property_type(item)
            if cleaned is None:
                raise ValueError("property types cannot be empty")
            if cleaned.casefold() not in {entry.casefold() for entry in normalized}:
                normalized.append(cleaned)
        return normalized

    @field_validator("statuses", mode="before")
    @classmethod
    def normalize_statuses(cls, value: Any) -> list[str]:
        if value is None:
            return []
        normalized = []
        for item in value:
            cleaned = normalize_status(item)
            if cleaned is None:
                raise ValueError("statuses cannot be empty")
            if cleaned.casefold() not in {entry.casefold() for entry in normalized}:
                normalized.append(cleaned)
        return normalized

    @model_validator(mode="after")
    def require_at_least_one_criterion(self) -> SearchCriteria:
        if not any(
            (
                self.locations,
                self.price is not None,
                self.beds is not None,
                self.baths is not None,
                self.sqft is not None,
                self.lot_size_acres is not None,
                self.property_types,
                self.statuses,
                self.max_days_on_market is not None,
                self.price_drop is not None,
                self.deal_score is not None,
            )
        ):
            raise ValueError("At least one search criterion is required")
        return self


class ListingCandidate(StrictModel):
    source: str
    source_listing_id: str
    address: str | None = None
    city: str | None = None
    county_name: str | None = None
    state: str | None = None
    zip: str | None = None
    list_price: Decimal | None = None
    previous_price: Decimal | None = None
    beds: Decimal | None = None
    baths: Decimal | None = None
    sqft: Decimal | None = None
    lot_size_acres: Decimal | None = None
    property_type: str | None = None
    status: str | None = None
    days_on_market: int | None = None
    market_score: int | None = None
    listing_state_timestamp: datetime


class EvaluationReason(StrictModel):
    criterion: str
    actual: Any
    expected: Any
    message: str


class CriteriaEvaluation(StrictModel):
    is_match: bool
    match_reasons: list[EvaluationReason]
    failed_reasons: list[EvaluationReason]


class SavedSearchCreate(StrictModel):
    name: str = Field(min_length=1, max_length=200)
    description: str | None = Field(default=None, max_length=2000)
    criteria: SearchCriteria
    enabled: bool = True
    notification_mode: NotificationMode = "immediate"
    cooldown_minutes: int = Field(default=0, ge=0, le=10080)

    @field_validator("name", mode="before")
    @classmethod
    def normalize_name(cls, value: Any) -> str:
        return _required_clean_text(value, "name")

    @field_validator("description", mode="before")
    @classmethod
    def normalize_description(cls, value: Any) -> str | None:
        return _optional_clean_text(value)


class SavedSearchUpdate(StrictModel):
    name: str | None = Field(default=None, min_length=1, max_length=200)
    description: str | None = Field(default=None, max_length=2000)
    criteria: SearchCriteria | None = None
    enabled: bool | None = None
    notification_mode: NotificationMode | None = None
    cooldown_minutes: int | None = Field(default=None, ge=0, le=10080)

    @field_validator("name", mode="before")
    @classmethod
    def normalize_name(cls, value: Any) -> str:
        return _required_clean_text(value, "name")

    @field_validator("description", mode="before")
    @classmethod
    def normalize_description(cls, value: Any) -> str | None:
        return _optional_clean_text(value)

    @model_validator(mode="after")
    def validate_patch(self) -> SavedSearchUpdate:
        if not self.model_fields_set:
            raise ValueError("At least one field must be provided")
        for field_name in (
            "name",
            "criteria",
            "enabled",
            "notification_mode",
            "cooldown_minutes",
        ):
            if (
                field_name in self.model_fields_set
                and getattr(self, field_name) is None
            ):
                raise ValueError(f"{field_name} cannot be null")
        return self


class SavedSearchRecord(StrictModel):
    id: int
    name: str
    description: str | None
    criteria: SearchCriteria
    criteria_version: int
    enabled: bool
    notification_mode: NotificationMode
    cooldown_minutes: int
    created_at: datetime
    updated_at: datetime


class ListingEvaluationRecord(StrictModel):
    id: int | None = None
    source: str
    source_listing_id: str
    address: str | None
    criteria_version: int
    listing_fingerprint: str
    event_fingerprint: str
    is_match: bool
    became_match: bool
    match_reasons: list[EvaluationReason]
    failed_reasons: list[EvaluationReason]
    listing_state_timestamp: datetime
    evaluated_at: datetime | None = None
    recorded: bool


class SearchEvaluationRun(StrictModel):
    saved_search_id: int
    criteria_version: int
    evaluated: int
    matched: int
    new_matches: int
    recorded: int
    results: list[ListingEvaluationRecord]


def _required_clean_text(value: Any, field_name: str) -> str:
    if value is None:
        raise ValueError(f"{field_name} cannot be null")
    cleaned = " ".join(str(value).strip().split())
    if not cleaned:
        raise ValueError(f"{field_name} cannot be empty")
    return cleaned


def _optional_clean_text(value: Any) -> str | None:
    if value is None:
        return None
    cleaned = " ".join(str(value).strip().split())
    return cleaned or None
