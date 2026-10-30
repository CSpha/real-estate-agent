from __future__ import annotations

import hashlib
import json
from decimal import Decimal, InvalidOperation
from typing import Any, Callable

from sqlalchemy import Engine, text

from app.normalization import normalize_listing


NORMALIZATION_STAGE = "normalization"
MAX_ERROR_MESSAGE_LENGTH = 500
NUMERIC_FIELDS = (
    "list_price",
    "beds",
    "baths",
    "sqft",
    "lot_size_acres",
    "price_per_sqft",
)

INGEST_ERROR_INSERT_SQL = text(
    """
    INSERT INTO ingest_errors (
        source,
        stage,
        source_record_id,
        payload_hash,
        error_code,
        error_message,
        raw_record_json
    )
    VALUES (
        :source,
        :stage,
        :source_record_id,
        :payload_hash,
        :error_code,
        :error_message,
        CAST(:raw_record_json AS JSONB)
    )
    ON CONFLICT (source, stage, payload_hash, error_code)
    DO NOTHING
    """
)

SQLITE_INGEST_ERROR_INSERT_SQL = text(
    """
    INSERT INTO ingest_errors (
        source,
        stage,
        source_record_id,
        payload_hash,
        error_code,
        error_message,
        raw_record_json
    )
    VALUES (
        :source,
        :stage,
        :source_record_id,
        :payload_hash,
        :error_code,
        :error_message,
        :raw_record_json
    )
    ON CONFLICT (source, stage, payload_hash, error_code)
    DO NOTHING
    """
)


def _check_numeric_fields(listing: dict[str, Any]) -> None:
    """Reject values Postgres would refuse, which would fail the whole batch."""

    for field in NUMERIC_FIELDS:
        value = listing.get(field)
        if value is None or isinstance(value, bool):
            continue
        if not Decimal(str(value)).is_finite():
            raise InvalidOperation(f"{field} is not finite")


def ingest_error_code(exc: Exception) -> str:
    if isinstance(exc, InvalidOperation):
        return "invalid_numeric"
    message = str(exc)
    if "no stable identifier" in message or "source_listing_id cannot be empty" in message:
        return "missing_identifier"
    if "isoformat" in message:
        return "invalid_date"
    return "normalization_error"


def prepare_ingest_error(
    source: str,
    raw_record: Any,
    exc: Exception,
) -> dict[str, Any]:
    encoded = json.dumps(
        raw_record,
        default=str,
        sort_keys=True,
        separators=(",", ":"),
        allow_nan=False,
    )
    record_id = raw_record.get("id") if isinstance(raw_record, dict) else None
    message = f"{type(exc).__name__}: {exc}"[:MAX_ERROR_MESSAGE_LENGTH]
    return {
        "source": source,
        "stage": NORMALIZATION_STAGE,
        "source_record_id": str(record_id) if record_id not in (None, "") else None,
        "payload_hash": hashlib.sha256(encoded.encode("utf-8")).hexdigest(),
        "error_code": ingest_error_code(exc),
        "error_message": message,
        "raw_record_json": encoded,
    }


def normalize_or_quarantine(
    raw_records: list[Any],
    normalize: Callable[[dict[str, Any]], dict[str, Any]],
    *,
    source: str,
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    """Split a provider page into loadable records and ingest-error rows.

    Each record is also run through the shared listing normalizer so a record
    that would abort the batch insert is set aside here instead.
    """
    records: list[dict[str, Any]] = []
    errors: list[dict[str, Any]] = []
    for raw_record in raw_records:
        try:
            if not isinstance(raw_record, dict):
                raise ValueError("Provider record is not an object")
            record = normalize(raw_record)
            _check_numeric_fields(normalize_listing(record))
        except (ValueError, TypeError, ArithmeticError) as exc:
            errors.append(prepare_ingest_error(source, raw_record, exc))
            continue
        records.append(record)
    return records, errors


def record_ingest_errors(errors: list[dict[str, Any]], engine: Engine) -> int:
    if not errors:
        return 0
    statement = (
        SQLITE_INGEST_ERROR_INSERT_SQL
        if engine.dialect.name == "sqlite"
        else INGEST_ERROR_INSERT_SQL
    )
    with engine.begin() as connection:
        result = connection.execute(statement, errors)
    inserted = max(result.rowcount, 0)
    print(
        f"Quarantined {len(errors)} provider record(s) in ingest_errors "
        f"({inserted} new)."
    )
    return inserted
