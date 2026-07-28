from __future__ import annotations

import argparse
import csv
import io
from collections.abc import Iterable, Mapping
from dataclasses import dataclass
from datetime import date, datetime
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Any

import requests
from sqlalchemy import Engine, text

from app.db import get_engine


PMMS_HISTORY_URL = "https://www.freddiemac.com/pmms/docs/PMMS_history.csv"
PMMS_SOURCE = "freddie_mac_pmms"
PMMS_METHODOLOGY_CHANGE = date(2022, 11, 17)
PRODUCT_COLUMNS = {
    "30_year_fixed": ("pmms30", "pmms30p"),
    "15_year_fixed": ("pmms15", "pmms15p"),
}

UPSERT_RATE_SQL = text(
    """
    INSERT INTO mortgage_rate_observations (
        source,
        product_type,
        observation_date,
        rate_percent,
        points,
        methodology_version,
        source_url
    )
    VALUES (
        :source,
        :product_type,
        :observation_date,
        :rate_percent,
        :points,
        :methodology_version,
        :source_url
    )
    ON CONFLICT (source, product_type, observation_date)
    DO UPDATE SET
        rate_percent = EXCLUDED.rate_percent,
        points = EXCLUDED.points,
        methodology_version = EXCLUDED.methodology_version,
        source_url = EXCLUDED.source_url,
        ingested_at = CURRENT_TIMESTAMP
    WHERE ROW(
        mortgage_rate_observations.rate_percent,
        mortgage_rate_observations.points,
        mortgage_rate_observations.methodology_version,
        mortgage_rate_observations.source_url
    ) IS DISTINCT FROM ROW(
        EXCLUDED.rate_percent,
        EXCLUDED.points,
        EXCLUDED.methodology_version,
        EXCLUDED.source_url
    )
    """
)


@dataclass(frozen=True)
class MortgageRateTrend:
    product_type: str
    as_of_date: date
    current_rate: Decimal
    change_4_weeks: Decimal | None
    change_13_weeks: Decimal | None
    change_52_weeks: Decimal | None
    average_4_weeks: Decimal
    direction: str
    observation_count: int


def fetch_pmms_csv(url: str = PMMS_HISTORY_URL) -> str:
    response = requests.get(url, timeout=30)
    response.raise_for_status()
    return response.text


def load_pmms_csv(
    csv_text: str,
    engine: Engine | None = None,
) -> dict[str, int]:
    records = parse_pmms_csv(csv_text)
    engine = engine or get_engine()
    with engine.begin() as conn:
        result = conn.execute(UPSERT_RATE_SQL, records) if records else None

    changed = max(result.rowcount, 0) if result is not None else 0
    counts = {"received": len(records), "inserted_or_changed": changed}
    print(
        "Mortgage rate load complete: "
        f"{counts['received']} observations received, "
        f"{counts['inserted_or_changed']} inserted or changed."
    )
    return counts


def parse_pmms_csv(csv_text: str) -> list[dict[str, Any]]:
    reader = csv.DictReader(io.StringIO(csv_text))
    required = {"date", "pmms30", "pmms15"}
    missing = required.difference(reader.fieldnames or [])
    if missing:
        raise ValueError(f"PMMS CSV is missing columns: {sorted(missing)}")

    records: list[dict[str, Any]] = []
    for row_number, row in enumerate(reader, start=2):
        observation_date = _parse_date(row.get("date"), row_number)
        for product_type, (rate_column, points_column) in PRODUCT_COLUMNS.items():
            rate = _optional_decimal(row.get(rate_column), rate_column, row_number)
            if rate is None:
                continue
            records.append(
                {
                    "source": PMMS_SOURCE,
                    "product_type": product_type,
                    "observation_date": observation_date,
                    "rate_percent": rate,
                    "points": _optional_decimal(
                        row.get(points_column),
                        points_column,
                        row_number,
                    ),
                    "methodology_version": (
                        "application_based"
                        if observation_date >= PMMS_METHODOLOGY_CHANGE
                        else "lender_survey"
                    ),
                    "source_url": PMMS_HISTORY_URL,
                }
            )
    return records


def calculate_rate_trend(
    observations: Iterable[Mapping[str, Any]],
    *,
    product_type: str,
) -> MortgageRateTrend:
    ordered = sorted(
        (
            (
                item["observation_date"],
                Decimal(str(item["rate_percent"])),
            )
            for item in observations
            if item["product_type"] == product_type
        ),
        reverse=True,
    )
    if not ordered:
        raise ValueError(f"No observations found for {product_type}")

    current_date, current_rate = ordered[0]
    recent_rates = [rate for _, rate in ordered[:4]]
    change_4 = _change_from_index(ordered, current_rate, 4)
    direction = "roughly_stable"
    if change_4 is not None and change_4 > Decimal("0.125"):
        direction = "higher"
    elif change_4 is not None and change_4 < Decimal("-0.125"):
        direction = "lower"

    return MortgageRateTrend(
        product_type=product_type,
        as_of_date=current_date,
        current_rate=current_rate,
        change_4_weeks=change_4,
        change_13_weeks=_change_from_index(ordered, current_rate, 13),
        change_52_weeks=_change_from_index(ordered, current_rate, 52),
        average_4_weeks=sum(recent_rates) / len(recent_rates),
        direction=direction,
        observation_count=len(ordered),
    )


def get_rate_trend(
    product_type: str = "30_year_fixed",
    engine: Engine | None = None,
) -> MortgageRateTrend:
    engine = engine or get_engine()
    with engine.connect() as conn:
        rows = conn.execute(
            text(
                """
                SELECT product_type, observation_date, rate_percent
                FROM mortgage_rate_observations
                WHERE source = :source
                  AND product_type = :product_type
                ORDER BY observation_date DESC
                LIMIT 53
                """
            ),
            {"source": PMMS_SOURCE, "product_type": product_type},
        ).mappings().all()
    return calculate_rate_trend(rows, product_type=product_type)


def _parse_date(raw_value: Any, row_number: int) -> date:
    value = str(raw_value or "").strip()
    try:
        return datetime.strptime(value, "%m/%d/%Y").date()
    except ValueError as exc:
        raise ValueError(
            f"Invalid PMMS date at CSV row {row_number}: {value!r}"
        ) from exc


def _optional_decimal(
    raw_value: Any,
    column: str,
    row_number: int,
) -> Decimal | None:
    value = str(raw_value or "").strip()
    if not value:
        return None
    try:
        parsed = Decimal(value)
    except InvalidOperation as exc:
        raise ValueError(
            f"Invalid {column} at CSV row {row_number}: {value!r}"
        ) from exc
    if parsed < 0:
        raise ValueError(f"{column} cannot be negative at CSV row {row_number}")
    return parsed


def _change_from_index(
    ordered: list[tuple[date, Decimal]],
    current_rate: Decimal,
    weeks: int,
) -> Decimal | None:
    if len(ordered) <= weeks:
        return None
    return current_rate - ordered[weeks][1]


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Load Freddie Mac PMMS mortgage-rate history.",
    )
    parser.add_argument(
        "--file",
        type=Path,
        help="Read a local PMMS CSV instead of downloading the official file.",
    )
    args = parser.parse_args()
    csv_text = (
        args.file.read_text(encoding="utf-8-sig")
        if args.file
        else fetch_pmms_csv()
    )
    load_pmms_csv(csv_text)
    for product_type in PRODUCT_COLUMNS:
        trend = get_rate_trend(product_type)
        print(
            f"{product_type}: {trend.current_rate}% as of "
            f"{trend.as_of_date.isoformat()}, "
            f"4-week change {trend.change_4_weeks}, "
            f"direction {trend.direction}."
        )


if __name__ == "__main__":
    main()
