from __future__ import annotations

import argparse
import csv
import io
import re
from datetime import date, datetime, timedelta
from decimal import Decimal, InvalidOperation
from typing import Any
from urllib.parse import urljoin

import pandas as pd
import requests
from dateutil.relativedelta import relativedelta
from sqlalchemy import Engine, text

from app.db import get_engine


FRED_SOURCE = "fred"
FRED_CSV_URL = "https://fred.stlouisfed.org/graph/fredgraph.csv"
NY_FED_SME_SOURCE = "new_york_fed_sme"
SME_ARCHIVE_URL = (
    "https://www.newyorkfed.org/markets/market-intelligence/"
    "survey-of-market-expectations"
)
DEFAULT_SME_URL = (
    "https://www.newyorkfed.org/medialibrary/media/markets/survey/"
    "2026/jun-2026-data.xlsx"
)
FRED_SERIES = {
    "DGS2": {
        "units": "percent",
        "frequency": "daily",
        "retention_years": 3,
    },
    "DGS10": {
        "units": "percent",
        "frequency": "daily",
        "retention_years": 3,
    },
    "DFF": {
        "units": "percent",
        "frequency": "daily",
        "retention_years": 3,
    },
    "CPIAUCSL": {
        "units": "index_1982_1984_100",
        "frequency": "monthly",
        "retention_years": 10,
    },
}


def fetch_fred_csv(series_id: str) -> str:
    metadata = FRED_SERIES.get(series_id)
    if metadata is None:
        raise ValueError(f"Unsupported FRED series: {series_id}")
    start_date = date.today() - relativedelta(
        years=metadata["retention_years"],
    )
    response = requests.get(
        FRED_CSV_URL,
        params={
            "id": series_id,
            "cosd": start_date.isoformat(),
        },
        timeout=30,
    )
    response.raise_for_status()
    return response.text


def parse_fred_csv(series_id: str, csv_text: str) -> list[dict[str, Any]]:
    metadata = FRED_SERIES.get(series_id)
    if metadata is None:
        raise ValueError(f"Unsupported FRED series: {series_id}")
    reader = csv.DictReader(io.StringIO(csv_text))
    date_column = next(
        (
            column
            for column in ("observation_date", "DATE", "date")
            if column in (reader.fieldnames or [])
        ),
        None,
    )
    if date_column is None or series_id not in (reader.fieldnames or []):
        raise ValueError(
            f"FRED CSV for {series_id} must include a date and {series_id}"
        )

    records = []
    for row_number, row in enumerate(reader, start=2):
        raw_value = str(row.get(series_id) or "").strip()
        if not raw_value or raw_value == ".":
            continue
        try:
            value = Decimal(raw_value)
            observation_date = date.fromisoformat(str(row[date_column]).strip())
        except (InvalidOperation, ValueError) as exc:
            raise ValueError(
                f"Invalid {series_id} observation at CSV row {row_number}"
            ) from exc
        records.append(
            {
                "source": FRED_SOURCE,
                "series_id": series_id,
                "observation_date": observation_date,
                "value": value,
                "units": metadata["units"],
                "frequency": metadata["frequency"],
                "source_url": f"{FRED_CSV_URL}?id={series_id}",
            }
        )
    return records


def load_fred_series(
    series_id: str,
    csv_text: str,
    engine: Engine | None = None,
) -> dict[str, int]:
    records = parse_fred_csv(series_id, csv_text)
    metadata = FRED_SERIES[series_id]
    latest_date = max(
        (record["observation_date"] for record in records),
        default=date.today(),
    )
    retention_cutoff = latest_date - relativedelta(
        years=metadata["retention_years"],
    )
    records = [
        record
        for record in records
        if record["observation_date"] >= retention_cutoff
    ]
    engine = engine or get_engine()
    with engine.begin() as conn:
        deleted = conn.execute(
            text(
                """
                DELETE FROM macro_rate_observations
                WHERE source = :source
                  AND series_id = :series_id
                  AND observation_date < :retention_cutoff
                """
            ),
            {
                "source": FRED_SOURCE,
                "series_id": series_id,
                "retention_cutoff": retention_cutoff,
            },
        )
        result = (
            conn.execute(
                text(
                    """
                    INSERT INTO macro_rate_observations (
                        source,
                        series_id,
                        observation_date,
                        value,
                        units,
                        frequency,
                        source_url
                    )
                    VALUES (
                        :source,
                        :series_id,
                        :observation_date,
                        :value,
                        :units,
                        :frequency,
                        :source_url
                    )
                    ON CONFLICT (source, series_id, observation_date)
                    DO UPDATE SET
                        value = EXCLUDED.value,
                        units = EXCLUDED.units,
                        frequency = EXCLUDED.frequency,
                        source_url = EXCLUDED.source_url,
                        ingested_at = CURRENT_TIMESTAMP
                    WHERE ROW(
                        macro_rate_observations.value,
                        macro_rate_observations.units,
                        macro_rate_observations.frequency,
                        macro_rate_observations.source_url
                    ) IS DISTINCT FROM ROW(
                        EXCLUDED.value,
                        EXCLUDED.units,
                        EXCLUDED.frequency,
                        EXCLUDED.source_url
                    )
                    """
                ),
                records,
            )
            if records
            else None
        )
    changed = max(result.rowcount, 0) if result is not None else 0
    return {
        "received": len(records),
        "inserted_or_changed": changed,
        "expired_deleted": max(deleted.rowcount, 0),
    }


def fetch_sme_workbook(url: str = DEFAULT_SME_URL) -> bytes:
    response = requests.get(url, timeout=30)
    response.raise_for_status()
    return response.content


def fetch_sme_archive_urls(
    archive_url: str = SME_ARCHIVE_URL,
) -> list[str]:
    response = requests.get(archive_url, timeout=30)
    response.raise_for_status()
    hrefs = re.findall(
        r"""href=["']([^"']+-data\.xlsx)["']""",
        response.text,
        flags=re.IGNORECASE,
    )
    return sorted({urljoin(archive_url, href) for href in hrefs})


def parse_sme_workbook(
    workbook_bytes: bytes,
    *,
    source_url: str,
) -> list[dict[str, Any]]:
    frame = pd.read_excel(io.BytesIO(workbook_bytes))
    required = {
        "survey_release_date",
        "panel_type",
        "subject",
        "value_tag",
        "horizon_date",
        "aggregation",
        "aggregation_value",
    }
    missing = required.difference(frame.columns)
    if missing:
        raise ValueError(f"SME workbook is missing columns: {sorted(missing)}")

    selected = frame[
        (frame["panel_type"] == "Combined")
        & (frame["subject"] == "fed_funds_target_range")
        & (frame["aggregation"].isin(["pctl25", "pctl50", "pctl75", "count"]))
        & (frame["horizon_date"].notna())
    ].copy()
    if selected.empty:
        return []
    first_horizon_date = min(
        _as_date(value)
        for value in selected["horizon_date"]
    )
    public_availability_date = first_horizon_date + timedelta(days=21)
    records = []
    grouped = selected.groupby(
        [
            "survey_release_date",
            "panel_type",
            "subject",
            "value_tag",
            "horizon_date",
        ],
        dropna=False,
    )
    for keys, group in grouped:
        release_date, panel_type, subject, value_tag, horizon_date = keys
        count_rows = group[group["aggregation"] == "count"]
        respondent_count = (
            int(count_rows.iloc[0]["aggregation_value"])
            if not count_rows.empty
            else None
        )
        for aggregation in ("pctl25", "pctl50", "pctl75"):
            rows = group[group["aggregation"] == aggregation]
            if rows.empty:
                continue
            raw_value = Decimal(str(rows.iloc[0]["aggregation_value"]))
            records.append(
                {
                    "source": NY_FED_SME_SOURCE,
                    "release_date": max(
                        _as_date(release_date),
                        public_availability_date,
                    ),
                    "panel_type": str(panel_type),
                    "subject": str(subject),
                    "value_tag": str(value_tag),
                    "horizon_date": _as_date(horizon_date),
                    "aggregation": aggregation,
                    "value_percent": raw_value * Decimal("100"),
                    "respondent_count": respondent_count,
                    "source_url": source_url,
                }
            )
    return records


def load_sme_expectations(
    workbook_bytes: bytes,
    *,
    source_url: str = DEFAULT_SME_URL,
    engine: Engine | None = None,
) -> dict[str, int]:
    records = parse_sme_workbook(workbook_bytes, source_url=source_url)
    engine = engine or get_engine()
    with engine.begin() as conn:
        result = (
            conn.execute(
                text(
                    """
                    INSERT INTO fed_expectation_observations (
                        source,
                        release_date,
                        panel_type,
                        subject,
                        value_tag,
                        horizon_date,
                        aggregation,
                        value_percent,
                        respondent_count,
                        source_url
                    )
                    VALUES (
                        :source,
                        :release_date,
                        :panel_type,
                        :subject,
                        :value_tag,
                        :horizon_date,
                        :aggregation,
                        :value_percent,
                        :respondent_count,
                        :source_url
                    )
                    ON CONFLICT (
                        source,
                        panel_type,
                        value_tag,
                        aggregation
                    )
                    DO UPDATE SET
                        release_date = EXCLUDED.release_date,
                        horizon_date = EXCLUDED.horizon_date,
                        value_percent = EXCLUDED.value_percent,
                        respondent_count = EXCLUDED.respondent_count,
                        source_url = EXCLUDED.source_url,
                        ingested_at = CURRENT_TIMESTAMP
                    WHERE ROW(
                        fed_expectation_observations.horizon_date,
                        fed_expectation_observations.value_percent,
                        fed_expectation_observations.respondent_count,
                        fed_expectation_observations.source_url
                    ) IS DISTINCT FROM ROW(
                        EXCLUDED.horizon_date,
                        EXCLUDED.value_percent,
                        EXCLUDED.respondent_count,
                        EXCLUDED.source_url
                    )
                    """
                ),
                records,
            )
            if records
            else None
        )
    changed = max(result.rowcount, 0) if result is not None else 0
    return {"received": len(records), "inserted_or_changed": changed}


def load_current_macro_signals(
    *,
    sme_url: str = DEFAULT_SME_URL,
    engine: Engine | None = None,
) -> dict[str, dict[str, int]]:
    engine = engine or get_engine()
    results = {
        series_id: load_fred_series(
            series_id,
            fetch_fred_csv(series_id),
            engine,
        )
        for series_id in FRED_SERIES
    }
    results["NY_FED_SME"] = load_sme_expectations(
        fetch_sme_workbook(sme_url),
        source_url=sme_url,
        engine=engine,
    )
    return results


def load_sme_archive(
    *,
    archive_url: str = SME_ARCHIVE_URL,
    engine: Engine | None = None,
) -> dict[str, int]:
    engine = engine or get_engine()
    totals = {
        "workbooks": 0,
        "received": 0,
        "inserted_or_changed": 0,
    }
    for workbook_url in fetch_sme_archive_urls(archive_url):
        counts = load_sme_expectations(
            fetch_sme_workbook(workbook_url),
            source_url=workbook_url,
            engine=engine,
        )
        totals["workbooks"] += 1
        totals["received"] += counts["received"]
        totals["inserted_or_changed"] += counts["inserted_or_changed"]
    return totals


def _as_date(value: Any) -> date:
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, date):
        return value
    return date.fromisoformat(str(value)[:10])


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Load Treasury, inflation, and Fed-expectation signals.",
    )
    parser.add_argument("--sme-url", default=DEFAULT_SME_URL)
    parser.add_argument(
        "--sme-archive",
        action="store_true",
        help="Load every structured survey workbook linked by the archive.",
    )
    args = parser.parse_args()
    results = load_current_macro_signals(sme_url=args.sme_url)
    for name, counts in results.items():
        print(
            f"{name}: {counts['received']} received, "
            f"{counts['inserted_or_changed']} inserted or changed, "
            f"{counts.get('expired_deleted', 0)} expired rows deleted."
        )
    if args.sme_archive:
        archive_counts = load_sme_archive()
        print(
            "NY_FED_SME_ARCHIVE: "
            f"{archive_counts['workbooks']} workbooks, "
            f"{archive_counts['received']} observations received, "
            f"{archive_counts['inserted_or_changed']} inserted or changed."
        )


if __name__ == "__main__":
    main()
