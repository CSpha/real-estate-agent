from __future__ import annotations

import argparse
import json
from datetime import date, datetime
from typing import Any

from sqlalchemy import Engine, text

from app.db import get_engine


SUMMARY_SQL = text(
    """
    SELECT
        source,
        stage,
        error_code,
        error_message,
        COUNT(*) AS record_count,
        MIN(detected_at) AS first_detected_at,
        MAX(detected_at) AS last_detected_at
    FROM ingest_errors
    WHERE (:source IS NULL OR source = :source)
    GROUP BY source, stage, error_code, error_message
    ORDER BY source, stage, record_count DESC, error_code
    """
)


def summarize_ingest_errors(
    *, source: str | None = None, engine: Engine | None = None
) -> list[dict[str, Any]]:
    engine = engine or get_engine()
    with engine.connect() as connection:
        return [
            dict(row)
            for row in connection.execute(SUMMARY_SQL, {"source": source}).mappings()
        ]


def _json_default(value: Any) -> str:
    if isinstance(value, (date, datetime)):
        return value.isoformat()
    raise TypeError(f"Cannot serialize {type(value).__name__}")


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Summarize stored provider ingest errors without raw payloads."
    )
    parser.add_argument("--source")
    args = parser.parse_args()
    print(
        json.dumps(
            summarize_ingest_errors(source=args.source),
            default=_json_default,
            indent=2,
        )
    )


if __name__ == "__main__":
    main()
