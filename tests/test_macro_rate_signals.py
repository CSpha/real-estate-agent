import io
from decimal import Decimal

import pandas as pd
import pytest

from app.market.macro_rate_signals import (
    parse_fred_csv,
    parse_sme_workbook,
)


def test_parse_fred_csv_skips_missing_observations():
    records = parse_fred_csv(
        "DGS10",
        """observation_date,DGS10
2026-07-21,4.40
2026-07-22,.
2026-07-23,4.45
""",
    )

    assert len(records) == 2
    assert records[0]["series_id"] == "DGS10"
    assert records[0]["value"] == Decimal("4.40")
    assert records[1]["value"] == Decimal("4.45")


def test_parse_fred_csv_rejects_unknown_series():
    with pytest.raises(ValueError, match="Unsupported FRED series"):
        parse_fred_csv(
            "UNKNOWN",
            "observation_date,UNKNOWN\n2026-01-01,1",
        )


def test_parse_sme_workbook_keeps_combined_fed_funds_percentiles():
    frame = pd.DataFrame(
        [
            {
                "survey_release_date": "2026-06-03",
                "panel_type": "Combined",
                "subject": "fed_funds_target_range",
                "value_tag": "fftr_pathofmodes_20260729",
                "horizon_date": "2026-07-29",
                "aggregation": "count",
                "aggregation_value": 61,
            },
            {
                "survey_release_date": "2026-06-03",
                "panel_type": "Combined",
                "subject": "fed_funds_target_range",
                "value_tag": "fftr_pathofmodes_20260729",
                "horizon_date": "2026-07-29",
                "aggregation": "pctl50",
                "aggregation_value": 0.0363,
            },
            {
                "survey_release_date": "2026-06-03",
                "panel_type": "Primary Dealers",
                "subject": "fed_funds_target_range",
                "value_tag": "ignored",
                "horizon_date": "2026-07-29",
                "aggregation": "pctl50",
                "aggregation_value": 0.04,
            },
        ]
    )
    workbook = io.BytesIO()
    frame.to_excel(workbook, index=False)

    records = parse_sme_workbook(
        workbook.getvalue(),
        source_url="https://example.test/sme.xlsx",
    )

    assert len(records) == 1
    assert records[0]["aggregation"] == "pctl50"
    assert records[0]["value_percent"] == Decimal("3.6300")
    assert records[0]["respondent_count"] == 61
