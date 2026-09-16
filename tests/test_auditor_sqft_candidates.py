from decimal import Decimal

import pytest

from app.transforms.apply_auditor_sqft_candidates import read_auditor_sqft_candidates


def test_reads_reviewed_single_dwelling_candidates(tmp_path):
    path = tmp_path / "candidates.md"
    path.write_text(
        "| `MLSNOW:1` | `01-00001.000` | 1,234 | 1975 | 510 |\n",
        encoding="utf-8",
    )

    assert read_auditor_sqft_candidates(path)[0].sqft == Decimal("1234")


def test_rejects_non_single_dwelling_candidate(tmp_path):
    path = tmp_path / "candidates.md"
    path.write_text(
        "| `MLSNOW:1` | `01-00001.000` | 1,234 | 1975 | 530 |\n",
        encoding="utf-8",
    )

    with pytest.raises(ValueError, match="single-dwelling"):
        read_auditor_sqft_candidates(path)
