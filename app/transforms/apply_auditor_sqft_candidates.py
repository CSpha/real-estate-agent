from __future__ import annotations

import argparse
import hashlib
import json
import re
from dataclasses import asdict, dataclass
from decimal import Decimal
from pathlib import Path
from typing import Any

from sqlalchemy import Engine, text

from app.db import get_engine


PROJECT_ROOT = Path(__file__).resolve().parents[2]
DEFAULT_CANDIDATE_PATH = PROJECT_ROOT / "data" / "sqft_auditor_candidates.md"
EVIDENCE_SOURCE = "wayne_county_auditor_property_record"

INSERT_EVIDENCE_SQL = text(
    """
    INSERT INTO listing_property_attribute_evidence (
        source, source_listing_id, attribute_name, numeric_value,
        evidence_source, source_record_id, match_method, verification_status,
        input_fingerprint, evidence_json
    ) VALUES (
        :source, :source_listing_id, 'sqft', :sqft,
        :evidence_source, :parcel_number, 'exact_normalized_address_single_dwelling',
        'verified', :input_fingerprint, CAST(:evidence_json AS JSONB)
    )
    ON CONFLICT ON CONSTRAINT uq_attribute_evidence_input DO NOTHING
    """
)

APPLY_SQFT_SQL = text(
    """
    UPDATE listings_current
    SET sqft = :sqft,
        price_per_sqft = ROUND(list_price / NULLIF(:sqft, 0), 2),
        updated_at = CURRENT_TIMESTAMP
    WHERE source = :source
      AND source_listing_id = :source_listing_id
      AND sqft IS NULL
    """
)


@dataclass(frozen=True)
class AuditorSqftCandidate:
    source_listing_id: str
    parcel_number: str
    sqft: Decimal
    year_built: int
    class_code: int


@dataclass(frozen=True)
class AuditorSqftApplyResult:
    candidates_read: int
    evidence_inserted: int
    listings_updated: int
    skipped_existing_sqft: int


def read_auditor_sqft_candidates(path: Path = DEFAULT_CANDIDATE_PATH) -> list[AuditorSqftCandidate]:
    """Read the reviewed, locally retained candidate table without scraping it again."""

    rows: list[AuditorSqftCandidate] = []
    for line in path.read_text(encoding="utf-8").splitlines():
        match = re.fullmatch(
            r"\| `(?P<listing>[^`]+)` \| `(?P<parcel>[^`]+)` \| (?P<sqft>[\d,]+) \| (?P<year>\d{4}) \| (?P<class>\d+) \|",
            line.strip(),
        )
        if match:
            rows.append(
                AuditorSqftCandidate(
                    source_listing_id=match["listing"],
                    parcel_number=match["parcel"],
                    sqft=Decimal(match["sqft"].replace(",", "")),
                    year_built=int(match["year"]),
                    class_code=int(match["class"]),
                )
            )
    if not rows:
        raise ValueError(f"No auditor square-footage candidates found in {path}")
    if len({row.source_listing_id for row in rows}) != len(rows):
        raise ValueError("Candidate file contains duplicate listing identifiers")
    if any(not 510 <= row.class_code < 520 for row in rows):
        raise ValueError("Candidates must be single-dwelling residential class 510-519")
    return rows


def _evidence_payload(candidate: AuditorSqftCandidate) -> tuple[str, str]:
    payload: dict[str, Any] = {
        "parcel_number": candidate.parcel_number,
        "living_area_sqft": str(candidate.sqft),
        "year_built": candidate.year_built,
        "class_code": candidate.class_code,
        "review_basis": "exact_normalized_address_match_and_one_dwelling",
    }
    encoded = json.dumps(payload, sort_keys=True, separators=(",", ":"))
    return encoded, hashlib.sha256(encoded.encode("utf-8")).hexdigest()


def apply_auditor_sqft_candidates(
    *,
    source: str = "rentcast",
    candidate_path: Path = DEFAULT_CANDIDATE_PATH,
    engine: Engine | None = None,
) -> AuditorSqftApplyResult:
    """Apply only reviewed, exact-match auditor living-area evidence to missing fields."""

    candidates = read_auditor_sqft_candidates(candidate_path)
    engine = engine or get_engine()
    evidence_inserted = 0
    listings_updated = 0
    skipped_existing_sqft = 0
    with engine.begin() as connection:
        for candidate in candidates:
            evidence_json, fingerprint = _evidence_payload(candidate)
            values = {
                "source": source,
                "source_listing_id": candidate.source_listing_id,
                "sqft": candidate.sqft,
                "evidence_source": EVIDENCE_SOURCE,
                "parcel_number": candidate.parcel_number,
                "input_fingerprint": fingerprint,
                "evidence_json": evidence_json,
            }
            evidence_inserted += max(connection.execute(INSERT_EVIDENCE_SQL, values).rowcount, 0)
            updated = max(connection.execute(APPLY_SQFT_SQL, values).rowcount, 0)
            listings_updated += updated
            skipped_existing_sqft += int(updated == 0)
    return AuditorSqftApplyResult(
        candidates_read=len(candidates),
        evidence_inserted=evidence_inserted,
        listings_updated=listings_updated,
        skipped_existing_sqft=skipped_existing_sqft,
    )


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Apply reviewed Wayne County Auditor square-footage evidence."
    )
    parser.add_argument("--source", default="rentcast")
    parser.add_argument("--candidate-file", type=Path, default=DEFAULT_CANDIDATE_PATH)
    args = parser.parse_args()
    result = apply_auditor_sqft_candidates(
        source=args.source,
        candidate_path=args.candidate_file,
    )
    print("Auditor square-footage evidence applied:")
    for key, value in asdict(result).items():
        print(f"  {key}: {value}")
    print("No provider requests or alerts were made.")


if __name__ == "__main__":
    main()
