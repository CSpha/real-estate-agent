# Scoring gap review

Reviewed the latest shadow scoring artifact at `data/shadow_scoring_review.csv` (98 rows; file timestamp 2026-09-11) and the eligibility and Deal Score v2 code. No database writes, provider refresh, or alert changes were made.

## Findings

| Check | Count | Interpretation |
| --- | ---: | --- |
| Listings reviewed | 98 | RentCast shadow review population |
| Comparable ready | 74 | Active supported property type with square footage |
| Comparable ready and valued | 73 | Supported valuation coverage: 73/74 (98.6%) |
| Comparable ready with no supported valuation | 1 | `ColumbusCORMLS:226017132`; no usable comparable candidates |
| Deal Score v2 complete | 73 | All six components available for valued listings |
| Deal Score unavailable | 1 | The unsupported valuation leaves the comparable discount component unavailable; coverage is 60% and the normalized score is 52.77 |
| Listings missing square footage | 17 | 8 eligible property-type listings are review-only county-context records; 9 are land/review-only property types |
| Listings missing beds | 7 | All are land or review-only property types; none is comparable ready |
| Listings missing baths | 8 | 7 are land/review-only property types; 1 comparable-ready SFR is still valued because square footage is the valuation prerequisite |

The single comparable-ready gap is the active 6-bedroom, 8.5-bath, 10,748-square-foot SFR at 254 W Wayne Ave, Wooster. Its candidate count is zero, status is `insufficient_comparables`, confidence is `very_low`, and no estimated value is produced. It is eligible for the policy but is marked high-priority manual review because comparable confidence is low. The correct action is to obtain or validate additional suitable sales; do not infer a value from the county median.

## Other coverage and component gaps

The 24 non-comparable-ready rows have no valuation or Deal Score record by design. They split into 8 supported property-type listings missing square footage (county context only), 6 land listings (excluded), 5 manufactured homes, and 5 multifamily homes (review-only pending type-specific valuation). The eight missing-square-footage supported rows are the actionable field-completeness backlog for the current valuation model.

Among the 74 comparable-ready rows, 73 have complete scores and one has an unavailable score. There are no partial component scores in the artifact. Comparable confidence is high for 7, medium for 50, low for 16 valued listings, and very low for the one unvalued listing. Low confidence is a review signal even when all score components are technically present: 17 rows carry that signal (16 valued plus the unsupported valuation).

## Validation note

The configured application connection was checked read-only against database `realestate_dev_20260908`. It independently returned 98 current RentCast listings, 74 valuation rows, 73 valued rows, and one `insufficient_comparables` row. The latest successful `shadow_pipeline_runs` record started 2026-09-09 01:26:39 UTC, finished 01:27:42 UTC, and records 74 comparable-ready listings and 73 supported valuations. The stored valuation calculation for the gap explicitly says that at least three comparable sales with price and square footage are required and contains an empty comparable set. No database writes were made.

## Recommended follow-up

1. Request square footage for the 8 supported-property listings in county-context-only status.
2. Source and validate property-level sales for 254 W Wayne Ave; keep it out of verified opportunity ranking while it has no supported valuation.
3. Add type-specific valuation coverage for manufactured and multifamily homes if those categories are in scope. Land remains excluded from residential scoring.
4. Treat the 16 low-confidence valued records as manual review candidates; their complete scores indicate component availability, not strong comparable evidence.
