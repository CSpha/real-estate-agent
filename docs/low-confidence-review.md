# Low-confidence valuation review

This read-only review covers each RentCast listing's latest stored valuation in
the configured application database after the September 15, 2026 shadow run.
It found 18 valued listings labeled `low` confidence. All reviewed valuations
have an analysis date of September 15. No database writes or provider calls were
made for this audit.

## Verified population

| Measure | Count |
| --- | ---: |
| Latest low-confidence valued listings | 18 |
| `strict_zip` selections | 12 |
| `broad_zip` selections | 5 |
| `city_fallback` selections | 1 |
| Three included comparables | 9 |
| Four included comparables | 4 |
| Five included comparables | 5 |

The exact listing set is retained in the configured database. This committed
report records aggregate evidence without exporting listing-level identifiers.

## Root causes

Every row has 4 of 10 completeness points. The stored selection evidence marks
year built, beds, and baths unavailable for every row. This affects all 18 and
is the most consistent confidence loss.

Secondary weaknesses overlap:

| Weak dimension | Listings |
| --- | ---: |
| Price-per-square-foot dispersion penalty of at least 10 points | 9 |
| Similarity points of 8 or fewer | 6 |
| Recency points of 8 or fewer | 5 |
| City fallback geography | 1 |

Nine valuations use only the minimum three included comparables, limiting their
sample-size score. Candidate scarcity is not the only problem: several listings
have larger candidate pools but retain only three to five records after selection.
Wide price-per-square-foot ranges, weak similarity, and older sales then reduce
confidence further.

## Follow-up

1. Add beds, baths, and year-built evidence to the comparable-sale source when
   an authoritative source supplies it. Preserve genuine nulls.
2. Review the nine high-dispersion cases for condition, acreage, property subtype,
   bundled sales, and outliers before treating their estimates as decision-grade.
3. Verify subject and comparable square footage, lot size, and property type for
   the six low-similarity cases.
4. Prioritize newer valid sales for the five low-recency cases and same-ZIP sales
   for the city-fallback case.
5. Keep all 18 in manual review. Complete Deal Score components show availability,
   not strong comparable evidence.

Validation queried the latest valuation per `(source, source_listing_id)` first,
then filtered on `confidence_label = 'low'`. This avoids counting historical low
valuations after a listing's current confidence improves.
