# Wayne County verified-sales source investigation

Reviewed September 11, 2026. Status: official validity evidence exists, but a
usable transaction-level export and the meaning of Valid are not yet verified.

## Findings

- The auditor publishes weekly Valid Sales reports, with historical date links
  covering the required 24-month lookback:
  https://waynecountyauditor.org/SalesReport/DateRanges
- A sample report for August 30 through September 5, 2026 exposes columns for
  parcel, date, prior/new owner, address, acres, and amount. The web reader did
  not expose parcel values (rendered as input controls). The report does not
  provide living area, year built, property class, or a transaction identifier.
  https://waynecountyauditor.org/SalesReport/Tabular?end=9%2F5%2F2026&start=8%2F30%2F2026
- Advanced Search exposes a Valid Sale filter:
  https://waynecountyauditor.org/Search
- Sales Report advertises Export CSV, but the visible page is an aggregate
  price-range report. Its CSV contents have NOT been downloaded or verified;
  do not assume this is a transaction-level valid-sales export.
  https://waynecountyauditor.org/SalesReport
- Direct requests to the report and tabular pages returned HTTP 403. No access
  control workaround was attempted. A supported download or supplied file is
  preferable to building automated ingestion against these pages.
- The current ArcGIS layer metadata was fetched successfully. It contains
  Parcel, PPAmount, PPSaleDate, PPLivingArea, PPYearBuilt, PPClassCode and other
  parcel attributes, but no explicit sale-validity or arm's-length field.
  https://services6.arcgis.com/WiOy9S7NUTWyXUe4/arcgis/rest/services/parcel_joined/FeatureServer/0?f=pjson
- The official GIS information page lists phone (330) 287-5411 as a data contact:
  https://waynecountyauditor.org/Posts?category=GIS+Information

## Recommended integration

Obtain a sample official transaction-level CSV and data dictionary before
implementing a parser. Request 24 months of records and a supported refresh
route. Confirm whether Valid specifically represents arm's-length eligibility;
county-valid and arms_length must not be treated as synonyms without evidence.

Match validity evidence to existing comparable sales using normalized parcel
ID (preserving leading zeros), exact sale date, and exact amount. Parcel alone
is insufficient because a property can transfer more than once. Keep missing,
ambiguous and mismatched evidence unverified, and preserve bundle exclusions.
Do not assume absence from a positive-only report means an invalid sale.

Retain raw export evidence, file hash, retrieval date, source sale identifier,
validity code and documented meaning. Resolve superseded/revised transactions
explicitly. Use a separate verified source/version and avoid counting the same
sale in both provisional and verified sources. Existing valuations must remain
immutable; create new snapshots when evidence changes.

Before promotion, audit exact-match coverage, mismatches, multi-parcel sales,
and changed valuation coverage in shadow mode. The scheduler readiness checks
still do not establish source validity by themselves.

## Draft request — not sent

Subject: Wayne County residential sales export and validity definitions

Hello,

I am building an internal residential property research tool for Wayne County.
Your website provides Valid Sales reports and a Valid Sale search filter. Could
you direct me to an official CSV or other machine-readable export covering the
most recent 24 months, plus an approved way to refresh it periodically?

The fields needed are parcel number, sale date, sale amount, sale/transfer ID,
validity flag and reason code, and any multi-parcel transaction identifier or
parcel count. Property class, living area and year built would also help.
Buyer and seller names are not required.

Could you also provide the data dictionary, explain whether Valid indicates an
arm's-length sale suitable for valuation, and describe how bundled sales,
corrections and later validity changes are represented? Please let me know any
fees and conditions for internal storage and recurring retrieval.

Thank you.

## Follow-up access check

A browser-interface attempt also could not proceed: no controllable browser is
available in this session. The web reader can display the Sales Report page,
but does not expose the Export CSV form action or downloadable file contents.
No sample CSV has been obtained, and no request has been sent to the auditor.

To unblock sample validation, open https://waynecountyauditor.org/SalesReport
in a normal browser, choose a completed week (for example August 30 through
September 5, 2026), and use Export CSV. Supply the downloaded file for inspection;
it may be an aggregate report rather than individual sales. If it contains only
price-range summaries, use the request draft above to ask for transaction-level
records and the validity definitions. Do not build a production parser around
an assumed format.
