"""Derive rent-growth assumption candidates from market rent history (NOT IMPLEMENTED).

Deriving annual rent growth requires a time series of market rent for the same
market (for example monthly median rent by ZIP or county over at least 24
months, ideally 36+). The database does not have one:
`listing_rent_estimates` holds one point-in-time AVM estimate per listing from a
single as_of_date, and `county_sales`/`comparable_sales` are sale prices, not
rents. Growth computed from either would be invented, so this command refuses.

To enable it: ingest a market rent history source (e.g. RentCast market
statistics rent history, Zillow ZORI, or HUD fair market rents by year) into a
dated table, then implement the derivation here to write a NEW INACTIVE version
via `manage_rental_assumptions.create_version(..., evidence=...)`. It must never
activate a version automatically.
"""
from __future__ import annotations

import argparse
import sys

REFUSAL = (
    "Refusing to derive rental growth assumptions: no market rent history exists.\n"
    "Needed: a dated series of market (not per-listing) rent, e.g. monthly median rent per\n"
    "ZIP/county covering at least 24 months. Currently listing_rent_estimates has one\n"
    "AVM point estimate per listing from a single date, which cannot yield a growth rate.\n"
    "Edit assumptions manually with app.transforms.manage_rental_assumptions --create."
)


def main() -> int:
    argparse.ArgumentParser(description=__doc__.split("\n")[0]).parse_args()
    print(REFUSAL, file=sys.stderr)
    return 2


if __name__ == "__main__":
    raise SystemExit(main())
