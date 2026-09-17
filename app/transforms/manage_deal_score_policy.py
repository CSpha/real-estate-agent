"""Create, inspect, and activate validated Deal Score threshold policies."""
from __future__ import annotations

import argparse
import json

from sqlalchemy import text

from app.market.deal_score_policy import validate_thresholds
from app.utils.db import get_engine


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    actions = parser.add_mutually_exclusive_group(required=True)
    actions.add_argument("--show-active", action="store_true")
    actions.add_argument("--activate", metavar="POLICY_VERSION")
    actions.add_argument("--create", metavar="POLICY_VERSION")
    parser.add_argument("--thresholds-file", help="JSON file required with --create")
    args = parser.parse_args()
    if args.thresholds_file and not args.create:
        parser.error("--thresholds-file is only valid with --create")
    if args.create and not args.thresholds_file:
        parser.error("--create requires --thresholds-file")

    engine = get_engine()
    if args.show_active:
        with engine.connect() as connection:
            row = connection.execute(text("SELECT policy_version, thresholds FROM deal_score_policies WHERE is_active")).mappings().one()
        print(json.dumps(dict(row), indent=2, default=str))
        return
    if args.create:
        with open(args.thresholds_file, encoding="utf-8") as handle:
            thresholds = validate_thresholds(json.load(handle))
        with engine.begin() as connection:
            connection.execute(text("INSERT INTO deal_score_policies (policy_version, thresholds, is_active) VALUES (:version, CAST(:thresholds AS JSONB), FALSE)"), {"version": args.create, "thresholds": json.dumps(thresholds)})
        print(f"Created inactive Deal Score policy: {args.create}")
        return
    with engine.begin() as connection:
        exists = connection.execute(text("SELECT 1 FROM deal_score_policies WHERE policy_version = :version"), {"version": args.activate}).scalar()
        if not exists:
            raise SystemExit(f"Unknown Deal Score policy: {args.activate}")
        connection.execute(text("UPDATE deal_score_policies SET is_active = FALSE WHERE is_active"))
        connection.execute(text("UPDATE deal_score_policies SET is_active = TRUE WHERE policy_version = :version"), {"version": args.activate})
    print(f"Activated Deal Score policy: {args.activate}")


if __name__ == "__main__":
    main()
