"""Create, inspect, and activate validated rental forecast assumption versions."""
from __future__ import annotations

import argparse
import json

from sqlalchemy import text

from app.market.rental_assumptions import validate_assumptions
from app.utils.db import get_engine


def create_version(connection, version: str, assumptions: dict, evidence: dict | None = None) -> None:
    normalized = validate_assumptions(assumptions)
    connection.execute(
        text(
            "INSERT INTO rental_assumption_policies (policy_version, assumptions, evidence, is_active) "
            "VALUES (:version, CAST(:assumptions AS JSONB), CAST(:evidence AS JSONB), FALSE)"
        ),
        {"version": version, "assumptions": json.dumps(normalized),
         "evidence": json.dumps(evidence) if evidence is not None else None},
    )


def activate_version(connection, version: str) -> None:
    exists = connection.execute(
        text("SELECT 1 FROM rental_assumption_policies WHERE policy_version = :version"), {"version": version}
    ).scalar()
    if not exists:
        raise ValueError(f"Unknown rental assumption version: {version}")
    connection.execute(text("UPDATE rental_assumption_policies SET is_active = FALSE WHERE is_active"))
    connection.execute(
        text("UPDATE rental_assumption_policies SET is_active = TRUE WHERE policy_version = :version"),
        {"version": version},
    )


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    actions = parser.add_mutually_exclusive_group(required=True)
    actions.add_argument("--show-active", action="store_true")
    actions.add_argument("--activate", metavar="POLICY_VERSION")
    actions.add_argument("--create", metavar="POLICY_VERSION")
    parser.add_argument("--assumptions-file", help="JSON file required with --create")
    args = parser.parse_args()
    if args.assumptions_file and not args.create:
        parser.error("--assumptions-file is only valid with --create")
    if args.create and not args.assumptions_file:
        parser.error("--create requires --assumptions-file")

    engine = get_engine()
    if args.show_active:
        with engine.connect() as connection:
            row = connection.execute(text(
                "SELECT policy_version, assumptions FROM rental_assumption_policies WHERE is_active"
            )).mappings().one()
        print(json.dumps(dict(row), indent=2, default=str))
    elif args.create:
        with open(args.assumptions_file, encoding="utf-8") as handle:
            assumptions = json.load(handle)
        with engine.begin() as connection:
            create_version(connection, args.create, assumptions)
        print(f"Created inactive rental assumption version: {args.create}")
    else:
        try:
            with engine.begin() as connection:
                activate_version(connection, args.activate)
        except ValueError as exc:
            raise SystemExit(str(exc)) from exc
        print(f"Activated rental assumption version: {args.activate}")


if __name__ == "__main__":
    main()
