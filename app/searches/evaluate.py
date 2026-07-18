from __future__ import annotations

import argparse

from app.searches.service import evaluate_saved_search


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Evaluate one saved search without sending notifications."
    )
    parser.add_argument("saved_search_id", type=int)
    parser.add_argument("--limit", type=int, default=1000)
    args = parser.parse_args()

    result = evaluate_saved_search(args.saved_search_id, limit=args.limit)
    print(result.model_dump_json(indent=2))


if __name__ == "__main__":
    main()
