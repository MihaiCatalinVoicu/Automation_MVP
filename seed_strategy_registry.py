from __future__ import annotations

import argparse

from db import init_db
from strategy_registry import upsert_strategy
from strategy_seed_data import SEED_STRATEGIES


def main() -> int:
    ap = argparse.ArgumentParser(description="Seed canonical strategy registry")
    ap.add_argument("--limit", type=int, default=0, help="Optional cap for smoke runs")
    args = ap.parse_args()

    init_db()
    rows = SEED_STRATEGIES[: args.limit] if args.limit > 0 else SEED_STRATEGIES
    for item in rows:
        upsert_strategy(item)
    print(f"Seeded {len(rows)} strategies into the registry.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
