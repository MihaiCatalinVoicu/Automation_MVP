from __future__ import annotations

import argparse
import json
from pathlib import Path

from db import init_db
from strategy_lifecycle import run_due_reviews

ROOT = Path(__file__).resolve().parent


def main() -> int:
    ap = argparse.ArgumentParser(description="Run automated strategy lifecycle reviews")
    ap.add_argument("--repo", default=None, help="Optional repo filter")
    ap.add_argument("--strategy-id", default=None, help="Optional single strategy review")
    ap.add_argument("--review-kind", default="daily", help="Review label written to artifacts")
    ap.add_argument(
        "--output-dir",
        default=str(ROOT / "data" / "strategy_reviews"),
        help="Where to store machine-readable review artifacts",
    )
    args = ap.parse_args()

    init_db()
    summary = run_due_reviews(
        output_dir=Path(args.output_dir),
        repo=args.repo,
        strategy_id=args.strategy_id,
        review_kind=args.review_kind,
    )
    print(json.dumps(summary, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
