#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any

from strategy_registry import get_strategy_children, list_strategies


def export_registry(repo: str | None = None) -> dict[str, Any]:
    rows = []
    for strategy in list_strategies(repo=repo):
        item = dict(strategy)
        item["children"] = get_strategy_children(item["id"])
        rows.append(item)
    return {
        "version": 1,
        "repo_filter": repo or "all",
        "strategies": rows,
    }


def _main() -> int:
    ap = argparse.ArgumentParser(description="Export canonical strategy registry snapshot")
    ap.add_argument("--repo", default=None, help="Optional repo filter")
    ap.add_argument(
        "--output",
        default="contracts/strategy_registry_export.json",
        help="Output JSON path",
    )
    args = ap.parse_args()

    payload = export_registry(repo=args.repo)
    output_path = Path(args.output).expanduser().resolve()
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(json.dumps(payload, indent=2), encoding="utf-8")
    print(f"Wrote: {output_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(_main())
