from __future__ import annotations

import argparse
import json

from db import init_db
from repo_registry import RepoRegistry
from strategy_registry import preflight_cross_reference


def main() -> int:
    ap = argparse.ArgumentParser(description="Preflight strategy cross-reference gate")
    ap.add_argument("--repo", required=True)
    ap.add_argument("--goal", required=True)
    ap.add_argument("--task-type", default="bugfix")
    ap.add_argument("--strategy-id", default=None)
    ap.add_argument("--category-id", default=None)
    ap.add_argument("--change-kind", default="code_change")
    ap.add_argument("--new-strategy-proposal", default=None)
    ap.add_argument("--constraints", nargs="*", default=[])
    ap.add_argument("--checks", nargs="*", default=[])
    args = ap.parse_args()

    init_db()
    repo_cfg = RepoRegistry().get(args.repo)
    task = {
        "repo": args.repo,
        "goal": args.goal,
        "task_type": args.task_type,
        "strategy_id": args.strategy_id,
        "category_id": args.category_id,
        "change_kind": args.change_kind,
        "new_strategy_proposal": args.new_strategy_proposal,
        "constraints": args.constraints,
        "checks": args.checks,
    }
    result = preflight_cross_reference(task, repo_cfg)
    print(
        json.dumps(
            {
                "decision": result.decision,
                "reason": result.reason,
                "strategy_id": result.strategy_id,
                "category_id": result.category_id,
                "requires_registry_update": result.requires_registry_update,
                "candidates": result.candidates or [],
            },
            indent=2,
        )
    )
    return 0 if result.decision in {"ALLOW", "ALLOW_WITH_REGISTRY_UPDATE"} else 1


if __name__ == "__main__":
    raise SystemExit(main())
