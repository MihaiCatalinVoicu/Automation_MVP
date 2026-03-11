from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from research_loop import run_loop


def _utc_date() -> str:
    return datetime.now(timezone.utc).date().isoformat()


def run_research_loop_manifest(manifest: dict[str, Any], automation_root: Path) -> dict[str, Any]:
    identity = json.loads(manifest["strategy_identity_json"])
    execution_spec = json.loads(manifest["execution_spec_json"])
    artifacts = json.loads(manifest["artifacts_json"])
    run_context = json.loads(manifest["run_context_template_json"])

    family_id = str(identity.get("family") or "")
    config_path = Path(str(execution_spec.get("config_path") or ""))
    recipe_path = str(execution_spec.get("recipe_path") or "")
    repo_root = Path(str(execution_spec.get("repo_root") or ""))
    if not family_id or not config_path or not recipe_path or not repo_root:
        return {
            "ok": False,
            "manifest_id": manifest["manifest_id"],
            "case_id": manifest["case_id"],
            "adapter_type": "research_loop",
            "errors": ["Missing required execution_spec: family/config_path/recipe_path/repo_root"],
            "warnings": [],
        }
    loop_root = Path(str(artifacts.get("output_root") or (automation_root / "data" / "research_loops" / manifest["manifest_id"])))
    state = run_loop(
        family_id=family_id,
        config_path=config_path.expanduser().resolve(),
        recipe_path=recipe_path,
        repo_root=repo_root.expanduser().resolve(),
        max_generations=max(1, int(execution_spec.get("max_generations", 3))),
        variants_per_generation=max(1, int(execution_spec.get("variants_per_generation", 2))),
        run_date=str(run_context.get("run_date") or _utc_date()),
        loop_root=loop_root.expanduser().resolve(),
    )
    run_id = f"research_loop:{manifest['manifest_id']}:{state.get('generation', 0)}"
    return {
        "ok": True,
        "manifest_id": manifest["manifest_id"],
        "case_id": manifest["case_id"],
        "run_id": run_id,
        "adapter_type": "research_loop",
        "artifacts_root": str(loop_root),
        "summary": {
            "status": str(state.get("status") or "UNKNOWN").lower(),
            "generation": int(state.get("generation", 0) or 0),
            "history_count": len(state.get("history") or []),
            "primary_metric": float((state.get("history") or [{}])[-1].get("metrics", {}).get("profit_factor", 0.0) or 0.0)
            if (state.get("history") or [])
            else 0.0,
            "trades": int((state.get("history") or [{}])[-1].get("metrics", {}).get("trade_count", 0) or 0)
            if (state.get("history") or [])
            else 0,
        },
        "warnings": [],
        "errors": [],
    }

