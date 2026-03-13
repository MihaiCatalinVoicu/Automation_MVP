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
    validation_level = str(execution_spec.get("validation_level") or "cheap").lower()
    batch_size = max(1, int(execution_spec.get("batch_size") or execution_spec.get("variants_per_generation") or 2))
    default_generations = {"cheap": 3, "medium": 2, "expensive": 1}
    state = run_loop(
        family_id=family_id,
        config_path=config_path.expanduser().resolve(),
        recipe_path=recipe_path,
        repo_root=repo_root.expanduser().resolve(),
        max_generations=max(1, int(execution_spec.get("max_generations", default_generations.get(validation_level, 3)))),
        variants_per_generation=batch_size,
        run_date=str(run_context.get("run_date") or _utc_date()),
        loop_root=loop_root.expanduser().resolve(),
    )
    history = list(state.get("history") or [])
    latest = history[-1] if history else {}
    metrics = dict(latest.get("metrics") or {})
    battery_metrics = dict(latest.get("battery_metrics") or {})
    family_summary_path = Path(str(latest.get("summary_path") or ""))
    family_summary: dict[str, Any] = {}
    if family_summary_path.exists():
        family_summary = json.loads(family_summary_path.read_text(encoding="utf-8"))
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
            "primary_metric": float(metrics.get("profit_factor", 0.0) or 0.0),
            "profit_factor": float(metrics.get("profit_factor", 0.0) or 0.0),
            "max_drawdown_pct": float(metrics.get("max_drawdown_pct", 0.0) or 0.0),
            "trades": int(metrics.get("trade_count", 0) or 0),
            "trade_count": int(metrics.get("trade_count", 0) or 0),
            "top3_share_pct": float(metrics.get("top3_share_pct", 0.0) or 0.0),
            "window_passes": float(battery_metrics.get("window_passes", 0.0) or 0.0),
            "average_profit_factor": float(battery_metrics.get("average_profit_factor", 0.0) or 0.0),
            "oos_profit_factor": float(
                battery_metrics.get("average_profit_factor", metrics.get("profit_factor", 0.0)) or 0.0
            ),
            "max_cost_passed_bps": float(battery_metrics.get("max_cost_passed_bps", 0.0) or 0.0),
            "max_slip_passed_bps": float(battery_metrics.get("max_slip_passed_bps", 0.0) or 0.0),
            "dominant_failure_mode": str(latest.get("dominant_failure_mode") or ""),
            "config_fingerprint": str(latest.get("config_fingerprint") or ""),
            "validation_level": validation_level,
            "batch_size": batch_size,
            "candidate_count": int(family_summary.get("candidate_count", 0) or 0),
            "validation_ready": bool(family_summary.get("validation_ready", False)),
            "regime_breakdown": family_summary.get("regime_breakdown") or {},
        },
        "warnings": [],
        "errors": [],
    }

