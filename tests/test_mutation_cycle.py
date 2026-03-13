#!/usr/bin/env python3
from __future__ import annotations

import importlib
import json
import os
import sys
import tempfile
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))


def _bootstrap(tmp_db: str):
    os.environ["DB_PATH"] = tmp_db
    import db
    import mutation_cycle

    db = importlib.reload(db)
    mutation_cycle = importlib.reload(mutation_cycle)
    db.init_db()
    return db, mutation_cycle


def test_mutation_cycle_creates_child_manifest_from_next_batch_config() -> None:
    tmp = tempfile.NamedTemporaryFile(suffix=".db", delete=False)
    tmp.close()
    db, mutation_cycle = _bootstrap(tmp.name)
    work_dir = Path(tempfile.mkdtemp())
    parent_cfg = work_dir / "seed_config.json"
    next_cfg = work_dir / "loop_artifacts" / "next_batch_config.json"
    next_cfg.parent.mkdir(parents=True, exist_ok=True)
    parent_cfg.write_text(
        json.dumps(
            {
                "cohort_name": "seed",
                "dataset": {"hard_stop_pct": -0.03},
                "families": {"breakout_momentum": {"variants": [{"variant_name": "seed_a", "breakout_lookback": 20}]}},
            }
        ),
        encoding="utf-8",
    )
    next_cfg.write_text(
        json.dumps(
            {
                "cohort_name": "seed_mutated",
                "dataset": {"hard_stop_pct": -0.025},
                "families": {"breakout_momentum": {"variants": [{"variant_name": "seed_b", "breakout_lookback": 24}]}},
            }
        ),
        encoding="utf-8",
    )

    case_id = "sc_mut_cycle"
    parent_manifest_id = "em_mut_cycle_parent"
    verdict_id = "ev_mut_cycle_parent"
    db.create_search_case(
        case_id=case_id,
        case_type="family_search",
        title="mutation cycle candidate",
        status="active",
        stage="awaiting_verdict",
        family="breakout_momentum",
        hypothesis="mutation cycle should propose next batch",
        objective_type="find_edge",
        repo_scope="crypto-bot",
        market="crypto",
        created_from="test",
        owner="test",
        search_budget={"max_manifests_per_day": 8, "max_pending_manifests": 10},
        risk_budget={"min_trades": 10},
    )
    db.create_experiment_manifest(
        manifest_id=parent_manifest_id,
        case_id=case_id,
        status="completed",
        repo="crypto-bot",
        adapter_type="research_loop",
        entrypoint="research_loop.py",
        strategy_identity={"strategy_id": "breakout_momentum", "family": "breakout_momentum"},
        run_context_template={"environment": "research"},
        dataset_spec={"dataset_id": "test"},
        execution_spec={
            "family": "breakout_momentum",
            "config_path": str(parent_cfg),
            "recipe_path": "/root/automation-mvp/recipes/breakout_momentum_daily.json",
            "repo_root": "/root/crypto-bot",
            "validation_level": "cheap",
            "batch_size": 3,
            "variants_per_generation": 3,
        },
        cost_model={},
        gates={},
        created_by="test",
        artifacts={"output_root": str(next_cfg.parent)},
    )
    db.create_edge_verdict(
        verdict_id=verdict_id,
        case_id=case_id,
        manifest_id=parent_manifest_id,
        run_id="rl_mut_cycle",
        verdict_type="research_evaluation",
        status="final",
        decision="MUTATE_WITH_POLICY",
        decision_reason="edge_near_miss_refine",
        confidence=0.8,
        verdict_score=1.1,
        experiment_score=0.7,
        near_miss_score=0.74,
        validation_level="cheap",
        batch_size=3,
        config_fingerprint="fp_parent",
        metrics_snapshot={"trades": 120, "profit_factor": 1.2},
        gate_results={"min_trades_pass": True},
        artifacts_root=str(next_cfg.parent),
        policy_selected="EDGE_UP",
        mutation_recommendation={"mutation_class": "filter", "max_children": 3},
    )

    summary = mutation_cycle.run_mutation_cycle(since_hours=72, limit=10, dry_run=False)
    assert summary["created_count"] == 1
    created = summary["created_manifests"][0]
    child = db.get_experiment_manifest(created["manifest_id"])
    assert child is not None
    child_spec = json.loads(child["execution_spec_json"])
    child_hints = json.loads(child["planner_hints_json"])
    child_diff = json.loads(child["param_diff_json"])
    assert Path(child_spec["config_path"]).resolve() == next_cfg.resolve()
    assert child_hints.get("config_fingerprint")
    assert child_diff.get("config_fingerprint") == child_hints.get("config_fingerprint")
    assert child_spec.get("variants_per_generation") == 3

    os.unlink(tmp.name)


def test_mutation_cycle_skips_when_live_edge_search_is_frozen() -> None:
    tmp = tempfile.NamedTemporaryFile(suffix=".db", delete=False)
    tmp.close()
    db, mutation_cycle = _bootstrap(tmp.name)
    db.upsert_edge_search_runtime_state(
        mode="FROZEN",
        status="freeze_required",
        freeze_reason="duplicate_waste_high:0.500>0.350",
        review={"mode": "FROZEN", "status": "freeze_required"},
    )

    summary = mutation_cycle.run_mutation_cycle(since_hours=72, limit=10, dry_run=False)
    assert summary["created_count"] == 0
    assert summary["candidate_count"] == 0
    assert summary["live_edge_search"]["mode"] == "FROZEN"
    assert summary["live_edge_search"]["reasons"]

    os.unlink(tmp.name)


if __name__ == "__main__":
    test_mutation_cycle_creates_child_manifest_from_next_batch_config()
    print("All tests passed.")
