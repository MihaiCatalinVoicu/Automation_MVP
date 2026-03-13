#!/usr/bin/env python3
from __future__ import annotations

import importlib
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
    import research_guardrails

    db = importlib.reload(db)
    research_guardrails = importlib.reload(research_guardrails)
    db.init_db()
    return db, research_guardrails


def test_guardrails_block_medium_when_family_score_is_low() -> None:
    tmp = tempfile.NamedTemporaryFile(suffix=".db", delete=False)
    tmp.close()
    db, research_guardrails = _bootstrap(tmp.name)

    case_id = "sc_guardrails"
    manifest_id = "em_guardrails"
    db.create_search_case(
        case_id=case_id,
        case_type="family_search",
        title="guardrail case",
        status="active",
        stage="manifest_ready",
        family="breakout_momentum",
        hypothesis="guardrail test",
        objective_type="find_edge",
        repo_scope="crypto-bot",
        market="crypto",
        created_from="test",
        owner="test",
        search_budget={"max_manifests_per_day": 3},
        risk_budget={"min_trades": 10},
    )
    db.create_experiment_manifest(
        manifest_id=manifest_id,
        case_id=case_id,
        status="ready",
        repo="crypto-bot",
        adapter_type="research_loop",
        entrypoint="research_loop.py",
        strategy_identity={"strategy_id": "breakout_momentum", "family": "breakout_momentum"},
        run_context_template={"environment": "research"},
        dataset_spec={"dataset_id": "test"},
        execution_spec={
            "family": "breakout_momentum",
            "config_path": "/root/crypto-bot/config.json",
            "recipe_path": "/root/automation-mvp/recipes/breakout_momentum_daily.json",
            "repo_root": "/root/crypto-bot",
            "validation_level": "medium",
            "batch_size": 6,
            "variants_per_generation": 6,
        },
        cost_model={},
        gates={},
        created_by="test",
    )
    db.upsert_family_budget_state(
        family_id="breakout_momentum",
        status="active",
        priority=75,
        maturity="canonical",
        family_score=0.30,
        near_miss_rate=0.3,
        mutation_improvement_rate=0.2,
        robustness_survival_rate=0.2,
        dead_manifest_penalty=1.0,
        active_cases_count=1,
        total_cases_count=1,
        ready_manifest_count=1,
        running_manifest_count=0,
        completed_manifest_count=0,
        dead_manifest_count=0,
        latest_near_miss_score=0.7,
        recommended_action="CHEAP_ONLY",
        budget_state={"validation_caps": {"cheap": 4, "medium": 0, "expensive": 0}},
        motifs={},
    )
    manifest = db.get_experiment_manifest(manifest_id)
    allowed, reason = research_guardrails.evaluate_manifest_guardrails(manifest)
    assert allowed is False
    assert reason.startswith("family_score_cheap_only")

    os.unlink(tmp.name)


def test_guardrails_block_low_confidence_near_miss_child() -> None:
    tmp = tempfile.NamedTemporaryFile(suffix=".db", delete=False)
    tmp.close()
    db, research_guardrails = _bootstrap(tmp.name)

    case_id = "sc_guardrails_nm"
    parent_manifest_id = "em_guardrails_parent"
    child_manifest_id = "em_guardrails_child"
    verdict_id = "ev_guardrails_parent"
    db.create_search_case(
        case_id=case_id,
        case_type="family_search",
        title="guardrail low confidence near miss",
        status="active",
        stage="manifest_ready",
        family="breakout_momentum",
        hypothesis="guardrail low confidence test",
        objective_type="find_edge",
        repo_scope="crypto-bot",
        market="crypto",
        created_from="test",
        owner="test",
        search_budget={"max_manifests_per_day": 6},
        risk_budget={"min_trades": 10},
    )
    common_spec = {
        "family": "breakout_momentum",
        "config_path": "/root/crypto-bot/config.json",
        "recipe_path": "/root/automation-mvp/recipes/breakout_momentum_daily.json",
        "repo_root": "/root/crypto-bot",
        "validation_level": "cheap",
        "batch_size": 3,
        "variants_per_generation": 3,
    }
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
        execution_spec=common_spec,
        cost_model={},
        gates={},
        created_by="test",
    )
    db.create_edge_verdict(
        verdict_id=verdict_id,
        case_id=case_id,
        manifest_id=parent_manifest_id,
        run_id="rl_guardrails_low_conf",
        verdict_type="research_evaluation",
        status="final",
        decision="MUTATE_WITH_POLICY",
        decision_reason="edge_near_miss_refine",
        confidence=0.8,
        experiment_score=0.6,
        near_miss_score=0.62,
        validation_level="cheap",
        batch_size=3,
        config_fingerprint="fp_guardrails_low_conf",
        metrics_snapshot={"trades": 40, "profit_factor": 1.2},
        gate_results={"min_trades_pass": True},
    )
    db.create_experiment_manifest(
        manifest_id=child_manifest_id,
        case_id=case_id,
        manifest_version=2,
        status="ready",
        repo="crypto-bot",
        adapter_type="research_loop",
        entrypoint="research_loop.py",
        strategy_identity={"strategy_id": "breakout_momentum", "family": "breakout_momentum"},
        run_context_template={"environment": "research"},
        dataset_spec={"dataset_id": "test"},
        execution_spec=common_spec,
        cost_model={},
        gates={},
        created_by="test",
        parent_manifest_id=parent_manifest_id,
        derived_from_verdict_id=verdict_id,
    )
    manifest = db.get_experiment_manifest(child_manifest_id)
    allowed, reason = research_guardrails.evaluate_manifest_guardrails(manifest)
    assert allowed is False
    assert reason.startswith("near_miss_low_confidence")

    os.unlink(tmp.name)


if __name__ == "__main__":
    test_guardrails_block_medium_when_family_score_is_low()
    print("All tests passed.")
