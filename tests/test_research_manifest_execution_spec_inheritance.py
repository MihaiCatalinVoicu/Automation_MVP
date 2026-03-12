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
    import approval_service
    import db

    db = importlib.reload(db)
    approval_service = importlib.reload(approval_service)
    db.init_db()
    return db, approval_service


def test_initial_research_manifest_requires_execution_spec_and_child_inherits() -> None:
    tmp = tempfile.NamedTemporaryFile(suffix=".db", delete=False)
    tmp.close()

    db, approval_service = _bootstrap(tmp.name)

    case_id = "sc_inherit_execspec_001"
    parent_manifest_id = "em_inherit_execspec_parent"
    verdict_id = "ev_inherit_execspec_parent"

    db.create_search_case(
        case_id=case_id,
        case_type="family_search",
        title="Execution spec inheritance test",
        status="active",
        stage="manifest_ready",
        family="breakout_momentum",
        hypothesis="parent should carry required execution spec",
        objective_type="find_edge",
        repo_scope="crypto-bot",
        market="crypto",
        created_from="test",
        owner="test",
        search_budget={"max_manifests": 2},
        risk_budget={"min_trades": 10},
    )

    required_execution_spec = {
        "family": "breakout_momentum",
        "config_path": "/root/crypto-bot/configs/research_cohort_breakout_v2.json",
        "recipe_path": "/root/crypto-bot/recipes/breakout_momentum_daily.json",
        "repo_root": "/root/crypto-bot",
        "max_generations": 2,
    }

    db.create_experiment_manifest(
        manifest_id=parent_manifest_id,
        case_id=case_id,
        status="completed",
        repo="crypto-bot",
        adapter_type="research_loop",
        entrypoint="research_loop.py",
        strategy_identity={"strategy_id": "crypto_breakout_momentum", "family": "breakout_momentum", "variant_id": "bo_test"},
        run_context_template={"environment": "research"},
        dataset_spec={"dataset_id": "crypto_top10_4h_v3"},
        execution_spec=required_execution_spec,
        cost_model={"fee_bps": 5, "slippage_bps": 5},
        gates={"min_trades": 10},
        created_by="test",
        approved_by="test",
    )

    db.create_edge_verdict(
        verdict_id=verdict_id,
        case_id=case_id,
        manifest_id=parent_manifest_id,
        run_id="rl_test_001",
        verdict_type="research_evaluation",
        status="final",
        decision="MUTATE_WITH_POLICY",
        decision_reason="test mutate",
        metrics_snapshot={"trades": 30, "profit_factor": 1.1},
        gate_results={"min_trades_pass": True},
    )

    result = approval_service.apply_research_decision(
        case_id=case_id,
        action="MUTATE_WITH_POLICY",
        actor="test",
        source="manual",
    )
    assert result["ok"] is True
    child_id = result["new_manifest_id"]
    child = db.get_experiment_manifest(child_id)
    assert child is not None

    # Child execution_spec must inherit required fields from parent.
    import json

    child_spec = json.loads(child["execution_spec_json"])
    for key in ("family", "config_path", "recipe_path", "repo_root"):
        assert child_spec.get(key) == required_execution_spec[key]

    # Child spec is structurally valid for adapter preconditions.
    missing = [k for k in ("family", "config_path", "recipe_path", "repo_root") if not str(child_spec.get(k) or "").strip()]
    assert missing == []

    os.unlink(tmp.name)


if __name__ == "__main__":
    test_initial_research_manifest_requires_execution_spec_and_child_inherits()
    print("All tests passed.")
