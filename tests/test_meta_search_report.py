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
    import meta_search_report

    db = importlib.reload(db)
    meta_search_report = importlib.reload(meta_search_report)
    db.init_db()
    return db, meta_search_report


def test_meta_search_report_builds_family_scores() -> None:
    tmp = tempfile.NamedTemporaryFile(suffix=".db", delete=False)
    tmp.close()
    db, meta_search_report = _bootstrap(tmp.name)

    case_id = "sc_meta_score"
    manifest_id = "em_meta_score"
    db.create_search_case(
        case_id=case_id,
        case_type="family_search",
        title="meta score case",
        status="active",
        stage="manifest_ready",
        family="breakout_momentum",
        hypothesis="meta report",
        objective_type="find_edge",
        repo_scope="crypto-bot",
        market="crypto",
        created_from="test",
        owner="test",
        search_budget={"max_manifests": 3},
        risk_budget={"min_trades": 10},
    )
    db.create_experiment_manifest(
        manifest_id=manifest_id,
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
            "config_path": "/root/crypto-bot/config.json",
            "recipe_path": "/root/automation-mvp/recipes/breakout_momentum_daily.json",
            "repo_root": "/root/crypto-bot",
            "validation_level": "cheap",
        },
        cost_model={},
        gates={},
        created_by="test",
    )
    db.create_edge_verdict(
        verdict_id="ev_meta_score",
        case_id=case_id,
        manifest_id=manifest_id,
        run_id="rl_meta",
        verdict_type="research_evaluation",
        status="final",
        decision="MUTATE_WITH_POLICY",
        decision_reason="good_pf_bad_dd",
        confidence=0.8,
        verdict_score=1.15,
        experiment_score=0.66,
        near_miss_score=0.72,
        validation_level="cheap",
        batch_size=12,
        config_fingerprint="fp_meta_1",
        metrics_snapshot={"profit_factor": 1.15, "trades": 120},
        gate_results={"min_trades_pass": True},
        postmortem_summary={"regime_failure_mode": "sideways_collapse"},
    )

    loops_root = Path(tempfile.mkdtemp())
    loop_dir = loops_root / "breakout_momentum_loop"
    loop_dir.mkdir(parents=True)
    (loop_dir / "loop_state.json").write_text(
        json.dumps(
            {
                "loop_id": loop_dir.name,
                "family_id": "breakout_momentum",
                "policy_version": "v2",
                "status": "SUCCESS",
                "generation": 2,
                "history": [
                    {
                        "config_fingerprint": "fp_a",
                        "dominant_failure_mode": "good_pf_bad_dd",
                        "metrics": {"profit_factor": 1.1, "trade_count": 100, "max_drawdown_pct": -20.0},
                        "battery_metrics": {"window_passes": 0.0, "average_profit_factor": 1.0},
                    },
                    {
                        "config_fingerprint": "fp_b",
                        "dominant_failure_mode": "robustness_warn",
                        "metrics": {"profit_factor": 1.2, "trade_count": 120, "max_drawdown_pct": -18.0},
                        "battery_metrics": {"window_passes": 1.0, "average_profit_factor": 1.2},
                    },
                ],
            },
            indent=2,
        ),
        encoding="utf-8",
    )
    (loop_dir / "mutation_log.jsonl").write_text(
        json.dumps({"generation": 1, "policy": "LOSS_SHAPE_DOWN"}) + "\n",
        encoding="utf-8",
    )

    payload = meta_search_report.build_meta_payload(loops_root=loops_root, since_days=365)
    bucket = next(item for item in payload["family_ranking"] if item["family_id"] == "breakout_momentum")
    assert bucket["family_score"] > 0.0
    assert bucket["near_miss_rate"] > 0.0
    assert bucket["recommended_action"] in {"CHEAP_ONLY", "CHEAP_MEDIUM", "EXPAND_CHEAP"}

    os.unlink(tmp.name)


if __name__ == "__main__":
    test_meta_search_report_builds_family_scores()
    print("All tests passed.")
