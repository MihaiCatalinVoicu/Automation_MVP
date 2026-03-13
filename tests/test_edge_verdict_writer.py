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
    import edge_verdict_writer

    db = importlib.reload(db)
    edge_verdict_writer = importlib.reload(edge_verdict_writer)
    db.init_db()
    edge_verdict_writer.send_research_governance_message = lambda **_: {"result": {"message_id": "1"}}
    return db, edge_verdict_writer


def test_edge_verdict_writer_records_near_miss_metadata() -> None:
    tmp = tempfile.NamedTemporaryFile(suffix=".db", delete=False)
    tmp.close()
    db, edge_verdict_writer = _bootstrap(tmp.name)

    case_id = "sc_verdict_meta"
    manifest_id = "em_verdict_meta"
    db.create_search_case(
        case_id=case_id,
        case_type="family_search",
        title="verdict metadata test",
        status="active",
        stage="manifest_ready",
        family="breakout_momentum",
        hypothesis="test near miss",
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
        gates={"min_trades": 100, "min_profit_factor": 1.2, "max_drawdown_pct": 25.0, "max_cost_bps_for_survival": 40.0},
        created_by="test",
    )

    result = edge_verdict_writer.write_edge_verdict_for_manifest(
        manifest_id,
        {
            "run_id": "rl_test_meta",
            "artifacts_root": "/tmp/artifacts",
            "summary": {
                "profit_factor": 1.18,
                "primary_metric": 1.18,
                "max_drawdown_pct": -27.0,
                "trades": 140,
                "trade_count": 140,
                "average_profit_factor": 1.16,
                "window_passes": 1,
                "max_cost_passed_bps": 40,
                "config_fingerprint": "abc123",
                "validation_level": "medium",
                "batch_size": 6,
                "regime_breakdown": {
                    "TREND_STRONG": {"trade_count": 80, "profit_factor": 1.25},
                    "RANGE_CHOP": {"trade_count": 20, "profit_factor": 0.9},
                },
            },
        },
    )
    verdict = db.get_edge_verdict(result["verdict_id"])
    assert verdict is not None
    assert float(verdict["near_miss_score"]) > 0.0
    assert verdict["validation_level"] == "medium"
    assert int(verdict["batch_size"]) == 6
    assert verdict["config_fingerprint"] == "abc123"

    os.unlink(tmp.name)


if __name__ == "__main__":
    test_edge_verdict_writer_records_near_miss_metadata()
    print("All tests passed.")
