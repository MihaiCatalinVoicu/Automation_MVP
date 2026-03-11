#!/usr/bin/env python3
from __future__ import annotations

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from research_loop import (
    LoopDecision,
    _default_loop_root_for_args,
    _no_progress_churn_details,
    decide_next_action,
    mutate_config,
)


def test_decide_success() -> None:
    summary = {"candidate_count": 1, "sanity_pass": True, "validation_ready": True, "best_variant": {"variant_name": "x"}}
    gates = {"min_profit_factor": 1.2, "min_trade_count": 100, "max_drawdown_pct": -25.0, "max_top3_share_pct": 70.0}
    decision = decide_next_action(summary, gates, generation=1, max_generations=3, history=[])
    assert decision.decision == "SUCCESS"


def test_decide_freeze_on_severe_failure() -> None:
    summary = {
        "candidate_count": 0,
        "sanity_pass": False,
        "validation_ready": False,
        "best_variant": {
            "variant_name": "x",
            "metrics": {"profit_factor": 0.8, "max_drawdown_pct": -60.0, "trade_count": 1000, "top3_share_pct": 10.0},
            "failures": ["profit_factor below gate", "max_drawdown below gate"],
        },
    }
    gates = {"min_profit_factor": 1.2, "min_trade_count": 100, "max_drawdown_pct": -25.0, "max_top3_share_pct": 70.0}
    decision = decide_next_action(summary, gates, generation=1, max_generations=3, history=[])
    assert decision.decision == "FREEZE"
    assert decision.reason == "pf_bad_dd_bad"


def test_decide_mutate_on_good_pf_low_trades() -> None:
    summary = {
        "candidate_count": 0,
        "sanity_pass": False,
        "validation_ready": False,
        "best_variant": {
            "variant_name": "x",
            "metrics": {"profit_factor": 1.4, "max_drawdown_pct": -20.0, "trade_count": 60, "top3_share_pct": 10.0},
            "failures": ["trade_count below gate"],
        },
    }
    gates = {"min_profit_factor": 1.2, "min_trade_count": 100, "max_drawdown_pct": -25.0, "max_top3_share_pct": 70.0}
    decision = decide_next_action(summary, gates, generation=1, max_generations=3, history=[])
    assert decision.decision == "MUTATE"
    assert decision.reason == "low_trades_good_pf"


def test_mutate_config_tighten_risk() -> None:
    cohort = {
        "cohort_name": "spike_loop",
        "dataset": {"fwd_hours": 16, "hard_stop_pct": -0.03},
        "families": {
            "spike_mean_reversion": {
                "variants": [
                    {
                        "variant_name": "mr_base",
                        "spike_drop_pct": 0.10,
                        "spike_vol_mult": 2.0,
                        "spike_reclaim_min": 0.01,
                    }
                ]
            }
        },
    }
    decision = LoopDecision("MUTATE", "dd_bad_good_pf", "mr_base", "tighten_risk", "max_drawdown below gate")
    out = mutate_config(cohort, "spike_mean_reversion", decision, variants_per_generation=2)
    assert out["dataset"]["fwd_hours"] == 12
    assert out["dataset"]["hard_stop_pct"] == -0.025
    variants = out["families"]["spike_mean_reversion"]["variants"]
    assert len(variants) == 2
    assert variants[0]["spike_drop_pct"] > 0.10


def test_default_loop_root_reuses_existing_loop(tmp_path: Path) -> None:
    loop_dir = tmp_path / "breakout_momentum_20260310T220943Z"
    loop_dir.mkdir(parents=True)
    (loop_dir / "loop_state.json").write_text("{}", encoding="utf-8")
    config_path = loop_dir / "next_batch_config.json"
    config_path.write_text("{}", encoding="utf-8")
    resolved = _default_loop_root_for_args("breakout_momentum", config_path, None)
    assert resolved == loop_dir.resolve()


def test_no_progress_churn_details_triggers_freeze() -> None:
    gates = {"min_profit_factor": 1.2, "min_trade_count": 100, "max_drawdown_pct": -25.0}
    history = [
        {
            "decision": "MUTATE",
            "reason": "low_trades_good_pf",
            "failure_signature": "trade_count below gate",
            "metrics": {"profit_factor": 1.3, "max_drawdown_pct": -10.0, "trade_count": 80},
            "battery_metrics": {"window_passes": 0.0, "average_profit_factor": 0.8},
        },
        {
            "decision": "MUTATE",
            "reason": "robustness_warn",
            "failure_signature": "none",
            "metrics": {"profit_factor": 1.4, "max_drawdown_pct": -15.0, "trade_count": 90},
            "battery_metrics": {"window_passes": 0.0, "average_profit_factor": 0.9},
        },
        {
            "decision": "MUTATE",
            "reason": "low_trades_good_pf",
            "failure_signature": "trade_count below gate",
            "metrics": {"profit_factor": 1.35, "max_drawdown_pct": -14.0, "trade_count": 85},
            "battery_metrics": {"window_passes": 0.0, "average_profit_factor": 1.0},
        },
        {
            "decision": "MUTATE",
            "reason": "robustness_warn",
            "failure_signature": "none",
            "metrics": {"profit_factor": 1.45, "max_drawdown_pct": -16.0, "trade_count": 95},
            "battery_metrics": {"window_passes": 1.0, "average_profit_factor": 1.1},
        },
    ]
    current = {
        "decision": "MUTATE",
        "reason": "robustness_warn",
        "failure_signature": "none",
        "metrics": {"profit_factor": 1.5, "max_drawdown_pct": -20.0, "trade_count": 99},
        "battery_metrics": {"window_passes": 1.0, "average_profit_factor": 1.19},
    }
    details = _no_progress_churn_details(history, current, gates, window_size=5)
    assert details is not None
    assert details["freeze_reason"] == "no_progress_churn"
    assert details["generations_without_success"] == 5


def test_no_progress_churn_details_skips_when_progress_exists() -> None:
    gates = {"min_profit_factor": 1.2, "min_trade_count": 100, "max_drawdown_pct": -25.0}
    history = [
        {
            "decision": "MUTATE",
            "reason": "robustness_warn",
            "failure_signature": "none",
            "metrics": {"profit_factor": 1.5, "max_drawdown_pct": -20.0, "trade_count": 110},
            "battery_metrics": {"window_passes": 2.0, "average_profit_factor": 1.25},
        }
    ]
    current = {
        "decision": "MUTATE",
        "reason": "robustness_warn",
        "failure_signature": "none",
        "metrics": {"profit_factor": 1.5, "max_drawdown_pct": -20.0, "trade_count": 110},
        "battery_metrics": {"window_passes": 2.0, "average_profit_factor": 1.25},
    }
    details = _no_progress_churn_details(history, current, gates, window_size=2)
    assert details is None


if __name__ == "__main__":
    test_decide_success()
    test_decide_freeze_on_severe_failure()
    test_decide_mutate_on_good_pf_low_trades()
    test_mutate_config_tighten_risk()
    print("All tests passed.")
