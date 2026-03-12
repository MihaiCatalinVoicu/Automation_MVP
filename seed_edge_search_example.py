#!/usr/bin/env python3
"""
Seed example: one complete breakout_momentum search case with manifest and verdict.

Run once to populate the edge search tables with a realistic example:
    python seed_edge_search_example.py
"""
from __future__ import annotations

import db


def seed_breakout_case_mutate() -> None:
    case_id = "sc_20260311_breakout_majors_001"
    manifest_id = "em_20260311_breakout_001"
    verdict_id = "ev_20260311_breakout_001"

    if db.get_search_case(case_id):
        print(f"Case {case_id} already exists, skipping.")
        return

    # -- 1. Search Case --
    db.create_search_case(
        case_id=case_id,
        case_type="family_search",
        title="Breakout momentum on liquid crypto majors with regime gate",
        status="active",
        stage="awaiting_verdict",
        family="breakout_momentum",
        hypothesis=(
            "Breakout entries in risk-on regime on liquid majors can produce "
            "positive expectancy after realistic costs when filtered by breadth "
            "and volatility."
        ),
        objective_type="find_edge",
        repo_scope="crypto-bot",
        market="crypto",
        created_from="manual_prompt",
        owner="mihai",
        priority="high",
        venue="binanceusdm",
        instrument_scope="perpetuals",
        universe_id="majors_top10",
        timeframe="4h",
        strategy_id="crypto_breakout_momentum",
        variant_seed_id="bo_m2_majors_strict",
        objective_metric="oos_profit_factor",
        objective_threshold=1.15,
        planner_mode="llm_guided",
        planner_agent="composer",
        reviewer_agent="premium",
        source_ref="chat_2026_03_11_edge_mapping",
        search_budget={
            "max_manifests": 30,
            "max_total_experiments": 120,
            "max_family_mutations": 80,
            "max_llm_calls": 40,
            "max_days_open": 14,
        },
        risk_budget={
            "max_backtest_dd_pct": 18.0,
            "min_trades": 40,
            "must_include_costs": True,
            "must_include_walkforward": True,
            "must_include_oos": True,
        },
        tags=["tier1", "crypto", "breakout", "regime-gated"],
        notes="Use control-plane orchestration only. No live promotion without explicit verdict.",
    )
    db.create_case_event(
        case_id=case_id,
        event_type="case_created",
        payload={"source": "seed_edge_search_example"},
    )

    # -- 2. Experiment Manifest --
    db.create_experiment_manifest(
        manifest_id=manifest_id,
        case_id=case_id,
        status="completed",
        repo="crypto-bot",
        adapter_type="research_loop",
        entrypoint="research_loop.py",
        strategy_identity={
            "strategy_id": "crypto_breakout_momentum",
            "family": "breakout_momentum",
            "variant_id": "bo_m2_majors_strict",
            "profile_id": None,
        },
        run_context_template={
            "environment": "research",
            "market": "crypto",
            "venue": "binanceusdm",
            "timeframe": "4h",
            "universe_id": "majors_top10",
        },
        dataset_spec={
            "dataset_id": "crypto_top10_4h_v3",
            "date_from": "2021-01-01",
            "date_to": "2026-03-01",
            "train_windows": [
                ["2021-01-01", "2022-12-31"],
                ["2022-01-01", "2023-12-31"],
            ],
            "oos_windows": [
                ["2024-01-01", "2024-12-31"],
                ["2025-01-01", "2026-03-01"],
            ],
            "walkforward": True,
        },
        execution_spec={
            "mode": "paper_research",
            "family": "breakout_momentum",
            "config_path": "/root/crypto-bot/configs/research_cohort_breakout_v2.json",
            "recipe_path": "/root/crypto-bot/recipes/breakout_momentum_daily.json",
            "repo_root": "/root/crypto-bot",
            "max_generations": 6,
            "variants_per_generation": 2,
            "seed_count": 1,
            "search_policy_version": "v2",
            "mutation_budget": 8,
            "allow_family_change": False,
            "allow_timeframe_change": False,
            "allow_universe_change": False,
        },
        cost_model={
            "fee_bps": 5,
            "slippage_bps": 5,
            "latency_model": "next_bar_open",
            "funding_included": False,
        },
        gates={
            "min_trades": 40,
            "min_profit_factor": 1.15,
            "max_drawdown_pct": 18.0,
            "max_concentration_pct": 35.0,
            "min_oos_profit_factor": 1.05,
            "require_cost_adjusted_edge": True,
            "require_walkforward": True,
            "require_leakage_check": True,
        },
        created_by="composer",
        idempotency_key="idem_em_breakout_001",
        approved_by="mihai",
        planner_hints={
            "goal": "search_for_edge",
            "preferred_actions_on_fail": ["mutate_with_policy", "retest_oos", "kill_case"],
            "premium_on_repeat_failures": True,
        },
        artifacts={
            "output_root": "data/research_runs/sc_20260311_breakout_majors_001/em_20260311_breakout_001",
            "expected_files": [
                "summary.json",
                "metrics.json",
                "policy_benchmark.json",
                "mutation_log.jsonl",
                "loop_state.json",
            ],
        },
        notes="No promotion beyond paper candidate without explicit edge verdict.",
    )
    db.create_case_event(
        case_id=case_id,
        manifest_id=manifest_id,
        event_type="manifest_created",
        payload={"adapter_type": "research_loop", "created_by": "composer"},
    )

    # -- 3. Edge Verdict --
    db.create_edge_verdict(
        verdict_id=verdict_id,
        case_id=case_id,
        manifest_id=manifest_id,
        run_id="rl_breakout_20260311T120700Z",
        verdict_type="research_evaluation",
        status="final",
        decision="MUTATE_WITH_POLICY",
        decision_reason="good_pf_bad_dd",
        confidence=0.71,
        metrics_snapshot={
            "trades": 58,
            "profit_factor": 1.19,
            "winrate": 0.43,
            "total_return_pct": 7.4,
            "max_drawdown_pct": -22.1,
            "oos_profit_factor": 1.07,
            "avg_trade_pct": 0.18,
            "concentration_pct": 24.0,
        },
        gate_results={
            "min_trades_pass": True,
            "min_profit_factor_pass": True,
            "max_drawdown_pass": False,
            "oos_pf_pass": True,
            "cost_adjusted_edge_pass": True,
            "leakage_check_pass": True,
            "walkforward_pass": True,
        },
        dominant_failure_mode="good_pf_bad_dd",
        policy_selected="LOSS_SHAPE_DOWN",
        mutation_recommendation={
            "allowed": True,
            "max_new_variants": 2,
            "max_param_changes_each": 2,
        },
        promotion_state={
            "paper_candidate": False,
            "live_candidate": False,
            "needs_more_data": False,
        },
        next_action="launch_followup_manifest",
        next_action_payload={
            "action": "mutate_with_policy",
            "policy": "LOSS_SHAPE_DOWN",
            "new_manifest_type": "research_loop",
        },
        postmortem_summary={
            "what_we_believed": "breakout had positive expectancy after regime filtering",
            "what_happened": "PF held above threshold but DD remained too deep",
            "what_was_ignored": "loss clustering under high-vol subregimes",
            "what_to_change": "tighten loss shape, preserve signal frequency",
        },
        review_mode="auto_plus_human",
        reviewed_by="premium",
        approved_by="mihai",
    )
    db.create_case_event(
        case_id=case_id,
        manifest_id=manifest_id,
        verdict_id=verdict_id,
        event_type="verdict_issued",
        payload={"decision": "MUTATE_WITH_POLICY", "reason": "good_pf_bad_dd"},
    )

    # -- 4. Telegram decision example --
    db.create_telegram_decision(
        approval_id="td_20260311_001",
        case_id=case_id,
        manifest_id=manifest_id,
        run_id="rl_breakout_20260311T120700Z",
        action="MUTATE_WITH_POLICY",
        actor="mihai",
        message_id="tg_4821",
        payload={
            "policy": "LOSS_SHAPE_DOWN",
            "reason": "DD too deep but PF solid, worth mutating",
        },
    )

    print(f"Seeded case {case_id}")
    print(f"  manifest: {manifest_id}")
    print(f"  verdict:  {verdict_id}")
    print(f"  events:   {len(db.list_case_events(case_id))}")


def seed_breakout_case_reject() -> None:
    case_id = "sc_20260311_breakout_majors_002"
    manifest_id = "em_20260311_breakout_002"
    verdict_id = "ev_20260311_breakout_002"
    if db.get_search_case(case_id):
        print(f"Case {case_id} already exists, skipping.")
        return

    db.create_search_case(
        case_id=case_id,
        case_type="family_search",
        title="Breakout variant reject example",
        idempotency_key="idem_sc_breakout_002",
        status="active",
        stage="manifest_ready",
        family="breakout_momentum",
        hypothesis="A stricter breakout variant may reduce DD while preserving edge.",
        objective_type="find_edge",
        repo_scope="crypto-bot",
        market="crypto",
        created_from="manual_prompt",
        owner="mihai",
        strategy_id="crypto_breakout_momentum",
        registry_binding_status="provisional",
        search_budget={"max_manifests": 8},
        risk_budget={"min_trades": 40, "must_include_costs": True},
        tags=["crypto", "reject-flow"],
    )
    db.create_experiment_manifest(
        manifest_id=manifest_id,
        case_id=case_id,
        idempotency_key="idem_em_breakout_002",
        status="completed",
        repo="crypto-bot",
        adapter_type="research_loop",
        entrypoint="research_loop.py",
        strategy_identity={"strategy_id": "crypto_breakout_momentum", "family": "breakout_momentum", "variant_id": "bo_reject"},
        run_context_template={"environment": "research"},
        dataset_spec={"dataset_id": "crypto_top10_4h_v3"},
        execution_spec={
            "mode": "paper_research",
            "family": "breakout_momentum",
            "config_path": "/root/crypto-bot/configs/research_cohort_breakout_v2.json",
            "recipe_path": "/root/crypto-bot/recipes/breakout_momentum_daily.json",
            "repo_root": "/root/crypto-bot",
            "max_generations": 2,
        },
        cost_model={"fee_bps": 5, "slippage_bps": 5},
        gates={"min_trades": 40, "require_cost_adjusted_edge": True, "walkforward_pass": True, "leakage_check_pass": True},
        created_by="composer",
        approved_by="mihai",
    )
    db.create_edge_verdict(
        verdict_id=verdict_id,
        case_id=case_id,
        manifest_id=manifest_id,
        run_id="rl_breakout_20260311T130700Z",
        verdict_type="research_evaluation",
        status="final",
        decision="REJECT_EDGE",
        decision_reason="edge_collapsed_after_costs",
        confidence=0.86,
        verdict_score=0.18,
        metrics_snapshot={"trades": 74, "profit_factor": 0.92, "max_drawdown_pct": -21.0},
        gate_results={
            "min_trades_pass": True,
            "cost_adjusted_edge_pass": False,
            "walkforward_pass": True,
            "leakage_check_pass": True,
        },
        artifacts_root="data/research_runs/sc_20260311_breakout_majors_002/em_20260311_breakout_002",
        review_mode="auto_plus_human",
        reviewed_by="premium",
        approved_by="mihai",
    )
    db.create_case_event(case_id=case_id, verdict_id=verdict_id, event_type="verdict_issued", payload={"decision": "REJECT_EDGE"})
    print(f"Seeded case {case_id} (reject)")


def seed_breakout_case_promote() -> None:
    case_id = "sc_20260311_breakout_majors_003"
    manifest_id = "em_20260311_breakout_003"
    verdict_id = "ev_20260311_breakout_003"
    if db.get_search_case(case_id):
        print(f"Case {case_id} already exists, skipping.")
        return

    db.create_search_case(
        case_id=case_id,
        case_type="promotion_review",
        title="Breakout promotion candidate example",
        idempotency_key="idem_sc_breakout_003",
        status="active",
        stage="promotion_review",
        family="breakout_momentum",
        hypothesis="A mature breakout variant is ready for paper promotion gate.",
        objective_type="promotion_gate",
        repo_scope="crypto-bot",
        market="crypto",
        created_from="manual_prompt",
        owner="mihai",
        strategy_id="crypto_breakout_momentum",
        registry_binding_status="registered",
        search_budget={"max_manifests": 5},
        risk_budget={"min_trades": 80, "must_include_costs": True, "must_include_oos": True},
        tags=["crypto", "promote-flow"],
    )
    db.create_experiment_manifest(
        manifest_id=manifest_id,
        case_id=case_id,
        idempotency_key="idem_em_breakout_003",
        status="completed",
        repo="crypto-bot",
        adapter_type="policy_benchmark",
        entrypoint="policy_benchmark.py",
        strategy_identity={"strategy_id": "crypto_breakout_momentum", "family": "breakout_momentum", "variant_id": "bo_promote"},
        run_context_template={"environment": "research"},
        dataset_spec={"dataset_id": "crypto_top10_4h_v3_oos"},
        execution_spec={"mode": "paper_research", "max_generations": 1},
        cost_model={"fee_bps": 5, "slippage_bps": 5},
        gates={"min_trades": 80, "require_cost_adjusted_edge": True, "walkforward_pass": True, "leakage_check_pass": True},
        created_by="composer",
        approved_by="mihai",
    )
    db.create_edge_verdict(
        verdict_id=verdict_id,
        case_id=case_id,
        manifest_id=manifest_id,
        run_id="rl_breakout_20260311T140700Z",
        verdict_type="research_evaluation",
        status="final",
        decision="PROMOTE_TO_PAPER",
        decision_reason="all_primary_gates_passed",
        confidence=0.91,
        verdict_score=0.87,
        metrics_snapshot={"trades": 124, "profit_factor": 1.26, "max_drawdown_pct": -13.4, "oos_profit_factor": 1.14},
        gate_results={
            "min_trades_pass": True,
            "cost_adjusted_edge_pass": True,
            "walkforward_pass": True,
            "leakage_check_pass": True,
        },
        artifacts_root="data/research_runs/sc_20260311_breakout_majors_003/em_20260311_breakout_003",
        promotion_state={"paper_candidate": True, "live_candidate": False},
        review_mode="auto_plus_human",
        reviewed_by="premium",
        approved_by="mihai",
    )
    db.create_telegram_decision(
        approval_id="td_20260311_003",
        case_id=case_id,
        manifest_id=manifest_id,
        run_id="rl_breakout_20260311T140700Z",
        decision_scope="promotion_review",
        action="PROMOTE_TO_PAPER",
        actor="mihai",
        message_id="tg_4921",
        payload={"note": "Approved for paper lane"},
    )
    print(f"Seeded case {case_id} (promote)")


def main() -> int:
    db.init_db()
    seed_breakout_case_mutate()
    seed_breakout_case_reject()
    seed_breakout_case_promote()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
