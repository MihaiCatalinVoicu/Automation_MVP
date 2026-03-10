#!/usr/bin/env python3
"""
Bounded research loop:
- run one family at a time
- auto-score and auto-reject via existing validation battery
- mutate in small, deterministic steps
- stop after a bounded number of generations
"""
from __future__ import annotations

import argparse
import copy
import json
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from recipe_runner import run_validation_battery

AUTOMATION_ROOT = Path(__file__).resolve().parent


@dataclass
class LoopDecision:
    decision: str
    reason: str
    best_variant: str | None
    next_action: str | None
    failure_signature: str


def _load_json(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8"))


def _write_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2), encoding="utf-8")


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _slugify(text: str) -> str:
    safe = []
    for ch in text:
        if ch.isalnum() or ch in ("-", "_"):
            safe.append(ch)
        else:
            safe.append("_")
    return "".join(safe).strip("_") or "loop"


def _best_variant(summary: dict[str, Any]) -> dict[str, Any]:
    item = summary.get("best_variant")
    return item if isinstance(item, dict) else {}


def _has_repeated_alternating_signatures(history: list[dict[str, Any]], current_signature: str) -> bool:
    """
    Detect oscillation like A,B,A,B... on failure signatures.
    Freeze once an alternating pair repeats enough times.
    """
    signatures = [str(item.get("failure_signature") or "none") for item in history]
    signatures.append(current_signature)
    if len(signatures) < 4:
        return False
    tail = signatures[-6:]
    unique = list(dict.fromkeys(tail))
    if len(unique) != 2:
        return False
    a, b = tail[0], tail[1]
    if a == b:
        return False
    for idx, value in enumerate(tail):
        expected = a if idx % 2 == 0 else b
        if value != expected:
            return False
    # Require at least two full A/B cycles.
    return len(tail) >= 4


def decide_next_action(
    family_summary: dict[str, Any],
    gates: dict[str, Any],
    generation: int,
    max_generations: int,
    history: list[dict[str, Any]],
) -> LoopDecision:
    candidate_count = int(family_summary.get("candidate_count", 0) or 0)
    sanity_pass = bool(family_summary.get("sanity_pass"))
    validation_ready = bool(family_summary.get("validation_ready"))
    best = _best_variant(family_summary)
    metrics = best.get("metrics") or {}
    failures = list(best.get("failures") or [])
    failure_signature = "|".join(sorted(failures)) if failures else "none"

    if candidate_count >= 1 and sanity_pass and validation_ready:
        return LoopDecision("SUCCESS", "candidate_ready", best.get("variant_name"), None, failure_signature)

    gate_pf = float(gates.get("min_profit_factor", 1.2))
    gate_dd = float(gates.get("max_drawdown_pct", -25.0))
    gate_trades = int(gates.get("min_trade_count", 100))
    gate_top3 = float(gates.get("max_top3_share_pct", 70.0))

    pf = float(metrics.get("profit_factor", 0.0) or 0.0)
    dd = float(metrics.get("max_drawdown_pct", 0.0) or 0.0)
    trades = int(metrics.get("trade_count", 0) or 0)
    top3 = float(metrics.get("top3_share_pct", 0.0) or 0.0)

    repeated_failures = sum(1 for item in history if item.get("failure_signature") == failure_signature)
    if _has_repeated_alternating_signatures(history, failure_signature):
        return LoopDecision(
            "FREEZE",
            "alternating_failure_oscillation",
            best.get("variant_name"),
            "stop_branch",
            failure_signature,
        )

    if pf < 1.0 and dd < gate_dd - 15.0:
        return LoopDecision("FREEZE", "pf_bad_dd_bad", best.get("variant_name"), "stop_branch", failure_signature)

    if repeated_failures >= 1 and {"profit_factor below gate", "max_drawdown below gate"}.issubset(set(failures)):
        return LoopDecision("FREEZE", "repeat_pf_dd_failure", best.get("variant_name"), "stop_branch", failure_signature)

    if pf >= gate_pf and dd >= gate_dd and trades < gate_trades:
        return LoopDecision("MUTATE", "low_trades_good_pf", best.get("variant_name"), "increase_frequency", failure_signature)

    if pf >= gate_pf and dd < gate_dd and trades >= gate_trades:
        return LoopDecision("MUTATE", "dd_bad_good_pf", best.get("variant_name"), "tighten_risk", failure_signature)

    if pf >= gate_pf and dd < gate_dd and trades < gate_trades:
        return LoopDecision(
            "MUTATE",
            "dd_bad_low_trades_good_pf",
            best.get("variant_name"),
            "balance_risk_and_frequency",
            failure_signature,
        )

    if top3 > gate_top3 and pf >= gate_pf * 0.95:
        return LoopDecision("MUTATE", "concentration_bad", best.get("variant_name"), "deconcentrate", failure_signature)

    if pf >= gate_pf * 0.9 and dd < gate_dd and trades >= max(50, gate_trades // 2):
        return LoopDecision("MUTATE", "dd_bad_near_pf", best.get("variant_name"), "tighten_risk", failure_signature)

    if generation >= max_generations:
        return LoopDecision("PIVOT_SUGGEST", "max_generations_reached", best.get("variant_name"), "pivot_family", failure_signature)

    return LoopDecision("FREEZE", "insufficient_signal", best.get("variant_name"), "stop_branch", failure_signature)


def _find_variant(family_cfg: dict[str, Any], variant_name: str | None) -> dict[str, Any]:
    variants = family_cfg.get("variants") or []
    for variant in variants:
        if variant.get("variant_name") == variant_name:
            return copy.deepcopy(variant)
    if variants:
        return copy.deepcopy(variants[0])
    raise ValueError("Family config has no variants to mutate")


def _tighten_variant(variant: dict[str, Any], suffix: str) -> dict[str, Any]:
    item = copy.deepcopy(variant)
    if "breakout_vol_mult" in item:
        item["breakout_vol_mult"] = round(float(item["breakout_vol_mult"]) + 0.15, 4)
    if "breakout_lookback" in item:
        item["breakout_lookback"] = max(3, int(item["breakout_lookback"]) + 2)
    if "breakout_rsi_max" in item:
        item["breakout_rsi_max"] = round(float(item["breakout_rsi_max"]) - 2.0, 4)
    if "spike_drop_pct" in item:
        item["spike_drop_pct"] = round(float(item["spike_drop_pct"]) + 0.01, 4)
    if "spike_vol_mult" in item:
        item["spike_vol_mult"] = round(float(item["spike_vol_mult"]) + 0.1, 4)
    if "spike_reclaim_min" in item:
        item["spike_reclaim_min"] = round(max(0.0, float(item["spike_reclaim_min"]) + 0.002), 4)
    if "oi_jump_min" in item:
        item["oi_jump_min"] = round(float(item["oi_jump_min"]) + 0.02, 4)
    if "price_drop_min" in item:
        item["price_drop_min"] = round(float(item["price_drop_min"]) + 0.01, 4)
    if "funding_abs_max" in item:
        item["funding_abs_max"] = round(max(0.0005, float(item["funding_abs_max"]) - 0.0002), 4)
    if "wick_reclaim_min" in item:
        item["wick_reclaim_min"] = round(float(item["wick_reclaim_min"]) + 0.003, 4)
    if "pullback_near_atr_mult" in item:
        item["pullback_near_atr_mult"] = round(max(0.3, float(item["pullback_near_atr_mult"]) - 0.1), 4)
    if "pullback_vol_max" in item:
        item["pullback_vol_max"] = round(max(0.5, float(item["pullback_vol_max"]) - 0.05), 4)
    if "pb2_btc_ret_min" in item:
        item["pb2_btc_ret_min"] = round(float(item["pb2_btc_ret_min"]) + 0.001, 4)
    if "pb2_reclaim_lookback" in item:
        item["pb2_reclaim_lookback"] = max(3, int(item["pb2_reclaim_lookback"]) + 1)
    item["variant_name"] = f"{item.get('variant_name', 'variant')}_{suffix}"
    return item


def _relax_variant(variant: dict[str, Any], suffix: str) -> dict[str, Any]:
    item = copy.deepcopy(variant)
    if "breakout_vol_mult" in item:
        item["breakout_vol_mult"] = round(max(1.0, float(item["breakout_vol_mult"]) - 0.15), 4)
    if "breakout_lookback" in item:
        item["breakout_lookback"] = max(3, int(item["breakout_lookback"]) - 2)
    if "breakout_rsi_max" in item:
        item["breakout_rsi_max"] = round(float(item["breakout_rsi_max"]) + 2.0, 4)
    if "spike_drop_pct" in item:
        item["spike_drop_pct"] = round(max(0.03, float(item["spike_drop_pct"]) - 0.01), 4)
    if "spike_vol_mult" in item:
        item["spike_vol_mult"] = round(max(1.0, float(item["spike_vol_mult"]) - 0.1), 4)
    if "spike_reclaim_min" in item:
        item["spike_reclaim_min"] = round(max(0.0, float(item["spike_reclaim_min"]) - 0.002), 4)
    if "oi_jump_min" in item:
        item["oi_jump_min"] = round(max(0.01, float(item["oi_jump_min"]) - 0.02), 4)
    if "price_drop_min" in item:
        item["price_drop_min"] = round(max(0.01, float(item["price_drop_min"]) - 0.01), 4)
    if "funding_abs_max" in item:
        item["funding_abs_max"] = round(float(item["funding_abs_max"]) + 0.0003, 4)
    if "wick_reclaim_min" in item:
        item["wick_reclaim_min"] = round(max(0.0, float(item["wick_reclaim_min"]) - 0.003), 4)
    if "pullback_near_atr_mult" in item:
        item["pullback_near_atr_mult"] = round(float(item["pullback_near_atr_mult"]) + 0.1, 4)
    if "pullback_vol_max" in item:
        item["pullback_vol_max"] = round(min(1.5, float(item["pullback_vol_max"]) + 0.05), 4)
    if "pb2_btc_ret_min" in item:
        item["pb2_btc_ret_min"] = round(max(0.0, float(item["pb2_btc_ret_min"]) - 0.001), 4)
    if "pb2_reclaim_lookback" in item:
        item["pb2_reclaim_lookback"] = max(3, int(item["pb2_reclaim_lookback"]) - 1)
    item["variant_name"] = f"{item.get('variant_name', 'variant')}_{suffix}"
    return item


def _deconcentrate_variant(variant: dict[str, Any], suffix: str) -> dict[str, Any]:
    item = copy.deepcopy(variant)
    if "top_k" in item:
        item["top_k"] = int(item["top_k"]) + 2
    if "hold_bars" in item:
        item["hold_bars"] = max(1, int(item["hold_bars"]) - 1)
    if "stop_atr_mult" in item:
        item["stop_atr_mult"] = round(max(1.0, float(item["stop_atr_mult"]) - 0.1), 4)
    item["variant_name"] = f"{item.get('variant_name', 'variant')}_{suffix}"
    return item


def mutate_config(
    cohort_cfg: dict[str, Any],
    family_id: str,
    decision: LoopDecision,
    variants_per_generation: int,
) -> dict[str, Any]:
    cfg = copy.deepcopy(cohort_cfg)
    if family_id not in cfg.get("families", {}):
        raise KeyError(f"Family not found in cohort config: {family_id}")
    family_cfg = cfg["families"][family_id]
    base_variant = _find_variant(family_cfg, decision.best_variant)
    new_variants: list[dict[str, Any]]

    if decision.reason == "low_trades_good_pf":
        cfg["dataset"]["fwd_hours"] = min(24, int(cfg["dataset"].get("fwd_hours", 16)) + 4)
        new_variants = [
            _relax_variant(base_variant, "g1"),
            _relax_variant(_relax_variant(base_variant, "tmp"), "g2"),
        ]
    elif decision.reason == "robustness_warn":
        # Candidate exists but robustness is weak (window passes / avg PF warn).
        # Push toward cleaner, more selective entries.
        cfg["dataset"]["fwd_hours"] = max(8, int(cfg["dataset"].get("fwd_hours", 16)) - 4)
        new_variants = [
            _tighten_variant(base_variant, "g1"),
            _tighten_variant(_tighten_variant(base_variant, "tmp"), "g2"),
        ]
    elif decision.reason in {"dd_bad_good_pf", "dd_bad_near_pf"}:
        cfg["dataset"]["fwd_hours"] = max(4, int(cfg["dataset"].get("fwd_hours", 16)) - 4)
        cfg["dataset"]["hard_stop_pct"] = round(min(-0.01, float(cfg["dataset"].get("hard_stop_pct", -0.03)) + 0.005), 4)
        new_variants = [
            _tighten_variant(base_variant, "g1"),
            _tighten_variant(_tighten_variant(base_variant, "tmp"), "g2"),
        ]
    elif decision.reason == "dd_bad_low_trades_good_pf":
        cfg["dataset"]["fwd_hours"] = max(4, int(cfg["dataset"].get("fwd_hours", 16)) - 4)
        cfg["dataset"]["hard_stop_pct"] = round(min(-0.01, float(cfg["dataset"].get("hard_stop_pct", -0.03)) + 0.005), 4)
        new_variants = [
            _tighten_variant(base_variant, "g1"),
            _relax_variant(base_variant, "g2"),
        ]
    elif decision.reason == "concentration_bad":
        new_variants = [
            _deconcentrate_variant(base_variant, "g1"),
            _deconcentrate_variant(_deconcentrate_variant(base_variant, "tmp"), "g2"),
        ]
    else:
        new_variants = [
            _tighten_variant(base_variant, "g1"),
            _relax_variant(base_variant, "g2"),
        ]

    family_cfg["variants"] = new_variants[: max(1, variants_per_generation)]
    cfg["families"] = {family_id: family_cfg}
    cfg["cohort_name"] = f"{cfg.get('cohort_name', family_id)}_{decision.reason}"
    return cfg


def run_loop(
    *,
    family_id: str,
    config_path: Path,
    recipe_path: str,
    repo_root: Path,
    max_generations: int,
    variants_per_generation: int,
    run_date: str,
    loop_root: Path,
) -> dict[str, Any]:
    loop_root.mkdir(parents=True, exist_ok=True)
    state: dict[str, Any] = {
        "loop_id": loop_root.name,
        "family_id": family_id,
        "config_path": str(config_path.resolve()),
        "recipe_path": recipe_path,
        "repo_root": str(repo_root.resolve()),
        "policy_version": "v1",
        "started_at": _utc_now(),
        "generation": 0,
        "status": "INIT",
        "history": [],
    }
    _write_json(loop_root / "loop_state.json", state)

    current_cfg = _load_json(config_path)
    current_path = config_path

    for generation in range(1, max_generations + 1):
        generation_dir = loop_root / f"generation_{generation}"
        generation_dir.mkdir(parents=True, exist_ok=True)

        generation_config_path = generation_dir / "cohort_config.json"
        if generation == 1:
            _write_json(generation_config_path, current_cfg)
            current_path = generation_config_path

        run_context = {
            "run_dir": str(generation_dir / "candidate_run"),
            "cwd": str(repo_root.resolve()),
            "cohort_config": str(current_path.resolve()),
            "run_date": run_date,
        }
        battery_summary = run_validation_battery(recipe_path, run_context, generation_dir, base_path=AUTOMATION_ROOT)
        family_summary_path = generation_dir / "family_summary.json"
        if not family_summary_path.exists():
            raise FileNotFoundError(f"Expected family summary missing: {family_summary_path}")
        family_summary = _load_json(family_summary_path)
        decision = decide_next_action(
            family_summary,
            current_cfg.get("sanity_gates", {}),
            generation,
            max_generations,
            state["history"],
        )
        # Do not stop on candidate-only success when robustness battery is WARN.
        if decision.decision == "SUCCESS" and battery_summary.get("verdict") == "WARN":
            decision = LoopDecision(
                "MUTATE",
                "robustness_warn",
                decision.best_variant,
                "improve_window_and_avg_pf",
                decision.failure_signature,
            )

        decision_payload = {
            "generation": generation,
            "decision": asdict(decision),
            "battery_summary": battery_summary,
            "family_summary_path": str(family_summary_path),
        }
        _write_json(generation_dir / "decision.json", decision_payload)
        _write_json(loop_root / "decision.json", decision_payload)

        state["generation"] = generation
        state["status"] = decision.decision
        state["history"].append(
            {
                "generation": generation,
                "decision": decision.decision,
                "reason": decision.reason,
                "best_variant": decision.best_variant,
                "failure_signature": decision.failure_signature,
                "metrics": (_best_variant(family_summary).get("metrics") or {}),
                "summary_path": str(family_summary_path),
            }
        )
        _write_json(loop_root / "loop_state.json", state)

        if decision.decision != "MUTATE" or generation >= max_generations:
            break

        current_cfg = mutate_config(current_cfg, family_id, decision, variants_per_generation)
        next_config_path = loop_root / f"generation_{generation + 1}" / "cohort_config.json"
        _write_json(next_config_path, current_cfg)
        _write_json(loop_root / "next_batch_config.json", current_cfg)
        current_path = next_config_path

    state["ended_at"] = _utc_now()
    _write_json(loop_root / "loop_state.json", state)
    return state


def _main() -> int:
    ap = argparse.ArgumentParser(description="Run bounded research loop v1")
    ap.add_argument("--family", required=True, help="Active family ID")
    ap.add_argument("--config", required=True, help="Starting cohort config path")
    ap.add_argument("--recipe", required=True, help="Recipe path relative to automation-mvp or absolute")
    ap.add_argument("--repo-root", required=True, help="Crypto repo root")
    ap.add_argument("--max-generations", type=int, default=3)
    ap.add_argument("--variants-per-generation", type=int, default=2)
    ap.add_argument("--run-date", default=datetime.now(timezone.utc).date().isoformat())
    ap.add_argument("--loop-root", default=None, help="Optional explicit output directory")
    args = ap.parse_args()

    config_path = Path(args.config).expanduser().resolve()
    repo_root = Path(args.repo_root).expanduser().resolve()
    loop_root = (
        Path(args.loop_root).expanduser().resolve()
        if args.loop_root
        else AUTOMATION_ROOT
        / "data"
        / "research_loops"
        / f"{_slugify(args.family)}_{datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%SZ')}"
    )

    state = run_loop(
        family_id=args.family,
        config_path=config_path,
        recipe_path=args.recipe,
        repo_root=repo_root,
        max_generations=max(1, args.max_generations),
        variants_per_generation=max(1, args.variants_per_generation),
        run_date=args.run_date,
        loop_root=loop_root,
    )
    print(f"Loop status: {state['status']}")
    print(f"Loop root: {loop_root}")
    return 0


if __name__ == "__main__":
    raise SystemExit(_main())
