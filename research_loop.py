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
import hashlib
import json
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from recipe_runner import run_validation_battery

AUTOMATION_ROOT = Path(__file__).resolve().parent
POLICY_VERSION = "v2"

MUTATION_POLICY_MAP = {
    "low_trades_good_pf": "FREQUENCY_UP",
    "high_trades_bad_dd": "RISK_DOWN",
    "high_trades_low_pf": "EDGE_UP",
    "good_pf_bad_dd": "LOSS_SHAPE_DOWN",
    "edge_near_miss_refine": "EDGE_UP",
    "bull_only_viability": "EDGE_UP",
    "bear_only_viability": "EDGE_UP",
    "sideways_collapse": "LOSS_SHAPE_DOWN",
    "high_concentration": "DIVERSIFY",
    "pf_bad_dd_bad": "FREEZE_NOW",
    "robustness_warn": "EDGE_UP",
}


@dataclass
class LoopDecision:
    decision: str
    reason: str
    best_variant: str | None
    next_action: str | None
    failure_signature: str


def _no_progress_churn_details(
    history: list[dict[str, Any]],
    current: dict[str, Any],
    gates: dict[str, Any],
    window_size: int = 5,
) -> dict[str, Any] | None:
    entries = [*history, current]
    if len(entries) < window_size:
        return None
    tail = entries[-window_size:]

    if any(str(item.get("decision")) == "SUCCESS" for item in tail):
        return None

    gate_pf = float(gates.get("min_profit_factor", 1.2))

    max_window_passes = 0.0
    max_average_profit_factor = 0.0
    dominant_counter: dict[str, int] = {}

    for item in tail:
        battery_metrics = item.get("battery_metrics") or {}
        max_window_passes = max(max_window_passes, float(battery_metrics.get("window_passes", 0.0) or 0.0))
        max_average_profit_factor = max(
            max_average_profit_factor, float(battery_metrics.get("average_profit_factor", 0.0) or 0.0)
        )

        best_metrics = item.get("metrics") or {}
        reason = str(item.get("reason") or "").strip()
        if reason:
            dominant_counter[reason] = dominant_counter.get(reason, 0) + 1
        signature = str(item.get("failure_signature") or "none")
        if signature != "none":
            for chunk in signature.split("|"):
                token = chunk.strip()
                if token:
                    dominant_counter[token] = dominant_counter.get(token, 0) + 1

    # Freeze on robustness stagnation. Triplet pass (PF/DD/trades) alone is not enough:
    # if we pass sanity gates but robustness never improves (window_passes, avg_pf), that's churn.
    if max_window_passes >= 2 or max_average_profit_factor >= gate_pf:
        return None

    dominant_failure_modes = [k for k, _ in sorted(dominant_counter.items(), key=lambda kv: (-kv[1], kv[0]))[:4]]
    return {
        "freeze_reason": "no_progress_churn",
        "generations_without_success": len(tail),
        "max_window_passes": max_window_passes,
        "max_average_profit_factor": max_average_profit_factor,
        "dominant_failure_modes": dominant_failure_modes,
    }


def _load_json(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8"))


def _write_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2), encoding="utf-8")


def _append_jsonl(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a", encoding="utf-8") as f:
        f.write(json.dumps(payload) + "\n")


def _canonicalize_for_fingerprint(value: Any) -> Any:
    if isinstance(value, dict):
        return {str(key): _canonicalize_for_fingerprint(value[key]) for key in sorted(value)}
    if isinstance(value, list):
        return [_canonicalize_for_fingerprint(item) for item in value]
    if isinstance(value, tuple):
        return [_canonicalize_for_fingerprint(item) for item in value]
    if isinstance(value, float):
        rounded = round(value, 6)
        if rounded.is_integer():
            return int(rounded)
        return rounded
    return value


def _config_fingerprint(cfg: dict[str, Any]) -> str:
    payload = json.dumps(_canonicalize_for_fingerprint(cfg), sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()[:16]


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


def _default_loop_root_for_args(family_id: str, config_path: Path, explicit_loop_root: str | None) -> Path:
    if explicit_loop_root:
        return Path(explicit_loop_root).expanduser().resolve()
    # Continuation mode: reuse existing loop folder when config is from a prior loop.
    candidate = config_path.parent
    if (candidate / "loop_state.json").exists():
        return candidate
    timestamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    return AUTOMATION_ROOT / "data" / "research_loops" / f"{_slugify(family_id)}_{timestamp}"


def _best_variant(summary: dict[str, Any]) -> dict[str, Any]:
    item = summary.get("best_variant")
    return item if isinstance(item, dict) else {}


def _is_redundant_replay_no_progress(
    history: list[dict[str, Any]],
    current_fp: str,
    current_mode: str,
    current_battery_metrics: dict[str, Any],
) -> bool:
    if not history:
        return False
    prev = history[-1]
    if str(prev.get("config_fingerprint", "")) != current_fp:
        return False
    if str(prev.get("dominant_failure_mode", "")) != current_mode:
        return False
    prev_metrics = prev.get("battery_metrics") or {}
    prev_windows = float(prev_metrics.get("window_passes", 0.0) or 0.0)
    prev_avg_pf = float(prev_metrics.get("average_profit_factor", 0.0) or 0.0)
    now_windows = float(current_battery_metrics.get("window_passes", 0.0) or 0.0)
    now_avg_pf = float(current_battery_metrics.get("average_profit_factor", 0.0) or 0.0)
    return now_windows <= prev_windows and now_avg_pf <= prev_avg_pf


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

    if pf < 1.0 and dd < gate_dd:
        return LoopDecision("FREEZE", "pf_bad_dd_bad", best.get("variant_name"), "stop_branch", failure_signature)

    if repeated_failures >= 1 and {"profit_factor below gate", "max_drawdown below gate"}.issubset(set(failures)):
        return LoopDecision("FREEZE", "repeat_pf_dd_failure", best.get("variant_name"), "stop_branch", failure_signature)

    if pf >= gate_pf and dd >= gate_dd and trades < gate_trades:
        return LoopDecision("MUTATE", "low_trades_good_pf", best.get("variant_name"), "increase_frequency", failure_signature)

    if pf >= gate_pf and dd < gate_dd and trades >= gate_trades:
        return LoopDecision("MUTATE", "good_pf_bad_dd", best.get("variant_name"), "tighten_risk", failure_signature)

    if pf < gate_pf and trades >= gate_trades and dd < gate_dd:
        return LoopDecision("MUTATE", "high_trades_bad_dd", best.get("variant_name"), "tighten_risk", failure_signature)

    if pf < gate_pf and trades >= gate_trades and dd >= gate_dd:
        return LoopDecision("MUTATE", "high_trades_low_pf", best.get("variant_name"), "improve_signal_quality", failure_signature)

    if top3 > gate_top3 and pf >= gate_pf * 0.95:
        return LoopDecision("MUTATE", "high_concentration", best.get("variant_name"), "deconcentrate", failure_signature)

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


def _bump(item: dict[str, Any], key: str, delta: float, min_value: float | None = None, max_value: float | None = None) -> bool:
    if key not in item:
        return False
    value = float(item[key]) + delta
    if min_value is not None:
        value = max(min_value, value)
    if max_value is not None:
        value = min(max_value, value)
    item[key] = round(value, 4)
    return True


def _bump_int(item: dict[str, Any], key: str, delta: int, min_value: int | None = None, max_value: int | None = None) -> bool:
    if key not in item:
        return False
    value = int(item[key]) + delta
    if min_value is not None:
        value = max(min_value, value)
    if max_value is not None:
        value = min(max_value, value)
    item[key] = value
    return True


def _variant_signature(variant: dict[str, Any]) -> str:
    payload = {k: v for k, v in variant.items() if k != "variant_name"}
    return json.dumps(_canonicalize_for_fingerprint(payload), sort_keys=True, separators=(",", ":"))


def _apply_family_policy_variant(family_id: str, policy: str, variant: dict[str, Any], dataset: dict[str, Any], suffix: str) -> dict[str, Any]:
    item = copy.deepcopy(variant)
    changed = 0
    suffix_num = int("".join(ch for ch in suffix if ch.isdigit()) or "1")
    alt = suffix_num % 2 == 0
    if family_id == "spike_mean_reversion":
        if policy == "FREQUENCY_UP":
            changed += 1 if _bump(item, "spike_drop_pct", -0.01 if not alt else -0.015, min_value=0.03) else 0
            changed += 1 if changed < 2 and _bump(item, "spike_rsi_max", 5.0 if not alt else 3.0, max_value=40.0) else 0
        elif policy == "LOSS_SHAPE_DOWN":
            changed += 1 if _bump_int(item, "hold_bars", -1 if not alt else -2, min_value=1) else 0
            changed += 1 if changed < 2 and _bump(dataset, "hard_stop_pct", 0.005 if not alt else 0.0075, max_value=-0.01) else 0
            changed += 1 if changed < 2 and _bump(item, "spike_reclaim_min", 0.002 if not alt else 0.004, min_value=0.0) else 0
        elif policy == "EDGE_UP":
            changed += 1 if _bump(item, "spike_vol_mult", 0.5 if not alt else 0.3) else 0
            changed += 1 if changed < 2 and _bump(item, "spike_rsi_max", -5.0 if not alt else -3.0, min_value=10.0) else 0
    elif family_id == "breakout_momentum":
        if policy == "FREQUENCY_UP":
            changed += 1 if _bump_int(item, "breakout_lookback", -2 if not alt else -4, min_value=3) else 0
            changed += 1 if changed < 2 and _bump(item, "breakout_vol_mult", -0.2 if not alt else -0.1, min_value=1.0) else 0
        elif policy in {"RISK_DOWN", "LOSS_SHAPE_DOWN"}:
            changed += 1 if _bump(dataset, "hard_stop_pct", 0.005 if not alt else 0.0075, max_value=-0.01) else 0
            changed += 1 if changed < 2 and _bump_int(item, "hold_bars", -1 if not alt else -2, min_value=1) else 0
        elif policy == "EDGE_UP":
            changed += 1 if _bump(item, "breakout_vol_mult", 0.5 if not alt else 0.25) else 0
            changed += 1 if changed < 2 and _bump(item, "breakout_rsi_max", -4.0 if not alt else -2.0, min_value=40.0) else 0
    elif family_id == "cross_sectional_momentum":
        if policy == "DIVERSIFY":
            changed += 1 if _bump_int(item, "top_k", 2 if not alt else 3, min_value=2) else 0
            if changed < 2 and item.get("weighting") == "rank_weighted":
                item["weighting"] = "equal_weight"
                changed += 1
        elif policy == "EDGE_UP":
            changed += 1 if _bump_int(item, "ranking_bars", 2 if not alt else 4, min_value=4) else 0
            changed += 1 if changed < 2 and _bump_int(item, "hold_bars", -1 if not alt else -2, min_value=1) else 0
    elif family_id in {"oi_cascade", "liquidation_sweep"}:
        if policy == "FREQUENCY_UP":
            changed += 1 if _bump(item, "oi_jump_min", -0.02 if not alt else -0.03, min_value=0.01) else 0
            changed += 1 if changed < 2 and _bump(item, "price_drop_min", -0.01 if not alt else -0.005, min_value=0.01) else 0
        elif policy == "LOSS_SHAPE_DOWN":
            changed += 1 if _bump(dataset, "hard_stop_pct", 0.005 if not alt else 0.0075, max_value=-0.01) else 0
            changed += 1 if changed < 2 and _bump_int(item, "hold_bars", -1 if not alt else -2, min_value=1) else 0
        elif policy == "EDGE_UP":
            changed += 1 if _bump(item, "oi_jump_min", 0.02 if not alt else 0.03) else 0
            changed += 1 if changed < 2 and _bump(item, "spike_vol_mult", 0.5 if not alt else 0.3) else 0
    elif family_id == "trend_volatility_expansion":
        mode = (suffix_num - 1) % 4
        if policy == "FREQUENCY_UP":
            changed += 1 if _bump_int(item, "compression_window", (-2, 2, -1, 3)[mode], min_value=8) else 0
            changed += 1 if changed < 2 and _bump_int(item, "breakout_lookback", (-2, -4, 2, 4)[mode], min_value=6) else 0
        elif policy in {"RISK_DOWN", "LOSS_SHAPE_DOWN"}:
            changed += 1 if _bump(item, "atr_stop_mult", (-0.2, -0.35, -0.15, -0.3)[mode], min_value=1.0) else 0
            changed += 1 if changed < 2 and _bump(item, "atr_trail_mult", (-0.2, -0.1, -0.15, -0.05)[mode], min_value=1.2) else 0
        elif policy == "EDGE_UP":
            changed += 1 if _bump(item, "compression_atr_ratio_max", (-0.05, -0.02, -0.04, -0.01)[mode], min_value=0.3) else 0
            changed += 1 if changed < 2 and _bump(item, "volume_zscore_min", (0.2, -0.1, 0.1, 0.3)[mode], min_value=0.5) else 0
    elif family_id == "relative_strength_rotation":
        if policy == "DIVERSIFY":
            changed += 1 if _bump_int(item, "top_k", 2 if not alt else 1, min_value=3) else 0
            changed += 1 if changed < 2 and _bump_int(item, "hold_bars", -1 if not alt else 1, min_value=1) else 0
        elif policy == "FREQUENCY_UP":
            changed += 1 if _bump_int(item, "ranking_bars", -2 if not alt else 2, min_value=3) else 0
            changed += 1 if changed < 2 and _bump_int(item, "hold_bars", -1 if not alt else 1, min_value=1) else 0
        elif policy == "EDGE_UP":
            changed += 1 if _bump_int(item, "ranking_bars", 2 if not alt else 4, min_value=3) else 0
            changed += 1 if changed < 2 and _bump(item, "stop_atr_mult", -0.1 if not alt else 0.1, min_value=1.0) else 0
    elif family_id == "pullback_in_trend":
        if policy == "FREQUENCY_UP":
            changed += 1 if _bump(item, "pullback_near_atr_mult", 0.1 if not alt else 0.2, min_value=0.4) else 0
            changed += 1 if changed < 2 and _bump(item, "pullback_vol_max", 0.05 if not alt else 0.1, min_value=0.5, max_value=1.5) else 0
        elif policy in {"RISK_DOWN", "LOSS_SHAPE_DOWN"}:
            changed += 1 if _bump(dataset, "hard_stop_pct", 0.005 if not alt else 0.0075, max_value=-0.01) else 0
            changed += 1 if changed < 2 and _bump(item, "pullback_near_atr_mult", -0.05 if not alt else -0.1, min_value=0.4) else 0
        elif policy == "EDGE_UP":
            changed += 1 if _bump(item, "pullback_vol_max", -0.05 if not alt else -0.1, min_value=0.5, max_value=1.5) else 0
            changed += 1 if changed < 2 and _bump(item, "pullback_near_atr_mult", -0.05 if not alt else 0.05, min_value=0.4) else 0
    # Fallback keeps loop alive with bounded, small changes.
    if changed == 0:
        _bump(item, "breakout_vol_mult", 0.1)
        _bump(item, "spike_vol_mult", 0.1)
    item["variant_name"] = f"{item.get('variant_name', 'variant')}_{suffix}"
    return item


def mutate_config(
    cohort_cfg: dict[str, Any],
    family_id: str,
    decision: LoopDecision,
    variants_per_generation: int,
) -> tuple[dict[str, Any], dict[str, Any]]:
    cfg = copy.deepcopy(cohort_cfg)
    if family_id not in cfg.get("families", {}):
        raise KeyError(f"Family not found in cohort config: {family_id}")
    family_cfg = cfg["families"][family_id]
    base_variant = _find_variant(family_cfg, decision.best_variant)
    policy = MUTATION_POLICY_MAP.get(decision.reason, "FREEZE_NOW")
    if policy == "FREEZE_NOW":
        raise ValueError(f"No mutation allowed for reason={decision.reason}")
    base_dataset = copy.deepcopy(cfg["dataset"])
    chosen_dataset: dict[str, Any] | None = None
    generated: list[dict[str, Any]] = []
    seen_signatures: set[str] = set()
    for idx in range(1, max(1, variants_per_generation) + 3):
        dataset_copy = copy.deepcopy(base_dataset)
        candidate = _apply_family_policy_variant(family_id, policy, base_variant, dataset_copy, f"g{idx}")
        signature = _variant_signature(candidate)
        if signature in seen_signatures:
            continue
        seen_signatures.add(signature)
        generated.append(candidate)
        if chosen_dataset is None:
            chosen_dataset = dataset_copy
        if len(generated) >= max(1, variants_per_generation):
            break
    if not generated:
        dataset_copy = copy.deepcopy(base_dataset)
        generated = [_apply_family_policy_variant(family_id, policy, base_variant, dataset_copy, "g1")]
        chosen_dataset = dataset_copy

    family_cfg["variants"] = generated[: max(1, variants_per_generation)]
    if chosen_dataset is not None:
        cfg["dataset"] = chosen_dataset
    cfg["families"] = {family_id: family_cfg}
    cfg["cohort_name"] = f"{cfg.get('cohort_name', family_id)}_{decision.reason}"
    mutation_meta = {
        "policy": policy,
        "mutation_reason": decision.reason,
        "base_variant": base_variant.get("variant_name"),
        "new_variants": [x.get("variant_name") for x in family_cfg["variants"]],
    }
    return cfg, mutation_meta


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
    state_path = loop_root / "loop_state.json"
    if state_path.exists():
        state = _load_json(state_path)
        state.setdefault("history", [])
        state.setdefault("started_at", _utc_now())
        state["config_path"] = str(config_path.resolve())
        state["recipe_path"] = recipe_path
        state["repo_root"] = str(repo_root.resolve())
        state["policy_version"] = POLICY_VERSION
        state["family_id"] = family_id
        state["loop_id"] = loop_root.name
    else:
        state = {
            "loop_id": loop_root.name,
            "family_id": family_id,
            "config_path": str(config_path.resolve()),
            "recipe_path": recipe_path,
            "repo_root": str(repo_root.resolve()),
            "policy_version": POLICY_VERSION,
            "started_at": _utc_now(),
            "generation": 0,
            "status": "INIT",
            "history": [],
        }
    _write_json(state_path, state)

    current_cfg = _load_json(config_path)
    current_path = config_path

    start_generation = int(state.get("generation", 0)) + 1
    final_generation = int(state.get("generation", 0)) + max_generations
    for generation in range(start_generation, final_generation + 1):
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
        config_fp = _config_fingerprint(current_cfg)
        family_summary_path = generation_dir / "family_summary.json"
        if not family_summary_path.exists():
            raise FileNotFoundError(f"Expected family summary missing: {family_summary_path}")
        family_summary = _load_json(family_summary_path)
        decision = decide_next_action(
            family_summary,
            current_cfg.get("sanity_gates", {}),
            generation,
            final_generation,
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

        if decision.decision == "MUTATE" and _is_redundant_replay_no_progress(
            state["history"],
            config_fp,
            decision.reason,
            (battery_summary.get("metrics") or {}),
        ):
            decision = LoopDecision(
                "FREEZE",
                "redundant_replay_no_progress",
                decision.best_variant,
                "stop_branch",
                decision.failure_signature,
            )

        churn_details = _no_progress_churn_details(
            state["history"],
            {
                "generation": generation,
                "decision": decision.decision,
                "reason": decision.reason,
                "failure_signature": decision.failure_signature,
                "metrics": (_best_variant(family_summary).get("metrics") or {}),
                "battery_metrics": (battery_summary.get("metrics") or {}),
            },
            current_cfg.get("sanity_gates", {}),
        )
        if churn_details is not None:
            decision = LoopDecision(
                "FREEZE",
                "no_progress_churn",
                decision.best_variant,
                "stop_branch",
                decision.failure_signature,
            )

        decision_payload = {
            "generation": generation,
            "decision": asdict(decision),
            "battery_summary": battery_summary,
            "family_summary_path": str(family_summary_path),
        }
        if churn_details is not None:
            decision_payload["freeze_details"] = churn_details
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
                "battery_metrics": (battery_summary.get("metrics") or {}),
                "dominant_failure_mode": decision.reason,
                "config_fingerprint": config_fp,
                "summary_path": str(family_summary_path),
            }
        )
        if churn_details is not None:
            state["freeze_details"] = churn_details
        _write_json(loop_root / "loop_state.json", state)

        if decision.decision == "MUTATE" and generation >= final_generation:
            _append_jsonl(
                loop_root / "mutation_log.jsonl",
                {
                    "generation": generation,
                    "family": family_id,
                    "event": "mutation_skipped_due_to_budget",
                    "reason": decision.reason,
                    "policy": MUTATION_POLICY_MAP.get(decision.reason, "FREEZE_NOW"),
                    "base_variant": decision.best_variant,
                    "max_evaluations_reached": True,
                },
            )
            break

        if decision.decision != "MUTATE":
            break

        current_cfg, mutation_meta = mutate_config(current_cfg, family_id, decision, variants_per_generation)
        next_config_path = loop_root / f"generation_{generation + 1}" / "cohort_config.json"
        _write_json(next_config_path, current_cfg)
        _write_json(loop_root / "next_batch_config.json", current_cfg)
        _append_jsonl(
            loop_root / "mutation_log.jsonl",
            {
                "generation": generation,
                "family": family_id,
                "base_variant": mutation_meta.get("base_variant"),
                "policy": mutation_meta.get("policy"),
                "reason": mutation_meta.get("mutation_reason"),
                "new_variants": mutation_meta.get("new_variants"),
            },
        )
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
    loop_root = _default_loop_root_for_args(args.family, config_path, args.loop_root)

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
