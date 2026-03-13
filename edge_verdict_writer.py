from __future__ import annotations

import json
from typing import Any

from db import (
    case_event_exists,
    create_case_event,
    create_edge_verdict,
    create_telegram_decision,
    get_experiment_manifest,
    get_search_case,
)
from telegram_bot import send_research_governance_message


def _bool_gate(value: Any) -> bool:
    if isinstance(value, bool):
        return value
    if isinstance(value, (int, float)):
        return bool(value)
    if isinstance(value, str):
        return value.strip().lower() in {"1", "true", "yes", "pass"}
    return False


def _clamp01(value: float) -> float:
    if value < 0.0:
        return 0.0
    if value > 1.0:
        return 1.0
    return value


def _safe_float(value: Any) -> float:
    try:
        return float(value)
    except (TypeError, ValueError):
        return 0.0


def _safe_int(value: Any) -> int:
    try:
        return int(value)
    except (TypeError, ValueError):
        return 0


def _pf_proximity(pf: float) -> float:
    if pf <= 1.0:
        return 0.0
    return _clamp01((pf - 1.0) / 0.4)


def _dd_proximity(dd_abs: float, max_dd: float) -> float:
    if max_dd <= 0:
        return 0.0
    if dd_abs <= max_dd:
        return 1.0
    overflow = (dd_abs - max_dd) / max(1e-9, max_dd * 0.5)
    return _clamp01(1.0 - overflow)


def _trade_count_score(trades: int, min_trades: int) -> float:
    floor = max(1, min_trades)
    return _clamp01(trades / floor)


def _oos_stability_score(avg_pf: float, window_passes: float, min_pf: float) -> float:
    pf_score = _clamp01(avg_pf / max(min_pf, 1.0))
    window_score = _clamp01(window_passes / 2.0)
    return round((0.6 * pf_score) + (0.4 * window_score), 4)


def _near_miss_score(*, pf: float, dd_abs: float, trades: int, min_trades: int, avg_pf: float, window_passes: float, max_dd: float) -> float:
    score = (
        0.40 * _pf_proximity(pf)
        + 0.25 * _dd_proximity(dd_abs, max_dd)
        + 0.20 * _trade_count_score(trades, min_trades)
        + 0.15 * _oos_stability_score(avg_pf, window_passes, max(1.0, pf))
    )
    evidence_floor = max(80, int(min_trades * 0.8))
    if trades < evidence_floor:
        score *= 0.5
    if window_passes < 1.0:
        score *= 0.85
    return round(_clamp01(score), 4)


def _experiment_score(*, pf: float, dd_abs: float, trades: int, min_trades: int, avg_pf: float, window_passes: float, max_dd: float) -> float:
    score = (
        0.45 * _pf_proximity(pf)
        + 0.25 * _dd_proximity(dd_abs, max_dd)
        + 0.20 * _trade_count_score(trades, min_trades)
        + 0.10 * _oos_stability_score(avg_pf, window_passes, max(1.0, pf))
    )
    return round(_clamp01(score), 4)


def _regime_failure_mode(regime_breakdown: dict[str, Any]) -> str | None:
    if not regime_breakdown:
        return None
    scores = {}
    for regime_name, bucket in regime_breakdown.items():
        pf = _safe_float(bucket.get("profit_factor"))
        trades = _safe_int(bucket.get("trade_count"))
        if trades <= 0:
            continue
        scores[str(regime_name)] = pf
    if not scores:
        return None
    strong = {k: v for k, v in scores.items() if v >= 1.05}
    weak = {k: v for k, v in scores.items() if v < 1.0}
    if strong and weak and len(strong) == 1:
        name = next(iter(strong.keys()))
        if "BULL" in name or "TREND_STRONG" in name:
            return "bull_only_viability"
        if "BEAR" in name or "RISK_OFF" in name:
            return "bear_only_viability"
    if all(("RANGE" in name or "SIDE" in name) and value < 1.0 for name, value in scores.items()):
        return "sideways_collapse"
    if any(("RANGE" in name or "SIDE" in name) and value < 1.0 for name, value in scores.items()):
        return "sideways_collapse"
    return None


def _mutation_recommendation(reason: str, validation_level: str) -> tuple[str | None, dict[str, Any] | None]:
    mapping: dict[str, tuple[str, dict[str, Any]]] = {
        "good_pf_bad_dd": ("LOSS_SHAPE_DOWN", {"mutation_class": "risk", "max_children": 3}),
        "edge_collapsed_after_costs": ("EDGE_UP", {"mutation_class": "filter", "max_children": 3}),
        "low_trades_good_pf": ("FREQUENCY_UP", {"mutation_class": "parameter", "max_children": 3}),
        "high_trades_bad_dd": ("RISK_DOWN", {"mutation_class": "risk", "max_children": 3}),
        "high_trades_low_pf": ("EDGE_UP", {"mutation_class": "filter", "max_children": 3}),
        "bull_only_viability": ("EDGE_UP", {"mutation_class": "filter", "focus": "regime_gate", "max_children": 2}),
        "bear_only_viability": ("EDGE_UP", {"mutation_class": "filter", "focus": "regime_gate", "max_children": 2}),
        "sideways_collapse": ("LOSS_SHAPE_DOWN", {"mutation_class": "filter", "focus": "regime_gate", "max_children": 2}),
    }
    policy = mapping.get(reason)
    if not policy:
        return None, None
    selected, payload = policy
    out = dict(payload)
    out["validation_level"] = validation_level
    return selected, out


def _decide_verdict(metrics: dict[str, Any], gate_results: dict[str, Any], near_miss_score: float) -> tuple[str, str]:
    if not all(_bool_gate(v) for v in gate_results.values()):
        if not _bool_gate(gate_results.get("max_drawdown_pass", True)):
            return "MUTATE_WITH_POLICY", "good_pf_bad_dd"
        if not _bool_gate(gate_results.get("cost_adjusted_edge_pass", True)):
            return "REJECT_EDGE", "edge_collapsed_after_costs"
        regime_failure = str(metrics.get("regime_failure_mode") or "").strip()
        if regime_failure:
            return "MUTATE_WITH_POLICY", regime_failure
        return "RETEST_OOS", "critical_gate_missing"
    oos_pf = float(metrics.get("oos_profit_factor") or metrics.get("primary_metric") or 0.0)
    if oos_pf >= 1.05:
        return "PROMOTE_TO_PAPER", "all_primary_gates_passed"
    if near_miss_score >= 0.60:
        return "MUTATE_WITH_POLICY", "edge_near_miss_refine"
    return "MUTATE_WITH_POLICY", "edge_below_promotion_threshold"


def write_edge_verdict_for_manifest(manifest_id: str, adapter_result: dict[str, Any]) -> dict[str, Any]:
    manifest = get_experiment_manifest(manifest_id)
    if not manifest:
        raise KeyError(f"Manifest not found: {manifest_id}")

    case_id = manifest["case_id"]
    metrics = dict(adapter_result.get("summary") or {})
    execution_spec = json.loads(manifest.get("execution_spec_json") or "{}")
    gates_cfg = json.loads(manifest["gates_json"])
    min_trades = int(gates_cfg.get("min_trades", 0) or 0)
    min_pf = float(gates_cfg.get("min_profit_factor", 0.0) or 0.0)
    max_dd = abs(float(gates_cfg.get("max_drawdown_pct", 100.0) or 100.0))
    trades = int(metrics.get("trades", 0) or 0)
    pf = float(metrics.get("profit_factor", metrics.get("primary_metric", 0.0)) or 0.0)
    dd_raw = float(metrics.get("max_drawdown_pct", 0.0) or 0.0)
    dd_abs = abs(dd_raw) if dd_raw < 0 else dd_raw
    avg_pf = _safe_float(metrics.get("average_profit_factor"))
    window_passes = _safe_float(metrics.get("window_passes"))
    max_cost_passed_bps = _safe_float(metrics.get("max_cost_passed_bps"))
    cost_gate = float(gates_cfg.get("max_cost_bps_for_survival", 0.0) or 0.0)
    regime_breakdown = dict(metrics.get("regime_breakdown") or {})
    regime_failure_mode = _regime_failure_mode(regime_breakdown)
    if regime_failure_mode:
        metrics["regime_failure_mode"] = regime_failure_mode
    near_miss_score = _near_miss_score(
        pf=pf,
        dd_abs=dd_abs,
        trades=trades,
        min_trades=max(min_trades, 1),
        avg_pf=avg_pf,
        window_passes=window_passes,
        max_dd=max(max_dd, 1.0),
    )
    experiment_score = _experiment_score(
        pf=pf,
        dd_abs=dd_abs,
        trades=trades,
        min_trades=max(min_trades, 1),
        avg_pf=avg_pf,
        window_passes=window_passes,
        max_dd=max(max_dd, 1.0),
    )
    validation_level = str(execution_spec.get("validation_level") or metrics.get("validation_level") or "cheap")
    batch_size = int(execution_spec.get("batch_size") or metrics.get("batch_size") or execution_spec.get("variants_per_generation") or 1)
    gate_results = {
        "min_trades_pass": trades >= min_trades if min_trades > 0 else True,
        "min_profit_factor_pass": pf >= min_pf if min_pf > 0 else True,
        "max_drawdown_pass": dd_abs <= max_dd if max_dd > 0 else True,
        "cost_adjusted_edge_pass": max_cost_passed_bps >= cost_gate if cost_gate > 0 else True,
        "walkforward_pass": window_passes >= 1.0 and avg_pf >= max(1.0, min_pf * 0.95),
        "leakage_check_pass": True,
    }
    decision, reason = _decide_verdict(metrics, gate_results, near_miss_score)
    if decision == "MUTATE_WITH_POLICY" and regime_failure_mode and reason in {"critical_gate_missing", "edge_below_promotion_threshold"}:
        reason = regime_failure_mode
    policy_selected, mutation_recommendation = _mutation_recommendation(reason, validation_level)
    verdict_id = f"ev_{manifest_id}_{adapter_result.get('run_id', 'run').replace(':', '_')}"
    create_edge_verdict(
        verdict_id=verdict_id,
        case_id=case_id,
        manifest_id=manifest_id,
        run_id=str(adapter_result.get("run_id") or ""),
        verdict_type="research_evaluation",
        status="final",
        decision=decision,
        decision_reason=reason,
        confidence=0.8,
        verdict_score=float(metrics.get("primary_metric") or 0.0),
        experiment_score=experiment_score,
        near_miss_score=near_miss_score,
        validation_level=validation_level,
        batch_size=batch_size,
        config_fingerprint=str(metrics.get("config_fingerprint") or ""),
        metrics_snapshot=metrics,
        gate_results=gate_results,
        artifacts_root=str(adapter_result.get("artifacts_root") or ""),
        dominant_failure_mode=reason,
        policy_selected=policy_selected,
        mutation_recommendation=mutation_recommendation,
        next_action="LOCAL_REFINE" if near_miss_score >= 0.60 else None,
        next_action_payload={
            "near_miss_score": near_miss_score,
            "validation_level": validation_level,
            "batch_size": batch_size,
        },
        postmortem_summary={
            "regime_breakdown": regime_breakdown,
            "regime_failure_mode": regime_failure_mode,
            "max_cost_passed_bps": max_cost_passed_bps,
            "window_passes": window_passes,
            "average_profit_factor": avg_pf,
        },
        review_mode="auto",
        reviewed_by="manifest_worker",
    )
    create_case_event(
        case_id=case_id,
        manifest_id=manifest_id,
        verdict_id=verdict_id,
        event_type="verdict_issued",
        payload={
            "decision": decision,
            "reason": reason,
            "near_miss_score": near_miss_score,
            "experiment_score": experiment_score,
            "validation_level": validation_level,
        },
    )
    case = get_search_case(case_id)
    should_notify = decision in {
        "MUTATE_WITH_POLICY",
        "RETEST_OOS",
        "RUN_BIGGER_SAMPLE",
        "PROMOTE_TO_PAPER",
        "HOLD_FOR_MORE_DATA",
        "ASK_PREMIUM_REVIEW",
        "REJECT_EDGE",
    }
    if (
        should_notify
        and case
        and str(case.get("status") or "") not in {"done", "killed", "archived"}
        and str(case.get("latest_verdict_id") or "") == verdict_id
        and not case_event_exists(case_id=case_id, event_type="research_governance_message_sent", verdict_id=verdict_id)
    ):
        try:
            tg = send_research_governance_message(
                case_id=case_id,
                family=str(case.get("family") or ""),
                strategy_id=case.get("strategy_id"),
                stage=str(case.get("stage") or ""),
                proposed_decision=decision,
                verdict_id=verdict_id,
                manifest_id=manifest_id,
                metrics=metrics,
                dominant_failure_mode=reason,
                verdict_score=float(metrics.get("primary_metric") or 0.0),
                artifacts_root=str(adapter_result.get("artifacts_root") or ""),
            )
            message_id = str((tg.get("result") or {}).get("message_id") or "")
            create_case_event(
                case_id=case_id,
                manifest_id=manifest_id,
                verdict_id=verdict_id,
                event_type="research_governance_message_sent",
                payload={"message_id": message_id, "decision": decision},
            )
            create_telegram_decision(
                approval_id=f"td_{verdict_id}",
                case_id=case_id,
                manifest_id=manifest_id,
                run_id=str(adapter_result.get("run_id") or ""),
                decision_scope="research_case",
                action=decision,
                actor="system",
                message_id=message_id,
                payload={"source": "edge_verdict_writer", "reason": reason},
            )
        except Exception as exc:
            create_case_event(
                case_id=case_id,
                manifest_id=manifest_id,
                verdict_id=verdict_id,
                event_type="research_governance_message_failed",
                payload={"error": str(exc)},
            )
    return {
        "verdict_id": verdict_id,
        "decision": decision,
        "reason": reason,
        "near_miss_score": near_miss_score,
        "experiment_score": experiment_score,
    }

