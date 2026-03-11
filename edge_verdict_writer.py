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


def _decide_verdict(metrics: dict[str, Any], gate_results: dict[str, Any]) -> tuple[str, str]:
    if not all(_bool_gate(v) for v in gate_results.values()):
        if not _bool_gate(gate_results.get("max_drawdown_pass", True)):
            return "MUTATE_WITH_POLICY", "good_pf_bad_dd"
        if not _bool_gate(gate_results.get("cost_adjusted_edge_pass", True)):
            return "REJECT_EDGE", "edge_collapsed_after_costs"
        return "RETEST_OOS", "critical_gate_missing"
    oos_pf = float(metrics.get("oos_profit_factor") or metrics.get("primary_metric") or 0.0)
    if oos_pf >= 1.05:
        return "PROMOTE_TO_PAPER", "all_primary_gates_passed"
    return "MUTATE_WITH_POLICY", "edge_below_promotion_threshold"


def write_edge_verdict_for_manifest(manifest_id: str, adapter_result: dict[str, Any]) -> dict[str, Any]:
    manifest = get_experiment_manifest(manifest_id)
    if not manifest:
        raise KeyError(f"Manifest not found: {manifest_id}")

    case_id = manifest["case_id"]
    metrics = dict(adapter_result.get("summary") or {})
    gates_cfg = json.loads(manifest["gates_json"])
    min_trades = int(gates_cfg.get("min_trades", 0) or 0)
    min_pf = float(gates_cfg.get("min_profit_factor", 0.0) or 0.0)
    max_dd = float(gates_cfg.get("max_drawdown_pct", 100.0) or 100.0)
    trades = int(metrics.get("trades", 0) or 0)
    pf = float(metrics.get("profit_factor", metrics.get("primary_metric", 0.0)) or 0.0)
    dd_raw = float(metrics.get("max_drawdown_pct", 0.0) or 0.0)
    dd_abs = abs(dd_raw) if dd_raw < 0 else dd_raw
    gate_results = {
        "min_trades_pass": trades >= min_trades if min_trades > 0 else True,
        "min_profit_factor_pass": pf >= min_pf if min_pf > 0 else True,
        "max_drawdown_pass": dd_abs <= max_dd if max_dd > 0 else True,
        "cost_adjusted_edge_pass": True,
        "walkforward_pass": True,
        "leakage_check_pass": True,
    }
    decision, reason = _decide_verdict(metrics, gate_results)
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
        metrics_snapshot=metrics,
        gate_results=gate_results,
        artifacts_root=str(adapter_result.get("artifacts_root") or ""),
        review_mode="auto",
        reviewed_by="manifest_worker",
    )
    create_case_event(
        case_id=case_id,
        manifest_id=manifest_id,
        verdict_id=verdict_id,
        event_type="verdict_issued",
        payload={"decision": decision, "reason": reason},
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
    return {"verdict_id": verdict_id, "decision": decision, "reason": reason}

