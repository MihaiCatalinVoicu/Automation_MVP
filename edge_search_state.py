from __future__ import annotations

import json
import os
from typing import Any

from db import (
    count_pending_manifests,
    get_edge_search_runtime_state,
    record_edge_search_trigger_review,
    upsert_edge_search_runtime_state,
)


def _env_int(name: str, default: int) -> int:
    try:
        return int(os.getenv(name, str(default)))
    except ValueError:
        return default


def _env_float(name: str, default: float) -> float:
    try:
        return float(os.getenv(name, str(default)))
    except ValueError:
        return default


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


def _load_json(text: str | None) -> dict[str, Any]:
    if not text:
        return {}
    try:
        return json.loads(text)
    except json.JSONDecodeError:
        return {}


def _family_counts(row: dict[str, Any]) -> tuple[int, int, int]:
    manifest_counts = row.get("manifest_counts") or {}
    total = _safe_int(manifest_counts.get("total"))
    completed = _safe_int(manifest_counts.get("completed"))
    dead = _safe_int(manifest_counts.get("dead"))
    return total, completed, dead


def _family_duplicate_ratio(row: dict[str, Any]) -> float:
    fingerprints = row.get("fingerprints") or {}
    unique = _safe_int(fingerprints.get("unique_fingerprints"))
    repeated = _safe_int(fingerprints.get("repeated_fingerprints"))
    if unique <= 0:
        return 0.0
    return round(repeated / unique, 4)


def evaluate_live_edge_search_review(payload: dict[str, Any]) -> dict[str, Any]:
    queue_health = payload.get("queue_health") or {}
    family_rows = list(payload.get("family_ranking") or [])
    pending_total = _safe_int(queue_health.get("pending_total"))
    completed_total = _safe_int(queue_health.get("completed_total"))
    dead_total = _safe_int(queue_health.get("dead_total"))
    ready_total = _safe_int(queue_health.get("ready_total"))

    max_pending = _env_int("RESEARCH_MAX_PENDING_MANIFESTS", 200)
    queue_target = _env_int("RESEARCH_TARGET_QUEUE_DEPTH_PER_WORKER", 20) * _env_int("RESEARCH_ACTIVE_WORKERS_FALLBACK", 1)
    trigger_a_min_experiments = _env_int("EDGE_SEARCH_TRIGGER_A_MIN_EXPERIMENTS", 200)
    trigger_a_min_near_miss = _env_int("EDGE_SEARCH_TRIGGER_A_MIN_NEAR_MISS_COUNT", 10)
    trigger_b_min_experiments = _env_int("EDGE_SEARCH_TRIGGER_B_MIN_EXPERIMENTS", 1000)
    trigger_b_min_near_miss = _env_int("EDGE_SEARCH_TRIGGER_B_MIN_NEAR_MISS_COUNT", 80)
    trigger_c_min_experiments = _env_int("EDGE_SEARCH_TRIGGER_C_MIN_EXPERIMENTS", 1000)
    trigger_d_min_experiments = _env_int("EDGE_SEARCH_TRIGGER_D_MIN_EXPERIMENTS", 2000)
    trigger_e_min_outcomes = _env_int("EDGE_SEARCH_TRIGGER_E_MIN_OUTCOMES", 50)
    max_duplicate_ratio = _env_float("EDGE_SEARCH_MAX_DUPLICATE_RATIO", 0.35)
    min_family_score = _env_float("EDGE_SEARCH_TRIGGER_A_MIN_FAMILY_SCORE", 0.50)
    min_near_miss_rate = _env_float("EDGE_SEARCH_TRIGGER_A_MIN_NEAR_MISS_RATE", 0.10)

    evaluated_total = completed_total + dead_total
    manifest_total = sum(_family_counts(row)[0] for row in family_rows)
    if manifest_total <= 0:
        manifest_total = evaluated_total + ready_total
    near_miss_total = sum(_safe_int(row.get("near_miss_count")) for row in family_rows)
    dominant_families = [
        row
        for row in family_rows
        if _safe_float(row.get("family_score")) >= min_family_score
        and _safe_float(row.get("near_miss_rate")) >= min_near_miss_rate
    ]
    duplicate_ratios = [_family_duplicate_ratio(row) for row in family_rows if _family_counts(row)[0] > 0]
    duplicate_ratio = round(sum(duplicate_ratios) / len(duplicate_ratios), 4) if duplicate_ratios else 0.0
    top_family_share = 0.0
    if family_rows and manifest_total > 0:
        top_family_total = max(_family_counts(row)[0] for row in family_rows)
        top_family_share = round(top_family_total / manifest_total, 4)

    reasons: list[str] = []
    freeze_reason = ""
    if pending_total >= max(1, int(max_pending * 0.9)):
        reasons.append(f"backlog_near_cap:{pending_total}/{max_pending}")
    if queue_target > 0 and pending_total >= max(queue_target, 1) * 2:
        reasons.append(f"worker_queue_saturated:{pending_total}/{queue_target}")
    if duplicate_ratio > max_duplicate_ratio:
        reasons.append(f"duplicate_waste_high:{duplicate_ratio:.3f}>{max_duplicate_ratio:.3f}")
    if manifest_total >= 50 and top_family_share >= 0.75 and near_miss_total <= trigger_a_min_near_miss:
        reasons.append(f"family_concentration_without_signal:{top_family_share:.3f}")
    if evaluated_total >= trigger_a_min_experiments and len(dominant_families) == 0:
        reasons.append("no_dominant_families_after_trigger_a_window")

    trigger_a_ready = (
        evaluated_total >= trigger_a_min_experiments
        and near_miss_total >= trigger_a_min_near_miss
        and len(dominant_families) >= 2
        and duplicate_ratio <= max_duplicate_ratio
    )
    trigger_b_ready = trigger_a_ready and evaluated_total >= trigger_b_min_experiments and near_miss_total >= trigger_b_min_near_miss
    trigger_c_ready = trigger_b_ready and evaluated_total >= trigger_c_min_experiments
    stable_candidate_count = sum(
        1 for row in family_rows if _safe_float(row.get("family_score")) >= 0.70 and _safe_float(row.get("latest_near_miss_score")) >= 0.70
    )
    trigger_d_ready = trigger_c_ready and evaluated_total >= trigger_d_min_experiments and stable_candidate_count >= 2
    trigger_e_ready = trigger_d_ready and completed_total >= trigger_e_min_outcomes

    if reasons:
        mode = "FROZEN"
        status = "freeze_required"
        freeze_reason = reasons[0]
    elif queue_target > 0 and pending_total >= queue_target:
        mode = "SAFE_IDLE"
        status = "queue_saturated"
        freeze_reason = f"queue_saturated:{pending_total}/{queue_target}"
    elif trigger_a_ready:
        mode = "REFINE"
        status = "stable_signal_detected"
    elif evaluated_total > 0:
        mode = "REVIEW"
        status = "collecting_evidence"
    else:
        mode = "EXPLORE"
        status = "bootstrap"

    review = {
        "mode": mode,
        "status": status,
        "freeze_reason": freeze_reason,
        "reasons": reasons,
        "metrics": {
            "pending_total": pending_total,
            "ready_total": ready_total,
            "evaluated_total": evaluated_total,
            "manifest_total": manifest_total,
            "near_miss_total": near_miss_total,
            "dominant_family_count": len(dominant_families),
            "duplicate_ratio": duplicate_ratio,
            "top_family_share": top_family_share,
            "stable_candidate_count": stable_candidate_count,
        },
        "triggers": {
            "trigger_a": {
                "status": "ready" if trigger_a_ready else "locked",
                "thresholds": {
                    "min_experiments": trigger_a_min_experiments,
                    "min_near_miss": trigger_a_min_near_miss,
                },
            },
            "trigger_b": {
                "status": "ready" if trigger_b_ready else "locked",
                "thresholds": {
                    "min_experiments": trigger_b_min_experiments,
                    "min_near_miss": trigger_b_min_near_miss,
                },
            },
            "trigger_c": {
                "status": "ready" if trigger_c_ready else "locked",
                "thresholds": {"min_experiments": trigger_c_min_experiments},
            },
            "trigger_d": {
                "status": "ready" if trigger_d_ready else "locked",
                "thresholds": {"min_experiments": trigger_d_min_experiments, "min_stable_candidates": 2},
            },
            "trigger_e": {
                "status": "ready" if trigger_e_ready else "locked",
                "thresholds": {"min_linked_outcomes": trigger_e_min_outcomes},
            },
        },
    }
    return review


def persist_live_edge_search_review(payload: dict[str, Any]) -> dict[str, Any]:
    review = evaluate_live_edge_search_review(payload)
    upsert_edge_search_runtime_state(
        mode=review["mode"],
        status=review["status"],
        freeze_reason=review.get("freeze_reason") or None,
        health={"queue_health": payload.get("queue_health") or {}},
        review=review,
    )
    for trigger_name, trigger_payload in (review.get("triggers") or {}).items():
        record_edge_search_trigger_review(trigger_name, str(trigger_payload.get("status") or "locked"), trigger_payload)
    return review


def preflight_mutation_cycle() -> dict[str, Any]:
    pending_total = count_pending_manifests()
    max_pending = _env_int("RESEARCH_MAX_PENDING_MANIFESTS", 200)
    queue_target = _env_int("RESEARCH_TARGET_QUEUE_DEPTH_PER_WORKER", 20) * _env_int("RESEARCH_ACTIVE_WORKERS_FALLBACK", 1)
    state = get_edge_search_runtime_state() or {}
    review = _load_json(state.get("review_json"))
    mode = str(state.get("mode") or review.get("mode") or "EXPLORE")
    status = str(state.get("status") or review.get("status") or "bootstrap")
    freeze_reason = str(state.get("freeze_reason") or review.get("freeze_reason") or "")

    reasons: list[str] = []
    if mode == "FROZEN":
        reasons.append(freeze_reason or "runtime_state_frozen")
    if pending_total >= max_pending:
        reasons.append(f"pending_backlog_exhausted:{pending_total}>={max_pending}")
    if queue_target > 0 and pending_total >= queue_target:
        reasons.append(f"worker_queue_saturated:{pending_total}>={queue_target}")

    allowed = not reasons
    return {
        "allowed": allowed,
        "mode": "SAFE_IDLE" if not allowed and mode != "FROZEN" else mode,
        "status": status,
        "freeze_reason": freeze_reason,
        "pending_total": pending_total,
        "max_pending": max_pending,
        "queue_target": queue_target,
        "reasons": reasons,
    }
