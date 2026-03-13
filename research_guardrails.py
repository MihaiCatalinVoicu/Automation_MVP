from __future__ import annotations

import json
import os
from datetime import datetime, timezone
from typing import Any

from db import (
    count_active_manifest_workers,
    count_manifests_created_since,
    count_pending_manifests,
    get_edge_verdict,
    get_family_budget_state,
    get_search_case,
)
from family_registry import allowed_family_ids, family_batch_size, get_family_definition, sync_family_registry_db

DEFAULT_ALLOWED_FAMILIES = ",".join(sorted(allowed_family_ids()))


def _utc_day_start_iso() -> str:
    now = datetime.now(timezone.utc)
    day_start = datetime(now.year, now.month, now.day, tzinfo=timezone.utc)
    return day_start.isoformat()


def _allowed_families() -> set[str]:
    raw = os.getenv("RESEARCH_ALLOWED_FAMILIES", DEFAULT_ALLOWED_FAMILIES)
    return {item.strip() for item in raw.split(",") if item.strip()}


def _load_json(text: str | None) -> dict[str, Any]:
    if not text:
        return {}
    try:
        return json.loads(text)
    except json.JSONDecodeError:
        return {}


def _default_budget_for(validation_level: str) -> int:
    if validation_level == "expensive":
        return 1
    if validation_level == "medium":
        return 2
    return 6


def _env_int(name: str, default: int) -> int:
    try:
        return int(os.getenv(name, str(default)))
    except ValueError:
        return default


def evaluate_manifest_plan_guardrails(
    *,
    case: dict[str, Any],
    family: str,
    execution_spec: dict[str, Any],
    search_budget: dict[str, Any] | None = None,
    derived_from_verdict_id: str | None = None,
    enforce_backlog: bool = False,
) -> tuple[bool, str]:
    sync_family_registry_db()
    allowed = _allowed_families()
    if family not in allowed:
        return False, f"family_not_allowed:{family}"

    registry_item = get_family_definition(family)
    if registry_item is None:
        return False, f"family_unregistered:{family}"

    search_budget = search_budget or {}
    case_id = str(case.get("case_id") or "")
    validation_level = str(execution_spec.get("validation_level") or "cheap").lower()
    if validation_level not in {"cheap", "medium", "expensive"}:
        return False, f"invalid_validation_level:{validation_level}"

    allowed_levels = set(registry_item.allowed_validation_levels)
    if validation_level not in allowed_levels:
        return False, f"validation_level_not_allowed:{family}:{validation_level}"

    requested_batch_size = int(
        execution_spec.get("batch_size")
        or execution_spec.get("variants_per_generation")
        or family_batch_size(family, validation_level)
    )
    max_batch_size = int(search_budget.get("max_batch_size") or family_batch_size(family, validation_level))
    if requested_batch_size > max_batch_size:
        return False, f"batch_size_exhausted:{requested_batch_size}>{max_batch_size}"

    family_state = get_family_budget_state(family)
    if family_state:
        family_status = str(family_state.get("status") or "")
        family_score = float(family_state.get("family_score") or 0.0)
        budget_state = _load_json(family_state.get("budget_state_json"))
        validation_caps = budget_state.get("validation_caps") or {}
        if family_status in {"paused", "frozen", "archived"}:
            return False, f"family_budget_frozen:{family_status}"
        if family_score < 0.20:
            return False, f"family_score_frozen:{family_score:.3f}"
        if family_score < 0.50 and validation_level != "cheap":
            return False, f"family_score_cheap_only:{family_score:.3f}"
        if family_score < 0.70 and validation_level == "expensive":
            return False, f"family_score_expensive_blocked:{family_score:.3f}"
        if validation_level in validation_caps:
            level_cap = int(validation_caps.get(validation_level) or 0)
            if level_cap <= 0:
                return False, f"family_validation_cap_blocked:{validation_level}"
            family_pending = count_pending_manifests(family=family, validation_level=validation_level)
            if family_pending >= level_cap:
                return False, f"family_validation_cap_exhausted:{validation_level}:{family_pending}>={level_cap}"

    if derived_from_verdict_id:
        verdict = get_edge_verdict(derived_from_verdict_id)
        if not verdict:
            return False, f"derived_verdict_missing:{derived_from_verdict_id}"
        near_miss_floor = float(os.getenv("RESEARCH_MIN_NEAR_MISS_SCORE_FOR_MUTATION", "0.60"))
        min_trades_for_mutation = _env_int("RESEARCH_MIN_TRADES_FOR_MUTATION", 80)
        near_miss_score = float(verdict.get("near_miss_score") or 0.0)
        metrics_snapshot = _load_json(verdict.get("metrics_snapshot_json"))
        trades = int(metrics_snapshot.get("trades") or metrics_snapshot.get("trade_count") or 0)
        if near_miss_score < near_miss_floor:
            return False, f"near_miss_too_weak:{near_miss_score:.3f}"
        if trades < min_trades_for_mutation:
            return False, f"near_miss_low_confidence:{trades}<{min_trades_for_mutation}"

    if enforce_backlog:
        max_pending = _env_int("RESEARCH_MAX_PENDING_MANIFESTS", 200)
        pending_total = count_pending_manifests()
        if pending_total >= max_pending:
            return False, f"pending_backlog_exhausted:{pending_total}>={max_pending}"
        family_pending_total = count_pending_manifests(family=family)
        default_family_cap = {"trend_volatility_expansion": 80, "pullback_in_trend": 60, "relative_strength_rotation": 40}.get(
            family,
            _env_int("RESEARCH_DEFAULT_MAX_PENDING_PER_FAMILY", 40),
        )
        max_family_pending = int(search_budget.get("max_pending_manifests") or search_budget.get("max_family_pending_manifests") or default_family_cap)
        if family_pending_total >= max_family_pending:
            return False, f"family_pending_exhausted:{family_pending_total}>={max_family_pending}"
        queue_depth_per_worker = _env_int("RESEARCH_TARGET_QUEUE_DEPTH_PER_WORKER", 20)
        active_workers = count_active_manifest_workers()
        if active_workers <= 0:
            active_workers = _env_int("RESEARCH_ACTIVE_WORKERS_FALLBACK", 1)
        target_queue = max(1, active_workers) * max(1, queue_depth_per_worker)
        if pending_total >= target_queue:
            return False, f"worker_queue_saturated:{pending_total}>={target_queue}"

    since_ts = _utc_day_start_iso()
    max_per_case = int(
        search_budget.get("max_manifests_per_day")
        or os.getenv("RESEARCH_MAX_MANIFESTS_PER_CASE_PER_DAY", "3")
    )
    max_total = int(os.getenv("RESEARCH_MAX_MANIFESTS_PER_DAY", "20"))
    max_per_family = int(search_budget.get("max_family_manifests_per_day") or os.getenv("RESEARCH_MAX_MANIFESTS_PER_FAMILY_PER_DAY", "8"))
    max_per_level = int(search_budget.get(f"max_{validation_level}_manifests_per_day") or _default_budget_for(validation_level))

    per_case_today = count_manifests_created_since(since_ts, case_id=case_id)
    per_family_today = count_manifests_created_since(since_ts, family=family)
    per_level_today = count_manifests_created_since(since_ts, case_id=case_id, validation_level=validation_level)
    total_today = count_manifests_created_since(since_ts)
    if per_case_today > max_per_case:
        return False, f"case_budget_exhausted:{per_case_today}>{max_per_case}"
    if per_family_today > max_per_family:
        return False, f"family_budget_exhausted:{per_family_today}>{max_per_family}"
    if per_level_today > max_per_level:
        return False, f"level_budget_exhausted:{validation_level}:{per_level_today}>{max_per_level}"
    if total_today > max_total:
        return False, f"global_budget_exhausted:{total_today}>{max_total}"
    return True, "ok"


def evaluate_manifest_guardrails(manifest: dict[str, Any]) -> tuple[bool, str]:
    case_id = str(manifest.get("case_id") or "")
    case = get_search_case(case_id)
    if not case:
        return False, "case_missing"
    execution_spec = _load_json(manifest.get("execution_spec_json"))
    search_budget = _load_json(case.get("search_budget_json"))
    family = str(case.get("family") or "")
    return evaluate_manifest_plan_guardrails(
        case=case,
        family=family,
        execution_spec=execution_spec,
        search_budget=search_budget,
        derived_from_verdict_id=str(manifest.get("derived_from_verdict_id") or "") or None,
        enforce_backlog=False,
    )

