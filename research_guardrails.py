from __future__ import annotations

import os
from datetime import datetime, timezone
from typing import Any

from db import (
    count_manifests_created_since,
    get_search_case,
)

DEFAULT_ALLOWED_FAMILIES = "btc_structural_daily,breakout_momentum,oi_cascade,cross_sectional_momentum"


def _utc_day_start_iso() -> str:
    now = datetime.now(timezone.utc)
    day_start = datetime(now.year, now.month, now.day, tzinfo=timezone.utc)
    return day_start.isoformat()


def _allowed_families() -> set[str]:
    raw = os.getenv("RESEARCH_ALLOWED_FAMILIES", DEFAULT_ALLOWED_FAMILIES)
    return {item.strip() for item in raw.split(",") if item.strip()}


def evaluate_manifest_guardrails(manifest: dict[str, Any]) -> tuple[bool, str]:
    case_id = str(manifest.get("case_id") or "")
    case = get_search_case(case_id)
    if not case:
        return False, "case_missing"

    family = str(case.get("family") or "")
    allowed = _allowed_families()
    if family not in allowed:
        return False, f"family_not_allowed:{family}"

    since_ts = _utc_day_start_iso()
    max_per_case = int(os.getenv("RESEARCH_MAX_MANIFESTS_PER_CASE_PER_DAY", "3"))
    max_total = int(os.getenv("RESEARCH_MAX_MANIFESTS_PER_DAY", "20"))

    per_case_today = count_manifests_created_since(since_ts, case_id=case_id)
    total_today = count_manifests_created_since(since_ts)
    if per_case_today > max_per_case:
        return False, f"case_budget_exhausted:{per_case_today}>{max_per_case}"
    if total_today > max_total:
        return False, f"global_budget_exhausted:{total_today}>{max_total}"
    return True, "ok"

