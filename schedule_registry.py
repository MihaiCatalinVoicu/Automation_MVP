from __future__ import annotations

import json
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from db import get_conn, insert_event, insert_run, utc_now
from strategy_registry import create_change_log

ROOT = Path(__file__).resolve().parent


DEFAULT_RESEARCH_SCHEDULES = [
    {
        "id": "trend_volatility_expansion_daily",
        "repo": "crypto-bot",
        "strategy_id": "trend_volatility_expansion",
        "family_name": "trend_volatility_expansion",
        "recipe_path": "recipes/trend_volatility_expansion_daily.json",
        "cohort_config_path": "configs/research_cohort_edge_discovery_v2.json",
        "cadence": "daily",
        "enabled": True,
        "artifact_root": "data/research_artifacts/trend_volatility_expansion",
        "config": {
            "run_hour_utc": 0,
            "summary_family": "trend_volatility_expansion",
        },
    },
    {
        "id": "relative_strength_rotation_daily",
        "repo": "crypto-bot",
        "strategy_id": "relative_strength_rotation",
        "family_name": "relative_strength_rotation",
        "recipe_path": "recipes/relative_strength_rotation_daily.json",
        "cohort_config_path": "configs/research_cohort_edge_discovery_v2.json",
        "cadence": "daily",
        "enabled": True,
        "artifact_root": "data/research_artifacts/relative_strength_rotation",
        "config": {
            "run_hour_utc": 4,
            "summary_family": "relative_strength_rotation",
        },
    },
    {
        "id": "pullback_in_trend_daily",
        "repo": "crypto-bot",
        "strategy_id": "pullback_in_trend",
        "family_name": "pullback_in_trend",
        "recipe_path": "recipes/pullback_in_trend_daily.json",
        "cohort_config_path": "configs/research_cohort_edge_discovery_v2.json",
        "cadence": "daily",
        "enabled": True,
        "artifact_root": "data/research_artifacts/pullback_in_trend",
        "config": {
            "run_hour_utc": 5,
            "summary_family": "pullback_in_trend",
        },
    },
    {
        "id": "breakout_momentum_daily",
        "repo": "crypto-bot",
        "strategy_id": "breakout_momentum",
        "family_name": "breakout_momentum",
        "recipe_path": "recipes/breakout_momentum_daily.json",
        "cohort_config_path": "configs/research_cohort_month1.json",
        "cadence": "daily",
        "enabled": True,
        "artifact_root": "data/research_artifacts/breakout_momentum",
        "config": {
            "run_hour_utc": 1,
            "summary_family": "breakout_momentum",
        },
    },
    {
        "id": "spike_mean_reversion_daily",
        "repo": "crypto-bot",
        "strategy_id": "spike_mean_reversion",
        "family_name": "spike_mean_reversion",
        "recipe_path": "recipes/spike_mean_reversion_daily.json",
        "cohort_config_path": "configs/research_cohort_month1.json",
        "cadence": "daily",
        "enabled": True,
        "artifact_root": "data/research_artifacts/spike_mean_reversion",
        "config": {
            "run_hour_utc": 2,
            "summary_family": "spike_mean_reversion",
        },
    },
    {
        "id": "cross_sectional_momentum_daily",
        "repo": "crypto-bot",
        "strategy_id": "cross_sectional_momentum",
        "family_name": "cross_sectional_momentum",
        "recipe_path": "recipes/cross_sectional_momentum_daily.json",
        "cohort_config_path": "configs/research_cohort_month1.json",
        "cadence": "daily",
        "enabled": True,
        "artifact_root": "data/research_artifacts/cross_sectional_momentum",
        "config": {
            "run_hour_utc": 3,
            "summary_family": "cross_sectional_momentum",
        },
    },
]


def _json(obj: Any) -> str:
    return json.dumps(obj, ensure_ascii=True, sort_keys=True)


def _ensure_strategy_stubs() -> None:
    """Insert minimal strategy rows for schedule strategy_ids so FK constraints pass."""
    ids_needed = {s["strategy_id"] for s in DEFAULT_RESEARCH_SCHEDULES}
    now = utc_now()
    with get_conn() as conn:
        for sid in ids_needed:
            exists = conn.execute("SELECT id FROM strategies WHERE id=?", (sid,)).fetchone()
            if exists:
                continue
            name = sid.replace("_", " ").title()
            conn.execute(
                """
                INSERT INTO strategies (
                    id, name, repo, bot, category, purpose, business_hypothesis,
                    status_state, status_pct, operational_status, current_verdict,
                    owner, tags_json, notes, last_reviewed_at, created_at, updated_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    sid,
                    name,
                    "crypto-bot",
                    "crypto",
                    "research_family",
                    "Research-driven strategy",
                    "To be validated by research cycles",
                    "provisional",
                    0,
                    "research_only",
                    "PENDING_VALIDATION",
                    "schedule_registry",
                    "[]",
                    "Stub for research schedule",
                    now,
                    now,
                    now,
                ),
            )


def upsert_default_schedules() -> None:
    _ensure_strategy_stubs()
    now = utc_now()
    with get_conn() as conn:
        for item in DEFAULT_RESEARCH_SCHEDULES:
            existing = conn.execute(
                "SELECT id FROM research_schedules WHERE id=?",
                (item["id"],),
            ).fetchone()
            if existing:
                conn.execute(
                    """
                    UPDATE research_schedules
                    SET repo=?, strategy_id=?, family_name=?, recipe_path=?, cohort_config_path=?,
                        cadence=?, enabled=?, config_json=?, artifact_root=?, updated_at=?
                    WHERE id=?
                    """,
                    (
                        item["repo"],
                        item["strategy_id"],
                        item["family_name"],
                        item["recipe_path"],
                        item["cohort_config_path"],
                        item["cadence"],
                        1 if item.get("enabled", True) else 0,
                        _json(item.get("config", {})),
                        item["artifact_root"],
                        now,
                        item["id"],
                    ),
                )
            else:
                conn.execute(
                    """
                    INSERT INTO research_schedules (
                        id, repo, strategy_id, family_name, recipe_path, cohort_config_path,
                        cadence, enabled, config_json, artifact_root, last_materialized_at,
                        created_at, updated_at
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, NULL, ?, ?)
                    """,
                    (
                        item["id"],
                        item["repo"],
                        item["strategy_id"],
                        item["family_name"],
                        item["recipe_path"],
                        item["cohort_config_path"],
                        item["cadence"],
                        1 if item.get("enabled", True) else 0,
                        _json(item.get("config", {})),
                        item["artifact_root"],
                        now,
                        now,
                    ),
                )


def list_research_schedules(enabled_only: bool = False) -> list[dict[str, Any]]:
    with get_conn() as conn:
        if enabled_only:
            rows = conn.execute(
                "SELECT * FROM research_schedules WHERE enabled=1 ORDER BY id"
            ).fetchall()
        else:
            rows = conn.execute("SELECT * FROM research_schedules ORDER BY id").fetchall()
    out = []
    for row in rows:
        item = dict(row)
        item["config"] = json.loads(item.get("config_json") or "{}")
        out.append(item)
    return out


def list_schedule_runs(limit: int = 100) -> list[dict[str, Any]]:
    with get_conn() as conn:
        rows = conn.execute(
            "SELECT * FROM schedule_runs ORDER BY created_at DESC LIMIT ?",
            (limit,),
        ).fetchall()
    return [dict(r) for r in rows]


def _run_exists_for_date(schedule_id: str, run_date: str) -> bool:
    with get_conn() as conn:
        row = conn.execute(
            "SELECT id FROM schedule_runs WHERE schedule_id=? AND run_date=? LIMIT 1",
            (schedule_id, run_date),
        ).fetchone()
    return row is not None


def _make_task(schedule: dict[str, Any], run_date: str) -> dict[str, Any]:
    artifact_root = ROOT / schedule["artifact_root"] / run_date
    return {
        "repo": schedule["repo"],
        "goal": f"Daily edge research cycle: {schedule['family_name']} ({run_date})",
        "branch": f"auto/research/{schedule['family_name']}/{run_date.replace('-', '')}",
        "task_type": "validation_battery",
        "constraints": [
            "server canonical run",
            "shadow recommendation only",
            "do not mutate paper or live configs",
        ],
        "checks": [],
        "preferred_executor": "composer",
        "recipe": schedule["recipe_path"],
        "run_context": {
            "run_date": run_date,
            "cohort_config": schedule["cohort_config_path"],
            "research_output_dir": str(artifact_root.resolve()),
            "family_name": schedule["family_name"],
        },
        "strategy_id": schedule["strategy_id"],
        "category_id": "research_family",
        "change_kind": "research_cycle",
        "metadata": {
            "schedule_id": schedule["id"],
            "family_name": schedule["family_name"],
            "artifact_root": str(artifact_root.resolve()),
            "treat_reject_as_completed": "true",
        },
    }


def materialize_due_runs(now: datetime | None = None) -> list[dict[str, Any]]:
    now = now or datetime.now(timezone.utc)
    run_date = now.date().isoformat()
    created: list[dict[str, Any]] = []
    for schedule in list_research_schedules(enabled_only=True):
        run_hour = int(schedule.get("config", {}).get("run_hour_utc", 0))
        if now.hour < run_hour:
            continue
        if _run_exists_for_date(schedule["id"], run_date):
            continue

        run_id = uuid.uuid4().hex[:12]
        routing = {"planner_agent": "none", "executor_agent": "composer", "reviewer_agent": "none"}
        task_payload = _make_task(schedule, run_date)
        insert_run(
            run_id=run_id,
            repo=schedule["repo"],
            goal=task_payload["goal"],
            branch=task_payload["branch"],
            task_type=task_payload["task_type"],
            task_json=task_payload,
            routing_json=routing,
            status="QUEUED",
            preferred_executor=task_payload["preferred_executor"],
        )
        insert_event(
            run_id,
            "research_schedule_materialized",
            {
                "schedule_id": schedule["id"],
                "family_name": schedule["family_name"],
                "run_date": run_date,
            },
        )
        create_change_log(
            repo=schedule["repo"],
            strategy_id=schedule["strategy_id"],
            run_id=run_id,
            category_id="research_family",
            change_kind="research_cycle",
            summary=task_payload["goal"],
            requested_by="schedule_registry",
            status="SUBMITTED",
            expected_impact={"schedule_id": schedule["id"], "family_name": schedule["family_name"]},
        )
        with get_conn() as conn:
            conn.execute(
                """
                INSERT INTO schedule_runs (id, schedule_id, run_id, run_date, status, created_at, updated_at)
                VALUES (?, ?, ?, ?, ?, ?, ?)
                """,
                (uuid.uuid4().hex[:12], schedule["id"], run_id, run_date, "QUEUED", utc_now(), utc_now()),
            )
            conn.execute(
                "UPDATE research_schedules SET last_materialized_at=?, updated_at=? WHERE id=?",
                (utc_now(), utc_now(), schedule["id"]),
            )
        created.append({"schedule_id": schedule["id"], "run_id": run_id, "run_date": run_date})
    return created
