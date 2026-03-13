#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import os
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

from db import (
    create_case_event,
    create_experiment_manifest,
    get_conn,
    get_search_case,
    init_db,
    list_experiment_manifests,
    manifest_config_fingerprint_exists,
    record_maintenance_job_run,
)
from edge_search_state import preflight_mutation_cycle
from family_registry import family_batch_size, sync_family_registry_db
from research_guardrails import evaluate_manifest_plan_guardrails
from research_loop import _config_fingerprint

AUTOMATION_ROOT = Path(__file__).resolve().parent

_VALIDATION_DEFAULTS = {
    "MUTATE_WITH_POLICY": {"validation_level": "cheap", "budget_cost": 1},
    "RETEST_OOS": {"validation_level": "medium", "budget_cost": 3},
    "RUN_BIGGER_SAMPLE": {"validation_level": "expensive", "budget_cost": 8},
}


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


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


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


def _list_candidate_rows(*, since_hours: int, min_near_miss_score: float, limit: int) -> list[dict[str, Any]]:
    cutoff = (datetime.now(timezone.utc) - timedelta(hours=max(1, since_hours))).isoformat()
    with get_conn() as conn:
        rows = conn.execute(
            """
            SELECT
                ev.*,
                sc.family AS case_family,
                sc.status AS case_status,
                sc.stage AS case_stage,
                sc.search_budget_json,
                em.manifest_id AS parent_manifest_id,
                em.manifest_version AS parent_manifest_version,
                em.repo,
                em.adapter_type,
                em.entrypoint,
                em.strategy_identity_json,
                em.run_context_template_json,
                em.dataset_spec_json,
                em.execution_spec_json,
                em.cost_model_json,
                em.gates_json,
                em.planner_hints_json,
                em.artifacts_json
            FROM edge_verdicts ev
            JOIN search_cases sc ON sc.case_id = ev.case_id
            JOIN experiment_manifests em ON em.manifest_id = ev.manifest_id
            WHERE ev.status = 'final'
              AND sc.latest_verdict_id = ev.verdict_id
              AND sc.status NOT IN ('done', 'killed', 'archived')
              AND ev.decision = 'MUTATE_WITH_POLICY'
              AND COALESCE(ev.near_miss_score, 0) >= ?
              AND ev.created_at >= ?
            ORDER BY COALESCE(ev.near_miss_score, 0) DESC, ev.created_at DESC
            LIMIT ?
            """,
            (min_near_miss_score, cutoff, max(1, limit)),
        ).fetchall()
    return [dict(row) for row in rows]


def _candidate_config_path(candidate: dict[str, Any]) -> Path | None:
    artifacts_root_raw = str(candidate.get("artifacts_root") or "").strip()
    if artifacts_root_raw:
        artifacts_root = Path(artifacts_root_raw).expanduser()
        next_cfg = artifacts_root / "next_batch_config.json"
        if next_cfg.exists():
            return next_cfg.resolve()
    execution_spec = _load_json(candidate.get("execution_spec_json"))
    raw = str(execution_spec.get("config_path") or "").strip()
    if not raw:
        return None
    path = Path(raw).expanduser()
    if path.exists():
        return path.resolve()
    return None


def _case_next_manifest_version(case_id: str) -> int:
    manifests = list_experiment_manifests(case_id=case_id)
    current = max((_safe_int(item.get("manifest_version")) for item in manifests), default=0)
    return current + 1


def _derived_manifest_exists(case_id: str, verdict_id: str) -> bool:
    manifests = list_experiment_manifests(case_id=case_id, derived_from_verdict_id=verdict_id)
    return bool(manifests)


def _build_child_manifest_payload(candidate: dict[str, Any], *, config_path: Path, config_fingerprint: str) -> dict[str, Any]:
    action = str(candidate.get("decision") or "MUTATE_WITH_POLICY").upper()
    family = str(candidate.get("case_family") or "")
    defaults = _VALIDATION_DEFAULTS.get(action, _VALIDATION_DEFAULTS["MUTATE_WITH_POLICY"])
    execution_spec = _load_json(candidate.get("execution_spec_json"))
    dataset_spec = _load_json(candidate.get("dataset_spec_json"))
    strategy_identity = _load_json(candidate.get("strategy_identity_json"))
    run_context_template = _load_json(candidate.get("run_context_template_json"))
    cost_model = _load_json(candidate.get("cost_model_json"))
    gates = _load_json(candidate.get("gates_json"))
    planner_hints = _load_json(candidate.get("planner_hints_json"))
    artifacts = _load_json(candidate.get("artifacts_json"))
    search_budget = _load_json(candidate.get("search_budget_json"))
    mutation_recommendation = _load_json(candidate.get("mutation_recommendation_json"))

    validation_level = str(execution_spec.get("validation_level") or defaults["validation_level"]).lower()
    batch_size = _safe_int(
        mutation_recommendation.get("max_children")
        or execution_spec.get("batch_size")
        or execution_spec.get("variants_per_generation")
        or family_batch_size(family, validation_level)
    )
    batch_size = max(1, min(batch_size, _env_int("RESEARCH_MAX_MUTATION_BATCH_SIZE", 6)))
    budget_cost = _safe_int(execution_spec.get("budget_cost") or defaults["budget_cost"])
    execution_spec["config_path"] = str(config_path)
    execution_spec["validation_level"] = validation_level
    execution_spec["budget_cost"] = budget_cost
    execution_spec["batch_size"] = batch_size
    execution_spec["variants_per_generation"] = batch_size
    if candidate.get("policy_selected"):
        execution_spec["policy_selected"] = candidate["policy_selected"]
    if mutation_recommendation:
        execution_spec["mutation_intent"] = mutation_recommendation.get("mutation_class")

    manifest_id = f"{candidate['parent_manifest_id']}_auto_{str(candidate['verdict_id'])[-8:]}"
    idempotency_key = (
        f"auto:{candidate['verdict_id']}:{validation_level}:{batch_size}:{config_fingerprint or 'nofp'}"
    )
    output_root = AUTOMATION_ROOT / "data" / "research_loops" / manifest_id
    artifacts["output_root"] = str(output_root)
    planner_hints["config_fingerprint"] = config_fingerprint
    planner_hints["mutation_cycle_source"] = "auto_server"
    planner_hints["derived_from_verdict_id"] = candidate["verdict_id"]
    planner_hints["selected_near_miss_score"] = round(_safe_float(candidate.get("near_miss_score")), 4)
    planner_hints["selected_reason"] = str(candidate.get("decision_reason") or "")

    payload = {
        "manifest_id": manifest_id,
        "idempotency_key": idempotency_key,
        "manifest_version": _case_next_manifest_version(str(candidate["case_id"])),
        "status": "ready",
        "repo": str(candidate.get("repo") or "crypto-bot"),
        "adapter_type": str(candidate.get("adapter_type") or "research_loop"),
        "entrypoint": str(candidate.get("entrypoint") or "research_loop.py"),
        "strategy_identity": strategy_identity,
        "run_context_template": run_context_template,
        "dataset_spec": dataset_spec,
        "execution_spec": execution_spec,
        "cost_model": cost_model,
        "gates": gates,
        "planner_hints": planner_hints,
        "artifacts": artifacts,
        "approved_by": "mutation_cycle",
        "created_by": "mutation_cycle",
        "parent_manifest_id": str(candidate["parent_manifest_id"]),
        "derived_from_verdict_id": str(candidate["verdict_id"]),
        "derivation_reason": "auto_mutation_cycle",
        "notes": f"Auto-proposed from verdict {candidate['verdict_id']}",
        "param_diff": {
            "action": action,
            "decision_reason": str(candidate.get("decision_reason") or ""),
            "validation_level": validation_level,
            "budget_cost": budget_cost,
            "batch_size": batch_size,
            "config_fingerprint": config_fingerprint,
            "source": "mutation_cycle",
            "config_path": str(config_path),
            "policy_selected": execution_spec.get("policy_selected"),
            "mutation_intent": execution_spec.get("mutation_intent"),
        },
        "search_budget": search_budget,
    }
    return payload


def run_mutation_cycle(*, since_hours: int, limit: int, dry_run: bool = False) -> dict[str, Any]:
    init_db()
    sync_family_registry_db()
    preflight = preflight_mutation_cycle()
    if not preflight["allowed"]:
        summary = {
            "generated_at": _utc_now(),
            "since_hours": since_hours,
            "candidate_count": 0,
            "created_count": 0,
            "dry_run": dry_run,
            "created_manifests": [],
            "skipped": [],
            "live_edge_search": preflight,
        }
        record_maintenance_job_run("mutation_cycle", "skipped", summary)
        return summary
    min_near_miss_score = _env_float("RESEARCH_MIN_NEAR_MISS_SCORE_FOR_MUTATION", 0.60)
    candidates = _list_candidate_rows(
        since_hours=since_hours,
        min_near_miss_score=min_near_miss_score,
        limit=limit,
    )
    max_created = _env_int("MUTATION_MAX_MANIFESTS_PER_CYCLE", 10)
    summary: dict[str, Any] = {
        "generated_at": _utc_now(),
        "since_hours": since_hours,
        "candidate_count": len(candidates),
        "created_count": 0,
        "dry_run": dry_run,
        "created_manifests": [],
        "skipped": [],
        "live_edge_search": preflight,
    }

    for candidate in candidates:
        case_id = str(candidate.get("case_id") or "")
        verdict_id = str(candidate.get("verdict_id") or "")
        family = str(candidate.get("case_family") or "")
        case = get_search_case(case_id)
        if not case:
            summary["skipped"].append({"case_id": case_id, "reason": "case_missing"})
            continue
        if _derived_manifest_exists(case_id, verdict_id):
            summary["skipped"].append({"case_id": case_id, "verdict_id": verdict_id, "reason": "derived_manifest_exists"})
            continue
        config_path = _candidate_config_path(candidate)
        if config_path is None:
            summary["skipped"].append({"case_id": case_id, "verdict_id": verdict_id, "reason": "next_config_missing"})
            create_case_event(
                case_id=case_id,
                manifest_id=str(candidate.get("parent_manifest_id") or ""),
                verdict_id=verdict_id,
                event_type="mutation_cycle_skipped",
                payload={"reason": "next_config_missing"},
            )
            continue
        try:
            cfg = json.loads(config_path.read_text(encoding="utf-8"))
        except Exception as exc:
            summary["skipped"].append({"case_id": case_id, "verdict_id": verdict_id, "reason": f"config_unreadable:{exc}"})
            continue
        config_fingerprint = _config_fingerprint(cfg)
        if manifest_config_fingerprint_exists(config_fingerprint):
            summary["skipped"].append(
                {
                    "case_id": case_id,
                    "verdict_id": verdict_id,
                    "reason": "config_fingerprint_exists",
                    "config_fingerprint": config_fingerprint,
                }
            )
            create_case_event(
                case_id=case_id,
                manifest_id=str(candidate.get("parent_manifest_id") or ""),
                verdict_id=verdict_id,
                event_type="mutation_cycle_skipped",
                payload={"reason": "config_fingerprint_exists", "config_fingerprint": config_fingerprint},
            )
            continue
        child = _build_child_manifest_payload(candidate, config_path=config_path, config_fingerprint=config_fingerprint)
        allowed, reason = evaluate_manifest_plan_guardrails(
            case=case,
            family=family,
            execution_spec=child["execution_spec"],
            search_budget=child["search_budget"],
            derived_from_verdict_id=verdict_id,
            enforce_backlog=True,
        )
        if not allowed:
            summary["skipped"].append(
                {"case_id": case_id, "verdict_id": verdict_id, "reason": reason, "family": family}
            )
            create_case_event(
                case_id=case_id,
                manifest_id=str(candidate.get("parent_manifest_id") or ""),
                verdict_id=verdict_id,
                event_type="mutation_cycle_skipped",
                payload={"reason": reason, "family": family},
            )
            continue
        if dry_run:
            summary["created_manifests"].append(
                {
                    "manifest_id": child["manifest_id"],
                    "case_id": case_id,
                    "verdict_id": verdict_id,
                    "family": family,
                    "config_fingerprint": config_fingerprint,
                    "config_path": str(config_path),
                }
            )
            summary["created_count"] += 1
        else:
            create_experiment_manifest(
                manifest_id=child["manifest_id"],
                case_id=case_id,
                idempotency_key=child["idempotency_key"],
                manifest_version=child["manifest_version"],
                status=child["status"],
                repo=child["repo"],
                adapter_type=child["adapter_type"],
                entrypoint=child["entrypoint"],
                strategy_identity=child["strategy_identity"],
                run_context_template=child["run_context_template"],
                dataset_spec=child["dataset_spec"],
                execution_spec=child["execution_spec"],
                cost_model=child["cost_model"],
                gates=child["gates"],
                planner_hints=child["planner_hints"],
                artifacts=child["artifacts"],
                parent_manifest_id=child["parent_manifest_id"],
                derived_from_verdict_id=child["derived_from_verdict_id"],
                derivation_reason=child["derivation_reason"],
                param_diff=child["param_diff"],
                created_by=child["created_by"],
                approved_by=child["approved_by"],
                notes=child["notes"],
                force_stage_transition=True,
            )
            create_case_event(
                case_id=case_id,
                manifest_id=child["manifest_id"],
                verdict_id=verdict_id,
                event_type="mutation_cycle_manifest_created",
                payload={
                    "family": family,
                    "config_fingerprint": config_fingerprint,
                    "config_path": str(config_path),
                    "validation_level": child["execution_spec"]["validation_level"],
                    "batch_size": child["execution_spec"]["batch_size"],
                },
            )
            summary["created_manifests"].append(
                {
                    "manifest_id": child["manifest_id"],
                    "case_id": case_id,
                    "verdict_id": verdict_id,
                    "family": family,
                    "config_fingerprint": config_fingerprint,
                }
            )
            summary["created_count"] += 1
        if summary["created_count"] >= max_created:
            break

    record_maintenance_job_run("mutation_cycle", "ok", summary)
    return summary


def _main() -> int:
    ap = argparse.ArgumentParser(description="Propose bounded edge-search mutation manifests")
    ap.add_argument("--since-hours", type=int, default=72)
    ap.add_argument("--limit", type=int, default=25)
    ap.add_argument("--dry-run", action="store_true")
    ap.add_argument("--output-json", default="")
    args = ap.parse_args()

    summary = run_mutation_cycle(
        since_hours=max(1, args.since_hours),
        limit=max(1, args.limit),
        dry_run=bool(args.dry_run),
    )
    output_path = str(args.output_json or "").strip()
    if output_path:
        path = Path(output_path).expanduser().resolve()
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(json.dumps(summary, indent=2), encoding="utf-8")
        print(f"Wrote: {path}")
    print(json.dumps(summary, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(_main())
