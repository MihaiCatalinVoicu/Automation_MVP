from __future__ import annotations

import json
import uuid

from dotenv import load_dotenv
from fastapi import FastAPI, HTTPException

from approval_service import apply_decision as approval_apply_decision
from approval_service import create_pre_execution_approval
from artifact_store import list_artifacts
from db import get_run, init_db, insert_event, insert_run, list_events
from models import RunResponse, TaskCreate
from policies import choose_routing
from policy_engine import validate_strategy_reference, validate_task
from repo_registry import RepoRegistry
from schedule_registry import list_research_schedules, list_schedule_runs, upsert_default_schedules
from shadow_recommendations import build_shadow_board
from strategy_registry import create_change_log

load_dotenv()

app = FastAPI(title="Automation Orchestrator MVP")


@app.on_event("startup")
def on_startup() -> None:
    init_db()
    upsert_default_schedules()


@app.get("/health")
def health() -> dict:
    return {"ok": True}


@app.post("/runs", response_model=RunResponse)
def create_run(task: TaskCreate) -> RunResponse:
    task_payload = task.model_dump()
    try:
        registry = RepoRegistry()
        repo_cfg = registry.get(task.repo)
    except Exception:
        repo_cfg = {}

    result = validate_task(repo_cfg, task_payload)
    if result.status == "failed":
        raise HTTPException(
            status_code=400,
            detail={"policy_rejected": True, "reason": result.reason, "risk_level": result.risk_level},
        )

    strategy_result = validate_strategy_reference(repo_cfg, task_payload)
    if strategy_result.status == "failed":
        raise HTTPException(
            status_code=400,
            detail={
                "strategy_rejected": True,
                "reason": strategy_result.reason,
                "decision": strategy_result.decision,
                "candidates": strategy_result.candidates or [],
            },
        )
    if strategy_result.resolved_strategy_id:
        task_payload["strategy_id"] = strategy_result.resolved_strategy_id
    if strategy_result.resolved_category_id:
        task_payload["category_id"] = strategy_result.resolved_category_id

    run_id = uuid.uuid4().hex[:12]
    routing = choose_routing(task_payload)

    if result.needs_pre_approval:
        status = "NEEDS_APPROVAL"
        message = "Run created. Pre-execution approval required."
    else:
        status = "QUEUED"
        message = "Run created. Worker will pick it up."

    insert_run(
        run_id=run_id,
        repo=task.repo,
        goal=task.goal,
        branch=task.branch,
        task_type=task.task_type,
        task_json=task_payload,
        routing_json=routing,
        status=status,
        preferred_executor=task.preferred_executor,
    )

    insert_event(
        run_id,
        "run_created",
        {"task": task_payload, "routing": routing},
    )
    insert_event(
        run_id,
        "policy_validation_passed",
        {"status": result.status, "risk_level": result.risk_level, "reason": result.reason},
    )
    insert_event(
        run_id,
        "strategy_crossref_passed",
        {
            "decision": strategy_result.decision,
            "reason": strategy_result.reason,
            "strategy_id": task_payload.get("strategy_id"),
            "category_id": task_payload.get("category_id"),
            "requires_registry_update": strategy_result.requires_registry_update,
        },
    )
    if result.needs_pre_approval:
        insert_event(run_id, "policy_escalation_required", {"reason": "needs_pre_execution_approval"})

    create_change_log(
        repo=task.repo,
        strategy_id=task_payload.get("strategy_id"),
        run_id=run_id,
        category_id=task_payload.get("category_id"),
        change_kind=task.change_kind,
        summary=task.goal,
        proposed_strategy_name=task.new_strategy_proposal,
        requested_by="api",
        status="SUBMITTED",
        expected_impact={"task_type": task.task_type, "decision": strategy_result.decision},
    )

    if result.needs_pre_approval:
        create_pre_execution_approval(run_id, task.goal, result.reason)

    return RunResponse(
        run_id=run_id,
        status=status,
        routing=routing,
        message=message,
    )


@app.post("/runs/{run_id}/approve")
def approve_run(run_id: str, decision: str = "RETRY_SAFE") -> dict:
    """Apply approval decision (fallback when Telegram fails). For local testing."""
    run = get_run(run_id)
    if not run:
        raise HTTPException(status_code=404, detail="Run not found")
    pending = __import__("db").get_pending_approval_for_run(run_id)
    if not pending:
        raise HTTPException(status_code=400, detail="No pending approval for this run")
    return approval_apply_decision(pending["id"], decision)


@app.get("/runs/{run_id}")
def run_status(run_id: str) -> dict:
    run = get_run(run_id)
    if not run:
        raise HTTPException(status_code=404, detail="Run not found")

    return {
        "run": {
            **run,
            "task_json": json.loads(run["task_json"]),
            "routing_json": json.loads(run["routing_json"]),
        },
        "events": list_events(run_id),
    }


@app.get("/research/schedules")
def research_schedules() -> dict:
    return {"schedules": list_research_schedules(enabled_only=False), "recent_schedule_runs": list_schedule_runs(limit=50)}


@app.get("/research/artifacts")
def research_artifacts(family_name: str | None = None, limit: int = 50) -> dict:
    return {"artifacts": list_artifacts(family_name=family_name, limit=limit)}


@app.get("/research/shadow-board")
def research_shadow_board(lookback_days: int = 7) -> dict:
    out_dir = __import__("pathlib").Path("data") / "shadow_recommendations"
    return build_shadow_board(out_dir, lookback_days=lookback_days)
