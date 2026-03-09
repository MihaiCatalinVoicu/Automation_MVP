from __future__ import annotations

import json
import uuid

from dotenv import load_dotenv
from fastapi import FastAPI, HTTPException

from approval_service import apply_decision as approval_apply_decision
from approval_service import create_pre_execution_approval
from db import get_run, init_db, insert_event, insert_run, list_events
from models import RunResponse, TaskCreate
from policies import choose_routing
from policy_engine import validate_task
from repo_registry import RepoRegistry

load_dotenv()

app = FastAPI(title="Automation Orchestrator MVP")


@app.on_event("startup")
def on_startup() -> None:
    init_db()


@app.get("/health")
def health() -> dict:
    return {"ok": True}


@app.post("/runs", response_model=RunResponse)
def create_run(task: TaskCreate) -> RunResponse:
    try:
        registry = RepoRegistry()
        repo_cfg = registry.get(task.repo)
    except Exception:
        repo_cfg = {}

    result = validate_task(repo_cfg, task.model_dump())
    if result.status == "failed":
        raise HTTPException(
            status_code=400,
            detail={"policy_rejected": True, "reason": result.reason, "risk_level": result.risk_level},
        )

    run_id = uuid.uuid4().hex[:12]
    routing = choose_routing(task.model_dump())

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
        task_json=task.model_dump(),
        routing_json=routing,
        status=status,
        preferred_executor=task.preferred_executor,
    )

    insert_event(
        run_id,
        "run_created",
        {"task": task.model_dump(), "routing": routing},
    )
    insert_event(
        run_id,
        "policy_validation_passed",
        {"status": result.status, "risk_level": result.risk_level, "reason": result.reason},
    )
    if result.needs_pre_approval:
        insert_event(run_id, "policy_escalation_required", {"reason": "needs_pre_execution_approval"})

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
