from __future__ import annotations

from typing import Dict, List, Literal, Optional

from pydantic import BaseModel, Field

DecisionType = Literal["RETRY_SAFE", "PLAN_B", "ABORT", "APPROVE_PUSH", "ASK_PREMIUM"]
RunStatus = Literal[
    "QUEUED",
    "RUNNING",
    "NEEDS_APPROVAL",
    "RETRY_PENDING",
    "FAILED",
    "COMPLETED",
    "ABORTED",
]


class ApprovalPolicy(BaseModel):
    repeat_failures_threshold: int = 3
    push_requires_approval: bool = True
    non_allowlisted_shell_requires_approval: bool = True


class TaskCreate(BaseModel):
    repo: str
    goal: str
    branch: str = "auto/mvp-task"
    task_type: str = "bugfix"
    constraints: List[str] = Field(default_factory=list)
    checks: List[str] = Field(default_factory=list)
    preferred_executor: str = "composer"
    metadata: Dict[str, str] = Field(default_factory=dict)
    approval_policy: ApprovalPolicy = Field(default_factory=ApprovalPolicy)

    # validation_battery only: recipe path + run_context (run_dir required)
    recipe: Optional[str] = None
    run_context: Optional[Dict[str, str]] = None


class RunResponse(BaseModel):
    run_id: str
    status: str
    routing: dict
    message: str


class ApprovalRequestSummary(BaseModel):
    reason: str
    failed_command: Optional[str] = None
    repeat_count: int = 0
    last_error: Optional[str] = None
    executor_agent: Optional[str] = None
    plan_b_hint: Optional[str] = None
