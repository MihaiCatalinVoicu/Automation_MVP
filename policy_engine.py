"""Policy engine: enforce repo profiles at submit and during execution."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Literal

from strategy_registry import CrossRefResult, preflight_cross_reference

RiskLevel = Literal["LOW", "MEDIUM", "HIGH"]
PolicyStatus = Literal["passed", "failed", "needs_approval"]


FORBIDDEN_GOAL_FRAGMENTS = (
    "scoring",
    "strategy",
    "ingest",
    "pipeline",
    "filter",
    "ranking",
    "outcome logic",
    "src/",
    "config.yaml",
    "config live",
    "verdict",
)

SAFE_GOAL_FRAGMENTS = (
    "read-only",
    "read only",
    "docs",
    "readme",
    "preflight",
    "status",
    "validation",
    "operational",
    "tooling",
    "script",
    "template",
)

SAFE_CONSTRAINT_FRAGMENTS = (
    "touch only docs",
    "touch only readme",
    "do not modify src",
    "do not modify strategy",
    "read-only",
    "read only",
)


@dataclass
class PolicyResult:
    status: PolicyStatus
    risk_level: RiskLevel
    reason: str
    escalation_required: bool = False
    needs_pre_approval: bool = False


@dataclass
class StrategyPolicyResult:
    status: Literal["passed", "failed"]
    reason: str
    decision: str
    resolved_strategy_id: str | None = None
    resolved_category_id: str | None = None
    requires_registry_update: bool = False
    candidates: list[dict] | None = None


def validate_task(repo_cfg: dict, task: dict) -> PolicyResult:
    profiles = repo_cfg.get("profiles", [])
    if not profiles:
        return PolicyResult(
            status="passed",
            risk_level="MEDIUM",
            reason="no profiles configured",
        )

    goal = (task.get("goal") or "").lower()
    constraints = [str(c).lower() for c in task.get("constraints", [])]
    task_type = (task.get("task_type") or "").lower()
    checks = task.get("checks", [])

    # safe_docs: only docs, README, templates
    if "safe_docs" in profiles:
        if _has_forbidden_goal(goal):
            return PolicyResult(
                status="failed",
                risk_level="HIGH",
                reason=f"safe_docs: goal mentions forbidden scope ({goal[:80]}...)",
            )
        if not _has_safe_scope(goal, constraints):
            return PolicyResult(
                status="failed",
                risk_level="HIGH",
                reason="safe_docs: task must be limited to docs/README/templates",
            )
        return PolicyResult(status="passed", risk_level="LOW", reason="safe_docs compatible")

    # safe_readonly: no strategy/code changes, read-only tooling only
    if "safe_readonly" in profiles:
        if _has_forbidden_goal(goal):
            return PolicyResult(
                status="failed",
                risk_level="HIGH",
                reason=f"safe_readonly: goal mentions forbidden scope ({goal[:80]}...)",
            )
        if not _checks_are_readonly_safe(checks):
            return PolicyResult(
                status="failed",
                risk_level="HIGH",
                reason="safe_readonly: checks must be read-only (py_compile, status scripts)",
            )
        return PolicyResult(status="passed", risk_level="LOW", reason="safe_readonly compatible")

    # needs_approval_for_code: non-docs tasks require pre-approval
    if "needs_approval_for_code" in profiles:
        if _is_docs_only(goal, constraints):
            return PolicyResult(status="passed", risk_level="LOW", reason="docs-only, no pre-approval")
        return PolicyResult(
            status="needs_approval",
            risk_level="MEDIUM",
            reason="needs_approval_for_code: task may modify code, requires pre-execution approval",
            needs_pre_approval=True,
        )

    return PolicyResult(status="passed", risk_level="MEDIUM", reason="no blocking profile")


def _has_forbidden_goal(goal: str) -> bool:
    return any(f in goal for f in FORBIDDEN_GOAL_FRAGMENTS)


def _has_safe_scope(goal: str, constraints: list[str]) -> bool:
    if any(f in goal for f in SAFE_GOAL_FRAGMENTS):
        return True
    if any(any(s in c for s in SAFE_CONSTRAINT_FRAGMENTS) for c in constraints):
        return True
    return False


def _is_docs_only(goal: str, constraints: list[str]) -> bool:
    docs_keywords = ("docs", "readme", "read-me", "operational note", "documentation")
    if any(k in goal for k in docs_keywords):
        return True
    for c in constraints:
        if "touch only docs" in c or "touch only readme" in c or "docs only" in c:
            return True
    return False


def _checks_are_readonly_safe(checks: list[str]) -> bool:
    """Allow py_compile, status/preflight scripts, git status/diff."""
    for cmd in (checks or []):
        cmd_lower = cmd.strip().lower()
        if cmd_lower.startswith("python -m py_compile"):
            continue
        if cmd_lower.startswith("python ") and (
            "status" in cmd_lower or "preflight" in cmd_lower or "-c " in cmd_lower
        ):
            continue
        if "git status" in cmd_lower or "git diff" in cmd_lower:
            continue
        # Unknown command for safe_readonly
        return False
    return True


def should_auto_escalate_to_premium(repo_cfg: dict) -> bool:
    return "premium_on_repeat_failures" in repo_cfg.get("profiles", [])


def validate_strategy_reference(repo_cfg: dict, task: dict) -> StrategyPolicyResult:
    task_type = (task.get("task_type") or "").lower()
    if task_type in {"registry_audit", "strategy_review", "daily_strategy_review"}:
        return StrategyPolicyResult(
            status="passed",
            reason="internal governance task bypasses strategy_id requirement",
            decision="ALLOW",
            resolved_strategy_id=task.get("strategy_id"),
            resolved_category_id=task.get("category_id"),
        )
    result: CrossRefResult = preflight_cross_reference(task, repo_cfg)
    if result.decision in {"BLOCK_DUPLICATE", "BLOCK_UNSCOPED_CHANGE", "REQUIRES_NEW_STRATEGY_ENTRY"}:
        return StrategyPolicyResult(
            status="failed",
            reason=result.reason,
            decision=result.decision,
            resolved_strategy_id=result.strategy_id,
            resolved_category_id=result.category_id,
            requires_registry_update=result.requires_registry_update,
            candidates=result.candidates,
        )
    return StrategyPolicyResult(
        status="passed",
        reason=result.reason,
        decision=result.decision,
        resolved_strategy_id=result.strategy_id,
        resolved_category_id=result.category_id,
        requires_registry_update=result.requires_registry_update,
        candidates=result.candidates,
    )
