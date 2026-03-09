"""Policy engine: enforce repo profiles at submit and during execution."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Literal

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
