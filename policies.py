from __future__ import annotations

import json
import os
from typing import Dict, Literal

DEFAULT_EXECUTOR_AGENT = os.getenv("DEFAULT_EXECUTOR_AGENT", "composer")
PREMIUM_PLANNER_AGENT = os.getenv("PREMIUM_PLANNER_AGENT", "premium")
PREMIUM_REVIEWER_AGENT = os.getenv("PREMIUM_REVIEWER_AGENT", "premium")

PREMIUM_TASK_TYPES = {"greenfield", "architecture", "security", "migration"}
PREMIUM_KEYWORDS = {
    "new project",
    "from scratch",
    "architecture",
    "redesign",
    "security",
    "auth",
    "oauth",
    "jwt",
    "migration",
    "infra",
    "deployment",
    "kubernetes",
    "cross-repo",
}

DecisionAction = Literal["rerun_same_path", "reroute_plan_b", "reroute_premium", "abort", "allow_execution"]

SIDE_EFFECT_FRAGMENTS = [
    "git push",
    "gh pr create",
    "terraform apply",
    "kubectl apply",
    "docker compose up -d",
    "docker-compose up -d",
]

FORBIDDEN_FRAGMENTS = [
    "rm -rf",
    "mkfs",
    "shutdown ",
    "reboot ",
]


def choose_routing(task: dict) -> Dict[str, str]:
    goal = (task.get("goal") or "").lower()
    task_type = (task.get("task_type") or "").lower()

    planner = "none"
    reviewer = "none"
    executor = task.get("preferred_executor") or DEFAULT_EXECUTOR_AGENT

    if task_type in PREMIUM_TASK_TYPES or any(k in goal for k in PREMIUM_KEYWORDS):
        planner = PREMIUM_PLANNER_AGENT

    return {
        "planner_agent": planner,
        "executor_agent": executor,
        "reviewer_agent": reviewer,
    }


def classify_command(command: str, allowed_prefixes: list[str]) -> str:
    normalized = command.strip().lower()

    if any(fragment in normalized for fragment in FORBIDDEN_FRAGMENTS):
        return "forbidden"

    if any(fragment in normalized for fragment in SIDE_EFFECT_FRAGMENTS):
        return "side_effect"

    if normalized.endswith(".env") or " .env" in normalized:
        return "side_effect"

    if any(normalized.startswith(prefix.lower()) for prefix in allowed_prefixes):
        return "allowed_check"

    return "unknown"


def decision_to_action(decision: str) -> DecisionAction:
    mapping: dict[str, DecisionAction] = {
        "RETRY_SAFE": "rerun_same_path",
        "PLAN_B": "reroute_plan_b",
        "ASK_PREMIUM": "reroute_premium",
        "ABORT": "abort",
        "APPROVE_PUSH": "rerun_same_path",
        "ALLOW_EXECUTION": "allow_execution",
    }
    if decision not in mapping:
        raise ValueError(f"Unsupported decision: {decision}")
    return mapping[decision]
