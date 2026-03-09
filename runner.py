from __future__ import annotations

import json
import os
import subprocess
import time
from typing import Tuple

from approval_service import create_approval
from cursor_executor import (
    CursorExecutionError,
    build_task_packet,
    invoke_cursor_workflow,
)
from db import (
    clear_execution_owner,
    get_run,
    insert_event,
    list_events,
    update_run_status,
)
from policies import classify_command
from repo_registry import RepoRegistry, RepoRegistryError

REPEAT_FAILURE_THRESHOLD = int(os.getenv("REPEAT_FAILURE_THRESHOLD", "3"))
EXECUTION_MODE = os.getenv("EXECUTION_MODE", "simulate")


class RunError(RuntimeError):
    pass


def run_pipeline(run_id: str, worker_id: str) -> None:
    try:
        _run_pipeline(run_id)
    except Exception as exc:
        update_run_status(run_id, "FAILED")
        insert_event(run_id, "run_failed_exception", {"error": str(exc)})
    finally:
        clear_execution_owner(run_id)


def _run_pipeline(run_id: str) -> None:
    run = get_run(run_id)
    if not run:
        raise RunError(f"Run {run_id} not found")

    task = json.loads(run["task_json"])
    routing = json.loads(run["routing_json"])

    registry = RepoRegistry()
    repo_cfg = registry.get(run["repo"])
    repo_path = repo_cfg["path"]
    allowed_prefixes = repo_cfg.get("allowed_check_prefixes", [])

    insert_event(run_id, "run_started", {"routing": routing, "repo_path": repo_path})

    planner = routing.get("planner_agent", "none")
    executor = routing.get("executor_agent", "composer")

    if planner != "none":
        insert_event(run_id, "planner_requested", {"planner_agent": planner})

    branch_name = run["branch"]
    prepare_workspace(run_id, repo_path, branch_name, repo_cfg)
    invoke_executor(run_id, repo_path, task, routing, repo_cfg)

    checks = task.get("checks", [])
    if not checks:
        update_run_status(run_id, "COMPLETED")
        insert_event(run_id, "run_completed", {"message": "No checks configured"})
        return

    for command in checks:
        classification = classify_command(command, allowed_prefixes)
        if classification in {"side_effect", "unknown", "forbidden"}:
            approval_summary = {
                "reason": f"command_{classification}",
                "failed_command": command,
                "repeat_count": 1,
                "last_error": f"Command classification: {classification}",
                "executor_agent": executor,
                "plan_b_hint": "Replace with approved check command or review policy",
            }
            create_approval(run_id, approval_summary)
            return

        resolved_cmd = command
        python_bin = repo_cfg.get("python_bin")
        if python_bin and command.strip().lower().startswith("python "):
            resolved_cmd = python_bin + " " + command.strip()[7:]
        ok, stdout, stderr = execute_check(run_id, resolved_cmd, repo_path)
        if ok:
            continue

        failure_count = count_failures_for_command(run_id, resolved_cmd)
        if failure_count >= REPEAT_FAILURE_THRESHOLD:
            approval_summary = {
                "reason": "repeat_failure",
                "failed_command": command,
                "repeat_count": failure_count,
                "last_error": stderr[:1000],
                "executor_agent": executor,
                "plan_b_hint": "Ask premium planner or alter check path",
            }
            create_approval(run_id, approval_summary)
            return

        update_run_status(run_id, "FAILED")
        insert_event(
            run_id,
            "run_failed",
            {"command": command, "stderr": stderr[:1000], "stdout": stdout[:1000]},
        )
        return

    update_run_status(run_id, "COMPLETED")
    insert_event(run_id, "run_completed", {"message": "All checks passed"})


def prepare_workspace(run_id: str, repo_path: str, branch_name: str, repo_cfg: dict) -> None:
    if EXECUTION_MODE == "simulate":
        insert_event(run_id, "workspace_prepared", {"mode": "simulate", "branch": branch_name})
        return

    if repo_cfg.get("skip_git_prep"):
        insert_event(run_id, "workspace_prepared", {"mode": "skip_git", "branch": branch_name})
        return

    allow_create_branch = bool(repo_cfg.get("allow_create_branch", True))
    commands = [
        ["git", "status", "--short"],
    ]
    if allow_create_branch:
        commands.append(["git", "checkout", "-B", branch_name])

    for cmd in commands:
        proc = subprocess.run(cmd, cwd=repo_path, capture_output=True, text=True, timeout=60)
        insert_event(
            run_id,
            "workspace_command",
            {
                "command": " ".join(cmd),
                "returncode": proc.returncode,
                "stdout": proc.stdout[:1000],
                "stderr": proc.stderr[:1000],
            },
        )
        if proc.returncode != 0:
            raise RunError(f"Workspace preparation failed for {' '.join(cmd)}")


def invoke_executor(run_id: str, repo_path: str, task: dict, routing: dict, repo_cfg: dict | None = None) -> None:
    executor = routing.get("executor_agent", "composer")
    planner = routing.get("planner_agent", "none")
    reviewer = routing.get("reviewer_agent", "none")

    packet = build_task_packet(task, routing, repo_cfg=repo_cfg)
    insert_event(
        run_id,
        "executor_invoked",
        {
            "executor_agent": executor,
            "planner_agent": planner,
            "reviewer_agent": reviewer,
            "mode": EXECUTION_MODE,
        },
    )

    if EXECUTION_MODE == "simulate":
        time.sleep(0.2)
        return

    try:
        result = invoke_cursor_workflow(repo_path=repo_path, packet=packet, routing=routing)
    except CursorExecutionError as exc:
        insert_event(run_id, "executor_result", {"error": str(exc)})
        raise RunError(str(exc)) from exc

    insert_event(
        run_id,
        "executor_result",
        {
            "task_file": result["task_file"],
            "plan_summary": result["plan_summary"][:4000],
            "steps": result["step_results"],
        },
    )


def execute_check(run_id: str, command: str, repo_path: str) -> Tuple[bool, str, str]:
    if EXECUTION_MODE == "simulate":
        return simulate_check(run_id, command)

    proc = subprocess.run(
        command,
        shell=True,
        cwd=repo_path,
        capture_output=True,
        text=True,
        timeout=300,
    )

    ok = proc.returncode == 0
    event_type = "check_passed" if ok else "check_failed"
    insert_event(
        run_id,
        event_type,
        {
            "command": command,
            "returncode": proc.returncode,
            "stdout": proc.stdout[:2000],
            "stderr": proc.stderr[:2000],
        },
    )
    return ok, proc.stdout, proc.stderr


def simulate_check(run_id: str, command: str):
    events = list_events(run_id)
    decision_events = [e for e in events if e["event_type"] == "approval_decision"]
    last_decision_payload = None
    if decision_events:
        last_decision_payload = json.loads(decision_events[-1]["payload_json"])

    if not decision_events:
        prior_failures = count_failures_for_command(run_id, command)
        if prior_failures < REPEAT_FAILURE_THRESHOLD:
            stderr = f"Simulated failure #{prior_failures + 1} for: {command}"
            insert_event(
                run_id,
                "check_failed",
                {"command": command, "returncode": 1, "stdout": "", "stderr": stderr},
            )
            return False, "", stderr

    if last_decision_payload and last_decision_payload.get("action") == "reroute_plan_b":
        stdout = f"Simulated PLAN_B success for: {command}"
    elif last_decision_payload and last_decision_payload.get("action") == "reroute_premium":
        stdout = f"Simulated PREMIUM reroute success for: {command}"
    else:
        stdout = f"Simulated RETRY_SAFE success for: {command}"

    insert_event(
        run_id,
        "check_passed",
        {"command": command, "returncode": 0, "stdout": stdout, "stderr": ""},
    )
    return True, stdout, ""


def count_failures_for_command(run_id: str, command: str) -> int:
    events = list_events(run_id)
    failures = 0
    for e in events:
        if e["event_type"] != "check_failed":
            continue
        payload = json.loads(e["payload_json"])
        if payload.get("command") == command:
            failures += 1
    return failures
