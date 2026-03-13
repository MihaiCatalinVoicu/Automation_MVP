from __future__ import annotations

import json
import os
import subprocess
import time
from pathlib import Path
from typing import Tuple

from approval_service import create_approval
from artifact_store import register_validation_artifacts
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
    update_run_routing,
    update_run_status,
)
from policies import classify_command
from research_pipeline import research_metadata
from experiment_ingest import collect_research_artifact_summary
from policy_engine import should_auto_escalate_to_premium, validate_strategy_reference
from registry_audit import run_registry_audit
from recipe_runner import run_validation_battery
from repo_registry import RepoRegistry, RepoRegistryError
from strategy_lifecycle import run_due_reviews
from strategy_registry import add_experiment_result, create_experiment, update_change_log

AUTOMATION_ROOT = Path(__file__).resolve().parent

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
        update_change_log(run_id=run_id, status="FAILED", actual_impact={"exception": str(exc)})
    finally:
        clear_execution_owner(run_id)


def _run_validation_battery(run_id: str, task: dict, repo_path: str) -> None:
    recipe = task.get("recipe") or (task.get("metadata") or {}).get("recipe")
    run_context = task.get("run_context") or {}
    run_dir = run_context.get("run_dir") or (task.get("metadata") or {}).get("run_dir")
    metadata = task.get("metadata") or {}
    if not recipe or not run_dir:
        update_run_status(run_id, "FAILED")
        insert_event(
            run_id,
            "validation_battery_config_error",
            {"error": "recipe and run_dir (in run_context) required"},
        )
        return

    context = {"run_dir": run_dir, "cwd": repo_path, **run_context}
    output_dir = AUTOMATION_ROOT / "data" / "validation_artifacts" / run_id
    output_dir.mkdir(parents=True, exist_ok=True)

    try:
        summary = run_validation_battery(
            recipe,
            context,
            output_dir,
            base_path=AUTOMATION_ROOT,
        )
    except Exception as exc:
        update_run_status(run_id, "FAILED")
        insert_event(
            run_id,
            "validation_battery_failed",
            {"error": str(exc)},
        )
        update_change_log(run_id=run_id, status="FAILED", actual_impact={"validation_battery_error": str(exc)})
        return

    experiment_id = create_experiment(
        strategy_id=task.get("strategy_id"),
        repo=task.get("repo", "unknown"),
        name=Path(recipe).stem,
        hypothesis=task.get("goal", "validation_battery"),
        run_dir=run_dir,
        search_space={"recipe": recipe},
        status=summary["verdict"],
    )
    add_experiment_result(
        experiment_id=experiment_id,
        strategy_id=task.get("strategy_id"),
        run_dir=run_dir,
        source_file=str(output_dir / "summary.json"),
        result=summary,
        verdict=summary["verdict"],
    )
    research_meta = research_metadata(task)
    research_summary = collect_research_artifact_summary(output_dir)
    family_summary = research_summary.get("family_summary", {}) if isinstance(research_summary, dict) else {}
    robustness_summary = research_summary.get("robustness", {}) if isinstance(research_summary, dict) else {}
    derived_research_metrics = {
        "candidate_count": family_summary.get("candidate_count"),
        "validation_ready": 1.0 if family_summary.get("research_verdict") == "SHADOW_CANDIDATE" else 0.0,
        "window_passes": robustness_summary.get("window_passes"),
        "cost_passes": robustness_summary.get("cost_passes"),
    }
    register_validation_artifacts(
        run_id=run_id,
        repo=task.get("repo", "unknown"),
        output_dir=output_dir,
        summary={**summary, **research_summary},
        schedule_id=research_meta.get("schedule_id"),
        strategy_id=task.get("strategy_id"),
        family_name=research_meta.get("family_name"),
    )
    insert_event(
        run_id,
        "validation_battery_completed",
        {
            "verdict": summary["verdict"],
            "metrics": summary.get("metrics", {}),
            "research_summary": research_summary,
        },
    )
    reject_is_completed = str(metadata.get("treat_reject_as_completed", "")).lower() == "true"
    status = "COMPLETED" if (summary["verdict"] != "REJECT" or reject_is_completed) else "FAILED"
    update_run_status(run_id, status)
    update_change_log(
        run_id=run_id,
        status="COMPLETED" if status == "COMPLETED" else "FAILED",
        actual_impact={
            "validation_battery_verdict": summary["verdict"],
            "metrics": summary.get("metrics", {}),
            "research_summary": research_summary,
            **{k: v for k, v in derived_research_metrics.items() if v is not None},
        },
        strategy_id=task.get("strategy_id"),
    )
    insert_event(run_id, "run_completed", {"message": f"Validation battery: {summary['verdict']}"})


def _run_strategy_review(run_id: str, task: dict) -> None:
    output_dir = AUTOMATION_ROOT / "data" / "strategy_reviews" / run_id
    output_dir.mkdir(parents=True, exist_ok=True)
    try:
        summary = run_due_reviews(
            output_dir=output_dir,
            repo=task.get("repo"),
            strategy_id=task.get("strategy_id"),
            review_kind=task.get("task_type") or "daily_strategy_review",
        )
    except Exception as exc:
        update_run_status(run_id, "FAILED")
        insert_event(run_id, "strategy_review_failed", {"error": str(exc)})
        update_change_log(run_id=run_id, status="FAILED", actual_impact={"strategy_review_error": str(exc)})
        return

    insert_event(
        run_id,
        "strategy_review_completed",
        {
            "review_count": summary["review_count"],
            "output_dir": str(output_dir),
        },
    )
    update_run_status(run_id, "COMPLETED")
    update_change_log(
        run_id=run_id,
        status="COMPLETED",
        actual_impact={"strategy_reviews": summary["review_count"], "artifact_dir": str(output_dir)},
        strategy_id=task.get("strategy_id"),
    )
    insert_event(run_id, "run_completed", {"message": "Strategy lifecycle review completed"})


def _run_registry_audit(run_id: str, task: dict) -> None:
    output_dir = AUTOMATION_ROOT / "data" / "registry_audits" / run_id
    output_dir.mkdir(parents=True, exist_ok=True)
    repo_names = [task.get("repo")] if task.get("repo") else []
    try:
        audits, exit_code = run_registry_audit(repo_names or ["crypto-bot", "stocks-bot"], output_dir)
    except Exception as exc:
        update_run_status(run_id, "FAILED")
        insert_event(run_id, "registry_audit_failed", {"error": str(exc)})
        update_change_log(run_id=run_id, status="FAILED", actual_impact={"registry_audit_error": str(exc)})
        return

    insert_event(
        run_id,
        "registry_audit_completed",
        {
            "repo_count": len(audits),
            "output_dir": str(output_dir),
            "summaries": [item["summary"] for item in audits],
        },
    )
    status = "COMPLETED" if exit_code == 0 else "FAILED"
    update_run_status(run_id, status)
    update_change_log(
        run_id=run_id,
        status=status,
        actual_impact={"registry_audit_exit_code": exit_code, "artifact_dir": str(output_dir)},
    )
    insert_event(run_id, "run_completed", {"message": "Registry audit completed", "exit_code": exit_code})


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
    strategy_result = validate_strategy_reference(repo_cfg, task)
    if strategy_result.status == "failed":
        update_run_status(run_id, "FAILED")
        insert_event(
            run_id,
            "strategy_crossref_failed",
            {
                "reason": strategy_result.reason,
                "decision": strategy_result.decision,
                "candidates": strategy_result.candidates or [],
            },
        )
        update_change_log(run_id=run_id, status="FAILED", actual_impact={"strategy_crossref_error": strategy_result.reason})
        return
    insert_event(
        run_id,
        "strategy_crossref_runner_passed",
        {
            "decision": strategy_result.decision,
            "reason": strategy_result.reason,
            "strategy_id": task.get("strategy_id") or strategy_result.resolved_strategy_id,
            "category_id": task.get("category_id") or strategy_result.resolved_category_id,
        },
    )
    update_change_log(run_id=run_id, status="RUNNING", strategy_id=task.get("strategy_id") or strategy_result.resolved_strategy_id)

    task_type = (task.get("task_type") or "").lower()
    if task_type == "validation_battery":
        _run_validation_battery(run_id, task, repo_path)
        return
    if task_type == "registry_audit":
        _run_registry_audit(run_id, task)
        return
    if task_type in {"strategy_review", "daily_strategy_review"}:
        _run_strategy_review(run_id, task)
        return

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
            if should_auto_escalate_to_premium(repo_cfg):
                routing["planner_agent"] = "premium"
                routing["reviewer_agent"] = "premium"
                update_run_routing(run_id, routing)
                insert_event(
                    run_id,
                    "policy_auto_escalation",
                    {
                        "reason": "premium_on_repeat_failures",
                        "failed_command": command,
                        "repeat_count": failure_count,
                    },
                )
                update_run_status(run_id, "RETRY_PENDING")
                return
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
        update_change_log(run_id=run_id, status="FAILED", actual_impact={"failed_check": command, "stderr": stderr[:500]})
        return

    update_run_status(run_id, "COMPLETED")
    update_change_log(run_id=run_id, status="COMPLETED", actual_impact={"result": "checks_passed"})
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
