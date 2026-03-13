from __future__ import annotations

import json
import os
import shutil
import subprocess
import tempfile
from dataclasses import dataclass
from pathlib import Path
from typing import Any


CURSOR_AGENT_BIN = os.getenv("CURSOR_AGENT_BIN", "agent")
CURSOR_TIMEOUT_SECONDS = int(os.getenv("CURSOR_TIMEOUT_SECONDS", "1800"))
CURSOR_FORCE_EXECUTOR = os.getenv("CURSOR_FORCE_EXECUTOR", "true").lower() == "true"
CURSOR_TRUST_WORKSPACE = os.getenv("CURSOR_TRUST_WORKSPACE", "true").lower() == "true"
CURSOR_APPROVE_MCPS = os.getenv("CURSOR_APPROVE_MCPS", "false").lower() == "true"
CURSOR_CLOUD_MODE = os.getenv("CURSOR_CLOUD_MODE", "false").lower() == "true"
CURSOR_SANDBOX = os.getenv("CURSOR_SANDBOX", "")
CURSOR_COMPOSER_MODEL = os.getenv("CURSOR_COMPOSER_MODEL", "").strip()
CURSOR_PREMIUM_MODEL = os.getenv("CURSOR_PREMIUM_MODEL", "").strip()


class CursorExecutionError(RuntimeError):
    pass


@dataclass
class CursorStepResult:
    step_name: str
    command: list[str]
    prompt_file: str
    returncode: int
    stdout: str
    stderr: str
    model: str | None
    mode: str | None
    changed_files: bool


def build_task_packet(task: dict, routing: dict, repo_cfg: dict | None = None) -> dict[str, Any]:
    metadata = dict(task.get("metadata", {}))
    if repo_cfg:
        profiles = repo_cfg.get("profiles", [])
        if profiles:
            metadata["repo_profiles"] = profiles
    if task.get("strategy_id"):
        metadata["strategy_id"] = task.get("strategy_id")
    if task.get("category_id"):
        metadata["category_id"] = task.get("category_id")
    if task.get("change_kind"):
        metadata["change_kind"] = task.get("change_kind")
    if task.get("new_strategy_proposal"):
        metadata["new_strategy_proposal"] = task.get("new_strategy_proposal")
    return {
        "repo": task.get("repo"),
        "goal": task.get("goal"),
        "constraints": task.get("constraints", []),
        "checks": task.get("checks", []),
        "metadata": metadata,
        "routing": routing,
    }


def write_json_packet(packet: dict[str, Any]) -> str:
    fd, path = tempfile.mkstemp(prefix="automation_task_", suffix=".json")
    with os.fdopen(fd, "w", encoding="utf-8") as fh:
        json.dump(packet, fh, indent=2)
    return path


def invoke_cursor_workflow(repo_path: str, packet: dict[str, Any], routing: dict[str, str]) -> dict[str, Any]:
    task_file = write_json_packet(packet)
    plan_summary = ""
    step_results: list[CursorStepResult] = []

    planner_agent = routing.get("planner_agent", "none")
    executor_agent = routing.get("executor_agent", "composer")
    reviewer_agent = routing.get("reviewer_agent", "none")

    if planner_agent != "none":
        planner_result = _run_step(
            step_name="planner",
            repo_path=repo_path,
            prompt=_build_planner_prompt(task_file, packet),
            mode="plan",
            model=_model_for_agent(planner_agent),
            force=False,
        )
        step_results.append(planner_result)
        plan_summary = planner_result.stdout.strip()

    executor_result = _run_step(
        step_name="executor",
        repo_path=repo_path,
        prompt=_build_executor_prompt(task_file, packet, plan_summary),
        mode=None,
        model=_model_for_agent(executor_agent),
        force=CURSOR_FORCE_EXECUTOR,
    )
    step_results.append(executor_result)

    if reviewer_agent != "none":
        reviewer_result = _run_step(
            step_name="reviewer",
            repo_path=repo_path,
            prompt=_build_reviewer_prompt(task_file, packet, plan_summary),
            mode="ask",
            model=_model_for_agent(reviewer_agent),
            force=False,
        )
        step_results.append(reviewer_result)

    return {
        "task_file": task_file,
        "plan_summary": plan_summary,
        "step_results": [step_result_to_dict(r) for r in step_results],
    }


def step_result_to_dict(result: CursorStepResult) -> dict[str, Any]:
    return {
        "step_name": result.step_name,
        "command": result.command,
        "prompt_file": result.prompt_file,
        "returncode": result.returncode,
        "stdout": result.stdout,
        "stderr": result.stderr,
        "model": result.model,
        "mode": result.mode,
        "changed_files": result.changed_files,
    }


def _run_step(
    *,
    step_name: str,
    repo_path: str,
    prompt: str,
    mode: str | None,
    model: str | None,
    force: bool,
) -> CursorStepResult:
    agent_bin = _resolve_agent_bin()
    prompt_file = _write_prompt_file(step_name, prompt)
    command = _build_command(
        agent_bin=agent_bin,
        repo_path=repo_path,
        prompt=prompt,
        mode=mode,
        model=model,
        force=force,
    )

    before_snapshot = _snapshot_repo(repo_path)
    proc = subprocess.run(
        command,
        cwd=repo_path,
        capture_output=True,
        text=True,
        timeout=CURSOR_TIMEOUT_SECONDS,
    )
    after_snapshot = _snapshot_repo(repo_path)

    if proc.returncode != 0:
        raise CursorExecutionError(
            f"{step_name} step failed with exit code {proc.returncode}: {(proc.stderr or proc.stdout)[:400]}"
        )

    return CursorStepResult(
        step_name=step_name,
        command=command,
        prompt_file=prompt_file,
        returncode=proc.returncode,
        stdout=proc.stdout[:20000],
        stderr=proc.stderr[:4000],
        model=model,
        mode=mode,
        changed_files=before_snapshot != after_snapshot,
    )


def _build_command(
    *,
    agent_bin: str,
    repo_path: str,
    prompt: str,
    mode: str | None,
    model: str | None,
    force: bool,
) -> list[str]:
    command = [agent_bin, "-p", "--output-format", "text", "--workspace", repo_path]
    if CURSOR_TRUST_WORKSPACE:
        command.append("--trust")
    if CURSOR_APPROVE_MCPS:
        command.append("--approve-mcps")
    if CURSOR_CLOUD_MODE:
        command.append("--cloud")
    if CURSOR_SANDBOX:
        command.extend(["--sandbox", CURSOR_SANDBOX])
    if mode:
        command.extend(["--mode", mode])
    if model:
        command.extend(["--model", model])
    if force:
        command.append("--force")
    command.append(prompt)
    return command


def _resolve_agent_bin() -> str:
    if os.path.isabs(CURSOR_AGENT_BIN) and Path(CURSOR_AGENT_BIN).exists():
        return CURSOR_AGENT_BIN
    resolved = shutil.which(CURSOR_AGENT_BIN)
    if resolved:
        return resolved
    raise CursorExecutionError(
        "Cursor Agent CLI was not found. Install it from https://cursor.com/docs/cli/headless "
        "or set CURSOR_AGENT_BIN to the executable path."
    )


def _model_for_agent(agent_name: str) -> str | None:
    normalized = (agent_name or "").strip().lower()
    if normalized == "premium":
        return CURSOR_PREMIUM_MODEL or None
    if normalized == "composer":
        return CURSOR_COMPOSER_MODEL or None
    return None


def _snapshot_repo(repo_path: str) -> list[str]:
    root = Path(repo_path)
    entries: list[str] = []
    for path in root.rglob("*"):
        if ".git" in path.parts:
            continue
        if path.is_file():
            try:
                stat = path.stat()
            except OSError:
                continue
            rel = path.relative_to(root).as_posix()
            entries.append(f"{rel}:{stat.st_size}:{int(stat.st_mtime_ns)}")
    entries.sort()
    return entries


def _write_prompt_file(step_name: str, prompt: str) -> str:
    fd, path = tempfile.mkstemp(prefix=f"automation_{step_name}_", suffix=".txt")
    with os.fdopen(fd, "w", encoding="utf-8") as fh:
        fh.write(prompt)
    return path


def _build_planner_prompt(task_file: str, packet: dict[str, Any]) -> str:
    metadata = packet.get("metadata", {})
    strategy_hint = ""
    if metadata.get("strategy_id") or metadata.get("category_id"):
        strategy_hint = (
            f"\nStrategy context: strategy_id={metadata.get('strategy_id', 'n/a')} "
            f"category_id={metadata.get('category_id', 'n/a')} change_kind={metadata.get('change_kind', 'n/a')}\n"
        )
    return f"""You are the premium planning step for an automation orchestrator.

Task packet: {task_file}
Goal: {packet.get("goal", "")}
{strategy_hint}

Produce a short execution plan for the coding executor.

Rules:
- Do not modify files.
- Focus on scope, likely files, risks, and validation steps.
- Keep the plan concise and practical.
- If the task is already straightforward, say so briefly.
"""


def _build_executor_prompt(task_file: str, packet: dict[str, Any], plan_summary: str) -> str:
    constraints = packet.get("constraints", [])
    checks = packet.get("checks", [])
    metadata = packet.get("metadata", {})
    profiles = metadata.get("repo_profiles", [])
    strategy_id = metadata.get("strategy_id")
    category_id = metadata.get("category_id")
    change_kind = metadata.get("change_kind")
    constraint_lines = "\n".join(f"- {item}" for item in constraints) or "- none"
    check_lines = "\n".join(f"- {item}" for item in checks) or "- none"
    plan_block = plan_summary.strip() or "No separate planner output."
    profile_line = ""
    if profiles:
        profile_line = f"\nRepo profiles: {', '.join(profiles)} – respect safe task classes (docs/read-only only when applicable).\n"
    strategy_line = ""
    if strategy_id or category_id:
        strategy_line = (
            f"Strategy registry context: strategy_id={strategy_id or 'n/a'}, "
            f"category_id={category_id or 'n/a'}, change_kind={change_kind or 'n/a'}.\n"
            "Treat this task as part of that registered strategy. Avoid creating duplicate parallel logic.\n"
        )

    return f"""You are the coding executor for an automation orchestrator.

Task packet: {task_file}
Goal: {packet.get("goal", "")}
{profile_line}
{strategy_line}
Constraints:
{constraint_lines}

Checks owned by orchestrator:
{check_lines}

Planner summary:
{plan_block}

Execution rules:
- Make the minimum code changes needed to satisfy the task.
- Stay within this workspace only.
- Do not push, open PRs, or perform destructive git actions.
- Prefer safe edits and concise changes.
- You may run repo-local checks if useful, but the orchestrator will run the declared checks afterward.
- End with a concise summary of what changed.
"""


def _build_reviewer_prompt(task_file: str, packet: dict[str, Any], plan_summary: str) -> str:
    metadata = packet.get("metadata", {})
    strategy_hint = ""
    if metadata.get("strategy_id") or metadata.get("category_id"):
        strategy_hint = (
            f"\nStrategy context: strategy_id={metadata.get('strategy_id', 'n/a')} "
            f"category_id={metadata.get('category_id', 'n/a')} change_kind={metadata.get('change_kind', 'n/a')}\n"
        )
    return f"""You are the premium review step for an automation orchestrator.

Task packet: {task_file}
Goal: {packet.get("goal", "")}
{strategy_hint}

Planner summary:
{plan_summary.strip() or "No planner summary."}

Review the current workspace changes for bugs, regressions, risky assumptions, and missing validation.
Do not modify files.
If there are no material findings, say that explicitly.
"""
