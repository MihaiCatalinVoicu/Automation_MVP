from __future__ import annotations

from pathlib import Path
from typing import Any


def is_scheduled_research_task(task: dict[str, Any]) -> bool:
    metadata = task.get("metadata") or {}
    return bool(metadata.get("schedule_id"))


def research_metadata(task: dict[str, Any]) -> dict[str, Any]:
    metadata = dict(task.get("metadata") or {})
    run_context = dict(task.get("run_context") or {})
    return {
        "schedule_id": metadata.get("schedule_id"),
        "family_name": metadata.get("family_name") or run_context.get("family_name"),
        "artifact_root": metadata.get("artifact_root") or run_context.get("research_output_dir"),
        "run_date": run_context.get("run_date"),
        "cohort_config": run_context.get("cohort_config"),
    }


def output_paths(output_dir: Path) -> dict[str, Path]:
    return {
        "summary": output_dir / "summary.json",
        "verdict": output_dir / "verdict.txt",
        "family_summary": output_dir / "family_summary.json",
        "profile_discovery_summary": output_dir / "profile_discovery_summary.json",
        "robustness_summary": output_dir / "robustness_summary.json",
    }
