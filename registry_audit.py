from __future__ import annotations

import argparse
import json
import os
from collections import defaultdict
from pathlib import Path
from typing import Any

from db import init_db
from repo_registry import RepoRegistry
from strategy_registry import get_strategy, get_strategy_children, list_strategies

ROOT = Path(__file__).resolve().parent

DEFAULT_CANONICAL_SOURCES = {
    "crypto-bot": {
        "ideas": "core/ideas.py",
        "regime": "core/regime_gate.py",
        "ml_risk": "core/ml_risk.py",
        "decision_logging": "core/decision_log.py",
        "exits": "core/exit_engine.py",
        "paper_runtime": "main.py",
    },
    "stocks-bot": {
        "ingest": "src/ingest/build_events_job.py",
        "scoring": "src/scoring/risk_gate.py",
        "ranking": "src/scoring/ranker.py",
        "strategy_runtime": "src/strategy/strategy_v1.py",
        "paper_runtime": "src/strategy/paper_job.py",
    },
}

LEGACY_PATHS = {
    "crypto-bot": {
        "ml_pipeline.py",
        "ml_gate.py",
        "ml_risk_gate.py",
        "trade_logger.py",
    },
    "stocks-bot": set(),
}


def _norm(path: Path | str) -> str:
    return os.path.normcase(str(Path(path).resolve()))


def _rel(path: Path, root: Path) -> str:
    try:
        return path.resolve().relative_to(root.resolve()).as_posix()
    except ValueError:
        return path.resolve().as_posix()


def _should_skip(path: Path) -> bool:
    skip_parts = {
        ".git",
        ".venv",
        "__pycache__",
        "data",
        ".cursor",
        "node_modules",
        ".pytest_cache",
    }
    return any(part in skip_parts for part in path.parts)


def _severity_for(repo_name: str, relative_path: str) -> str:
    if repo_name == "crypto-bot":
        if relative_path == "main.py":
            return "high"
        if relative_path.startswith("core/") or relative_path.startswith("paper_engine/"):
            return "high"
        if relative_path in {"risk_manager.py", "trade_logger.py", "ml_pipeline.py", "ml_gate.py", "ml_risk_gate.py"}:
            return "high"
        if relative_path.startswith("scripts/"):
            return "medium"
        if relative_path.startswith("docs/") or relative_path.endswith(".md"):
            return "low"
    if repo_name == "stocks-bot":
        if relative_path.startswith("src/strategy/") or relative_path.startswith("src/scoring/") or relative_path.startswith("src/ingest/"):
            return "high"
        if relative_path.startswith("src/backtest/") or relative_path.startswith("src/features/"):
            return "medium"
        if relative_path.startswith("configs/"):
            return "medium"
        if relative_path.endswith(".md"):
            return "low"
    if relative_path.endswith(".py"):
        return "medium"
    return "low"


def _concern_for(repo_name: str, relative_path: str) -> str | None:
    path = relative_path.replace("\\", "/")
    if repo_name == "crypto-bot":
        if path == "core/ideas.py":
            return "ideas"
        if path == "core/regime_gate.py":
            return "regime"
        if path in {"core/ml_risk.py", "ml_gate.py", "ml_risk_gate.py"}:
            return "ml_risk"
        if path in {"core/decision_log.py", "trade_logger.py", "ml_pipeline.py"}:
            return "decision_logging"
        if path == "core/exit_engine.py":
            return "exits"
        if path in {"main.py", "paper_engine/runner.py"}:
            return "paper_runtime"
        if path == "risk_manager.py":
            return "risk_sizing"
    if repo_name == "stocks-bot":
        if path == "src/ingest/build_events_job.py":
            return "ingest"
        if path == "src/scoring/risk_gate.py":
            return "scoring"
        if path == "src/scoring/ranker.py":
            return "ranking"
        if path == "src/strategy/strategy_v1.py":
            return "strategy_runtime"
        if path == "src/strategy/paper_job.py":
            return "paper_runtime"
    return None


def _collect_candidate_files(repo_name: str, repo_root: Path) -> list[Path]:
    files: list[Path] = []
    if repo_name == "crypto-bot":
        direct_files = [
            "main.py",
            "risk_manager.py",
            "trade_logger.py",
            "ml_pipeline.py",
            "ml_gate.py",
            "ml_risk_gate.py",
        ]
        for item in direct_files:
            path = repo_root / item
            if path.exists():
                files.append(path)
        for folder, pattern in [("core", "*.py"), ("paper_engine", "*.py"), ("scripts", "*.py"), ("scripts", "*.sh")]:
            base = repo_root / folder
            if not base.exists():
                continue
            files.extend(p for p in base.rglob(pattern) if not _should_skip(p))
    elif repo_name == "stocks-bot":
        for folder, pattern in [("src", "*.py"), ("scripts", "*.py"), ("configs", "*.yaml"), ("configs", "*.yml")]:
            base = repo_root / folder
            if not base.exists():
                continue
            files.extend(p for p in base.rglob(pattern) if not _should_skip(p))
    else:
        files.extend(p for p in repo_root.rglob("*.py") if not _should_skip(p))

    deduped: dict[str, Path] = {}
    for path in files:
        deduped[_norm(path)] = path
    return sorted(deduped.values(), key=lambda p: _rel(p, repo_root))


def _registry_file_rows(repo_name: str) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for strategy in list_strategies(repo=repo_name, include_shared=True):
        children = get_strategy_children(strategy["id"])
        for item in children["files"]:
            if item["repo"] != repo_name:
                continue
            rows.append(
                {
                    "strategy_id": strategy["id"],
                    "strategy_name": strategy["name"],
                    "strategy_status_state": strategy["status_state"],
                    "strategy_operational_status": strategy["operational_status"],
                    "strategy_verdict": strategy["current_verdict"],
                    "file_path": item["file_path"],
                    "role": item["role"],
                    "is_shadow": bool(item["is_shadow"]),
                    "notes": item.get("notes", ""),
                }
            )
    return rows


def build_repo_audit(
    repo_name: str,
    repo_root: Path,
    registry_rows: list[dict[str, Any]],
) -> dict[str, Any]:
    repo_root = repo_root.resolve()
    canonical_sources = DEFAULT_CANONICAL_SOURCES.get(repo_name, {})
    linked_by_path: dict[str, list[dict[str, Any]]] = defaultdict(list)
    dead_registry_links: list[dict[str, Any]] = []

    for row in registry_rows:
        raw_path = Path(row["file_path"])
        linked_path = raw_path if raw_path.is_absolute() else (repo_root / raw_path)
        rel_path = _rel(linked_path, repo_root)
        entry = dict(row)
        entry["relative_path"] = rel_path
        entry["severity"] = _severity_for(repo_name, rel_path)
        if linked_path.exists():
            entry["absolute_path"] = str(linked_path.resolve())
            linked_by_path[_norm(linked_path)].append(entry)
        else:
            dead_registry_links.append(
                {
                    **entry,
                    "absolute_path": str(linked_path),
                    "failure": "linked file missing",
                }
            )

    inventory: list[dict[str, Any]] = []
    unmapped_live_logic: list[dict[str, Any]] = []
    shadow_or_duplicate_logic: list[dict[str, Any]] = []
    concern_map: dict[str, list[dict[str, Any]]] = defaultdict(list)

    for path in _collect_candidate_files(repo_name, repo_root):
        relative_path = _rel(path, repo_root)
        linked = linked_by_path.get(_norm(path), [])
        concern = _concern_for(repo_name, relative_path)
        severity = _severity_for(repo_name, relative_path)
        linked_strategy_ids = [item["strategy_id"] for item in linked]

        status = "unknown"
        if relative_path in LEGACY_PATHS.get(repo_name, set()):
            status = "legacy"
        elif any(item["is_shadow"] or item["strategy_status_state"] == "shadow" or item["strategy_operational_status"] == "shadow" for item in linked):
            status = "shadow"
        elif linked:
            status = "canonical"

        item = {
            "relative_path": relative_path,
            "absolute_path": str(path.resolve()),
            "severity": severity,
            "concern": concern,
            "status": status,
            "linked_strategy_ids": linked_strategy_ids,
            "roles": sorted({entry["role"] for entry in linked}),
        }
        inventory.append(item)

        if concern:
            concern_map[concern].append(item)

        if not linked and severity in {"high", "medium"}:
            unmapped_live_logic.append(
                {
                    **item,
                    "failure": "runtime/discovery/validation file is not linked to any strategy",
                }
            )

        if status in {"shadow", "legacy"}:
            shadow_or_duplicate_logic.append(
                {
                    **item,
                    "failure": f"{status} path present in audit inventory",
                }
            )

    for concern, selected_relative in canonical_sources.items():
        candidates = concern_map.get(concern, [])
        selected = next((item for item in candidates if item["relative_path"] == selected_relative), None)
        alternate_paths = [item["relative_path"] for item in candidates if item["relative_path"] != selected_relative]
        if selected is None:
            shadow_or_duplicate_logic.append(
                {
                    "relative_path": selected_relative,
                    "severity": "high",
                    "concern": concern,
                    "status": "unknown",
                    "failure": "declared canonical source missing from audit inventory",
                }
            )
        elif alternate_paths:
            shadow_or_duplicate_logic.append(
                {
                    "relative_path": selected_relative,
                    "severity": selected["severity"],
                    "concern": concern,
                    "status": selected["status"],
                    "failure": "multiple files map to the same runtime concern",
                    "alternates": alternate_paths,
                }
            )

    high_severity_failures = [
        item for item in (unmapped_live_logic + dead_registry_links + shadow_or_duplicate_logic) if item.get("severity") == "high"
    ]
    canonical_runtime_declaration = {
        concern: {
            "selected_file": relative_path,
            "exists": (repo_root / relative_path).exists(),
            "alternates": [
                item["relative_path"]
                for item in concern_map.get(concern, [])
                if item["relative_path"] != relative_path
            ],
        }
        for concern, relative_path in canonical_sources.items()
    }

    return {
        "repo": repo_name,
        "repo_root": str(repo_root),
        "inventory": inventory,
        "unmapped_live_logic": sorted(unmapped_live_logic, key=lambda x: (x["severity"], x["relative_path"])),
        "shadow_or_duplicate_logic": sorted(
            shadow_or_duplicate_logic,
            key=lambda x: (x.get("severity", "low"), x.get("relative_path", "")),
        ),
        "dead_registry_links": sorted(dead_registry_links, key=lambda x: (x["severity"], x["relative_path"])),
        "canonical_runtime_declaration": canonical_runtime_declaration,
        "summary": {
            "inventory_count": len(inventory),
            "unmapped_count": len(unmapped_live_logic),
            "shadow_or_duplicate_count": len(shadow_or_duplicate_logic),
            "dead_link_count": len(dead_registry_links),
            "high_severity_failure_count": len(high_severity_failures),
        },
    }


def run_registry_audit(repo_names: list[str], output_dir: Path) -> tuple[list[dict[str, Any]], int]:
    init_db()
    registry = RepoRegistry()
    output_dir.mkdir(parents=True, exist_ok=True)

    audits: list[dict[str, Any]] = []
    exit_code = 0
    for repo_name in repo_names:
        repo_cfg = registry.get(repo_name)
        audit = build_repo_audit(
            repo_name,
            Path(repo_cfg["path"]),
            _registry_file_rows(repo_name),
        )
        audits.append(audit)
        output_path = output_dir / f"{repo_name}_registry_audit.json"
        with open(output_path, "w", encoding="utf-8") as fh:
            json.dump(audit, fh, indent=2, ensure_ascii=True)
        if audit["summary"]["high_severity_failure_count"] > 0:
            exit_code = 1
    return audits, exit_code


def _default_repo_names() -> list[str]:
    names = []
    for name in RepoRegistry()._load().keys():
        if name in {"crypto-bot", "stocks-bot"}:
            names.append(name)
    return names


def main() -> int:
    ap = argparse.ArgumentParser(description="Audit runtime truth against strategy registry file links")
    ap.add_argument("--repo", action="append", default=[], help="Repo name to audit (repeatable). Default: crypto-bot and stocks-bot")
    ap.add_argument(
        "--output-dir",
        default=str(ROOT / "data" / "registry_audits"),
        help="Directory for machine-readable audit outputs",
    )
    args = ap.parse_args()

    repo_names = args.repo or _default_repo_names()
    audits, exit_code = run_registry_audit(repo_names, Path(args.output_dir))

    for audit in audits:
        summary = audit["summary"]
        print(
            f"[registry_audit] repo={audit['repo']} inventory={summary['inventory_count']} "
            f"unmapped={summary['unmapped_count']} shadow_or_duplicate={summary['shadow_or_duplicate_count']} "
            f"dead_links={summary['dead_link_count']} high_severity_failures={summary['high_severity_failure_count']}"
        )
    return exit_code


if __name__ == "__main__":
    raise SystemExit(main())
