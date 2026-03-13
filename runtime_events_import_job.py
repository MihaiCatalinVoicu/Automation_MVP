#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any

from db import (
    get_runtime_import_state,
    init_db,
    record_maintenance_job_run,
    upsert_runtime_import_state,
)
from ingest_runtime_events import ingest_file
from repo_registry import RepoRegistry

ROOT = Path(__file__).resolve().parent


def _default_sources(repos: list[str] | None = None) -> list[dict[str, str]]:
    registry = RepoRegistry(str(ROOT / "repos.json"))
    out: list[dict[str, str]] = []
    wanted = repos or ["crypto-bot", "stocks-bot"]
    for repo_name in wanted:
        repo = registry.get(repo_name)
        source_path = Path(repo["path"]).resolve() / "data" / "runtime_events.jsonl"
        out.append({"repo": repo_name, "source_path": str(source_path)})
    return out


def run_import_job(*, repos: list[str] | None = None) -> dict[str, Any]:
    init_db()
    sources = _default_sources(repos)
    summary = {"sources": [], "totals": {"read": 0, "inserted": 0, "duplicates": 0, "invalid": 0}}
    job_status = "ok"
    for source in sources:
        source_path = str(Path(source["source_path"]).resolve())
        state = get_runtime_import_state(source_path) or {}
        start_line = int(state.get("last_line_processed") or 0)
        source_summary: dict[str, Any] = {"repo": source["repo"], "source_path": source_path, "start_line": start_line}
        try:
            stats = ingest_file(Path(source_path), start_line=start_line)
            upsert_runtime_import_state(
                source_path=source_path,
                last_line_processed=int(stats.get("last_line_processed") or start_line),
                last_status="ok",
                last_error=None,
            )
            for key in ("read", "inserted", "duplicates", "invalid"):
                summary["totals"][key] += int(stats.get(key, 0))
            source_summary.update(stats)
            source_summary["status"] = "ok"
        except FileNotFoundError as e:
            job_status = "warn"
            upsert_runtime_import_state(
                source_path=source_path,
                last_line_processed=start_line,
                last_status="missing",
                last_error=str(e),
            )
            source_summary["status"] = "missing"
            source_summary["error"] = str(e)
        except Exception as e:
            job_status = "error"
            upsert_runtime_import_state(
                source_path=source_path,
                last_line_processed=start_line,
                last_status="error",
                last_error=str(e),
            )
            source_summary["status"] = "error"
            source_summary["error"] = str(e)
        summary["sources"].append(source_summary)

    record_maintenance_job_run("runtime_events_import", job_status, summary)
    return summary


def _main() -> int:
    ap = argparse.ArgumentParser(description="Incremental runtime events import job with watermarks")
    ap.add_argument("--repos", default="crypto-bot,stocks-bot", help="Comma-separated repo names")
    ap.add_argument("--output", default="data/runtime_events_import_latest.json")
    args = ap.parse_args()

    repos = [item.strip() for item in args.repos.split(",") if item.strip()]
    summary = run_import_job(repos=repos)
    output_path = Path(args.output).expanduser().resolve()
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(json.dumps(summary, indent=2), encoding="utf-8")
    print(f"Wrote: {output_path}")
    print(json.dumps(summary["totals"], ensure_ascii=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(_main())
