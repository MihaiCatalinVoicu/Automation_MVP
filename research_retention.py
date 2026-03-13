#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import shutil
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

from db import get_conn, init_db, record_maintenance_job_run

AUTOMATION_ROOT = Path(__file__).resolve().parent


def _load_json(text: str | None) -> dict[str, Any]:
    if not text:
        return {}
    try:
        return json.loads(text)
    except json.JSONDecodeError:
        return {}


def _safe_ts(value: str | None) -> datetime | None:
    if not value:
        return None
    try:
        return datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    except ValueError:
        return None


def _is_within_automation_root(path: Path) -> bool:
    try:
        path.resolve().relative_to(AUTOMATION_ROOT.resolve())
        return True
    except ValueError:
        return False


def _candidate_artifact_path(row: dict[str, Any]) -> Path | None:
    verdict_path = str(row.get("artifacts_root") or "").strip()
    if verdict_path:
        return Path(verdict_path).expanduser()
    artifacts = _load_json(row.get("artifacts_json"))
    output_root = str(artifacts.get("output_root") or "").strip()
    if output_root:
        return Path(output_root).expanduser()
    return None


def run_retention(*, cheap_days: int, medium_days: int, dry_run: bool = False) -> dict[str, Any]:
    init_db()
    now = datetime.now(timezone.utc)
    cheap_cutoff = now - timedelta(days=max(1, cheap_days))
    medium_cutoff = now - timedelta(days=max(1, medium_days))
    with get_conn() as conn:
        rows = conn.execute(
            """
            SELECT
                em.manifest_id,
                em.created_at,
                em.execution_status,
                em.execution_spec_json,
                em.artifacts_json,
                (
                    SELECT ev.artifacts_root
                    FROM edge_verdicts ev
                    WHERE ev.manifest_id = em.manifest_id
                    ORDER BY ev.created_at DESC
                    LIMIT 1
                ) AS artifacts_root
            FROM experiment_manifests em
            WHERE em.execution_status IN ('completed', 'dead', 'cancelled')
            ORDER BY em.created_at ASC
            """
        ).fetchall()

    summary = {
        "generated_at": now.isoformat(),
        "dry_run": dry_run,
        "cheap_days": cheap_days,
        "medium_days": medium_days,
        "deleted_count": 0,
        "skipped_count": 0,
        "deleted": [],
        "skipped": [],
    }
    for raw in rows:
        row = dict(raw)
        execution_spec = _load_json(row.get("execution_spec_json"))
        validation_level = str(execution_spec.get("validation_level") or "cheap").lower()
        created_at = _safe_ts(row.get("created_at"))
        if created_at is None:
            summary["skipped_count"] += 1
            summary["skipped"].append({"manifest_id": row["manifest_id"], "reason": "invalid_created_at"})
            continue
        if validation_level == "expensive":
            summary["skipped_count"] += 1
            continue
        cutoff = cheap_cutoff if validation_level == "cheap" else medium_cutoff
        if created_at > cutoff:
            summary["skipped_count"] += 1
            continue
        artifact_path = _candidate_artifact_path(row)
        if artifact_path is None:
            summary["skipped_count"] += 1
            summary["skipped"].append({"manifest_id": row["manifest_id"], "reason": "artifact_path_missing"})
            continue
        if not artifact_path.exists():
            summary["skipped_count"] += 1
            continue
        if not _is_within_automation_root(artifact_path):
            summary["skipped_count"] += 1
            summary["skipped"].append({"manifest_id": row["manifest_id"], "reason": "artifact_outside_root"})
            continue
        if dry_run:
            summary["deleted"].append({"manifest_id": row["manifest_id"], "artifact_path": str(artifact_path.resolve())})
            summary["deleted_count"] += 1
            continue
        shutil.rmtree(artifact_path, ignore_errors=True)
        summary["deleted"].append({"manifest_id": row["manifest_id"], "artifact_path": str(artifact_path.resolve())})
        summary["deleted_count"] += 1

    record_maintenance_job_run("research_retention", "ok", summary)
    return summary


def _main() -> int:
    ap = argparse.ArgumentParser(description="Cleanup old edge-search artifacts")
    ap.add_argument("--cheap-days", type=int, default=3)
    ap.add_argument("--medium-days", type=int, default=14)
    ap.add_argument("--dry-run", action="store_true")
    ap.add_argument("--output-json", default="")
    args = ap.parse_args()

    summary = run_retention(
        cheap_days=max(1, args.cheap_days),
        medium_days=max(1, args.medium_days),
        dry_run=bool(args.dry_run),
    )
    output = str(args.output_json or "").strip()
    if output:
        path = Path(output).expanduser().resolve()
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(json.dumps(summary, indent=2), encoding="utf-8")
        print(f"Wrote: {path}")
    print(json.dumps(summary, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(_main())
