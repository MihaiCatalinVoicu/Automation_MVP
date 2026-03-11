#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import sqlite3
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

from db import (
    DB_PATH,
    get_last_maintenance_job_run,
    init_db,
    list_runtime_import_states,
    list_work_items,
    record_maintenance_job_run,
)


def _connect() -> sqlite3.Connection:
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn


def _load_recent_events(since_hours: int) -> list[dict[str, Any]]:
    cutoff = (datetime.now(timezone.utc) - timedelta(hours=since_hours)).isoformat()
    with _connect() as conn:
        rows = conn.execute(
            "SELECT * FROM raw_lifecycle_events WHERE ts >= ? ORDER BY id ASC",
            (cutoff,),
        ).fetchall()
    return [dict(r) for r in rows]


def _parse_payload(row: dict[str, Any]) -> dict[str, Any]:
    try:
        return json.loads(row.get("payload_json") or "{}")
    except json.JSONDecodeError:
        return {}


def build_daily_payload(since_hours: int = 24) -> dict[str, Any]:
    init_db()
    rows = _load_recent_events(since_hours)
    by_repo: dict[str, dict[str, Any]] = {}
    for row in rows:
        repo = str(row.get("repo") or "unknown")
        bucket = by_repo.setdefault(
            repo,
            {
                "events": 0,
                "event_counts": {},
                "last_event_ts": None,
                "scan_status_counts": {},
            },
        )
        bucket["events"] += 1
        event_type = str(row.get("event_type") or "")
        bucket["event_counts"][event_type] = int(bucket["event_counts"].get(event_type, 0)) + 1
        bucket["last_event_ts"] = row.get("ts")
        if event_type == "scan_summary":
            payload = _parse_payload(row)
            status = str(payload.get("scan_status") or "unknown")
            bucket["scan_status_counts"][status] = int(bucket["scan_status_counts"].get(status, 0)) + 1

    imports = list_runtime_import_states()
    import_job = get_last_maintenance_job_run("runtime_events_import")
    reconcile_job = get_last_maintenance_job_run("lifecycle_reconcile")
    open_items = [
        item
        for item in list_work_items()
        if str(item.get("status") or "") in {"proposed", "approved", "in_progress", "blocked", "deferred"}
    ]

    payload = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "since_hours": since_hours,
        "repos": by_repo,
        "runtime_import_states": imports,
        "last_import_job": import_job,
        "last_reconcile_job": reconcile_job,
        "open_work_items": open_items,
    }
    record_maintenance_job_run("daily_lifecycle_report", "ok", payload)
    return payload


def render_markdown(payload: dict[str, Any]) -> str:
    lines: list[str] = []
    lines.append("# Daily Lifecycle Report")
    lines.append("")
    lines.append(f"- Generated at: `{payload.get('generated_at')}`")
    lines.append(f"- Window: last `{payload.get('since_hours')}` hours")
    lines.append("")
    lines.append("## Runtime Summary")
    repos = payload.get("repos") or {}
    if not repos:
        lines.append("- no events in window")
    else:
        for repo, bucket in sorted(repos.items()):
            lines.append(f"- `{repo}` events={bucket.get('events')} last_event_ts=`{bucket.get('last_event_ts')}`")
            lines.append(f"  event_counts=`{bucket.get('event_counts')}`")
            if bucket.get("scan_status_counts"):
                lines.append(f"  scan_status_counts=`{bucket.get('scan_status_counts')}`")
    lines.append("")
    lines.append("## Import State")
    for state in payload.get("runtime_import_states") or []:
        lines.append(
            f"- `{state.get('source_path')}` status=`{state.get('last_status')}` "
            f"last_line=`{state.get('last_line_processed')}` imported_at=`{state.get('last_imported_at')}`"
        )
    lines.append("")
    lines.append("## Last Jobs")
    for label, key in (("import", "last_import_job"), ("reconcile", "last_reconcile_job")):
        item = payload.get(key)
        if item:
            lines.append(f"- `{label}` status=`{item.get('status')}` at `{item.get('created_at')}`")
        else:
            lines.append(f"- `{label}`: none")
    lines.append("")
    lines.append("## Open Work Items")
    open_items = payload.get("open_work_items") or []
    if not open_items:
        lines.append("- none")
    else:
        lines.append("| ID | Repo | Status | Progress | Priority | Title |")
        lines.append("|---|---|---|---:|---|---|")
        for item in open_items:
            lines.append(
                f"| {item.get('id')} | {item.get('repo')} | {item.get('status')} | "
                f"{item.get('progress_pct')} | {item.get('priority')} | {item.get('title')} |"
            )
    lines.append("")
    return "\n".join(lines)


def _main() -> int:
    ap = argparse.ArgumentParser(description="Generate a daily lifecycle report")
    ap.add_argument("--since-hours", type=int, default=24)
    ap.add_argument("--output-md", default="data/reports/daily_lifecycle_report_latest.md")
    ap.add_argument("--output-json", default="data/reports/daily_lifecycle_report_latest.json")
    args = ap.parse_args()

    payload = build_daily_payload(since_hours=max(1, args.since_hours))
    md_path = Path(args.output_md).expanduser().resolve()
    json_path = Path(args.output_json).expanduser().resolve()
    md_path.parent.mkdir(parents=True, exist_ok=True)
    json_path.parent.mkdir(parents=True, exist_ok=True)
    md_path.write_text(render_markdown(payload), encoding="utf-8")
    json_path.write_text(json.dumps(payload, indent=2), encoding="utf-8")
    print(f"Wrote: {md_path}")
    print(f"Wrote: {json_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(_main())
