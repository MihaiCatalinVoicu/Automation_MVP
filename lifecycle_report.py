#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import sqlite3
from collections import Counter, defaultdict
from pathlib import Path
from typing import Any

from db import DB_PATH


def _connect() -> sqlite3.Connection:
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn


def _load_rows(repo: str | None = None, strategy_id: str | None = None) -> list[dict[str, Any]]:
    where = []
    params: list[Any] = []
    if repo:
        where.append("repo=?")
        params.append(repo)
    if strategy_id:
        where.append("strategy_id=?")
        params.append(strategy_id)
    sql = "SELECT * FROM raw_lifecycle_events"
    if where:
        sql += " WHERE " + " AND ".join(where)
    sql += " ORDER BY id ASC"
    with _connect() as conn:
        rows = conn.execute(sql, params).fetchall()
    return [dict(r) for r in rows]


def build_report(rows: list[dict[str, Any]]) -> str:
    total = len(rows)
    by_repo = Counter()
    by_strategy = Counter()
    by_event = Counter()
    by_run: dict[str, Counter] = defaultdict(Counter)
    scan_summaries: list[dict[str, Any]] = []

    for row in rows:
        by_repo[str(row.get("repo") or "")] += 1
        by_strategy[str(row.get("strategy_id") or "")] += 1
        by_event[str(row.get("event_type") or "")] += 1
        run_id = str(row.get("run_id") or "")
        by_run[run_id][str(row.get("event_type") or "")] += 1
        if row.get("event_type") == "scan_summary":
            scan_summaries.append(row)

    lines: list[str] = []
    lines.append("# Lifecycle Report")
    lines.append("")
    lines.append(f"- Total events: `{total}`")
    lines.append("")

    lines.append("## By Repo")
    for name, count in sorted(by_repo.items()):
        lines.append(f"- `{name}`: {count}")
    lines.append("")

    lines.append("## By Strategy")
    for name, count in sorted(by_strategy.items()):
        lines.append(f"- `{name}`: {count}")
    lines.append("")

    lines.append("## By Event Type")
    for name, count in sorted(by_event.items()):
        lines.append(f"- `{name}`: {count}")
    lines.append("")

    lines.append("## Runs")
    for run_id, counts in sorted(by_run.items()):
        mix = ", ".join(f"{k}:{v}" for k, v in sorted(counts.items()))
        lines.append(f"- `{run_id}`: {mix}")
    lines.append("")

    lines.append("## Scan Summaries")
    if not scan_summaries:
        lines.append("- none")
    else:
        for row in scan_summaries[-10:]:
            payload = json.loads(row.get("payload_json") or "{}")
            lines.append(
                f"- `{row.get('run_id')}` | repo=`{row.get('repo')}` | strategy=`{row.get('strategy_id')}` | "
                f"scan_status=`{payload.get('scan_status')}` | reason=`{payload.get('reason')}`"
            )
    lines.append("")
    return "\n".join(lines)


def _main() -> int:
    ap = argparse.ArgumentParser(description="Summarize ingested lifecycle events from automation-mvp DB")
    ap.add_argument("--repo", default=None, help="Optional repo filter")
    ap.add_argument("--strategy-id", default=None, help="Optional strategy filter")
    ap.add_argument("--output", default=None, help="Optional markdown output path")
    args = ap.parse_args()

    rows = _load_rows(repo=args.repo, strategy_id=args.strategy_id)
    report = build_report(rows)
    if args.output:
        output_path = Path(args.output).expanduser().resolve()
        output_path.parent.mkdir(parents=True, exist_ok=True)
        output_path.write_text(report, encoding="utf-8")
        print(f"Wrote: {output_path}")
    else:
        print(report)
    return 0


if __name__ == "__main__":
    raise SystemExit(_main())
