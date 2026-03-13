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


def _load_rows(
    *,
    repo: str | None = None,
    strategy_id: str | None = None,
    run_id: str | None = None,
) -> list[dict[str, Any]]:
    where = []
    params: list[Any] = []
    if repo:
        where.append("repo=?")
        params.append(repo)
    if strategy_id:
        where.append("strategy_id=?")
        params.append(strategy_id)
    if run_id:
        where.append("run_id=?")
        params.append(run_id)
    sql = "SELECT * FROM raw_lifecycle_events"
    if where:
        sql += " WHERE " + " AND ".join(where)
    sql += " ORDER BY run_id ASC, id ASC"
    with _connect() as conn:
        rows = conn.execute(sql, params).fetchall()
    return [dict(r) for r in rows]


def _parse_payload(row: dict[str, Any]) -> dict[str, Any]:
    try:
        return json.loads(row.get("payload_json") or "{}")
    except json.JSONDecodeError:
        return {}


def build_reconciliation(rows: list[dict[str, Any]]) -> dict[str, Any]:
    by_run: dict[str, dict[str, Any]] = {}
    total_event_counts = Counter()
    total_issue_counts = Counter()

    for row in rows:
        run_id = str(row.get("run_id") or "")
        run_bucket = by_run.setdefault(
            run_id,
            {
                "run_id": run_id,
                "repo": row.get("repo"),
                "strategy_id": row.get("strategy_id"),
                "family": row.get("family"),
                "scan_summaries": [],
                "signals": {},
                "event_counts": Counter(),
                "issue_counts": Counter(),
            },
        )
        event_type = str(row.get("event_type") or "")
        payload = _parse_payload(row)
        run_bucket["event_counts"][event_type] += 1
        total_event_counts[event_type] += 1

        if event_type == "scan_summary":
            run_bucket["scan_summaries"].append(
                {
                    "ts": row.get("ts"),
                    "scan_status": payload.get("scan_status"),
                    "reason": payload.get("reason"),
                    "candidates_found": payload.get("candidates_found"),
                    "signals_emitted": payload.get("signals_emitted"),
                    "decisions_emitted": payload.get("decisions_emitted"),
                }
            )
            continue

        signal_id = str(row.get("signal_id") or "")
        sig = run_bucket["signals"].setdefault(
            signal_id,
            {
                "signal_id": signal_id,
                "symbol": row.get("symbol"),
                "side": row.get("side"),
                "event_sequence": [],
                "decision_ids": set(),
                "position_ids": set(),
                "issues": [],
            },
        )
        sig["event_sequence"].append(event_type)
        if row.get("decision_id"):
            sig["decision_ids"].add(str(row.get("decision_id")))
        if row.get("position_id"):
            sig["position_ids"].add(str(row.get("position_id")))

    # derive issues
    for run_bucket in by_run.values():
        for sig in run_bucket["signals"].values():
            seq = sig["event_sequence"]
            seq_set = set(seq)
            if "signal" not in seq_set:
                sig["issues"].append("missing_signal")
            if "decision" in seq_set and "signal" not in seq_set:
                sig["issues"].append("decision_without_signal")
            if "fill" in seq_set and "decision" not in seq_set:
                sig["issues"].append("fill_without_decision")
            if "exit" in seq_set and "fill" not in seq_set:
                sig["issues"].append("exit_without_fill")
            if "outcome" in seq_set and "signal" not in seq_set:
                sig["issues"].append("outcome_without_signal")
            if "outcome" in seq_set and "exit" not in seq_set and "fill" in seq_set:
                sig["issues"].append("outcome_without_exit")
            for issue in sig["issues"]:
                run_bucket["issue_counts"][issue] += 1
                total_issue_counts[issue] += 1
            sig["decision_ids"] = sorted(sig["decision_ids"])
            sig["position_ids"] = sorted(sig["position_ids"])

        run_bucket["event_counts"] = dict(run_bucket["event_counts"])
        run_bucket["issue_counts"] = dict(run_bucket["issue_counts"])
        run_bucket["signals"] = sorted(run_bucket["signals"].values(), key=lambda x: x["signal_id"])

    return {
        "db_path": str(Path(DB_PATH).resolve()),
        "run_count": len(by_run),
        "event_counts": dict(total_event_counts),
        "issue_counts": dict(total_issue_counts),
        "runs": list(by_run.values()),
    }


def render_markdown(payload: dict[str, Any]) -> str:
    lines: list[str] = []
    lines.append("# Lifecycle Reconciliation")
    lines.append("")
    lines.append(f"- Runs: `{payload.get('run_count', 0)}`")
    lines.append(f"- Events: `{payload.get('event_counts', {})}`")
    lines.append(f"- Issues: `{payload.get('issue_counts', {})}`")
    lines.append("")
    for run in payload.get("runs", []):
        lines.append(f"## Run `{run['run_id']}`")
        lines.append(f"- Repo: `{run.get('repo')}`")
        lines.append(f"- Strategy: `{run.get('strategy_id')}`")
        lines.append(f"- Family: `{run.get('family')}`")
        lines.append(f"- Event counts: `{run.get('event_counts')}`")
        if run.get("scan_summaries"):
            for summary in run["scan_summaries"]:
                lines.append(
                    f"- Scan summary: status=`{summary.get('scan_status')}` reason=`{summary.get('reason')}` "
                    f"candidates=`{summary.get('candidates_found')}`"
                )
        if not run.get("signals"):
            lines.append("- Signals: none")
        else:
            lines.append("| Signal ID | Symbol | Sequence | Issues |")
            lines.append("|---|---|---|---|")
            for sig in run["signals"]:
                lines.append(
                    f"| {sig['signal_id']} | {sig.get('symbol') or ''} | "
                    f"{' -> '.join(sig.get('event_sequence') or [])} | "
                    f"{', '.join(sig.get('issues') or []) or 'none'} |"
                )
        lines.append("")
    return "\n".join(lines)


def _main() -> int:
    ap = argparse.ArgumentParser(description="Reconcile lifecycle events by run and signal")
    ap.add_argument("--repo", default=None)
    ap.add_argument("--strategy-id", default=None)
    ap.add_argument("--run-id", default=None)
    ap.add_argument("--format", default="markdown", choices=["markdown", "json"])
    ap.add_argument("--output", default=None, help="Optional output path")
    args = ap.parse_args()

    rows = _load_rows(repo=args.repo, strategy_id=args.strategy_id, run_id=args.run_id)
    payload = build_reconciliation(rows)
    text = render_markdown(payload) if args.format == "markdown" else json.dumps(payload, indent=2)
    if args.output:
        output_path = Path(args.output).expanduser().resolve()
        output_path.parent.mkdir(parents=True, exist_ok=True)
        output_path.write_text(text, encoding="utf-8")
        print(f"Wrote: {output_path}")
    else:
        print(text)
    return 0


if __name__ == "__main__":
    raise SystemExit(_main())
