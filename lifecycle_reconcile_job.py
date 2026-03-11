#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
from pathlib import Path

from db import init_db, record_maintenance_job_run
from lifecycle_reconcile import build_reconciliation, render_markdown, _load_rows


def run_reconcile_job(*, repo: str | None = None, strategy_id: str | None = None, run_id: str | None = None) -> dict:
    init_db()
    rows = _load_rows(repo=repo, strategy_id=strategy_id, run_id=run_id)
    payload = build_reconciliation(rows)
    status = "ok" if not payload.get("issue_counts") else "warn"
    record_maintenance_job_run("lifecycle_reconcile", status, payload)
    return payload


def _main() -> int:
    ap = argparse.ArgumentParser(description="Persist lifecycle reconciliation artifacts")
    ap.add_argument("--repo", default=None)
    ap.add_argument("--strategy-id", default=None)
    ap.add_argument("--run-id", default=None)
    ap.add_argument("--output-md", default="data/reports/lifecycle_reconcile_latest.md")
    ap.add_argument("--output-json", default="data/reports/lifecycle_reconcile_latest.json")
    args = ap.parse_args()

    payload = run_reconcile_job(repo=args.repo, strategy_id=args.strategy_id, run_id=args.run_id)
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
