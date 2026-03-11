#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any

from db import init_db, insert_raw_lifecycle_event, upsert_runtime_run

ROOT = Path(__file__).resolve().parent
SCHEMA_PATH = ROOT / "contracts" / "lifecycle_v1.json"
REQUIRED_FIELDS = {
    "schema_version",
    "event_id",
    "idempotency_key",
    "event_type",
    "repo",
    "environment",
    "strategy_id",
    "family",
    "run_id",
    "ts",
    "metadata",
}


def _validate_record(record: dict[str, Any]) -> list[str]:
    errors: list[str] = []
    missing = sorted(field for field in REQUIRED_FIELDS if field not in record)
    if missing:
        errors.append(f"missing required fields: {', '.join(missing)}")
    if record.get("schema_version") != "lifecycle_v1":
        errors.append("unsupported schema_version")
    if not record.get("strategy_id"):
        errors.append("strategy_id is empty")
    if not record.get("family"):
        errors.append("family is empty")
    if not record.get("run_id"):
        errors.append("run_id is empty")
    variant_id = record.get("variant_id")
    profile_id = record.get("profile_id")
    if not variant_id and not profile_id:
        errors.append("one of variant_id/profile_id must be present")
    metadata = record.get("metadata")
    if not isinstance(metadata, dict):
        errors.append("metadata must be an object")
    event_type = str(record.get("event_type") or "")
    if event_type == "scan_summary":
        if not record.get("scan_status"):
            errors.append("scan_status is required for scan_summary")
    else:
        if not record.get("signal_id"):
            errors.append("signal_id is empty")
        if not record.get("symbol"):
            errors.append("symbol is empty")
        if record.get("side") not in {"long", "short"}:
            errors.append("side must be long or short")
    return errors


def ingest_file(path: Path, *, start_line: int = 0) -> dict[str, int]:
    stats = {"read": 0, "inserted": 0, "duplicates": 0, "invalid": 0, "last_line_processed": start_line}
    if not path.exists():
        raise FileNotFoundError(path)
    with path.open("r", encoding="utf-8") as fh:
        for line_no, line in enumerate(fh, start=1):
            if line_no <= start_line:
                continue
            line = line.strip()
            if not line:
                stats["last_line_processed"] = line_no
                continue
            stats["read"] += 1
            stats["last_line_processed"] = line_no
            try:
                record = json.loads(line)
            except json.JSONDecodeError:
                stats["invalid"] += 1
                continue
            record.setdefault("source_file", str(path))
            record.setdefault("source_line", line_no)
            errors = _validate_record(record)
            if errors:
                stats["invalid"] += 1
                continue
            upsert_runtime_run(
                run_id=record["run_id"],
                repo=record["repo"],
                environment=record["environment"],
                strategy_id=record["strategy_id"],
                family=record["family"],
                variant_id=record.get("variant_id"),
                profile_id=record.get("profile_id"),
                status="ACTIVE",
                first_event_ts=record["ts"],
                last_event_ts=record["ts"],
            )
            inserted = insert_raw_lifecycle_event(record)
            if inserted:
                stats["inserted"] += 1
            else:
                stats["duplicates"] += 1
    return stats


def _main() -> int:
    ap = argparse.ArgumentParser(description="Ingest runtime lifecycle event streams into automation-mvp")
    ap.add_argument("inputs", nargs="+", help="One or more runtime_events.jsonl paths")
    ap.add_argument("--start-line", type=int, default=0, help="Skip lines up to this 1-based watermark")
    args = ap.parse_args()

    init_db()
    print(f"Schema reference: {SCHEMA_PATH}")
    totals = {"read": 0, "inserted": 0, "duplicates": 0, "invalid": 0}
    for raw_path in args.inputs:
        stats = ingest_file(Path(raw_path).expanduser().resolve(), start_line=max(0, args.start_line))
        for key, value in stats.items():
            if key in totals:
                totals[key] += value
        print(f"{raw_path}: read={stats['read']} inserted={stats['inserted']} duplicates={stats['duplicates']} invalid={stats['invalid']}")
    print(
        f"Totals: read={totals['read']} inserted={totals['inserted']} duplicates={totals['duplicates']} invalid={totals['invalid']}"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(_main())
