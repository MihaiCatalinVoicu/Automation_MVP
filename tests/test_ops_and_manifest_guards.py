#!/usr/bin/env python3
from __future__ import annotations

import importlib
import json
import os
import sys
import tempfile
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))


def _write_json(path: Path, payload: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload), encoding="utf-8")


def test_daily_ops_scan_recency_warn_between_5h_and_6h() -> None:
    import daily_ops_review

    with tempfile.TemporaryDirectory() as tmp_dir:
        tmp = Path(tmp_dir)
        automation_path = tmp / "automation.json"
        runtime_path = tmp / "runtime.json"

        _write_json(
            automation_path,
            {"generated_at": "2026-03-14T00:00:00+00:00", "live_edge_search": {"status": "ok"}},
        )
        # 5h30m old -> warn
        from datetime import datetime, timedelta, timezone

        scan_ts = (datetime.now(timezone.utc) - timedelta(hours=5, minutes=30)).isoformat()
        _write_json(
            runtime_path,
            {
                "checks": {"canonical_runtime": True, "regime_gate_before_ideas": True, "heartbeat_recent": True},
                "latest_scan_summary": {"ts": scan_ts},
            },
        )

        payload = daily_ops_review.build_daily_ops_review(
            automation_report_path=str(automation_path),
            crypto_runtime_truth_path=str(runtime_path),
        )
        by_item = {row["item"]: row for row in payload["checklist"]}
        assert by_item["crypto_scan_recent"]["status"] == "warn"


def test_manifest_execution_spec_guard_fails_fast() -> None:
    tmp = tempfile.NamedTemporaryFile(suffix=".db", delete=False)
    tmp.close()
    os.environ["DB_PATH"] = tmp.name

    import db

    db = importlib.reload(db)
    db.init_db()

    try:
        db.ensure_required_execution_spec(
            {"family": "x", "config_path": "/tmp/x.json"},
            context="unit_test",
        )
        raise AssertionError("expected ValueError")
    except ValueError as exc:
        message = str(exc)
        assert "unit_test" in message
        assert "recipe_path" in message
        assert "repo_root" in message
