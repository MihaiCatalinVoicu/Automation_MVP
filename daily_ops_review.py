#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any


def _load_json(path_str: str) -> dict[str, Any]:
    if not path_str:
        return {}
    path = Path(path_str).expanduser().resolve()
    if not path.exists():
        return {}
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except json.JSONDecodeError:
        return {}


def _is_recent(ts_raw: str | None, *, max_age_hours: int) -> bool | None:
    if not ts_raw:
        return None
    try:
        ts = datetime.fromisoformat(ts_raw.replace("Z", "+00:00"))
    except ValueError:
        return None
    if ts.tzinfo is None:
        ts = ts.replace(tzinfo=timezone.utc)
    return ts >= datetime.now(timezone.utc) - timedelta(hours=max_age_hours)


def build_daily_ops_review(
    *,
    automation_report_path: str,
    crypto_runtime_truth_path: str = "",
    crypto_baseline_path: str = "",
    crypto_cost_gate_path: str = "",
    stocks_verdict_path: str = "",
) -> dict[str, Any]:
    automation = _load_json(automation_report_path)
    runtime_truth = _load_json(crypto_runtime_truth_path)
    baseline = _load_json(crypto_baseline_path)
    cost_gate = _load_json(crypto_cost_gate_path)
    stocks = _load_json(stocks_verdict_path)

    checks = runtime_truth.get("checks") or {}
    live_edge = automation.get("live_edge_search") or {}
    trigger_items = (automation.get("trigger_board") or {}).get("items") or []
    generated_recent = _is_recent(automation.get("generated_at"), max_age_hours=24)

    checklist = [
        {
            "item": "automation_report_recent",
            "status": "pass" if generated_recent else "warn",
            "detail": automation.get("generated_at"),
        },
        {
            "item": "edge_search_not_frozen",
            "status": "pass" if live_edge.get("status") != "freeze_required" else "fail",
            "detail": live_edge.get("status"),
        },
        {
            "item": "crypto_main_runtime_canonical",
            "status": "pass" if checks.get("canonical_runtime") else "fail",
            "detail": checks.get("canonical_runtime_detail"),
        },
        {
            "item": "regime_gate_proven_pre_entry",
            "status": "pass" if checks.get("regime_gate_before_ideas") else "fail",
            "detail": checks.get("regime_gate_before_ideas_detail"),
        },
        {
            "item": "crypto_heartbeat_recent",
            "status": "pass" if checks.get("heartbeat_recent") else "warn",
            "detail": checks.get("heartbeat_detail"),
        },
        {
            "item": "btc_structural_baseline",
            "status": "pass" if baseline.get("verdict") == "PASS" else "warn",
            "detail": baseline.get("verdict"),
        },
        {
            "item": "cost_gate",
            "status": "pass" if cost_gate.get("verdict") == "GO" else "warn",
            "detail": cost_gate.get("verdict"),
        },
        {
            "item": "stocks_verdict_signed",
            "status": "pass" if stocks.get("owner_signoff") and stocks.get("verdict") else "warn",
            "detail": f"{stocks.get('verdict')} / {stocks.get('owner_signoff')}",
        },
    ]

    return {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "checklist": checklist,
        "trigger_board": trigger_items,
    }


def render_markdown(payload: dict[str, Any]) -> str:
    lines = [
        "# Daily Ops Review",
        "",
        f"- Generated at: `{payload.get('generated_at')}`",
        "",
        "## Checklist",
    ]
    for item in payload.get("checklist") or []:
        lines.append(
            f"- `{item.get('item')}` status=`{item.get('status')}` detail=`{item.get('detail')}`"
        )
    lines.append("")
    lines.append("## Trigger Board")
    for item in payload.get("trigger_board") or []:
        lines.append(
            f"- `{item.get('trigger')}` status=`{item.get('status')}` "
            f"thresholds=`{item.get('thresholds')}`"
        )
    if not (payload.get("trigger_board") or []):
        lines.append("- none")
    lines.append("")
    return "\n".join(lines)


def _main() -> int:
    ap = argparse.ArgumentParser(description="Generate the 15-minute daily ops review checklist.")
    ap.add_argument("--automation-report", default="data/reports/meta_search_report_latest.json")
    ap.add_argument("--crypto-runtime-truth", default="")
    ap.add_argument("--crypto-baseline", default="")
    ap.add_argument("--crypto-cost-gate", default="")
    ap.add_argument("--stocks-verdict", default="")
    ap.add_argument("--output-json", default="data/reports/daily_ops_review_latest.json")
    ap.add_argument("--output-md", default="data/reports/daily_ops_review_latest.md")
    args = ap.parse_args()

    payload = build_daily_ops_review(
        automation_report_path=args.automation_report,
        crypto_runtime_truth_path=args.crypto_runtime_truth,
        crypto_baseline_path=args.crypto_baseline,
        crypto_cost_gate_path=args.crypto_cost_gate,
        stocks_verdict_path=args.stocks_verdict,
    )
    json_path = Path(args.output_json).expanduser().resolve()
    md_path = Path(args.output_md).expanduser().resolve()
    json_path.parent.mkdir(parents=True, exist_ok=True)
    md_path.parent.mkdir(parents=True, exist_ok=True)
    json_path.write_text(json.dumps(payload, indent=2), encoding="utf-8")
    md_path.write_text(render_markdown(payload), encoding="utf-8")
    print(f"Wrote: {json_path}")
    print(f"Wrote: {md_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(_main())
