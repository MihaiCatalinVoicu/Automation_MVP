#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
from datetime import datetime, timezone
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


def build_weekly_evidence_pack(
    *,
    automation_report_path: str,
    crypto_baseline_path: str = "",
    crypto_cost_gate_path: str = "",
    crypto_runtime_truth_path: str = "",
    stocks_verdict_path: str = "",
) -> dict[str, Any]:
    automation = _load_json(automation_report_path)
    baseline = _load_json(crypto_baseline_path)
    cost_gate = _load_json(crypto_cost_gate_path)
    runtime_truth = _load_json(crypto_runtime_truth_path)
    stocks_verdict = _load_json(stocks_verdict_path)

    live_edge = automation.get("live_edge_search") or {}
    convergence = automation.get("convergence_snapshot") or {}
    trigger_board = automation.get("trigger_board") or {}
    runtime_checks = runtime_truth.get("checks") or {}

    return {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "evidence_pack_version": "v1",
        "automation_mvp": {
            "mode": live_edge.get("mode"),
            "status": live_edge.get("status"),
            "convergence_snapshot": convergence,
            "trigger_board": trigger_board,
        },
        "crypto_bot": {
            "baseline_verdict": baseline.get("verdict"),
            "baseline_summary": baseline.get("summary") or {},
            "cost_gate_verdict": cost_gate.get("verdict"),
            "cost_gate_buffer_bps": (cost_gate.get("summary") or {}).get("buffer_bps"),
            "runtime_truth": {
                "canonical_runtime": runtime_checks.get("canonical_runtime"),
                "regime_gate_before_ideas": runtime_checks.get("regime_gate_before_ideas"),
                "paper_engine_is_sandbox": runtime_checks.get("paper_engine_is_sandbox"),
                "heartbeat_recent": runtime_checks.get("heartbeat_recent"),
            },
        },
        "stocks_bot": {
            "verdict": stocks_verdict.get("verdict"),
            "owner_signoff": stocks_verdict.get("owner_signoff"),
            "reasons": stocks_verdict.get("reasons") or [],
        },
    }


def render_markdown(payload: dict[str, Any]) -> str:
    automation = payload.get("automation_mvp") or {}
    crypto = payload.get("crypto_bot") or {}
    stocks = payload.get("stocks_bot") or {}
    convergence = automation.get("convergence_snapshot") or {}
    runtime_truth = crypto.get("runtime_truth") or {}

    lines = [
        "# Weekly Evidence Pack",
        "",
        f"- Generated at: `{payload.get('generated_at')}`",
        "",
        "## automation-mvp",
        f"- mode=`{automation.get('mode')}` status=`{automation.get('status')}`",
        f"- convergence_trend=`{convergence.get('candidate_quality_trend')}` clean_run_streak=`{convergence.get('clean_run_streak')}` duplicate_ratio=`{convergence.get('duplicate_ratio')}`",
        "",
        "## crypto-bot",
        f"- baseline_verdict=`{crypto.get('baseline_verdict')}` cost_gate_verdict=`{crypto.get('cost_gate_verdict')}` buffer_bps=`{crypto.get('cost_gate_buffer_bps')}`",
        f"- canonical_runtime=`{runtime_truth.get('canonical_runtime')}` regime_pre_entry=`{runtime_truth.get('regime_gate_before_ideas')}` paper_engine_sandbox=`{runtime_truth.get('paper_engine_is_sandbox')}` heartbeat_recent=`{runtime_truth.get('heartbeat_recent')}`",
        "",
        "## stocks-bot",
        f"- verdict=`{stocks.get('verdict')}` owner_signoff=`{stocks.get('owner_signoff')}`",
        f"- reasons=`{stocks.get('reasons')}`",
        "",
        "## Trigger Board",
    ]
    for item in (automation.get("trigger_board") or {}).get("items") or []:
        lines.append(
            f"- `{item.get('trigger')}` status=`{item.get('status')}` "
            f"thresholds=`{item.get('thresholds')}` owner=`{item.get('owner')}`"
        )
    if not ((automation.get("trigger_board") or {}).get("items") or []):
        lines.append("- none")
    lines.append("")
    return "\n".join(lines)


def _main() -> int:
    ap = argparse.ArgumentParser(description="Build the weekly evidence pack from latest repo artifacts.")
    ap.add_argument("--automation-report", default="data/reports/meta_search_report_latest.json")
    ap.add_argument("--crypto-baseline", default="")
    ap.add_argument("--crypto-cost-gate", default="")
    ap.add_argument("--crypto-runtime-truth", default="")
    ap.add_argument("--stocks-verdict", default="")
    ap.add_argument("--output-json", default="data/reports/weekly_evidence_pack_latest.json")
    ap.add_argument("--output-md", default="data/reports/weekly_evidence_pack_latest.md")
    args = ap.parse_args()

    payload = build_weekly_evidence_pack(
        automation_report_path=args.automation_report,
        crypto_baseline_path=args.crypto_baseline,
        crypto_cost_gate_path=args.crypto_cost_gate,
        crypto_runtime_truth_path=args.crypto_runtime_truth,
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
