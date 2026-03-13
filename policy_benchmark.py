#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
from collections import Counter, defaultdict
from pathlib import Path
from typing import Any


def _load_json(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8"))


def _load_jsonl(path: Path) -> list[dict[str, Any]]:
    if not path.exists():
        return []
    rows: list[dict[str, Any]] = []
    for line in path.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if not line:
            continue
        rows.append(json.loads(line))
    return rows


def _as_float(value: Any) -> float:
    try:
        return float(value)
    except (TypeError, ValueError):
        return 0.0


def _as_int(value: Any) -> int:
    try:
        return int(value)
    except (TypeError, ValueError):
        return 0


def _safe_rate(numerator: int, denominator: int) -> float | None:
    if denominator <= 0:
        return None
    value = numerator / denominator
    if value < 0.0:
        value = 0.0
    if value > 1.0:
        value = 1.0
    return round(value, 4)


def _loop_summary(loop_dir: Path) -> dict[str, Any] | None:
    state_path = loop_dir / "loop_state.json"
    if not state_path.exists():
        return None
    state = _load_json(state_path)
    history = list(state.get("history") or [])
    if not history:
        return None

    policy_version = str(state.get("policy_version") or "unknown")
    mutation_events = _load_jsonl(loop_dir / "mutation_log.jsonl")
    policy_counts = Counter()
    skipped_due_to_budget = 0
    for event in mutation_events:
        if event.get("event") == "mutation_skipped_due_to_budget":
            skipped_due_to_budget += 1
            continue
        policy = str(event.get("policy") or "")
        if policy:
            policy_counts[policy] += 1

    fingerprints = [str(item.get("config_fingerprint") or "") for item in history if item.get("config_fingerprint")]
    unique_fingerprints = sorted(set(fingerprints))

    failure_counts = Counter(str(item.get("dominant_failure_mode") or "unknown") for item in history)
    terminal_failure_mode = str((history[-1].get("dominant_failure_mode") or "unknown"))
    policies_exercised = sorted(policy_counts.keys())
    first_battery = history[0].get("battery_metrics") or {}
    max_window_passes = _as_float(first_battery.get("window_passes"))
    max_average_profit_factor = _as_float(first_battery.get("average_profit_factor"))

    robustness_improved_steps = 0
    any_metric_improved_steps = 0
    mutation_transition_count = 0
    mutation_improved_steps = 0
    for prev, cur in zip(history, history[1:]):
        prev_b = prev.get("battery_metrics") or {}
        cur_b = cur.get("battery_metrics") or {}
        prev_w = _as_float(prev_b.get("window_passes"))
        cur_w = _as_float(cur_b.get("window_passes"))
        prev_avg_pf = _as_float(prev_b.get("average_profit_factor"))
        cur_avg_pf = _as_float(cur_b.get("average_profit_factor"))
        max_window_passes = max(max_window_passes, cur_w)
        max_average_profit_factor = max(max_average_profit_factor, cur_avg_pf)
        robust_up = cur_w > prev_w or cur_avg_pf > prev_avg_pf
        if robust_up:
            robustness_improved_steps += 1

        prev_m = prev.get("metrics") or {}
        cur_m = cur.get("metrics") or {}
        pf_up = _as_float(cur_m.get("profit_factor")) > _as_float(prev_m.get("profit_factor"))
        dd_up = _as_float(cur_m.get("max_drawdown_pct")) > _as_float(prev_m.get("max_drawdown_pct"))
        trades_up = _as_int(cur_m.get("trade_count")) > _as_int(prev_m.get("trade_count"))
        any_up = robust_up or pf_up or dd_up or trades_up
        if any_up:
            any_metric_improved_steps += 1
        prev_fp = str(prev.get("config_fingerprint") or "")
        cur_fp = str(cur.get("config_fingerprint") or "")
        # Only score mutation quality on transitions where config actually changed.
        if prev_fp and cur_fp and prev_fp != cur_fp:
            mutation_transition_count += 1
            if any_up:
                mutation_improved_steps += 1

    success_terminal = 1 if str(state.get("status") or "") == "SUCCESS" else 0
    freeze_terminal = 1 if str(state.get("status") or "") == "FREEZE" else 0

    return {
        "loop_id": state.get("loop_id", loop_dir.name),
        "family_id": state.get("family_id"),
        "policy_version": policy_version,
        "is_v2": policy_version == "v2",
        "status": state.get("status"),
        "generation_count": _as_int(state.get("generation")),
        "history_count": len(history),
        "terminal_failure_mode": terminal_failure_mode,
        "policies_exercised": policies_exercised,
        "fingerprints_seen": unique_fingerprints,
        "unique_config_fingerprints": len(unique_fingerprints),
        "failure_mode_counts": dict(failure_counts),
        "policy_counts": dict(policy_counts),
        "mutation_events_count": len(mutation_events),
        "mutation_skipped_due_to_budget": skipped_due_to_budget,
        "max_window_passes": max_window_passes,
        "max_average_profit_factor": max_average_profit_factor,
        "robustness_survived": 1 if (max_window_passes >= 1.0 or max_average_profit_factor >= 1.05) else 0,
        "robustness_improved_steps": robustness_improved_steps,
        "any_metric_improved_steps": any_metric_improved_steps,
        "mutation_transition_count": mutation_transition_count,
        "mutation_improved_steps": mutation_improved_steps,
        "success_terminal": success_terminal,
        "freeze_terminal": freeze_terminal,
        "started_at": state.get("started_at"),
        "ended_at": state.get("ended_at"),
    }


def build_benchmark(
    loops_root: Path,
    families: set[str] | None = None,
    policy_version: str = "any",
) -> dict[str, Any]:
    loops: list[dict[str, Any]] = []
    by_family: dict[str, dict[str, Any]] = defaultdict(
        lambda: {
            "loops": 0,
            "status_counts": Counter(),
            "failure_mode_counts": Counter(),
            "policy_counts": Counter(),
            "mutation_events_count": 0,
            "mutation_skipped_due_to_budget": 0,
            "unique_config_fingerprints_total": 0,
            "robustness_improved_steps": 0,
            "any_metric_improved_steps": 0,
            "mutation_transition_count": 0,
            "mutation_improved_steps": 0,
            "robustness_survived_loops": 0,
            "success_loops": 0,
            "freeze_loops": 0,
        }
    )

    for loop_dir in sorted(loops_root.iterdir()) if loops_root.exists() else []:
        if not loop_dir.is_dir():
            continue
        row = _loop_summary(loop_dir)
        if row is None:
            continue
        family_id = str(row.get("family_id") or "")
        if families and family_id not in families:
            continue
        if policy_version != "any" and str(row.get("policy_version") or "unknown") != policy_version:
            continue
        loops.append(row)
        bucket = by_family[family_id]
        bucket["loops"] += 1
        bucket["status_counts"][str(row.get("status") or "unknown")] += 1
        bucket["failure_mode_counts"].update(row.get("failure_mode_counts") or {})
        bucket["policy_counts"].update(row.get("policy_counts") or {})
        bucket["mutation_events_count"] += _as_int(row.get("mutation_events_count"))
        bucket["mutation_skipped_due_to_budget"] += _as_int(row.get("mutation_skipped_due_to_budget"))
        bucket["unique_config_fingerprints_total"] += _as_int(row.get("unique_config_fingerprints"))
        bucket["robustness_improved_steps"] += _as_int(row.get("robustness_improved_steps"))
        bucket["any_metric_improved_steps"] += _as_int(row.get("any_metric_improved_steps"))
        bucket["mutation_transition_count"] += _as_int(row.get("mutation_transition_count"))
        bucket["mutation_improved_steps"] += _as_int(row.get("mutation_improved_steps"))
        bucket["robustness_survived_loops"] += _as_int(row.get("robustness_survived"))
        bucket["success_loops"] += _as_int(row.get("success_terminal"))
        bucket["freeze_loops"] += _as_int(row.get("freeze_terminal"))

    family_summary: dict[str, Any] = {}
    for family_id, bucket in by_family.items():
        loops_n = max(1, bucket["loops"])
        mutation_events = int(bucket["mutation_events_count"])
        mutation_transitions = int(bucket["mutation_transition_count"])
        mutation_improved = int(bucket["mutation_improved_steps"])
        successes = int(bucket["success_loops"])
        freezes = int(bucket["freeze_loops"])
        family_summary[family_id] = {
            "loops": bucket["loops"],
            "status_counts": dict(bucket["status_counts"]),
            "failure_mode_counts": dict(bucket["failure_mode_counts"]),
            "policy_counts": dict(bucket["policy_counts"]),
            "mutation_events_count": bucket["mutation_events_count"],
            "mutation_skipped_due_to_budget": bucket["mutation_skipped_due_to_budget"],
            "mutation_transition_count": mutation_transitions,
            "mutation_improved_steps": mutation_improved,
            "success_loops": successes,
            "freeze_loops": freezes,
            "avg_unique_config_fingerprints_per_loop": round(
                bucket["unique_config_fingerprints_total"] / loops_n, 3
            ),
            "robustness_improvement_steps_total": int(bucket["robustness_improved_steps"]),
            "robustness_survival_rate": _safe_rate(int(bucket["robustness_survived_loops"]), int(bucket["loops"])),
            "any_metric_improved_steps_total": int(bucket["any_metric_improved_steps"]),
            "mutation_improvement_rate": _safe_rate(mutation_improved, mutation_transitions),
            "loop_success_rate": _safe_rate(successes, int(bucket["loops"])),
            "loop_freeze_rate": _safe_rate(freezes, int(bucket["loops"])),
            "budget_skip_rate": _safe_rate(int(bucket["mutation_skipped_due_to_budget"]), mutation_events),
        }

    return {
        "version": 1,
        "loops_root": str(loops_root),
        "policy_version_filter": policy_version,
        "loop_count": len(loops),
        "families": family_summary,
        "loops": loops,
    }


def _main() -> int:
    ap = argparse.ArgumentParser(description="Aggregate research loop policy-quality benchmark")
    ap.add_argument("--loops-root", default="data/research_loops", help="Root folder containing loop directories")
    ap.add_argument("--families", default="", help="Comma-separated family IDs filter")
    ap.add_argument(
        "--policy-version",
        default="any",
        choices=["any", "v1", "v2", "unknown"],
        help="Filter loops by policy version",
    )
    ap.add_argument("--output", default="data/policy_benchmark.json", help="Output JSON path")
    args = ap.parse_args()

    loops_root = Path(args.loops_root).expanduser().resolve()
    output_path = Path(args.output).expanduser().resolve()
    families = {x.strip() for x in args.families.split(",") if x.strip()} or None

    payload = build_benchmark(loops_root, families=families, policy_version=args.policy_version)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(json.dumps(payload, indent=2), encoding="utf-8")
    print(f"Benchmark loops: {payload['loop_count']}")
    print(f"Wrote: {output_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(_main())
