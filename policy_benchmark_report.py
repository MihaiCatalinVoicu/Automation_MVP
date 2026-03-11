#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any


def _load_json(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8"))


def _fmt_rate(value: Any) -> str:
    if value is None:
        return "n/a"
    try:
        return f"{float(value):.3f}"
    except (TypeError, ValueError):
        return "n/a"


def _fmt_int(value: Any) -> str:
    try:
        return str(int(value))
    except (TypeError, ValueError):
        return "0"


def _family_row(name: str, family: dict[str, Any]) -> str:
    return (
        f"| {name} | {_fmt_int(family.get('loops'))} | "
        f"{_fmt_rate(family.get('loop_success_rate'))} | {_fmt_rate(family.get('loop_freeze_rate'))} | "
        f"{_fmt_rate(family.get('mutation_improvement_rate'))} | "
        f"{_fmt_rate(family.get('budget_skip_rate'))} | "
        f"{_fmt_int(family.get('mutation_events_count'))} | "
        f"{_fmt_int(family.get('mutation_transition_count'))} | "
        f"{_fmt_int(family.get('mutation_improved_steps'))} |"
    )


def _top_counts(counter_obj: dict[str, Any], limit: int = 4) -> str:
    items = sorted(((str(k), int(v)) for k, v in (counter_obj or {}).items()), key=lambda kv: (-kv[1], kv[0]))
    if not items:
        return "n/a"
    return ", ".join(f"{k}:{v}" for k, v in items[:limit])


def render_report(benchmarks: list[dict[str, Any]], source_files: list[Path], title: str) -> str:
    lines: list[str] = []
    lines.append(f"# {title}")
    lines.append("")
    lines.append("## Inputs")
    for path, data in zip(source_files, benchmarks):
        lines.append(
            f"- `{path}` | loops={data.get('loop_count', 0)} | policy_filter={data.get('policy_version_filter', 'any')}"
        )
    lines.append("")

    for data in benchmarks:
        policy_filter = str(data.get("policy_version_filter", "any"))
        loops = int(data.get("loop_count", 0) or 0)
        lines.append(f"## Summary ({policy_filter})")
        lines.append(f"- Loops: `{loops}`")
        lines.append("")
        lines.append(
            "| Family | Loops | Success Rate | Freeze Rate | Mutation Improvement Rate | Budget Skip Rate | Mutation Events | Mutation Transitions | Mutation Improved |"
        )
        lines.append("|---|---:|---:|---:|---:|---:|---:|---:|---:|")
        families = data.get("families") or {}
        for family_name in sorted(families.keys()):
            lines.append(_family_row(family_name, families[family_name]))
        lines.append("")
        lines.append("### Failure / Policy Mix")
        for family_name in sorted(families.keys()):
            fam = families[family_name]
            fail_mix = _top_counts(fam.get("failure_mode_counts") or {})
            policy_mix = _top_counts(fam.get("policy_counts") or {})
            lines.append(f"- `{family_name}` failures: {fail_mix}")
            lines.append(f"- `{family_name}` policies: {policy_mix}")
        lines.append("")

    # Lightweight per-loop appendix for traceability.
    lines.append("## Per-Loop Snapshot")
    lines.append("| Loop ID | Family | Policy | Status | Terminal Mode | Fingerprints | Policies Exercised |")
    lines.append("|---|---|---|---|---|---:|---|")
    for data in benchmarks:
        for loop in data.get("loops") or []:
            lines.append(
                f"| {loop.get('loop_id','')} | {loop.get('family_id','')} | {loop.get('policy_version','')} | "
                f"{loop.get('status','')} | {loop.get('terminal_failure_mode','')} | "
                f"{_fmt_int(loop.get('unique_config_fingerprints'))} | "
                f"{', '.join(loop.get('policies_exercised') or []) or 'n/a'} |"
            )
    lines.append("")
    return "\n".join(lines)


def _main() -> int:
    ap = argparse.ArgumentParser(description="Generate one-page markdown report from policy benchmark JSON files")
    ap.add_argument(
        "--inputs",
        required=True,
        help="Comma-separated benchmark JSON paths (example: data/policy_benchmark_v2.json,data/policy_benchmark_v1.json)",
    )
    ap.add_argument("--title", default="Policy Benchmark Report")
    ap.add_argument("--output", default="data/policy_benchmark_report.md")
    args = ap.parse_args()

    input_paths = [Path(p.strip()).expanduser().resolve() for p in args.inputs.split(",") if p.strip()]
    if not input_paths:
        raise ValueError("No input files provided")
    payloads = [_load_json(path) for path in input_paths]
    report = render_report(payloads, input_paths, args.title)

    output_path = Path(args.output).expanduser().resolve()
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(report, encoding="utf-8")
    print(f"Wrote: {output_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(_main())
