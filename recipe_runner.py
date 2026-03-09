"""
Validation battery runner: execute recipe commands, extract metrics, emit verdict.

Runs commands from a JSON recipe in sequence, captures output, extracts metrics
via regex, evaluates rules, outputs summary.json and verdict.txt.
"""
from __future__ import annotations

import json
import os
import re
import subprocess
from pathlib import Path
from typing import Any


def load_recipe(recipe_path: str, base_path: Path | None = None) -> dict:
    p = Path(recipe_path)
    if not p.is_absolute() and base_path:
        p = base_path / p
    if not p.exists():
        raise FileNotFoundError(f"Recipe not found: {p}")
    with open(p, "r", encoding="utf-8") as f:
        return json.load(f)


def apply_templates(text: str, context: dict[str, Any]) -> str:
    for k, v in context.items():
        text = text.replace("{{" + k + "}}", str(v))
    return text


def run_commands(
    recipe: dict,
    context: dict[str, Any],
    output_dir: Path,
    continue_on_error: bool = False,
) -> tuple[dict[str, dict], dict[str, float]]:
    """Run commands, write logs, return step_outputs and extracted metrics."""
    cwd = recipe.get("cwd")
    if not cwd:
        cwd = context.get("cwd", ".")
    cwd = apply_templates(str(cwd), context)
    cwd_path = Path(cwd).expanduser().resolve()

    commands = recipe.get("commands", [])
    step_outputs: dict[str, dict] = {}
    metrics: dict[str, float] = {}

    log_dir = output_dir / "command_logs"
    log_dir.mkdir(parents=True, exist_ok=True)

    for item in commands:
        cmd_id = item.get("id", "unknown")
        cmd = apply_templates(item.get("cmd", ""), context)
        if not cmd:
            continue

        proc = subprocess.run(
            cmd,
            shell=True,
            cwd=str(cwd_path),
            capture_output=True,
            text=True,
            timeout=600,
        )
        stdout = proc.stdout or ""
        stderr = proc.stderr or ""
        step_outputs[cmd_id] = {
            "returncode": proc.returncode,
            "stdout": stdout,
            "stderr": stderr,
        }
        combined = stdout + "\n" + stderr

        log_path = log_dir / f"{cmd_id}.log"
        with open(log_path, "w", encoding="utf-8") as f:
            f.write(f"=== {cmd_id} (returncode={proc.returncode}) ===\n")
            f.write(stdout)
            if stderr:
                f.write("\n--- stderr ---\n")
                f.write(stderr)

        if proc.returncode != 0 and not continue_on_error:
            raise RuntimeError(f"Command {cmd_id} failed with code {proc.returncode}: {stderr[:500]}")

        for ex in recipe.get("extractors", []):
            if ex.get("source") != cmd_id:
                continue
            regex = ex.get("regex")
            metric = ex.get("metric")
            if not regex or not metric:
                continue
            m = re.search(regex, combined)
            if m:
                try:
                    val = float(m.group(1).replace(",", "."))
                    metrics[metric] = val
                except (ValueError, IndexError):
                    pass

    return step_outputs, metrics


def evaluate_rules(metrics: dict[str, float], rules: list[dict]) -> tuple[list[str], list[tuple[str, dict]]]:
    passed: list[str] = []
    failed: list[tuple[str, dict]] = []

    ops = {
        ">=": lambda a, b: a >= b,
        "<=": lambda a, b: a <= b,
        ">": lambda a, b: a > b,
        "<": lambda a, b: a < b,
        "==": lambda a, b: a == b,
    }

    for r in rules:
        metric = r.get("metric")
        op = r.get("op", ">=")
        value = r.get("value")
        label = r.get("label", metric)
        if metric not in metrics or value is None:
            failed.append((f"{label}: missing value", r))
            continue
        if op not in ops:
            failed.append((f"{label}: unknown op {op}", r))
            continue
        if ops[op](metrics[metric], value):
            passed.append(label)
        else:
            failed.append((f"{label}: {metrics[metric]} {op} {value} (failed)", r))

    return passed, failed


def compute_verdict(
    recipe: dict,
    passed: list[str],
    failed: list[tuple[str, dict]],
) -> str:
    logic = recipe.get("verdict_logic", {})
    promote_if_all = logic.get("promote_if_all_pass", True)

    if promote_if_all and not failed:
        return "PROMOTE"
    reject_fails = [f for f in failed if not f[1].get("warn_only")]
    if reject_fails:
        return "REJECT"
    return "WARN"


def run_validation_battery(
    recipe_path: str,
    run_context: dict[str, Any],
    output_dir: Path,
    base_path: Path | None = None,
) -> dict[str, Any]:
    recipe = load_recipe(recipe_path, base_path)
    context = dict(run_context)
    continue_on_error = recipe.get("continue_on_error", False)

    step_outputs, metrics = run_commands(recipe, context, output_dir, continue_on_error)
    passed, failed = evaluate_rules(metrics, recipe.get("rules", []))
    verdict = compute_verdict(recipe, passed, failed)

    summary = {
        "recipe": recipe.get("name", recipe_path),
        "metrics": metrics,
        "rules_passed": passed,
        "rules_failed": [f[0] for f in failed],
        "verdict": verdict,
    }

    with open(output_dir / "summary.json", "w", encoding="utf-8") as f:
        json.dump(summary, f, indent=2)

    with open(output_dir / "verdict.txt", "w", encoding="utf-8") as f:
        f.write(verdict)

    return summary


def _main() -> int:
    import argparse
    ap = argparse.ArgumentParser(description="Run validation battery (dry-run)")
    ap.add_argument("recipe", help="Recipe path (e.g. recipes/crypto_phaseb_riskoff.json)")
    ap.add_argument("run_dir", help="Crypto run dir (e.g. .../data/batch/run_top50_xxx)")
    ap.add_argument("--output-dir", default=None, help="Output dir (default: data/validation_artifacts/<run_id>)")
    ap.add_argument("--base-path", default=None, help="Base path for recipe resolution (default: script dir)")
    args = ap.parse_args()

    base = Path(__file__).resolve().parent
    base_path = Path(args.base_path) if args.base_path else base
    run_dir = Path(args.run_dir).expanduser().resolve()
    cwd = run_dir
    for _ in range(3):
        cwd = cwd.parent
        if (cwd / "scripts" / "portfolio_replay.py").exists():
            break
    else:
        cwd = run_dir.parent.parent.parent
    context = {"run_dir": str(run_dir), "cwd": str(cwd)}

    out = args.output_dir or str(base / "data" / "validation_artifacts" / run_dir.name)
    output_dir = Path(out)
    output_dir.mkdir(parents=True, exist_ok=True)

    try:
        summary = run_validation_battery(args.recipe, context, output_dir, base_path)
    except Exception as e:
        print(f"ERROR: {e}")
        return 1

    print(f"Verdict: {summary['verdict']}")
    print(f"Output: {output_dir}")
    print(f"Metrics: {summary.get('metrics', {})}")
    return 0 if summary["verdict"] != "REJECT" else 1


if __name__ == "__main__":
    raise SystemExit(_main())
