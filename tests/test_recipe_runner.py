#!/usr/bin/env python3
"""Unit tests for recipe_runner: extractors and rule evaluation."""
from __future__ import annotations

import json
import sys
import tempfile
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from recipe_runner import _extract_json_metric, apply_templates, evaluate_rules, compute_verdict


def test_apply_templates() -> None:
    ctx = {"run_dir": "/data/batch/run_xxx", "cost_bps": "30"}
    s = "python --data-dir {{run_dir}} --cost {{cost_bps}}"
    out = apply_templates(s, ctx)
    assert out == "python --data-dir /data/batch/run_xxx --cost 30"


def test_extract_json_metric() -> None:
    with tempfile.TemporaryDirectory() as tmp:
        path = Path(tmp) / "summary.json"
        path.write_text(json.dumps({"summary_metrics": {"window_passes": 2}}), encoding="utf-8")
        val = _extract_json_metric(str(path), "summary_metrics.window_passes", {})
        assert val == 2.0


def test_evaluate_rules_pass() -> None:
    metrics = {"profit_factor": 1.8, "max_drawdown_pct": -5.0}
    rules = [
        {"metric": "profit_factor", "op": ">=", "value": 1.3, "label": "PF"},
        {"metric": "max_drawdown_pct", "op": ">=", "value": -15.0, "label": "DD"},
    ]
    passed, failed = evaluate_rules(metrics, rules)
    assert len(passed) == 2
    assert len(failed) == 0


def test_evaluate_rules_fail() -> None:
    metrics = {"profit_factor": 1.1, "max_drawdown_pct": -20.0}
    rules = [
        {"metric": "profit_factor", "op": ">=", "value": 1.3, "label": "PF"},
        {"metric": "max_drawdown_pct", "op": ">=", "value": -15.0, "label": "DD"},
    ]
    passed, failed = evaluate_rules(metrics, rules)
    assert len(passed) == 0
    assert len(failed) == 2


def test_compute_verdict_promote() -> None:
    recipe = {"verdict_logic": {"promote_if_all_pass": True}}
    passed, failed = ["PF", "DD"], []
    v = compute_verdict(recipe, passed, failed)
    assert v == "PROMOTE"


def test_compute_verdict_reject() -> None:
    recipe = {"verdict_logic": {"promote_if_all_pass": True}}
    passed, failed = ["PF"], [("DD: -20 >= -15 (failed)", {"warn_only": False})]
    v = compute_verdict(recipe, passed, failed)
    assert v == "REJECT"


def test_compute_verdict_warn() -> None:
    recipe = {"verdict_logic": {"promote_if_all_pass": True}}
    passed, failed = ["PF", "DD"], [("Concentration: 85 <= 80 (failed)", {"warn_only": True})]
    v = compute_verdict(recipe, passed, failed)
    assert v == "WARN"


if __name__ == "__main__":
    test_apply_templates()
    test_extract_json_metric()
    test_evaluate_rules_pass()
    test_evaluate_rules_fail()
    test_compute_verdict_promote()
    test_compute_verdict_reject()
    test_compute_verdict_warn()
    print("All tests passed.")
