from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from policy_benchmark import build_benchmark


def run_policy_benchmark_manifest(manifest: dict[str, Any], automation_root: Path) -> dict[str, Any]:
    execution_spec = json.loads(manifest["execution_spec_json"])
    identity = json.loads(manifest["strategy_identity_json"])
    artifacts = json.loads(manifest["artifacts_json"])
    loops_root = Path(str(execution_spec.get("loops_root") or (automation_root / "data" / "research_loops")))
    output_root = Path(str(artifacts.get("output_root") or (automation_root / "data" / "policy_benchmark" / manifest["manifest_id"])))
    output_root.mkdir(parents=True, exist_ok=True)
    family = identity.get("family")
    payload = build_benchmark(
        loops_root.expanduser().resolve(),
        families={str(family)} if family else None,
        policy_version=str(execution_spec.get("policy_version_filter", "any")),
    )
    output_file = output_root / "policy_benchmark.json"
    output_file.write_text(json.dumps(payload, indent=2), encoding="utf-8")
    primary_metric = 0.0
    loops = int(payload.get("loop_count", 0) or 0)
    if family and payload.get("families", {}).get(str(family)):
        row = payload["families"][str(family)]
        primary_metric = float(row.get("loop_success_rate") or 0.0)
    return {
        "ok": True,
        "manifest_id": manifest["manifest_id"],
        "case_id": manifest["case_id"],
        "run_id": f"policy_benchmark:{manifest['manifest_id']}",
        "adapter_type": "policy_benchmark",
        "artifacts_root": str(output_root),
        "summary": {
            "status": "completed",
            "primary_metric": primary_metric,
            "trades": 0,
            "loop_count": loops,
            "output_file": str(output_file),
        },
        "warnings": [],
        "errors": [],
    }

