from __future__ import annotations

import json
from pathlib import Path
from typing import Any


def load_json_if_exists(path: Path) -> dict[str, Any] | None:
    if not path.exists():
        return None
    try:
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return None


def collect_research_artifact_summary(output_dir: Path) -> dict[str, Any]:
    summary: dict[str, Any] = {}
    family_summary = load_json_if_exists(output_dir / "family_summary.json")
    if family_summary:
        summary["family_summary"] = {
            "family_name": family_summary.get("family_name"),
            "research_verdict": family_summary.get("research_verdict"),
            "variant_count": family_summary.get("variant_count"),
            "candidate_count": family_summary.get("candidate_count"),
            "best_variant_name": family_summary.get("best_variant", {}).get("variant_name") if isinstance(family_summary.get("best_variant"), dict) else None,
        }
    profile_summary = load_json_if_exists(output_dir / "profile_discovery_summary.json")
    if profile_summary:
        profiles = profile_summary.get("profiles", [])
        summary["profile_discovery"] = {
            "profile_count": len(profiles),
            "top_profile": profiles[0]["profile_name"] if profiles else None,
            "top_verdict": profiles[0]["verdict"] if profiles else None,
        }
    robustness_summary = load_json_if_exists(output_dir / "robustness_summary.json")
    if robustness_summary:
        summary["robustness"] = {
            "verdict": robustness_summary.get("verdict"),
            "window_passes": ((robustness_summary.get("summary_metrics") or {}).get("window_passes")),
            "cost_passes": ((robustness_summary.get("summary_metrics") or {}).get("cost_passes")),
        }
    return summary
