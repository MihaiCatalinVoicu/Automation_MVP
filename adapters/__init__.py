from __future__ import annotations

from .policy_benchmark_adapter import run_policy_benchmark_manifest
from .research_loop_adapter import run_research_loop_manifest

ADAPTERS = {
    "research_loop": run_research_loop_manifest,
    "policy_benchmark": run_policy_benchmark_manifest,
}

