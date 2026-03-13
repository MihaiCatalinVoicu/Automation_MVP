#!/usr/bin/env python3
from __future__ import annotations

import sys
import tempfile
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from registry_audit import build_repo_audit


def test_build_repo_audit_reports_unmapped_and_dead_links() -> None:
    with tempfile.TemporaryDirectory() as tmp:
        repo_root = Path(tmp)
        (repo_root / "main.py").write_text("print('runtime')\n", encoding="utf-8")
        (repo_root / "core").mkdir(parents=True, exist_ok=True)
        (repo_root / "core" / "ideas.py").write_text("def generate():\n    return []\n", encoding="utf-8")
        (repo_root / "core" / "regime_gate.py").write_text("def gate():\n    return True\n", encoding="utf-8")
        (repo_root / "scripts").mkdir(parents=True, exist_ok=True)
        (repo_root / "scripts" / "portfolio_replay.py").write_text("print('replay')\n", encoding="utf-8")

        registry_rows = [
            {
                "strategy_id": "breakout_setup",
                "strategy_name": "Breakout setup",
                "strategy_status_state": "functional",
                "strategy_operational_status": "active",
                "strategy_verdict": "WATCH",
                "file_path": str(repo_root / "main.py"),
                "role": "runtime",
                "is_shadow": False,
                "notes": "",
            },
            {
                "strategy_id": "breakout_setup",
                "strategy_name": "Breakout setup",
                "strategy_status_state": "functional",
                "strategy_operational_status": "active",
                "strategy_verdict": "WATCH",
                "file_path": str(repo_root / "core" / "missing_logic.py"),
                "role": "implementation",
                "is_shadow": False,
                "notes": "",
            },
        ]

        audit = build_repo_audit("crypto-bot", repo_root, registry_rows)

        unmapped_paths = {item["relative_path"] for item in audit["unmapped_live_logic"]}
        dead_paths = {item["relative_path"] for item in audit["dead_registry_links"]}

        assert "core/ideas.py" in unmapped_paths
        assert "core/regime_gate.py" in unmapped_paths
        assert "scripts/portfolio_replay.py" in unmapped_paths
        assert "core/missing_logic.py" in dead_paths
        assert audit["summary"]["high_severity_failure_count"] >= 1


if __name__ == "__main__":
    test_build_repo_audit_reports_unmapped_and_dead_links()
    print("All tests passed.")
