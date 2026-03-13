#!/usr/bin/env python3
from __future__ import annotations

import importlib
import os
import sys
import tempfile
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))


def _bootstrap(tmp_db: str):
    os.environ["DB_PATH"] = tmp_db
    import db

    db = importlib.reload(db)
    db.init_db()
    return db


def _seed_case(db) -> str:
    case_id = "sc_retry_policy"
    db.create_search_case(
        case_id=case_id,
        case_type="research",
        title="retry policy case",
        status="active",
        stage="manifest_ready",
        family="breakout_momentum",
        hypothesis="test",
        objective_type="pf",
        repo_scope="crypto-bot",
        market="crypto",
        created_from="test",
        owner="test",
        search_budget={"max_runs": 1},
        risk_budget={"max_dd_pct": 10},
    )
    return case_id


def _seed_manifest(db, case_id: str) -> str:
    manifest_id = "em_retry_policy"
    db.create_experiment_manifest(
        manifest_id=manifest_id,
        case_id=case_id,
        status="ready",
        repo="crypto-bot",
        adapter_type="research_loop",
        entrypoint="research_loop.py",
        strategy_identity={"family": "breakout_momentum"},
        run_context_template={"run_date": "2026-03-12"},
        dataset_spec={"symbol": "BTCUSDT"},
        execution_spec={
            "family": "breakout_momentum",
            "config_path": "configs/research_cohort_breakout.json",
            "recipe_path": "recipes/breakout.yaml",
            "repo_root": "/root/crypto-bot",
        },
        cost_model={},
        gates={},
        created_by="test",
    )
    return manifest_id


def test_manifest_transitions_to_dead_after_max_retries() -> None:
    tmp = tempfile.NamedTemporaryFile(suffix=".db", delete=False)
    tmp.close()
    db = _bootstrap(tmp.name)
    case_id = _seed_case(db)
    manifest_id = _seed_manifest(db, case_id)

    # 1st attempt -> failed
    db.claim_manifest("worker-a")
    status, attempts = db.set_manifest_failed_with_retry_policy(manifest_id, last_error="e1", max_retries=3)
    assert status == "failed"
    assert attempts == 1

    db.set_manifest_execution_state(manifest_id, "ready")

    # 2nd attempt -> failed
    db.claim_manifest("worker-a")
    status, attempts = db.set_manifest_failed_with_retry_policy(manifest_id, last_error="e2", max_retries=3)
    assert status == "failed"
    assert attempts == 2

    db.set_manifest_execution_state(manifest_id, "ready")

    # 3rd attempt -> dead
    db.claim_manifest("worker-a")
    status, attempts = db.set_manifest_failed_with_retry_policy(manifest_id, last_error="e3", max_retries=3)
    assert status == "dead"
    assert attempts == 3
    row = db.get_experiment_manifest(manifest_id)
    assert row["execution_status"] == "dead"

    os.unlink(tmp.name)


if __name__ == "__main__":
    test_manifest_transitions_to_dead_after_max_retries()
    print("All tests passed.")
