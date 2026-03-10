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
    import strategy_lifecycle
    import strategy_registry
    import strategy_seed_data

    db = importlib.reload(db)
    strategy_registry = importlib.reload(strategy_registry)
    strategy_lifecycle = importlib.reload(strategy_lifecycle)
    db.init_db()
    for item in strategy_seed_data.SEED_STRATEGIES[:8]:
        strategy_registry.upsert_strategy(item)
    return strategy_registry, strategy_lifecycle


def test_review_strategy_escalates_verdict() -> None:
    tmp = tempfile.NamedTemporaryFile(suffix=".db", delete=False)
    tmp.close()
    strategy_registry, strategy_lifecycle = _bootstrap(tmp.name)
    strategy_registry.add_experiment_result(
        experiment_id=None,
        strategy_id="btc_risk_off_filter",
        run_dir="/tmp/run_1",
        source_file="/tmp/summary.json",
        result={"metrics": {"profit_factor": 1.1, "top3_share_pct": 85.0}},
        verdict="REJECT",
    )

    with tempfile.TemporaryDirectory() as out_dir:
        review = strategy_lifecycle.review_strategy("btc_risk_off_filter", Path(out_dir))

    assert review["recommended_verdict"] == "FREEZE"
    assert Path(review["artifact_path"]).exists()
    os.unlink(tmp.name)


if __name__ == "__main__":
    test_review_strategy_escalates_verdict()
    print("All tests passed.")
