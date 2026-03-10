#!/usr/bin/env python3
from __future__ import annotations

import os
import tempfile
import importlib
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))


def _bootstrap(tmp_db: str):
    os.environ["DB_PATH"] = tmp_db
    import db
    import strategy_registry
    import strategy_seed_data

    db = importlib.reload(db)
    strategy_registry = importlib.reload(strategy_registry)

    db.init_db()
    for item in strategy_seed_data.SEED_STRATEGIES[:5]:
        strategy_registry.upsert_strategy(item)
    return db, strategy_registry


def test_get_strategy() -> None:
    tmp = tempfile.NamedTemporaryFile(suffix=".db", delete=False)
    tmp.close()
    _, strategy_registry = _bootstrap(tmp.name)
    item = strategy_registry.get_strategy("breakout_setup")
    assert item is not None
    assert item["category"] == "setup"
    os.unlink(tmp.name)


def test_preflight_allow_explicit() -> None:
    tmp = tempfile.NamedTemporaryFile(suffix=".db", delete=False)
    tmp.close()
    _, strategy_registry = _bootstrap(tmp.name)
    res = strategy_registry.preflight_cross_reference(
        {"repo": "crypto-bot", "goal": "Tune breakout validation", "strategy_id": "breakout_setup"},
        {"name": "crypto-bot"},
    )
    assert res.decision == "ALLOW"
    assert res.strategy_id == "breakout_setup"
    os.unlink(tmp.name)


def test_preflight_block_unscoped() -> None:
    tmp = tempfile.NamedTemporaryFile(suffix=".db", delete=False)
    tmp.close()
    _, strategy_registry = _bootstrap(tmp.name)
    res = strategy_registry.preflight_cross_reference(
        {"repo": "crypto-bot", "goal": "Do some random change"},
        {"name": "crypto-bot"},
    )
    assert res.decision == "BLOCK_UNSCOPED_CHANGE"
    os.unlink(tmp.name)


def test_preflight_duplicate_proposal() -> None:
    tmp = tempfile.NamedTemporaryFile(suffix=".db", delete=False)
    tmp.close()
    _, strategy_registry = _bootstrap(tmp.name)
    res = strategy_registry.preflight_cross_reference(
        {
            "repo": "crypto-bot",
            "goal": "Create a new breakout setup variant",
            "new_strategy_proposal": "Breakout variant",
            "category_id": "setup",
        },
        {"name": "crypto-bot"},
    )
    assert res.decision == "BLOCK_DUPLICATE"
    os.unlink(tmp.name)


if __name__ == "__main__":
    test_get_strategy()
    test_preflight_allow_explicit()
    test_preflight_block_unscoped()
    test_preflight_duplicate_proposal()
    print("All tests passed.")
