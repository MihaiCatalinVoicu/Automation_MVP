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
    import edge_search_state

    db = importlib.reload(db)
    edge_search_state = importlib.reload(edge_search_state)
    db.init_db()
    return db, edge_search_state


def test_live_edge_search_review_ready_for_refine() -> None:
    payload = {
        "queue_health": {"pending_total": 5, "ready_total": 2, "completed_total": 260, "dead_total": 8},
        "family_ranking": [
            {
                "family_id": "trend_volatility_expansion",
                "family_score": 0.81,
                "near_miss_rate": 0.22,
                "near_miss_count": 28,
                "latest_near_miss_score": 0.78,
                "manifest_counts": {"total": 140, "completed": 120, "dead": 4},
                "fingerprints": {"unique_fingerprints": 80, "repeated_fingerprints": 8},
            },
            {
                "family_id": "pullback_in_trend",
                "family_score": 0.68,
                "near_miss_rate": 0.14,
                "near_miss_count": 18,
                "latest_near_miss_score": 0.72,
                "manifest_counts": {"total": 90, "completed": 76, "dead": 3},
                "fingerprints": {"unique_fingerprints": 60, "repeated_fingerprints": 4},
            },
        ],
    }
    _, edge_search_state = _bootstrap(tempfile.NamedTemporaryFile(suffix=".db", delete=False).name)
    review = edge_search_state.evaluate_live_edge_search_review(payload)
    assert review["mode"] == "REFINE"
    assert review["triggers"]["trigger_a"]["status"] == "ready"


def test_live_edge_search_review_freezes_on_duplicate_waste() -> None:
    payload = {
        "queue_health": {"pending_total": 10, "ready_total": 4, "completed_total": 220, "dead_total": 20},
        "family_ranking": [
            {
                "family_id": "trend_volatility_expansion",
                "family_score": 0.35,
                "near_miss_rate": 0.03,
                "near_miss_count": 3,
                "latest_near_miss_score": 0.61,
                "manifest_counts": {"total": 160, "completed": 120, "dead": 20},
                "fingerprints": {"unique_fingerprints": 20, "repeated_fingerprints": 12},
            }
        ],
    }
    _, edge_search_state = _bootstrap(tempfile.NamedTemporaryFile(suffix=".db", delete=False).name)
    review = edge_search_state.evaluate_live_edge_search_review(payload)
    assert review["mode"] == "FROZEN"
    assert review["status"] == "freeze_required"
