#!/usr/bin/env python3
from __future__ import annotations

import importlib
import os
import sys
import tempfile
from datetime import datetime, timedelta, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))


def _bootstrap(tmp_db: str):
    os.environ["DB_PATH"] = tmp_db
    import approval_service
    import db

    db = importlib.reload(db)
    approval_service = importlib.reload(approval_service)
    db.init_db()
    return db, approval_service


def _seed_case_bundle(db, *, case_id: str = "sc_promo_guard") -> tuple[str, str]:
    manifest_id = f"{case_id}_manifest"
    verdict_id = f"{case_id}_verdict"
    db.create_search_case(
        case_id=case_id,
        case_type="family_search",
        title="promotion guard test",
        status="active",
        stage="promotion_review",
        family="breakout_momentum",
        hypothesis="promotion gate",
        objective_type="find_edge",
        repo_scope="crypto-bot",
        market="crypto",
        created_from="test",
        owner="test",
        search_budget={"max_manifests": 2},
        risk_budget={"min_trades": 10},
    )
    db.create_experiment_manifest(
        manifest_id=manifest_id,
        case_id=case_id,
        status="completed",
        repo="crypto-bot",
        adapter_type="research_loop",
        entrypoint="research_loop.py",
        strategy_identity={"strategy_id": "breakout_momentum", "family": "breakout_momentum"},
        run_context_template={"environment": "research"},
        dataset_spec={"dataset_id": "crypto_top10"},
        execution_spec={
            "family": "breakout_momentum",
            "config_path": "/root/crypto-bot/configs/research.json",
            "recipe_path": "/root/automation-mvp/recipes/breakout_momentum_daily.json",
            "repo_root": "/root/crypto-bot",
        },
        cost_model={},
        gates={},
        created_by="test",
        approved_by="test",
    )
    db.create_edge_verdict(
        verdict_id=verdict_id,
        case_id=case_id,
        manifest_id=manifest_id,
        run_id="rl_promo_guard",
        verdict_type="research_evaluation",
        status="final",
        decision="PROMOTE_TO_PAPER",
        decision_reason="all_primary_gates_passed",
        metrics_snapshot={"profit_factor": 1.2, "trades": 120},
        gate_results={
            "min_trades_pass": True,
            "cost_adjusted_edge_pass": True,
            "walkforward_pass": True,
            "leakage_check_pass": True,
        },
    )
    with db.get_conn() as conn:
        conn.execute(
            "UPDATE search_cases SET latest_manifest_id=?, latest_verdict_id=?, stage=? WHERE case_id=?",
            (manifest_id, verdict_id, "promotion_review", case_id),
        )
    return manifest_id, verdict_id


def test_promote_to_paper_blocked_inside_shadow_window(monkeypatch) -> None:
    tmp = tempfile.NamedTemporaryFile(suffix=".db", delete=False)
    tmp.close()
    db, approval_service = _bootstrap(tmp.name)
    _seed_case_bundle(db)

    monkeypatch.setenv("EDGE_SEARCH_SHADOW_ONLY_DAYS", "30")
    result = approval_service.apply_research_decision(
        case_id="sc_promo_guard",
        action="PROMOTE_TO_PAPER",
        actor="operator",
        source="manual",
        details="Promotion rationale is documented, but the first 30 days stay shadow-only.",
    )
    assert result["ok"] is False
    assert result["status"] == "PROMOTION_BLOCKED"
    assert str(result["reason"]).startswith("promotion_shadow_window_active")

    os.unlink(tmp.name)


def test_promote_to_paper_requires_written_rationale(monkeypatch) -> None:
    tmp = tempfile.NamedTemporaryFile(suffix=".db", delete=False)
    tmp.close()
    db, approval_service = _bootstrap(tmp.name)
    _seed_case_bundle(db, case_id="sc_promo_rationale")
    old_opened = (datetime.now(timezone.utc) - timedelta(days=45)).isoformat()
    with db.get_conn() as conn:
        conn.execute("UPDATE search_cases SET opened_at=? WHERE case_id=?", (old_opened, "sc_promo_rationale"))

    monkeypatch.setenv("EDGE_SEARCH_SHADOW_ONLY_DAYS", "30")
    result = approval_service.apply_research_decision(
        case_id="sc_promo_rationale",
        action="PROMOTE_TO_PAPER",
        actor="operator",
        source="manual",
        details="too short",
    )
    assert result["ok"] is False
    assert result["reason"] == "promotion_requires_written_rationale:24"

    os.unlink(tmp.name)


def test_promote_to_paper_allowed_after_shadow_window(monkeypatch) -> None:
    tmp = tempfile.NamedTemporaryFile(suffix=".db", delete=False)
    tmp.close()
    db, approval_service = _bootstrap(tmp.name)
    _seed_case_bundle(db, case_id="sc_promo_allowed")
    old_opened = (datetime.now(timezone.utc) - timedelta(days=45)).isoformat()
    with db.get_conn() as conn:
        conn.execute("UPDATE search_cases SET opened_at=? WHERE case_id=?", (old_opened, "sc_promo_allowed"))

    monkeypatch.setenv("EDGE_SEARCH_SHADOW_ONLY_DAYS", "30")
    result = approval_service.apply_research_decision(
        case_id="sc_promo_allowed",
        action="PROMOTE_TO_PAPER",
        actor="operator",
        source="manual",
        details="Manual promotion approved after shadow month with written rationale and gate review.",
    )
    assert result["ok"] is True
    case = db.get_search_case("sc_promo_allowed")
    assert case["stage"] == "paper_candidate"

    os.unlink(tmp.name)
