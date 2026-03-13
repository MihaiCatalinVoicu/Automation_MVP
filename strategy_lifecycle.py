from __future__ import annotations

import json
import re
import uuid
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

from db import get_conn, utc_now
from strategy_registry import get_strategy, get_strategy_children, list_strategies

VERDICT_RANK = {
    "KEEP": 0,
    "WATCH": 1,
    "TUNE": 2,
    "FREEZE": 3,
    "REMOVE": 4,
}

METRIC_ALIASES = {
    "pf": "profit_factor",
    "profit_factor": "profit_factor",
    "rows_after_filter": "rows_after_filter",
    "top3_share_pct": "top3_share_pct",
    "top5_trades_pct": "top5_trades_pct",
    "max_drawdown_pct": "max_drawdown_pct",
    "max_losing_streak": "max_losing_streak",
    "trades_executed": "trades_executed",
    "breakeven_bps": "breakeven_bps",
}


def _parse_iso(ts: str | None) -> datetime | None:
    if not ts:
        return None
    try:
        text = ts.strip()
        if text.endswith("Z"):
            text = text[:-1] + "+00:00"
        dt = datetime.fromisoformat(text)
        return dt if dt.tzinfo else dt.replace(tzinfo=timezone.utc)
    except Exception:
        return None


def _cadence_delta(cadence: str) -> timedelta:
    c = (cadence or "").strip().lower()
    if not c:
        return timedelta(days=7)
    if c.endswith("h") and c[:-1].isdigit():
        return timedelta(hours=int(c[:-1]))
    if c.endswith("d") and c[:-1].isdigit():
        return timedelta(days=int(c[:-1]))
    if c == "daily":
        return timedelta(days=1)
    if c == "weekly":
        return timedelta(days=7)
    if c == "monthly":
        return timedelta(days=30)
    return timedelta(days=7)


def _flatten_metrics(obj: Any, out: dict[str, float], prefix: str = "") -> None:
    if isinstance(obj, dict):
        for key, value in obj.items():
            new_prefix = f"{prefix}.{key}" if prefix else str(key)
            _flatten_metrics(value, out, new_prefix)
        return
    if isinstance(obj, bool):
        out[prefix] = float(obj)
        return
    if isinstance(obj, (int, float)):
        out[prefix] = float(obj)


def _normalize_metric_name(name: str) -> str:
    raw = (name or "").strip().lower().replace(" ", "_")
    return METRIC_ALIASES.get(raw, raw)


def _parse_trigger_rule(rule: str) -> tuple[str | None, str | None, float | None]:
    match = re.search(r"([A-Za-z0-9_]+)\s*(<=|>=|==|<|>)\s*(-?[0-9.]+)", rule or "")
    if not match:
        return None, None, None
    metric = _normalize_metric_name(match.group(1))
    op = match.group(2)
    value = float(match.group(3))
    return metric, op, value


def _compare(left: float, op: str, right: float) -> bool:
    if op == "<":
        return left < right
    if op == "<=":
        return left <= right
    if op == ">":
        return left > right
    if op == ">=":
        return left >= right
    if op == "==":
        return left == right
    return False


def _latest_strategy_metrics(strategy_id: str) -> tuple[dict[str, float], dict[str, Any]]:
    evidence: dict[str, Any] = {"experiment_result": None, "change_log": None}
    metrics: dict[str, float] = {}
    with get_conn() as conn:
        exp_row = conn.execute(
            """
            SELECT result_json, verdict, created_at, source_file
            FROM experiment_results
            WHERE strategy_id=?
            ORDER BY created_at DESC
            LIMIT 1
            """,
            (strategy_id,),
        ).fetchone()
        if exp_row:
            result = json.loads(exp_row["result_json"] or "{}")
            exp_metrics = result.get("metrics", {})
            if isinstance(exp_metrics, dict):
                _flatten_metrics(exp_metrics, metrics)
            _flatten_metrics(result, metrics)
            evidence["experiment_result"] = {
                "created_at": exp_row["created_at"],
                "verdict": exp_row["verdict"],
                "source_file": exp_row["source_file"],
            }

        change_row = conn.execute(
            """
            SELECT actual_impact_json, status, updated_at
            FROM change_log
            WHERE strategy_id=?
            ORDER BY updated_at DESC
            LIMIT 1
            """,
            (strategy_id,),
        ).fetchone()
        if change_row:
            impact = json.loads(change_row["actual_impact_json"] or "{}")
            _flatten_metrics(impact, metrics)
            evidence["change_log"] = {
                "updated_at": change_row["updated_at"],
                "status": change_row["status"],
            }
    return metrics, evidence


def _has_shadow_logic(strategy_id: str) -> bool:
    children = get_strategy_children(strategy_id)
    return any(item.get("is_shadow") for item in children["files"])


def _evaluate_watch_rule(strategy_id: str, rule_text: str, metrics: dict[str, float]) -> dict[str, Any]:
    metric, op, value = _parse_trigger_rule(rule_text)
    if metric and op and value is not None:
        metric_value = metrics.get(metric)
        if metric_value is None:
            return {
                "rule": rule_text,
                "kind": "numeric",
                "status": "missing_metric",
                "metric": metric,
            }
        return {
            "rule": rule_text,
            "kind": "numeric",
            "status": "triggered" if _compare(metric_value, op, value) else "clear",
            "metric": metric,
            "metric_value": metric_value,
            "threshold": value,
            "operator": op,
        }

    normalized = (rule_text or "").lower()
    if any(token in normalized for token in ["shadow", "duplicate", "legacy"]):
        return {
            "rule": rule_text,
            "kind": "shadow_check",
            "status": "triggered" if _has_shadow_logic(strategy_id) else "clear",
        }

    return {
        "rule": rule_text,
        "kind": "manual",
        "status": "manual_review",
    }


def _recommended_state(current_verdict: str, current_operational_status: str, evaluations: list[dict[str, Any]]) -> tuple[str, str, str]:
    recommended_verdict = current_verdict
    recommended_status = current_operational_status
    review_status = "NO_CHANGE"
    for item in evaluations:
        if item["status"] != "triggered":
            continue
        action = (item.get("trigger_action") or "").upper()
        if action == "AUDIT":
            recommended_status = "audit_required"
            review_status = "TRIGGERED"
            continue
        if action in VERDICT_RANK and VERDICT_RANK[action] > VERDICT_RANK.get(recommended_verdict, 0):
            recommended_verdict = action
            review_status = "TRIGGERED"
    return recommended_verdict, recommended_status, review_status


def list_due_reviews(
    *,
    repo: str | None = None,
    strategy_id: str | None = None,
    now: datetime | None = None,
) -> list[dict[str, Any]]:
    now = now or datetime.now(timezone.utc)
    targets: list[dict[str, Any]] = []
    for strategy in list_strategies(repo=repo, include_shared=False):
        if strategy_id and strategy["id"] != strategy_id:
            continue
        children = get_strategy_children(strategy["id"])
        active_watchers = [item for item in children["watchlist"] if item.get("active", 1)]
        if not active_watchers:
            continue
        last_reviewed = _parse_iso(strategy.get("last_reviewed_at"))
        due_watchers = []
        for watcher in active_watchers:
            cadence = watcher.get("reevaluation_cadence", "weekly")
            if last_reviewed is None or now - last_reviewed >= _cadence_delta(cadence):
                due_watchers.append(watcher)
        if due_watchers or strategy_id:
            targets.append(
                {
                    "strategy": strategy,
                    "watchers": due_watchers or active_watchers,
                }
            )
    return targets


def review_strategy(strategy_id: str, output_dir: Path, review_kind: str = "daily") -> dict[str, Any]:
    strategy = get_strategy(strategy_id)
    if not strategy:
        raise ValueError(f"Unknown strategy_id: {strategy_id}")
    children = get_strategy_children(strategy_id)
    watchers = [item for item in children["watchlist"] if item.get("active", 1)]
    metrics, evidence = _latest_strategy_metrics(strategy_id)

    evaluations = []
    for watcher in watchers:
        evaluated = _evaluate_watch_rule(strategy_id, watcher["trigger_rule"], metrics)
        evaluated["metric_name"] = watcher["metric_name"]
        evaluated["trigger_action"] = watcher["trigger_action"]
        evaluated["cadence"] = watcher["reevaluation_cadence"]
        evaluations.append(evaluated)

    recommended_verdict, recommended_operational_status, review_status = _recommended_state(
        strategy["current_verdict"],
        strategy["operational_status"],
        evaluations,
    )
    now = utc_now()
    artifact = {
        "strategy_id": strategy_id,
        "repo": strategy["repo"],
        "review_kind": review_kind,
        "created_at": now,
        "previous_verdict": strategy["current_verdict"],
        "recommended_verdict": recommended_verdict,
        "recommended_operational_status": recommended_operational_status,
        "metrics": metrics,
        "evidence": evidence,
        "evaluations": evaluations,
        "status": review_status,
    }
    output_dir.mkdir(parents=True, exist_ok=True)
    artifact_path = output_dir / f"{strategy_id}_{now[:19].replace(':', '').replace('-', '')}.json"
    artifact_path.write_text(json.dumps(artifact, indent=2), encoding="utf-8")

    review_id = uuid.uuid4().hex[:12]
    with get_conn() as conn:
        conn.execute(
            """
            INSERT INTO strategy_reviews (
                id, strategy_id, repo, review_kind, cadence, status,
                previous_verdict, recommended_verdict, recommended_operational_status,
                evidence_json, artifact_path, created_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                review_id,
                strategy_id,
                strategy["repo"],
                review_kind,
                ",".join(sorted({item.get("cadence", "") for item in evaluations})),
                review_status,
                strategy["current_verdict"],
                recommended_verdict,
                recommended_operational_status,
                json.dumps({"metrics": metrics, "evaluations": evaluations, "evidence": evidence}, ensure_ascii=True),
                str(artifact_path),
                now,
            ),
        )
        conn.execute(
            """
            UPDATE strategies
            SET current_verdict=?, operational_status=?, last_reviewed_at=?, updated_at=?
            WHERE id=?
            """,
            (recommended_verdict, recommended_operational_status, now, now, strategy_id),
        )
        if recommended_verdict != strategy["current_verdict"] or recommended_operational_status != strategy["operational_status"]:
            conn.execute(
                """
                INSERT INTO strategy_versions (
                    strategy_id, version, summary, reason_for_change, metrics_before_json,
                    metrics_after_json, decision, files_changed_json, reviewed_at, created_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    strategy_id,
                    f"review-{now[:10]}",
                    "Automated lifecycle review updated strategy state",
                    f"{review_kind} watchlist review",
                    json.dumps({"verdict": strategy["current_verdict"], "operational_status": strategy["operational_status"]}, ensure_ascii=True),
                    json.dumps({"verdict": recommended_verdict, "operational_status": recommended_operational_status}, ensure_ascii=True),
                    recommended_verdict,
                    json.dumps([], ensure_ascii=True),
                    now,
                    now,
                ),
            )
    artifact["artifact_path"] = str(artifact_path)
    return artifact


def run_due_reviews(
    *,
    output_dir: Path,
    repo: str | None = None,
    strategy_id: str | None = None,
    review_kind: str = "daily",
) -> dict[str, Any]:
    targets = list_due_reviews(repo=repo, strategy_id=strategy_id)
    reviews = []
    for target in targets:
        reviews.append(review_strategy(target["strategy"]["id"], output_dir=output_dir, review_kind=review_kind))
    return {
        "review_kind": review_kind,
        "review_count": len(reviews),
        "reviews": reviews,
    }
