from __future__ import annotations

import json
import re
import uuid
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Iterable

from db import get_conn, utc_now


@dataclass
class CrossRefResult:
    decision: str
    reason: str
    strategy_id: str | None = None
    category_id: str | None = None
    candidates: list[dict] | None = None
    requires_registry_update: bool = False


def _json(value: Any) -> str:
    return json.dumps(value, ensure_ascii=True, sort_keys=True)


def _normalize(text: str) -> str:
    return re.sub(r"[^a-z0-9]+", " ", (text or "").lower()).strip()


def list_strategies(repo: str | None = None, include_shared: bool = True) -> list[dict]:
    with get_conn() as conn:
        if repo and include_shared:
            rows = conn.execute(
                "SELECT * FROM strategies WHERE repo IN (?, 'shared') ORDER BY repo, category, name",
                (repo,),
            ).fetchall()
        elif repo:
            rows = conn.execute(
                "SELECT * FROM strategies WHERE repo=? ORDER BY category, name",
                (repo,),
            ).fetchall()
        else:
            rows = conn.execute("SELECT * FROM strategies ORDER BY repo, category, name").fetchall()
    out = []
    for row in rows:
        item = dict(row)
        item["tags"] = json.loads(item.get("tags_json") or "[]")
        out.append(item)
    return out


def get_strategy(strategy_id: str) -> dict | None:
    with get_conn() as conn:
        row = conn.execute("SELECT * FROM strategies WHERE id=?", (strategy_id,)).fetchone()
        if not row:
            return None
        item = dict(row)
        item["tags"] = json.loads(item.get("tags_json") or "[]")
        return item


def get_strategy_children(strategy_id: str) -> dict[str, list[dict]]:
    tables = {
        "versions": ("strategy_versions", "strategy_id"),
        "components": ("strategy_components", "strategy_id"),
        "files": ("strategy_file_links", "strategy_id"),
        "metrics": ("strategy_metrics", "strategy_id"),
        "rules": ("strategy_rules", "strategy_id"),
        "watchlist": ("strategy_watchlist", "strategy_id"),
    }
    out: dict[str, list[dict]] = {}
    with get_conn() as conn:
        for key, (table, col) in tables.items():
            rows = conn.execute(f"SELECT * FROM {table} WHERE {col}=? ORDER BY id ASC", (strategy_id,)).fetchall()
            items = [dict(r) for r in rows]
            if key == "rules":
                for item in items:
                    item["rule_config"] = json.loads(item.get("rule_config_json") or "{}")
            out[key] = items
    return out


def _replace_children(conn, table: str, strategy_id: str, rows: Iterable[dict], columns: list[str]) -> None:
    conn.execute(f"DELETE FROM {table} WHERE strategy_id=?", (strategy_id,))
    for row in rows:
        vals = [row.get(col) for col in columns]
        placeholders = ", ".join(["?"] * (len(columns) + 1))
        conn.execute(
            f"INSERT INTO {table} (strategy_id, {', '.join(columns)}) VALUES ({placeholders})",
            [strategy_id, *vals],
        )


def upsert_strategy(record: dict) -> None:
    now = utc_now()
    tags = _json(record.get("tags", []))
    with get_conn() as conn:
        exists = conn.execute("SELECT id FROM strategies WHERE id=?", (record["id"],)).fetchone()
        if exists:
            conn.execute(
                """
                UPDATE strategies
                SET name=?, repo=?, bot=?, category=?, purpose=?, business_hypothesis=?,
                    status_state=?, status_pct=?, operational_status=?, current_verdict=?,
                    owner=?, tags_json=?, notes=?, last_reviewed_at=?, updated_at=?
                WHERE id=?
                """,
                (
                    record["name"],
                    record["repo"],
                    record["bot"],
                    record["category"],
                    record["purpose"],
                    record["business_hypothesis"],
                    record["status_state"],
                    int(record["status_pct"]),
                    record["operational_status"],
                    record["current_verdict"],
                    record.get("owner", "unassigned"),
                    tags,
                    record.get("notes", ""),
                    record.get("last_reviewed_at", now),
                    now,
                    record["id"],
                ),
            )
        else:
            conn.execute(
                """
                INSERT INTO strategies (
                    id, name, repo, bot, category, purpose, business_hypothesis,
                    status_state, status_pct, operational_status, current_verdict,
                    owner, tags_json, notes, last_reviewed_at, created_at, updated_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    record["id"],
                    record["name"],
                    record["repo"],
                    record["bot"],
                    record["category"],
                    record["purpose"],
                    record["business_hypothesis"],
                    record["status_state"],
                    int(record["status_pct"]),
                    record["operational_status"],
                    record["current_verdict"],
                    record.get("owner", "unassigned"),
                    tags,
                    record.get("notes", ""),
                    record.get("last_reviewed_at", now),
                    now,
                    now,
                ),
            )

        _replace_children(
            conn,
            "strategy_versions",
            record["id"],
            [
                {
                    "version": item.get("version", "v1"),
                    "summary": item.get("summary", ""),
                    "reason_for_change": item.get("reason_for_change", ""),
                    "metrics_before_json": _json(item.get("metrics_before", {})),
                    "metrics_after_json": _json(item.get("metrics_after", {})),
                    "decision": item.get("decision", record["current_verdict"]),
                    "files_changed_json": _json(item.get("files_changed", [])),
                    "reviewed_at": item.get("reviewed_at", now),
                    "created_at": item.get("created_at", now),
                }
                for item in record.get("versions", [])
            ],
            [
                "version",
                "summary",
                "reason_for_change",
                "metrics_before_json",
                "metrics_after_json",
                "decision",
                "files_changed_json",
                "reviewed_at",
                "created_at",
            ],
        )
        _replace_children(
            conn,
            "strategy_components",
            record["id"],
            [
                {
                    "component_name": item.get("component_name", ""),
                    "component_category": item.get("component_category", ""),
                    "description": item.get("description", ""),
                    "status_state": item.get("status_state", record["status_state"]),
                    "notes": item.get("notes", ""),
                }
                for item in record.get("components", [])
            ],
            ["component_name", "component_category", "description", "status_state", "notes"],
        )
        _replace_children(
            conn,
            "strategy_file_links",
            record["id"],
            [
                {
                    "repo": item.get("repo", record["repo"]),
                    "file_path": item.get("file_path", ""),
                    "role": item.get("role", "implementation"),
                    "is_shadow": 1 if item.get("is_shadow") else 0,
                    "notes": item.get("notes", ""),
                }
                for item in record.get("file_links", [])
            ],
            ["repo", "file_path", "role", "is_shadow", "notes"],
        )
        _replace_children(
            conn,
            "strategy_metrics",
            record["id"],
            [
                {
                    "metric_name": item.get("metric_name", ""),
                    "target_value": item.get("target_value", ""),
                    "threshold_rule": item.get("threshold_rule", ""),
                    "notes": item.get("notes", ""),
                }
                for item in record.get("metrics", [])
            ],
            ["metric_name", "target_value", "threshold_rule", "notes"],
        )
        _replace_children(
            conn,
            "strategy_rules",
            record["id"],
            [
                {
                    "rule_name": item.get("rule_name", ""),
                    "rule_kind": item.get("rule_kind", ""),
                    "severity": item.get("severity", "warn"),
                    "rule_config_json": _json(item.get("rule_config", {})),
                    "notes": item.get("notes", ""),
                }
                for item in record.get("rules", [])
            ],
            ["rule_name", "rule_kind", "severity", "rule_config_json", "notes"],
        )
        _replace_children(
            conn,
            "strategy_watchlist",
            record["id"],
            [
                {
                    "metric_name": item.get("metric_name", ""),
                    "trigger_rule": item.get("trigger_rule", ""),
                    "reevaluation_cadence": item.get("reevaluation_cadence", ""),
                    "trigger_action": item.get("trigger_action", ""),
                    "active": 1 if item.get("active", True) else 0,
                    "notes": item.get("notes", ""),
                }
                for item in record.get("watchlist", [])
            ],
            ["metric_name", "trigger_rule", "reevaluation_cadence", "trigger_action", "active", "notes"],
        )


def search_strategies(repo: str, text: str, category_id: str | None = None, limit: int = 5) -> list[dict]:
    normalized = _normalize(text)
    tokens = [tok for tok in normalized.split() if len(tok) >= 3]
    if not tokens:
        return []
    matches = []
    for strategy in list_strategies(repo=repo, include_shared=True):
        if category_id and category_id != strategy["category"]:
            continue
        haystack_parts = [
            strategy["id"],
            strategy["name"],
            strategy["category"],
            strategy["purpose"],
            strategy["business_hypothesis"],
            " ".join(strategy.get("tags", [])),
            strategy.get("notes", ""),
        ]
        haystack = _normalize(" ".join(haystack_parts))
        score = 0
        for tok in tokens:
            if tok in haystack:
                score += 1
        if score > 0:
            item = dict(strategy)
            item["_score"] = score
            matches.append(item)
    matches.sort(key=lambda x: (-x["_score"], x["repo"], x["id"]))
    return matches[:limit]


def preflight_cross_reference(task: dict, repo_cfg: dict | None = None) -> CrossRefResult:
    repo = task.get("repo") or (repo_cfg or {}).get("name") or ""
    repo_rows = list_strategies(repo=repo, include_shared=True)
    if not repo_rows:
        return CrossRefResult(
            decision="BLOCK_UNSCOPED_CHANGE",
            reason=f"strategy registry is empty for repo {repo}; seed registry first",
        )
    strategy_id = task.get("strategy_id") or (task.get("metadata") or {}).get("strategy_id")
    category_id = task.get("category_id") or (task.get("metadata") or {}).get("category_id")
    new_strategy_proposal = task.get("new_strategy_proposal") or (task.get("metadata") or {}).get("new_strategy_proposal")
    goal = task.get("goal", "")
    constraints = " ".join(task.get("constraints", []))
    checks = " ".join(task.get("checks", []))
    recipe = task.get("recipe", "")
    task_text = " ".join(filter(None, [goal, constraints, checks, recipe]))

    if strategy_id:
        strategy = get_strategy(strategy_id)
        if not strategy:
            return CrossRefResult(
                decision="BLOCK_UNSCOPED_CHANGE",
                reason=f"Unknown strategy_id: {strategy_id}",
                category_id=category_id,
            )
        if strategy["repo"] not in {repo, "shared"}:
            return CrossRefResult(
                decision="BLOCK_UNSCOPED_CHANGE",
                reason=f"Strategy {strategy_id} belongs to repo {strategy['repo']}, not {repo}",
                strategy_id=strategy_id,
                category_id=strategy["category"],
            )
        if category_id and category_id != strategy["category"]:
            return CrossRefResult(
                decision="BLOCK_UNSCOPED_CHANGE",
                reason=f"category_id={category_id} does not match strategy category={strategy['category']}",
                strategy_id=strategy_id,
                category_id=category_id,
            )
        return CrossRefResult(
            decision="ALLOW",
            reason="strategy_id validated",
            strategy_id=strategy_id,
            category_id=strategy["category"],
        )

    candidates = search_strategies(repo, task_text, category_id=category_id)
    if new_strategy_proposal:
        if candidates:
            return CrossRefResult(
                decision="BLOCK_DUPLICATE",
                reason="new strategy proposal overlaps existing registry entries",
                category_id=category_id,
                candidates=candidates,
            )
        return CrossRefResult(
            decision="ALLOW_WITH_REGISTRY_UPDATE",
            reason="new strategy proposal accepted; registry entry required",
            category_id=category_id,
            requires_registry_update=True,
        )

    if len(candidates) == 1:
        return CrossRefResult(
            decision="ALLOW_WITH_REGISTRY_UPDATE",
            reason="single strategy candidate resolved from task text",
            strategy_id=candidates[0]["id"],
            category_id=candidates[0]["category"],
            candidates=candidates,
            requires_registry_update=True,
        )

    if len(candidates) > 1:
        return CrossRefResult(
            decision="BLOCK_UNSCOPED_CHANGE",
            reason="multiple strategy candidates match; choose one explicitly",
            category_id=category_id,
            candidates=candidates,
        )

    if category_id:
        return CrossRefResult(
            decision="REQUIRES_NEW_STRATEGY_ENTRY",
            reason="category provided but no matching strategy found; create a new strategy entry",
            category_id=category_id,
        )

    return CrossRefResult(
        decision="BLOCK_UNSCOPED_CHANGE",
        reason="task must include strategy_id or new_strategy_proposal",
    )


def create_change_log(
    *,
    repo: str,
    change_kind: str,
    summary: str,
    requested_by: str,
    strategy_id: str | None = None,
    run_id: str | None = None,
    category_id: str | None = None,
    proposed_strategy_name: str | None = None,
    file_paths: list[str] | None = None,
    expected_impact: dict | None = None,
    actual_impact: dict | None = None,
    status: str = "SUBMITTED",
) -> str:
    now = utc_now()
    change_id = uuid.uuid4().hex[:12]
    with get_conn() as conn:
        conn.execute(
            """
            INSERT INTO change_log (
                id, strategy_id, run_id, repo, category_id, change_kind, summary,
                proposed_strategy_name, requested_by, status, file_paths_json,
                expected_impact_json, actual_impact_json, created_at, updated_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                change_id,
                strategy_id,
                run_id,
                repo,
                category_id,
                change_kind,
                summary,
                proposed_strategy_name,
                requested_by,
                status,
                _json(file_paths or []),
                _json(expected_impact or {}),
                _json(actual_impact or {}),
                now,
                now,
            ),
        )
    return change_id


def update_change_log(
    *,
    run_id: str,
    status: str,
    actual_impact: dict | None = None,
    strategy_id: str | None = None,
) -> None:
    with get_conn() as conn:
        current = conn.execute(
            "SELECT * FROM change_log WHERE run_id=? ORDER BY created_at DESC LIMIT 1",
            (run_id,),
        ).fetchone()
        if not current:
            return
        payload = json.loads(current["actual_impact_json"] or "{}")
        if actual_impact:
            payload.update(actual_impact)
        conn.execute(
            """
            UPDATE change_log
            SET status=?, strategy_id=COALESCE(?, strategy_id), actual_impact_json=?, updated_at=?
            WHERE id=?
            """,
            (status, strategy_id, _json(payload), utc_now(), current["id"]),
        )


def create_experiment(
    *,
    strategy_id: str | None,
    repo: str,
    name: str,
    hypothesis: str,
    run_dir: str,
    search_space: dict | None = None,
    status: str = "RECORDED",
) -> str:
    experiment_id = uuid.uuid4().hex[:12]
    now = utc_now()
    with get_conn() as conn:
        conn.execute(
            """
            INSERT INTO experiments (id, strategy_id, repo, name, hypothesis, run_dir, search_space_json, status, created_at, updated_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                experiment_id,
                strategy_id,
                repo,
                name,
                hypothesis,
                run_dir,
                _json(search_space or {}),
                status,
                now,
                now,
            ),
        )
    return experiment_id


def add_experiment_result(
    *,
    experiment_id: str | None,
    strategy_id: str | None,
    run_dir: str,
    source_file: str,
    result: dict,
    verdict: str,
) -> None:
    with get_conn() as conn:
        conn.execute(
            """
            INSERT INTO experiment_results (experiment_id, strategy_id, run_dir, source_file, result_json, verdict, created_at)
            VALUES (?, ?, ?, ?, ?, ?, ?)
            """,
            (experiment_id, strategy_id, run_dir, source_file, _json(result), verdict, utc_now()),
        )

