from __future__ import annotations

import json
import os
import sqlite3
from contextlib import contextmanager
from datetime import datetime, timezone
from typing import Any, Iterable, Optional

DB_PATH = os.getenv("DB_PATH", "./data/orchestrator.db")
SQLITE_TIMEOUT_SECONDS = 10


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _connect() -> sqlite3.Connection:
    os.makedirs(os.path.dirname(DB_PATH) or ".", exist_ok=True)
    conn = sqlite3.connect(DB_PATH, timeout=SQLITE_TIMEOUT_SECONDS, check_same_thread=False)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL;")
    conn.execute("PRAGMA foreign_keys=ON;")
    conn.execute("PRAGMA busy_timeout=10000;")
    return conn


@contextmanager
def get_conn() -> Iterable[sqlite3.Connection]:
    conn = _connect()
    try:
        yield conn
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


def init_db() -> None:
    with get_conn() as conn:
        conn.executescript(
            """
            CREATE TABLE IF NOT EXISTS runs (
                id TEXT PRIMARY KEY,
                repo TEXT NOT NULL,
                goal TEXT NOT NULL,
                branch TEXT NOT NULL,
                task_type TEXT NOT NULL,
                task_json TEXT NOT NULL,
                routing_json TEXT NOT NULL,
                status TEXT NOT NULL,
                execution_owner TEXT,
                preferred_executor TEXT NOT NULL,
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL
            );

            CREATE TABLE IF NOT EXISTS events (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                run_id TEXT NOT NULL,
                event_type TEXT NOT NULL,
                payload_json TEXT NOT NULL,
                created_at TEXT NOT NULL,
                FOREIGN KEY(run_id) REFERENCES runs(id)
            );

            CREATE TABLE IF NOT EXISTS approvals (
                id TEXT PRIMARY KEY,
                run_id TEXT NOT NULL,
                reason TEXT NOT NULL,
                summary_json TEXT NOT NULL,
                status TEXT NOT NULL,
                decision TEXT,
                decision_details TEXT,
                telegram_message_id TEXT,
                created_at TEXT NOT NULL,
                resolved_at TEXT,
                FOREIGN KEY(run_id) REFERENCES runs(id)
            );

            CREATE INDEX IF NOT EXISTS idx_runs_status ON runs(status);
            CREATE INDEX IF NOT EXISTS idx_events_run_id ON events(run_id);
            CREATE INDEX IF NOT EXISTS idx_approvals_run_id ON approvals(run_id);

            CREATE TABLE IF NOT EXISTS strategies (
                id TEXT PRIMARY KEY,
                name TEXT NOT NULL,
                repo TEXT NOT NULL,
                bot TEXT NOT NULL,
                category TEXT NOT NULL,
                purpose TEXT NOT NULL,
                business_hypothesis TEXT NOT NULL,
                status_state TEXT NOT NULL,
                status_pct INTEGER NOT NULL,
                operational_status TEXT NOT NULL,
                current_verdict TEXT NOT NULL,
                owner TEXT NOT NULL,
                tags_json TEXT NOT NULL,
                notes TEXT NOT NULL,
                last_reviewed_at TEXT NOT NULL,
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL
            );

            CREATE TABLE IF NOT EXISTS strategy_versions (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                strategy_id TEXT NOT NULL,
                version TEXT NOT NULL,
                summary TEXT NOT NULL,
                reason_for_change TEXT NOT NULL,
                metrics_before_json TEXT NOT NULL,
                metrics_after_json TEXT NOT NULL,
                decision TEXT NOT NULL,
                files_changed_json TEXT NOT NULL,
                reviewed_at TEXT NOT NULL,
                created_at TEXT NOT NULL,
                FOREIGN KEY(strategy_id) REFERENCES strategies(id)
            );

            CREATE TABLE IF NOT EXISTS strategy_components (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                strategy_id TEXT NOT NULL,
                component_name TEXT NOT NULL,
                component_category TEXT NOT NULL,
                description TEXT NOT NULL,
                status_state TEXT NOT NULL,
                notes TEXT NOT NULL,
                FOREIGN KEY(strategy_id) REFERENCES strategies(id)
            );

            CREATE TABLE IF NOT EXISTS strategy_file_links (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                strategy_id TEXT NOT NULL,
                repo TEXT NOT NULL,
                file_path TEXT NOT NULL,
                role TEXT NOT NULL,
                is_shadow INTEGER NOT NULL DEFAULT 0,
                notes TEXT NOT NULL,
                FOREIGN KEY(strategy_id) REFERENCES strategies(id)
            );

            CREATE TABLE IF NOT EXISTS strategy_metrics (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                strategy_id TEXT NOT NULL,
                metric_name TEXT NOT NULL,
                target_value TEXT NOT NULL,
                threshold_rule TEXT NOT NULL,
                notes TEXT NOT NULL,
                FOREIGN KEY(strategy_id) REFERENCES strategies(id)
            );

            CREATE TABLE IF NOT EXISTS strategy_rules (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                strategy_id TEXT NOT NULL,
                rule_name TEXT NOT NULL,
                rule_kind TEXT NOT NULL,
                severity TEXT NOT NULL,
                rule_config_json TEXT NOT NULL,
                notes TEXT NOT NULL,
                FOREIGN KEY(strategy_id) REFERENCES strategies(id)
            );

            CREATE TABLE IF NOT EXISTS strategy_watchlist (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                strategy_id TEXT NOT NULL,
                metric_name TEXT NOT NULL,
                trigger_rule TEXT NOT NULL,
                reevaluation_cadence TEXT NOT NULL,
                trigger_action TEXT NOT NULL,
                active INTEGER NOT NULL DEFAULT 1,
                notes TEXT NOT NULL,
                FOREIGN KEY(strategy_id) REFERENCES strategies(id)
            );

            CREATE TABLE IF NOT EXISTS change_log (
                id TEXT PRIMARY KEY,
                strategy_id TEXT,
                run_id TEXT,
                repo TEXT NOT NULL,
                category_id TEXT,
                change_kind TEXT NOT NULL,
                summary TEXT NOT NULL,
                proposed_strategy_name TEXT,
                requested_by TEXT NOT NULL,
                status TEXT NOT NULL,
                file_paths_json TEXT NOT NULL,
                expected_impact_json TEXT NOT NULL,
                actual_impact_json TEXT NOT NULL,
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL,
                FOREIGN KEY(strategy_id) REFERENCES strategies(id),
                FOREIGN KEY(run_id) REFERENCES runs(id)
            );

            CREATE TABLE IF NOT EXISTS experiments (
                id TEXT PRIMARY KEY,
                strategy_id TEXT,
                repo TEXT NOT NULL,
                name TEXT NOT NULL,
                hypothesis TEXT NOT NULL,
                run_dir TEXT NOT NULL,
                search_space_json TEXT NOT NULL,
                status TEXT NOT NULL,
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL,
                FOREIGN KEY(strategy_id) REFERENCES strategies(id)
            );

            CREATE TABLE IF NOT EXISTS experiment_results (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                experiment_id TEXT,
                strategy_id TEXT,
                run_dir TEXT NOT NULL,
                source_file TEXT NOT NULL,
                result_json TEXT NOT NULL,
                verdict TEXT NOT NULL,
                created_at TEXT NOT NULL,
                FOREIGN KEY(experiment_id) REFERENCES experiments(id),
                FOREIGN KEY(strategy_id) REFERENCES strategies(id)
            );

            CREATE TABLE IF NOT EXISTS strategy_reviews (
                id TEXT PRIMARY KEY,
                strategy_id TEXT NOT NULL,
                repo TEXT NOT NULL,
                review_kind TEXT NOT NULL,
                cadence TEXT NOT NULL,
                status TEXT NOT NULL,
                previous_verdict TEXT NOT NULL,
                recommended_verdict TEXT NOT NULL,
                recommended_operational_status TEXT NOT NULL,
                evidence_json TEXT NOT NULL,
                artifact_path TEXT NOT NULL,
                created_at TEXT NOT NULL,
                FOREIGN KEY(strategy_id) REFERENCES strategies(id)
            );

            CREATE TABLE IF NOT EXISTS research_schedules (
                id TEXT PRIMARY KEY,
                repo TEXT NOT NULL,
                strategy_id TEXT NOT NULL,
                family_name TEXT NOT NULL,
                recipe_path TEXT NOT NULL,
                cohort_config_path TEXT NOT NULL,
                cadence TEXT NOT NULL,
                enabled INTEGER NOT NULL DEFAULT 1,
                config_json TEXT NOT NULL,
                artifact_root TEXT NOT NULL,
                last_materialized_at TEXT,
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL,
                FOREIGN KEY(strategy_id) REFERENCES strategies(id)
            );

            CREATE TABLE IF NOT EXISTS schedule_runs (
                id TEXT PRIMARY KEY,
                schedule_id TEXT NOT NULL,
                run_id TEXT NOT NULL,
                run_date TEXT NOT NULL,
                status TEXT NOT NULL,
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL,
                FOREIGN KEY(schedule_id) REFERENCES research_schedules(id),
                FOREIGN KEY(run_id) REFERENCES runs(id)
            );

            CREATE TABLE IF NOT EXISTS artifact_manifests (
                id TEXT PRIMARY KEY,
                schedule_id TEXT,
                run_id TEXT NOT NULL,
                repo TEXT NOT NULL,
                strategy_id TEXT,
                family_name TEXT,
                artifact_kind TEXT NOT NULL,
                artifact_path TEXT NOT NULL,
                summary_json TEXT NOT NULL,
                created_at TEXT NOT NULL,
                FOREIGN KEY(schedule_id) REFERENCES research_schedules(id),
                FOREIGN KEY(run_id) REFERENCES runs(id),
                FOREIGN KEY(strategy_id) REFERENCES strategies(id)
            );

            CREATE TABLE IF NOT EXISTS runtime_runs (
                run_id TEXT PRIMARY KEY,
                repo TEXT NOT NULL,
                environment TEXT NOT NULL,
                strategy_id TEXT NOT NULL,
                family TEXT NOT NULL,
                variant_id TEXT,
                profile_id TEXT,
                status TEXT NOT NULL DEFAULT 'ACTIVE',
                first_event_ts TEXT,
                last_event_ts TEXT,
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL
            );

            CREATE TABLE IF NOT EXISTS raw_lifecycle_events (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                event_id TEXT NOT NULL,
                schema_version TEXT NOT NULL,
                idempotency_key TEXT NOT NULL UNIQUE,
                event_type TEXT NOT NULL,
                repo TEXT NOT NULL,
                environment TEXT NOT NULL,
                strategy_id TEXT NOT NULL,
                family TEXT NOT NULL,
                variant_id TEXT,
                profile_id TEXT,
                run_id TEXT NOT NULL,
                signal_id TEXT NOT NULL,
                decision_id TEXT,
                position_id TEXT,
                symbol TEXT,
                side TEXT,
                ts TEXT NOT NULL,
                payload_json TEXT NOT NULL,
                source_file TEXT,
                source_line INTEGER,
                created_at TEXT NOT NULL,
                FOREIGN KEY(run_id) REFERENCES runtime_runs(run_id)
            );

            CREATE TABLE IF NOT EXISTS work_items (
                id TEXT PRIMARY KEY,
                title TEXT NOT NULL,
                repo TEXT NOT NULL,
                strategy_id TEXT,
                scope_type TEXT NOT NULL,
                status TEXT NOT NULL,
                progress_pct INTEGER NOT NULL,
                priority TEXT NOT NULL,
                phase TEXT,
                owner TEXT,
                blocked_by TEXT,
                deferred_reason TEXT,
                decision_ref TEXT,
                source_doc TEXT,
                source_item_id TEXT,
                acceptance_criteria TEXT,
                notes TEXT NOT NULL,
                target_date TEXT,
                completed_at TEXT,
                last_reviewed_at TEXT NOT NULL,
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL,
                FOREIGN KEY(strategy_id) REFERENCES strategies(id)
            );

            CREATE TABLE IF NOT EXISTS work_item_events (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                work_item_id TEXT NOT NULL,
                event_type TEXT NOT NULL,
                old_status TEXT,
                new_status TEXT,
                old_progress_pct INTEGER,
                new_progress_pct INTEGER,
                reason TEXT NOT NULL,
                old_payload_json TEXT NOT NULL,
                new_payload_json TEXT NOT NULL,
                created_at TEXT NOT NULL,
                FOREIGN KEY(work_item_id) REFERENCES work_items(id)
            );

            CREATE TABLE IF NOT EXISTS runtime_import_state (
                source_path TEXT PRIMARY KEY,
                last_line_processed INTEGER NOT NULL DEFAULT 0,
                last_imported_at TEXT,
                last_status TEXT,
                last_error TEXT,
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL
            );

            CREATE TABLE IF NOT EXISTS maintenance_job_runs (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                job_name TEXT NOT NULL,
                status TEXT NOT NULL,
                summary_json TEXT NOT NULL,
                created_at TEXT NOT NULL
            );

            CREATE INDEX IF NOT EXISTS idx_strategies_repo ON strategies(repo);
            CREATE INDEX IF NOT EXISTS idx_strategies_category ON strategies(category);
            CREATE INDEX IF NOT EXISTS idx_strategy_versions_strategy_id ON strategy_versions(strategy_id);
            CREATE INDEX IF NOT EXISTS idx_strategy_file_links_strategy_id ON strategy_file_links(strategy_id);
            CREATE INDEX IF NOT EXISTS idx_change_log_strategy_id ON change_log(strategy_id);
            CREATE INDEX IF NOT EXISTS idx_change_log_run_id ON change_log(run_id);
            CREATE INDEX IF NOT EXISTS idx_experiments_strategy_id ON experiments(strategy_id);
            CREATE INDEX IF NOT EXISTS idx_strategy_reviews_strategy_id ON strategy_reviews(strategy_id);
            CREATE INDEX IF NOT EXISTS idx_research_schedules_repo ON research_schedules(repo);
            CREATE INDEX IF NOT EXISTS idx_schedule_runs_schedule_date ON schedule_runs(schedule_id, run_date);
            CREATE INDEX IF NOT EXISTS idx_artifact_manifests_run_id ON artifact_manifests(run_id);
            CREATE INDEX IF NOT EXISTS idx_runtime_runs_strategy_id ON runtime_runs(strategy_id);
            CREATE INDEX IF NOT EXISTS idx_runtime_runs_repo_env ON runtime_runs(repo, environment);
            CREATE INDEX IF NOT EXISTS idx_raw_lifecycle_run_id ON raw_lifecycle_events(run_id);
            CREATE INDEX IF NOT EXISTS idx_raw_lifecycle_strategy_id ON raw_lifecycle_events(strategy_id);
            CREATE INDEX IF NOT EXISTS idx_raw_lifecycle_signal_id ON raw_lifecycle_events(signal_id);
            CREATE INDEX IF NOT EXISTS idx_raw_lifecycle_event_type ON raw_lifecycle_events(event_type);
            CREATE INDEX IF NOT EXISTS idx_work_items_repo ON work_items(repo);
            CREATE INDEX IF NOT EXISTS idx_work_items_status ON work_items(status);
            CREATE INDEX IF NOT EXISTS idx_work_items_strategy_id ON work_items(strategy_id);
            CREATE INDEX IF NOT EXISTS idx_work_items_source ON work_items(source_doc, source_item_id);
            CREATE INDEX IF NOT EXISTS idx_work_item_events_work_item_id ON work_item_events(work_item_id);
            CREATE INDEX IF NOT EXISTS idx_runtime_import_state_status ON runtime_import_state(last_status);
            CREATE INDEX IF NOT EXISTS idx_maintenance_job_runs_name ON maintenance_job_runs(job_name, created_at);

            CREATE VIEW IF NOT EXISTS scan_summaries_v AS
            SELECT
                id,
                event_id,
                repo,
                environment,
                strategy_id,
                family,
                variant_id,
                profile_id,
                run_id,
                signal_id,
                ts,
                payload_json
            FROM raw_lifecycle_events
            WHERE event_type='scan_summary';

            CREATE VIEW IF NOT EXISTS signals_v AS
            SELECT
                id,
                event_id,
                repo,
                environment,
                strategy_id,
                family,
                variant_id,
                profile_id,
                run_id,
                signal_id,
                symbol,
                side,
                ts,
                payload_json
            FROM raw_lifecycle_events
            WHERE event_type='signal';

            CREATE VIEW IF NOT EXISTS decisions_v AS
            SELECT
                id,
                event_id,
                repo,
                environment,
                strategy_id,
                family,
                variant_id,
                profile_id,
                run_id,
                signal_id,
                decision_id,
                symbol,
                side,
                ts,
                payload_json
            FROM raw_lifecycle_events
            WHERE event_type='decision';

            CREATE VIEW IF NOT EXISTS fills_v AS
            SELECT
                id,
                event_id,
                repo,
                environment,
                strategy_id,
                family,
                variant_id,
                profile_id,
                run_id,
                signal_id,
                decision_id,
                position_id,
                symbol,
                side,
                ts,
                payload_json
            FROM raw_lifecycle_events
            WHERE event_type='fill';

            CREATE VIEW IF NOT EXISTS exits_v AS
            SELECT
                id,
                event_id,
                repo,
                environment,
                strategy_id,
                family,
                variant_id,
                profile_id,
                run_id,
                signal_id,
                decision_id,
                position_id,
                symbol,
                side,
                ts,
                payload_json
            FROM raw_lifecycle_events
            WHERE event_type='exit';

            CREATE VIEW IF NOT EXISTS outcomes_v AS
            SELECT
                id,
                event_id,
                repo,
                environment,
                strategy_id,
                family,
                variant_id,
                profile_id,
                run_id,
                signal_id,
                decision_id,
                position_id,
                symbol,
                side,
                ts,
                payload_json
            FROM raw_lifecycle_events
            WHERE event_type='outcome';
            """
        )


def insert_run(
    run_id: str,
    repo: str,
    goal: str,
    branch: str,
    task_type: str,
    task_json: dict,
    routing_json: dict,
    status: str,
    preferred_executor: str,
) -> None:
    now = utc_now()
    with get_conn() as conn:
        conn.execute(
            """
            INSERT INTO runs (
                id, repo, goal, branch, task_type, task_json, routing_json,
                status, execution_owner, preferred_executor, created_at, updated_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, NULL, ?, ?, ?)
            """,
            (
                run_id,
                repo,
                goal,
                branch,
                task_type,
                json.dumps(task_json),
                json.dumps(routing_json),
                status,
                preferred_executor,
                now,
                now,
            ),
        )


def update_run_status(run_id: str, status: str) -> None:
    with get_conn() as conn:
        conn.execute(
            "UPDATE runs SET status=?, updated_at=? WHERE id=?",
            (status, utc_now(), run_id),
        )


def update_run_routing(run_id: str, routing: dict) -> None:
    with get_conn() as conn:
        conn.execute(
            "UPDATE runs SET routing_json=?, updated_at=? WHERE id=?",
            (json.dumps(routing), utc_now(), run_id),
        )


def clear_execution_owner(run_id: str) -> None:
    with get_conn() as conn:
        conn.execute(
            "UPDATE runs SET execution_owner=NULL, updated_at=? WHERE id=?",
            (utc_now(), run_id),
        )


def claim_run(worker_id: str) -> Optional[dict]:
    now = utc_now()
    with get_conn() as conn:
        conn.execute("BEGIN IMMEDIATE")
        row = conn.execute(
            """
            SELECT * FROM runs
            WHERE status IN ('QUEUED', 'RETRY_PENDING')
              AND execution_owner IS NULL
            ORDER BY created_at ASC
            LIMIT 1
            """
        ).fetchone()
        if not row:
            return None
        run_id = row["id"]
        conn.execute(
            """
            UPDATE runs
            SET status='RUNNING', execution_owner=?, updated_at=?
            WHERE id=? AND execution_owner IS NULL
            """,
            (worker_id, now, run_id),
        )
        claimed = conn.execute("SELECT * FROM runs WHERE id=?", (run_id,)).fetchone()
        if not claimed or claimed["execution_owner"] != worker_id:
            return None
        return dict(claimed)


def get_run(run_id: str) -> Optional[dict]:
    with get_conn() as conn:
        row = conn.execute("SELECT * FROM runs WHERE id=?", (run_id,)).fetchone()
        return dict(row) if row else None


def insert_event(run_id: str, event_type: str, payload: dict) -> None:
    with get_conn() as conn:
        conn.execute(
            """
            INSERT INTO events (run_id, event_type, payload_json, created_at)
            VALUES (?, ?, ?, ?)
            """,
            (run_id, event_type, json.dumps(payload), utc_now()),
        )


def upsert_runtime_run(
    *,
    run_id: str,
    repo: str,
    environment: str,
    strategy_id: str,
    family: str,
    variant_id: str | None = None,
    profile_id: str | None = None,
    status: str = "ACTIVE",
    first_event_ts: str | None = None,
    last_event_ts: str | None = None,
) -> None:
    now = utc_now()
    with get_conn() as conn:
        existing = conn.execute("SELECT run_id, first_event_ts, last_event_ts FROM runtime_runs WHERE run_id=?", (run_id,)).fetchone()
        if existing:
            first_ts = existing["first_event_ts"] or first_event_ts or last_event_ts or now
            last_ts = last_event_ts or existing["last_event_ts"] or first_event_ts or now
            conn.execute(
                """
                UPDATE runtime_runs
                SET repo=?, environment=?, strategy_id=?, family=?, variant_id=?, profile_id=?,
                    status=?, first_event_ts=?, last_event_ts=?, updated_at=?
                WHERE run_id=?
                """,
                (
                    repo,
                    environment,
                    strategy_id,
                    family,
                    variant_id,
                    profile_id,
                    status,
                    first_ts,
                    last_ts,
                    now,
                    run_id,
                ),
            )
        else:
            conn.execute(
                """
                INSERT INTO runtime_runs (
                    run_id, repo, environment, strategy_id, family, variant_id, profile_id,
                    status, first_event_ts, last_event_ts, created_at, updated_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    run_id,
                    repo,
                    environment,
                    strategy_id,
                    family,
                    variant_id,
                    profile_id,
                    status,
                    first_event_ts or last_event_ts or now,
                    last_event_ts or first_event_ts or now,
                    now,
                    now,
                ),
            )


def insert_raw_lifecycle_event(record: dict[str, Any]) -> bool:
    now = utc_now()
    with get_conn() as conn:
        existing = conn.execute(
            "SELECT id FROM raw_lifecycle_events WHERE idempotency_key=?",
            (record["idempotency_key"],),
        ).fetchone()
        if existing:
            return False
        conn.execute(
            """
            INSERT INTO raw_lifecycle_events (
                event_id, schema_version, idempotency_key, event_type, repo, environment,
                strategy_id, family, variant_id, profile_id, run_id, signal_id,
                decision_id, position_id, symbol, side, ts, payload_json,
                source_file, source_line, created_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                record["event_id"],
                record["schema_version"],
                record["idempotency_key"],
                record["event_type"],
                record["repo"],
                record["environment"],
                record["strategy_id"],
                record["family"],
                record.get("variant_id"),
                record.get("profile_id"),
                record["run_id"],
                record["signal_id"],
                record.get("decision_id"),
                record.get("position_id"),
                record.get("symbol"),
                record.get("side"),
                record["ts"],
                json.dumps(record, ensure_ascii=False, sort_keys=True),
                record.get("source_file"),
                record.get("source_line"),
                now,
            ),
        )
        return True


def _normalize_progress_pct(value: int | None) -> int:
    pct = int(value or 0)
    if pct < 0:
        return 0
    if pct > 100:
        return 100
    return pct


def create_work_item(
    *,
    work_item_id: str,
    title: str,
    repo: str,
    scope_type: str,
    status: str,
    progress_pct: int = 0,
    strategy_id: str | None = None,
    priority: str = "medium",
    phase: str | None = None,
    owner: str | None = None,
    blocked_by: str | None = None,
    deferred_reason: str | None = None,
    decision_ref: str | None = None,
    source_doc: str | None = None,
    source_item_id: str | None = None,
    acceptance_criteria: str | None = None,
    notes: str = "",
    target_date: str | None = None,
) -> None:
    now = utc_now()
    progress = _normalize_progress_pct(progress_pct)
    with get_conn() as conn:
        conn.execute(
            """
            INSERT INTO work_items (
                id, title, repo, strategy_id, scope_type, status, progress_pct, priority, phase,
                owner, blocked_by, deferred_reason, decision_ref, source_doc, source_item_id,
                acceptance_criteria, notes, target_date, completed_at, last_reviewed_at, created_at, updated_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                work_item_id,
                title,
                repo,
                strategy_id,
                scope_type,
                status,
                progress,
                priority,
                phase,
                owner,
                blocked_by,
                deferred_reason,
                decision_ref,
                source_doc,
                source_item_id,
                acceptance_criteria,
                notes,
                target_date,
                now if status == "done" else None,
                now,
                now,
                now,
            ),
        )


def get_work_item(work_item_id: str) -> Optional[dict]:
    with get_conn() as conn:
        row = conn.execute("SELECT * FROM work_items WHERE id=?", (work_item_id,)).fetchone()
        return dict(row) if row else None


def get_work_item_by_source(source_doc: str, source_item_id: str | None = None) -> Optional[dict]:
    with get_conn() as conn:
        if source_item_id is None:
            row = conn.execute(
                "SELECT * FROM work_items WHERE source_doc=? AND source_item_id IS NULL ORDER BY created_at DESC LIMIT 1",
                (source_doc,),
            ).fetchone()
        else:
            row = conn.execute(
                "SELECT * FROM work_items WHERE source_doc=? AND source_item_id=? LIMIT 1",
                (source_doc, source_item_id),
            ).fetchone()
        return dict(row) if row else None


def list_work_items(
    *,
    repo: str | None = None,
    strategy_id: str | None = None,
    status: str | None = None,
    scope_type: str | None = None,
) -> list[dict]:
    where = []
    params: list[Any] = []
    if repo:
        where.append("repo=?")
        params.append(repo)
    if strategy_id:
        where.append("strategy_id=?")
        params.append(strategy_id)
    if status:
        where.append("status=?")
        params.append(status)
    if scope_type:
        where.append("scope_type=?")
        params.append(scope_type)
    sql = "SELECT * FROM work_items"
    if where:
        sql += " WHERE " + " AND ".join(where)
    sql += " ORDER BY priority ASC, updated_at DESC, created_at DESC"
    with get_conn() as conn:
        rows = conn.execute(sql, params).fetchall()
        return [dict(r) for r in rows]


def create_work_item_event(
    *,
    work_item_id: str,
    event_type: str,
    reason: str,
    old_status: str | None = None,
    new_status: str | None = None,
    old_progress_pct: int | None = None,
    new_progress_pct: int | None = None,
    old_payload: dict[str, Any] | None = None,
    new_payload: dict[str, Any] | None = None,
) -> None:
    with get_conn() as conn:
        conn.execute(
            """
            INSERT INTO work_item_events (
                work_item_id, event_type, old_status, new_status, old_progress_pct, new_progress_pct,
                reason, old_payload_json, new_payload_json, created_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                work_item_id,
                event_type,
                old_status,
                new_status,
                old_progress_pct,
                _normalize_progress_pct(new_progress_pct) if new_progress_pct is not None else None,
                reason,
                json.dumps(old_payload or {}, ensure_ascii=True, sort_keys=True),
                json.dumps(new_payload or {}, ensure_ascii=True, sort_keys=True),
                utc_now(),
            ),
        )


def update_work_item(
    work_item_id: str,
    *,
    reason: str = "manual_update",
    event_type: str = "progress_update",
    **updates: Any,
) -> None:
    current = get_work_item(work_item_id)
    if not current:
        raise KeyError(f"Work item not found: {work_item_id}")
    allowed = {
        "title",
        "repo",
        "strategy_id",
        "scope_type",
        "status",
        "progress_pct",
        "priority",
        "phase",
        "owner",
        "blocked_by",
        "deferred_reason",
        "decision_ref",
        "source_doc",
        "source_item_id",
        "acceptance_criteria",
        "notes",
        "target_date",
        "completed_at",
        "last_reviewed_at",
    }
    payload = dict(current)
    changed = False
    for key, value in updates.items():
        if key not in allowed:
            continue
        if key == "progress_pct" and value is not None:
            value = _normalize_progress_pct(int(value))
        if payload.get(key) != value:
            payload[key] = value
            changed = True
    if not changed:
        return
    now = utc_now()
    payload["updated_at"] = now
    payload["last_reviewed_at"] = updates.get("last_reviewed_at") or now
    if payload.get("status") == "done" and not payload.get("completed_at"):
        payload["completed_at"] = now
    set_clause = ", ".join(f"{k}=?" for k in payload.keys() if k != "id")
    values = [payload[k] for k in payload.keys() if k != "id"]
    with get_conn() as conn:
        conn.execute(f"UPDATE work_items SET {set_clause} WHERE id=?", (*values, work_item_id))
    create_work_item_event(
        work_item_id=work_item_id,
        event_type=event_type,
        reason=reason,
        old_status=current.get("status"),
        new_status=payload.get("status"),
        old_progress_pct=current.get("progress_pct"),
        new_progress_pct=payload.get("progress_pct"),
        old_payload=current,
        new_payload=payload,
    )


def list_work_item_events(work_item_id: str) -> list[dict]:
    with get_conn() as conn:
        rows = conn.execute(
            "SELECT * FROM work_item_events WHERE work_item_id=? ORDER BY id ASC",
            (work_item_id,),
        ).fetchall()
        return [dict(r) for r in rows]


def get_runtime_import_state(source_path: str) -> Optional[dict]:
    with get_conn() as conn:
        row = conn.execute(
            "SELECT * FROM runtime_import_state WHERE source_path=?",
            (source_path,),
        ).fetchone()
        return dict(row) if row else None


def upsert_runtime_import_state(
    *,
    source_path: str,
    last_line_processed: int,
    last_status: str,
    last_error: str | None = None,
) -> None:
    now = utc_now()
    with get_conn() as conn:
        existing = conn.execute(
            "SELECT source_path FROM runtime_import_state WHERE source_path=?",
            (source_path,),
        ).fetchone()
        if existing:
            conn.execute(
                """
                UPDATE runtime_import_state
                SET last_line_processed=?, last_imported_at=?, last_status=?, last_error=?, updated_at=?
                WHERE source_path=?
                """,
                (last_line_processed, now, last_status, last_error, now, source_path),
            )
        else:
            conn.execute(
                """
                INSERT INTO runtime_import_state (
                    source_path, last_line_processed, last_imported_at, last_status, last_error, created_at, updated_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?)
                """,
                (source_path, last_line_processed, now, last_status, last_error, now, now),
            )


def list_runtime_import_states() -> list[dict]:
    with get_conn() as conn:
        rows = conn.execute(
            "SELECT * FROM runtime_import_state ORDER BY source_path ASC"
        ).fetchall()
        return [dict(r) for r in rows]


def record_maintenance_job_run(job_name: str, status: str, summary: dict[str, Any]) -> None:
    with get_conn() as conn:
        conn.execute(
            """
            INSERT INTO maintenance_job_runs (job_name, status, summary_json, created_at)
            VALUES (?, ?, ?, ?)
            """,
            (job_name, status, json.dumps(summary, ensure_ascii=True, sort_keys=True), utc_now()),
        )


def get_last_maintenance_job_run(job_name: str) -> Optional[dict]:
    with get_conn() as conn:
        row = conn.execute(
            "SELECT * FROM maintenance_job_runs WHERE job_name=? ORDER BY id DESC LIMIT 1",
            (job_name,),
        ).fetchone()
        return dict(row) if row else None


def list_events(run_id: str) -> list[dict]:
    with get_conn() as conn:
        rows = conn.execute(
            "SELECT * FROM events WHERE run_id=? ORDER BY id ASC",
            (run_id,),
        ).fetchall()
        return [dict(r) for r in rows]


def get_last_event(run_id: str, event_type: str) -> Optional[dict]:
    with get_conn() as conn:
        row = conn.execute(
            "SELECT * FROM events WHERE run_id=? AND event_type=? ORDER BY id DESC LIMIT 1",
            (run_id, event_type),
        ).fetchone()
        return dict(row) if row else None


def insert_approval(
    approval_id: str,
    run_id: str,
    reason: str,
    summary: dict,
    status: str = "PENDING",
    telegram_message_id: Optional[str] = None,
) -> None:
    with get_conn() as conn:
        conn.execute(
            """
            INSERT INTO approvals (id, run_id, reason, summary_json, status, telegram_message_id, created_at)
            VALUES (?, ?, ?, ?, ?, ?, ?)
            """,
            (approval_id, run_id, reason, json.dumps(summary), status, telegram_message_id, utc_now()),
        )


def get_approval(approval_id: str) -> Optional[dict]:
    with get_conn() as conn:
        row = conn.execute("SELECT * FROM approvals WHERE id=?", (approval_id,)).fetchone()
        return dict(row) if row else None


def get_pending_approval_for_run(run_id: str) -> Optional[dict]:
    with get_conn() as conn:
        row = conn.execute(
            "SELECT * FROM approvals WHERE run_id=? AND status='PENDING' ORDER BY created_at DESC LIMIT 1",
            (run_id,),
        ).fetchone()
        return dict(row) if row else None


def resolve_approval(
    approval_id: str,
    decision: str,
    decision_details: str = "",
    status: str = "RESOLVED",
) -> None:
    with get_conn() as conn:
        conn.execute(
            """
            UPDATE approvals
            SET status=?, decision=?, decision_details=?, resolved_at=?
            WHERE id=?
            """,
            (status, decision, decision_details, utc_now(), approval_id),
        )
