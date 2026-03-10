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
