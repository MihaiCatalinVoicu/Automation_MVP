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
