from __future__ import annotations

import json
import os
import sqlite3
import time
from contextlib import contextmanager
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable, Optional

DB_PATH = os.getenv("DB_PATH", "./data/orchestrator.db")
SQLITE_TIMEOUT_SECONDS = 10
DEBUG_LOG_PATH = Path("debug-0fff85.log")


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _debug_log(hypothesis_id: str, location: str, message: str, data: dict[str, Any]) -> None:
    payload = {
        "sessionId": "0fff85",
        "runId": "investigate_execution_spec_inheritance",
        "hypothesisId": hypothesis_id,
        "location": location,
        "message": message,
        "data": data,
        "timestamp": int(time.time() * 1000),
    }
    try:
        with DEBUG_LOG_PATH.open("a", encoding="utf-8") as fh:
            fh.write(json.dumps(payload, ensure_ascii=True) + "\n")
    except Exception:
        pass


def _connect() -> sqlite3.Connection:
    os.makedirs(os.path.dirname(DB_PATH) or ".", exist_ok=True)
    conn = sqlite3.connect(DB_PATH, timeout=SQLITE_TIMEOUT_SECONDS, check_same_thread=False)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL;")
    conn.execute("PRAGMA foreign_keys=ON;")
    conn.execute("PRAGMA busy_timeout=10000;")
    return conn


def _ensure_column(conn: sqlite3.Connection, table_name: str, column_name: str, definition: str) -> None:
    rows = conn.execute(f"PRAGMA table_info({table_name})").fetchall()
    existing = {str(r["name"]) for r in rows}
    if column_name in existing:
        return
    conn.execute(f"ALTER TABLE {table_name} ADD COLUMN {column_name} {definition}")


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

            CREATE TABLE IF NOT EXISTS edge_search_runtime_state (
                state_key TEXT PRIMARY KEY,
                mode TEXT NOT NULL,
                status TEXT NOT NULL,
                freeze_reason TEXT,
                health_json TEXT NOT NULL DEFAULT '{}',
                review_json TEXT NOT NULL DEFAULT '{}',
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL
            );

            CREATE TABLE IF NOT EXISTS edge_search_trigger_reviews (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                trigger_name TEXT NOT NULL,
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
            CREATE INDEX IF NOT EXISTS idx_edge_search_trigger_reviews_name ON edge_search_trigger_reviews(trigger_name, created_at);

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

            -- -------------------------------------------------------
            -- Edge Search Orchestrator tables
            -- -------------------------------------------------------

            CREATE TABLE IF NOT EXISTS search_cases (
                case_id TEXT PRIMARY KEY,
                case_type TEXT NOT NULL,
                title TEXT NOT NULL,
                idempotency_key TEXT UNIQUE,
                status TEXT NOT NULL,
                stage TEXT NOT NULL,
                priority TEXT NOT NULL DEFAULT 'medium',
                repo_scope TEXT NOT NULL,
                market TEXT NOT NULL,
                venue TEXT,
                instrument_scope TEXT,
                universe_id TEXT,
                timeframe TEXT,
                strategy_id TEXT,
                canonical_strategy_ref TEXT,
                registry_binding_status TEXT NOT NULL DEFAULT 'unbound',
                family TEXT NOT NULL,
                variant_seed_id TEXT,
                profile_id TEXT,
                hypothesis TEXT NOT NULL,
                objective_type TEXT NOT NULL,
                objective_metric TEXT,
                objective_threshold REAL,
                planner_mode TEXT,
                planner_agent TEXT,
                reviewer_agent TEXT,
                created_from TEXT NOT NULL,
                source_ref TEXT,
                search_budget_json TEXT NOT NULL,
                risk_budget_json TEXT NOT NULL,
                tags_json TEXT NOT NULL DEFAULT '[]',
                current_hypothesis_version INTEGER NOT NULL DEFAULT 1,
                latest_manifest_id TEXT,
                latest_verdict_id TEXT,
                final_outcome TEXT,
                opened_at TEXT NOT NULL,
                closed_at TEXT,
                owner TEXT NOT NULL,
                notes TEXT NOT NULL DEFAULT ''
            );

            CREATE TABLE IF NOT EXISTS experiment_manifests (
                manifest_id TEXT PRIMARY KEY,
                case_id TEXT NOT NULL,
                idempotency_key TEXT UNIQUE,
                manifest_version INTEGER NOT NULL DEFAULT 1,
                status TEXT NOT NULL,
                execution_status TEXT NOT NULL DEFAULT 'ready',
                claimed_by TEXT,
                claimed_at TEXT,
                last_run_id TEXT,
                attempt_count INTEGER NOT NULL DEFAULT 0,
                last_error TEXT,
                parent_manifest_id TEXT,
                derived_from_verdict_id TEXT,
                derivation_reason TEXT,
                repo TEXT NOT NULL,
                adapter_type TEXT NOT NULL,
                entrypoint TEXT NOT NULL,
                strategy_identity_json TEXT NOT NULL,
                run_context_template_json TEXT NOT NULL,
                dataset_spec_json TEXT NOT NULL,
                execution_spec_json TEXT NOT NULL,
                cost_model_json TEXT NOT NULL,
                gates_json TEXT NOT NULL,
                planner_hints_json TEXT NOT NULL DEFAULT '{}',
                artifacts_json TEXT NOT NULL DEFAULT '{}',
                param_diff_json TEXT NOT NULL DEFAULT '{}',
                created_at TEXT NOT NULL,
                created_by TEXT NOT NULL,
                approved_by TEXT,
                notes TEXT NOT NULL DEFAULT '',
                FOREIGN KEY(case_id) REFERENCES search_cases(case_id),
                FOREIGN KEY(parent_manifest_id) REFERENCES experiment_manifests(manifest_id),
                FOREIGN KEY(derived_from_verdict_id) REFERENCES edge_verdicts(verdict_id),
                UNIQUE(case_id, manifest_version)
            );

            CREATE TABLE IF NOT EXISTS edge_verdicts (
                verdict_id TEXT PRIMARY KEY,
                case_id TEXT NOT NULL,
                manifest_id TEXT NOT NULL,
                run_id TEXT,
                verdict_type TEXT NOT NULL,
                status TEXT NOT NULL,
                decision TEXT NOT NULL,
                decision_reason TEXT NOT NULL,
                confidence REAL,
                verdict_score REAL,
                experiment_score REAL,
                near_miss_score REAL,
                validation_level TEXT,
                batch_size INTEGER,
                config_fingerprint TEXT,
                metrics_snapshot_json TEXT NOT NULL,
                gate_results_json TEXT NOT NULL,
                artifacts_root TEXT,
                dominant_failure_mode TEXT,
                policy_selected TEXT,
                mutation_recommendation_json TEXT NOT NULL DEFAULT '{}',
                promotion_state_json TEXT NOT NULL DEFAULT '{}',
                next_action TEXT,
                next_action_payload_json TEXT NOT NULL DEFAULT '{}',
                postmortem_summary_json TEXT NOT NULL DEFAULT '{}',
                review_mode TEXT,
                reviewed_by TEXT,
                approved_by TEXT,
                created_at TEXT NOT NULL,
                FOREIGN KEY(case_id) REFERENCES search_cases(case_id),
                FOREIGN KEY(manifest_id) REFERENCES experiment_manifests(manifest_id)
            );

            CREATE TABLE IF NOT EXISTS case_events (
                event_id INTEGER PRIMARY KEY AUTOINCREMENT,
                case_id TEXT NOT NULL,
                manifest_id TEXT,
                verdict_id TEXT,
                event_type TEXT NOT NULL,
                payload_json TEXT NOT NULL DEFAULT '{}',
                created_at TEXT NOT NULL,
                FOREIGN KEY(case_id) REFERENCES search_cases(case_id),
                FOREIGN KEY(manifest_id) REFERENCES experiment_manifests(manifest_id),
                FOREIGN KEY(verdict_id) REFERENCES edge_verdicts(verdict_id)
            );

            CREATE TABLE IF NOT EXISTS telegram_decisions (
                approval_id TEXT PRIMARY KEY,
                case_id TEXT NOT NULL,
                manifest_id TEXT,
                run_id TEXT,
                decision_scope TEXT NOT NULL DEFAULT 'research_case',
                action TEXT NOT NULL,
                actor TEXT NOT NULL,
                message_id TEXT,
                payload_json TEXT NOT NULL DEFAULT '{}',
                created_at TEXT NOT NULL,
                FOREIGN KEY(case_id) REFERENCES search_cases(case_id),
                FOREIGN KEY(manifest_id) REFERENCES experiment_manifests(manifest_id)
            );

            CREATE TABLE IF NOT EXISTS family_budget_state (
                family_id TEXT PRIMARY KEY,
                status TEXT NOT NULL DEFAULT 'active',
                priority INTEGER NOT NULL DEFAULT 50,
                maturity TEXT NOT NULL DEFAULT 'experimental',
                family_score REAL,
                near_miss_rate REAL,
                mutation_improvement_rate REAL,
                robustness_survival_rate REAL,
                dead_manifest_penalty REAL,
                active_cases_count INTEGER NOT NULL DEFAULT 0,
                total_cases_count INTEGER NOT NULL DEFAULT 0,
                ready_manifest_count INTEGER NOT NULL DEFAULT 0,
                running_manifest_count INTEGER NOT NULL DEFAULT 0,
                completed_manifest_count INTEGER NOT NULL DEFAULT 0,
                dead_manifest_count INTEGER NOT NULL DEFAULT 0,
                latest_near_miss_score REAL,
                recommended_action TEXT,
                budget_state_json TEXT NOT NULL DEFAULT '{}',
                motifs_json TEXT NOT NULL DEFAULT '{}',
                updated_at TEXT NOT NULL
            );

            CREATE TABLE IF NOT EXISTS family_registry (
                family_id TEXT PRIMARY KEY,
                generator_type TEXT NOT NULL,
                strategy_id TEXT NOT NULL,
                setup_name TEXT,
                data_requirements_json TEXT NOT NULL DEFAULT '[]',
                allowed_validation_levels_json TEXT NOT NULL DEFAULT '[]',
                batch_defaults_json TEXT NOT NULL DEFAULT '{}',
                status TEXT NOT NULL DEFAULT 'experimental',
                priority INTEGER NOT NULL DEFAULT 50,
                maturity TEXT NOT NULL DEFAULT 'experimental',
                repo TEXT NOT NULL DEFAULT 'crypto-bot',
                notes TEXT NOT NULL DEFAULT '',
                updated_at TEXT NOT NULL
            );

            CREATE INDEX IF NOT EXISTS idx_search_cases_status ON search_cases(status);
            CREATE INDEX IF NOT EXISTS idx_search_cases_family ON search_cases(family);
            CREATE INDEX IF NOT EXISTS idx_search_cases_strategy_id ON search_cases(strategy_id);
            CREATE INDEX IF NOT EXISTS idx_search_cases_repo_scope ON search_cases(repo_scope);
            CREATE INDEX IF NOT EXISTS idx_search_cases_stage ON search_cases(stage);
            CREATE INDEX IF NOT EXISTS idx_experiment_manifests_case_id ON experiment_manifests(case_id);
            CREATE INDEX IF NOT EXISTS idx_experiment_manifests_status ON experiment_manifests(status);
            CREATE INDEX IF NOT EXISTS idx_experiment_manifests_adapter_type ON experiment_manifests(adapter_type);
            CREATE INDEX IF NOT EXISTS idx_edge_verdicts_case_id ON edge_verdicts(case_id);
            CREATE INDEX IF NOT EXISTS idx_edge_verdicts_manifest_id ON edge_verdicts(manifest_id);
            CREATE INDEX IF NOT EXISTS idx_edge_verdicts_decision ON edge_verdicts(decision);
            CREATE UNIQUE INDEX IF NOT EXISTS idx_edge_verdicts_one_final_per_case
            ON edge_verdicts(case_id)
            WHERE status='final';
            CREATE INDEX IF NOT EXISTS idx_case_events_case_id ON case_events(case_id);
            CREATE INDEX IF NOT EXISTS idx_telegram_decisions_case_id ON telegram_decisions(case_id);
            CREATE INDEX IF NOT EXISTS idx_family_budget_state_status ON family_budget_state(status, family_score);
            CREATE INDEX IF NOT EXISTS idx_family_registry_status ON family_registry(status, priority);

            -- Convenience views for edge search queries

            CREATE VIEW IF NOT EXISTS active_cases_v AS
            SELECT
                sc.case_id, sc.title, sc.status, sc.stage, sc.priority,
                sc.family, sc.strategy_id, sc.hypothesis, sc.market,
                sc.owner, sc.opened_at,
                ev.decision AS latest_decision,
                ev.decision_reason AS latest_reason,
                ev.created_at AS latest_verdict_at
            FROM search_cases sc
            LEFT JOIN edge_verdicts ev ON ev.verdict_id = sc.latest_verdict_id
            WHERE sc.status IN ('proposed', 'approved', 'active', 'on_hold');

            CREATE VIEW IF NOT EXISTS promotion_candidates_v AS
            SELECT
                ev.verdict_id, ev.case_id, ev.manifest_id, ev.decision,
                ev.metrics_snapshot_json, ev.gate_results_json,
                ev.promotion_state_json, ev.created_at,
                sc.title, sc.family, sc.strategy_id, sc.market
            FROM edge_verdicts ev
            JOIN search_cases sc ON sc.case_id = ev.case_id
            WHERE ev.decision = 'PROMOTE_TO_PAPER'
              AND ev.status = 'final'
              AND sc.latest_verdict_id = ev.verdict_id
              AND sc.status NOT IN ('done', 'killed', 'archived')
              AND COALESCE(json_extract(ev.gate_results_json, '$.min_trades_pass'), 0) = 1
              AND COALESCE(json_extract(ev.gate_results_json, '$.cost_adjusted_edge_pass'), 0) = 1
              AND COALESCE(json_extract(ev.gate_results_json, '$.walkforward_pass'), 0) = 1
              AND COALESCE(json_extract(ev.gate_results_json, '$.leakage_check_pass'), 0) = 1;
            """
        )
        _ensure_column(conn, "experiment_manifests", "execution_status", "TEXT NOT NULL DEFAULT 'ready'")
        _ensure_column(conn, "experiment_manifests", "claimed_by", "TEXT")
        _ensure_column(conn, "experiment_manifests", "claimed_at", "TEXT")
        _ensure_column(conn, "experiment_manifests", "last_run_id", "TEXT")
        _ensure_column(conn, "experiment_manifests", "attempt_count", "INTEGER NOT NULL DEFAULT 0")
        _ensure_column(conn, "experiment_manifests", "last_error", "TEXT")
        _ensure_column(conn, "edge_verdicts", "experiment_score", "REAL")
        _ensure_column(conn, "edge_verdicts", "near_miss_score", "REAL")
        _ensure_column(conn, "edge_verdicts", "validation_level", "TEXT")
        _ensure_column(conn, "edge_verdicts", "batch_size", "INTEGER")
        _ensure_column(conn, "edge_verdicts", "config_fingerprint", "TEXT")
        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_edge_verdicts_near_miss_score ON edge_verdicts(near_miss_score)"
        )
        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_edge_verdicts_validation_level ON edge_verdicts(validation_level)"
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


def list_maintenance_job_runs(job_name: str, *, limit: int = 10) -> list[dict]:
    with get_conn() as conn:
        rows = conn.execute(
            "SELECT * FROM maintenance_job_runs WHERE job_name=? ORDER BY id DESC LIMIT ?",
            (job_name, max(1, int(limit))),
        ).fetchall()
    return [dict(row) for row in rows]


def get_edge_search_runtime_state(state_key: str = "global") -> Optional[dict]:
    with get_conn() as conn:
        row = conn.execute(
            "SELECT * FROM edge_search_runtime_state WHERE state_key=?",
            (state_key,),
        ).fetchone()
        return dict(row) if row else None


def upsert_edge_search_runtime_state(
    *,
    state_key: str = "global",
    mode: str,
    status: str,
    freeze_reason: str | None = None,
    health: dict[str, Any] | None = None,
    review: dict[str, Any] | None = None,
) -> None:
    now = utc_now()
    with get_conn() as conn:
        existing = conn.execute(
            "SELECT state_key FROM edge_search_runtime_state WHERE state_key=?",
            (state_key,),
        ).fetchone()
        payload = (
            mode,
            status,
            freeze_reason,
            json.dumps(health or {}, ensure_ascii=True, sort_keys=True),
            json.dumps(review or {}, ensure_ascii=True, sort_keys=True),
            now,
        )
        if existing:
            conn.execute(
                """
                UPDATE edge_search_runtime_state
                SET mode=?, status=?, freeze_reason=?, health_json=?, review_json=?, updated_at=?
                WHERE state_key=?
                """,
                (*payload, state_key),
            )
        else:
            conn.execute(
                """
                INSERT INTO edge_search_runtime_state (
                    state_key, mode, status, freeze_reason, health_json, review_json, created_at, updated_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    state_key,
                    mode,
                    status,
                    freeze_reason,
                    json.dumps(health or {}, ensure_ascii=True, sort_keys=True),
                    json.dumps(review or {}, ensure_ascii=True, sort_keys=True),
                    now,
                    now,
                ),
            )


def record_edge_search_trigger_review(trigger_name: str, status: str, summary: dict[str, Any]) -> None:
    with get_conn() as conn:
        conn.execute(
            """
            INSERT INTO edge_search_trigger_reviews (trigger_name, status, summary_json, created_at)
            VALUES (?, ?, ?, ?)
            """,
            (trigger_name, status, json.dumps(summary, ensure_ascii=True, sort_keys=True), utc_now()),
        )


def get_last_edge_search_trigger_review(trigger_name: str) -> Optional[dict]:
    with get_conn() as conn:
        row = conn.execute(
            "SELECT * FROM edge_search_trigger_reviews WHERE trigger_name=? ORDER BY id DESC LIMIT 1",
            (trigger_name,),
        ).fetchone()
        return dict(row) if row else None


def list_edge_search_trigger_reviews(*, limit: int = 25) -> list[dict]:
    with get_conn() as conn:
        rows = conn.execute(
            "SELECT * FROM edge_search_trigger_reviews ORDER BY id DESC LIMIT ?",
            (max(1, int(limit)),),
        ).fetchall()
    return [dict(row) for row in rows]


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


# -------------------------------------------------------
# Edge Search Orchestrator – CRUD helpers
# -------------------------------------------------------

_VALID_CASE_STATUSES = frozenset(
    {"proposed", "approved", "active", "on_hold", "blocked", "done", "killed", "archived"}
)
_VALID_CASE_STAGES = frozenset(
    {"idea_intake", "manifest_ready", "running", "awaiting_verdict", "promotion_review", "paper_candidate", "closed"}
)
_VALID_REGISTRY_BINDING_STATUSES = frozenset({"unbound", "provisional", "registered", "rejected"})
_VALID_VERDICT_DECISIONS = frozenset(
    {
        "REJECT_EDGE",
        "MUTATE_WITH_POLICY",
        "RETEST_OOS",
        "RUN_BIGGER_SAMPLE",
        "HOLD_FOR_MORE_DATA",
        "PROMOTE_TO_PAPER",
        "ARCHIVE_CASE",
        "ASK_PREMIUM_REVIEW",
    }
)
_TERMINAL_CASE_STATUSES = frozenset({"done", "killed", "archived"})
_TERMINAL_CASE_STAGES = frozenset({"closed"})
_ALLOWED_STAGE_TRANSITIONS = {
    "idea_intake": {"manifest_ready", "running", "awaiting_verdict", "closed"},
    "manifest_ready": {"running", "awaiting_verdict", "promotion_review", "paper_candidate", "closed"},
    "running": {"awaiting_verdict", "manifest_ready", "promotion_review", "closed"},
    "awaiting_verdict": {"manifest_ready", "promotion_review", "paper_candidate", "closed"},
    "promotion_review": {"manifest_ready", "paper_candidate", "closed"},
    "paper_candidate": {"promotion_review", "closed"},
    "closed": set(),
}
_VALID_ADAPTER_TYPES = frozenset(
    {
        "research_loop",
        "cohort_research",
        "baseline_backtest",
        "validation_battery",
        "policy_benchmark",
        "paper_replay",
        "stocks_baseline_eval",
    }
)
_VALID_MANIFEST_EXECUTION_STATUSES = frozenset(
    {"ready", "claimed", "running", "completed", "failed", "awaiting_decision", "cancelled", "dead"}
)


def _json_col(value: Any) -> str:
    if isinstance(value, str):
        return value
    return json.dumps(value, ensure_ascii=False, sort_keys=True)


def _ensure_allowed(value: str, allowed: set[str] | frozenset[str], field_name: str) -> None:
    if value not in allowed:
        raise ValueError(f"Invalid {field_name}: {value}")


def _ensure_case_transition_allowed(current_stage: str, next_stage: str, *, force_transition: bool = False) -> None:
    if current_stage == next_stage:
        return
    if force_transition:
        return
    allowed_targets = _ALLOWED_STAGE_TRANSITIONS.get(current_stage, set())
    if next_stage not in allowed_targets:
        raise ValueError(f"Invalid stage transition: {current_stage} -> {next_stage}")


def create_search_case(
    *,
    case_id: str,
    case_type: str,
    title: str,
    status: str,
    stage: str,
    family: str,
    hypothesis: str,
    objective_type: str,
    repo_scope: str,
    market: str,
    created_from: str,
    owner: str,
    search_budget: dict[str, Any],
    risk_budget: dict[str, Any],
    idempotency_key: str | None = None,
    priority: str = "medium",
    venue: str | None = None,
    instrument_scope: str | None = None,
    universe_id: str | None = None,
    timeframe: str | None = None,
    strategy_id: str | None = None,
    canonical_strategy_ref: str | None = None,
    registry_binding_status: str = "unbound",
    variant_seed_id: str | None = None,
    profile_id: str | None = None,
    objective_metric: str | None = None,
    objective_threshold: float | None = None,
    planner_mode: str | None = None,
    planner_agent: str | None = None,
    reviewer_agent: str | None = None,
    source_ref: str | None = None,
    tags: list[str] | None = None,
    notes: str = "",
) -> None:
    _ensure_allowed(status, _VALID_CASE_STATUSES, "search_cases.status")
    _ensure_allowed(stage, _VALID_CASE_STAGES, "search_cases.stage")
    _ensure_allowed(registry_binding_status, _VALID_REGISTRY_BINDING_STATUSES, "search_cases.registry_binding_status")
    now = utc_now()
    with get_conn() as conn:
        conn.execute(
            """
            INSERT INTO search_cases (
                case_id, case_type, title, idempotency_key, status, stage, priority,
                repo_scope, market, venue, instrument_scope, universe_id, timeframe,
                strategy_id, canonical_strategy_ref, registry_binding_status, family, variant_seed_id, profile_id,
                hypothesis, objective_type, objective_metric, objective_threshold,
                planner_mode, planner_agent, reviewer_agent,
                created_from, source_ref,
                search_budget_json, risk_budget_json, tags_json,
                current_hypothesis_version, latest_manifest_id, latest_verdict_id,
                final_outcome, opened_at, closed_at, owner, notes
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                case_id, case_type, title, idempotency_key, status, stage, priority,
                repo_scope, market, venue, instrument_scope, universe_id, timeframe,
                strategy_id, canonical_strategy_ref, registry_binding_status, family, variant_seed_id, profile_id,
                hypothesis, objective_type, objective_metric, objective_threshold,
                planner_mode, planner_agent, reviewer_agent,
                created_from, source_ref,
                _json_col(search_budget),
                _json_col(risk_budget),
                _json_col(tags or []),
                1, None, None,
                None, now, None, owner, notes,
            ),
        )


def get_search_case(case_id: str) -> Optional[dict]:
    with get_conn() as conn:
        row = conn.execute("SELECT * FROM search_cases WHERE case_id=?", (case_id,)).fetchone()
        return dict(row) if row else None


def list_search_cases(
    *,
    status: str | None = None,
    family: str | None = None,
    repo_scope: str | None = None,
    market: str | None = None,
    stage: str | None = None,
) -> list[dict]:
    where: list[str] = []
    params: list[Any] = []
    if status:
        where.append("status=?")
        params.append(status)
    if family:
        where.append("family=?")
        params.append(family)
    if repo_scope:
        where.append("repo_scope=?")
        params.append(repo_scope)
    if market:
        where.append("market=?")
        params.append(market)
    if stage:
        where.append("stage=?")
        params.append(stage)
    sql = "SELECT * FROM search_cases"
    if where:
        sql += " WHERE " + " AND ".join(where)
    sql += " ORDER BY opened_at DESC"
    with get_conn() as conn:
        return [dict(r) for r in conn.execute(sql, params).fetchall()]


def update_search_case(case_id: str, *, force_transition: bool = False, **updates: Any) -> None:
    allowed = {
        "status", "stage", "priority", "latest_manifest_id", "latest_verdict_id",
        "final_outcome", "closed_at", "current_hypothesis_version", "notes",
        "strategy_id", "canonical_strategy_ref", "registry_binding_status", "variant_seed_id", "profile_id",
    }
    current = get_search_case(case_id)
    if not current:
        raise KeyError(f"Search case not found: {case_id}")
    current_status = str(current.get("status") or "")
    current_stage = str(current.get("stage") or "")
    if current_status in _TERMINAL_CASE_STATUSES and not force_transition:
        mutable_terminal_fields = {"latest_manifest_id", "latest_verdict_id", "notes", "closed_at"}
        requested = set(updates.keys()) & allowed
        if requested - mutable_terminal_fields:
            raise ValueError(f"Cannot update terminal case status={current_status} without force_transition=True")
    if "status" in updates and updates["status"] is not None:
        _ensure_allowed(str(updates["status"]), _VALID_CASE_STATUSES, "search_cases.status")
    if "stage" in updates and updates["stage"] is not None:
        next_stage = str(updates["stage"])
        _ensure_allowed(next_stage, _VALID_CASE_STAGES, "search_cases.stage")
        _ensure_case_transition_allowed(current_stage, next_stage, force_transition=force_transition)
    if "registry_binding_status" in updates and updates["registry_binding_status"] is not None:
        _ensure_allowed(
            str(updates["registry_binding_status"]),
            _VALID_REGISTRY_BINDING_STATUSES,
            "search_cases.registry_binding_status",
        )
    cols: list[str] = []
    vals: list[Any] = []
    for key, value in updates.items():
        if key not in allowed:
            continue
        cols.append(f"{key}=?")
        vals.append(value)
    if not cols:
        return
    vals.append(case_id)
    with get_conn() as conn:
        conn.execute(f"UPDATE search_cases SET {', '.join(cols)} WHERE case_id=?", vals)


def missing_required_execution_spec(
    execution_spec: dict[str, Any],
    required: tuple[str, ...] = ("family", "config_path", "recipe_path", "repo_root"),
) -> list[str]:
    return [k for k in required if not str((execution_spec or {}).get(k) or "").strip()]


def ensure_required_execution_spec(
    execution_spec: dict[str, Any],
    *,
    context: str = "research_loop",
    required: tuple[str, ...] = ("family", "config_path", "recipe_path", "repo_root"),
) -> None:
    missing = missing_required_execution_spec(execution_spec, required=required)
    if missing:
        raise ValueError(
            f"Missing required execution_spec for {context}: " + "/".join(missing)
        )


def create_experiment_manifest(
    *,
    manifest_id: str,
    case_id: str,
    status: str,
    repo: str,
    adapter_type: str,
    entrypoint: str,
    strategy_identity: dict[str, Any],
    run_context_template: dict[str, Any],
    dataset_spec: dict[str, Any],
    execution_spec: dict[str, Any],
    cost_model: dict[str, Any],
    gates: dict[str, Any],
    created_by: str,
    idempotency_key: str | None = None,
    manifest_version: int = 1,
    parent_manifest_id: str | None = None,
    derived_from_verdict_id: str | None = None,
    derivation_reason: str | None = None,
    param_diff: dict[str, Any] | None = None,
    planner_hints: dict[str, Any] | None = None,
    artifacts: dict[str, Any] | None = None,
    approved_by: str | None = None,
    notes: str = "",
    force_stage_transition: bool = False,
) -> None:
    _ensure_allowed(adapter_type, _VALID_ADAPTER_TYPES, "experiment_manifests.adapter_type")
    # region agent log
    _debug_log(
        "H2_execution_spec_lost_before_insert",
        "db.py:create_experiment_manifest",
        "create_manifest_input",
        {
            "manifest_id": manifest_id,
            "parent_manifest_id": parent_manifest_id or "",
            "derived_from_verdict_id": derived_from_verdict_id or "",
            "adapter_type": adapter_type,
            "execution_spec_keys": sorted(list((execution_spec or {}).keys())),
            "missing_required": [
                k for k in ("family", "config_path", "recipe_path", "repo_root")
                if not str((execution_spec or {}).get(k) or "").strip()
            ],
        },
    )
    # endregion
    if adapter_type == "research_loop":
        ensure_required_execution_spec(execution_spec, context="research_loop")
    case = get_search_case(case_id)
    if not case:
        raise KeyError(f"Search case not found: {case_id}")
    if case["status"] in _TERMINAL_CASE_STATUSES:
        raise ValueError(f"Cannot create manifest for terminal case status={case['status']}")
    execution_status = "ready"
    status_norm = str(status or "").lower()
    if status_norm in {"completed", "done", "success"}:
        execution_status = "completed"
    elif status_norm in {"failed", "error"}:
        execution_status = "failed"
    elif status_norm in {"cancelled", "canceled"}:
        execution_status = "cancelled"
    now = utc_now()
    with get_conn() as conn:
        conn.execute(
            """
            INSERT INTO experiment_manifests (
                manifest_id, case_id, idempotency_key, manifest_version, status,
                execution_status, claimed_by, claimed_at, last_run_id, attempt_count, last_error,
                parent_manifest_id, derived_from_verdict_id, derivation_reason,
                repo, adapter_type, entrypoint,
                strategy_identity_json, run_context_template_json,
                dataset_spec_json, execution_spec_json,
                cost_model_json, gates_json,
                planner_hints_json, artifacts_json, param_diff_json,
                created_at, created_by, approved_by, notes
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                manifest_id, case_id, idempotency_key, manifest_version, status,
                execution_status,
                None, None, None, 0, None,
                parent_manifest_id, derived_from_verdict_id, derivation_reason,
                repo, adapter_type, entrypoint,
                _json_col(strategy_identity),
                _json_col(run_context_template),
                _json_col(dataset_spec),
                _json_col(execution_spec),
                _json_col(cost_model),
                _json_col(gates),
                _json_col(planner_hints or {}),
                _json_col(artifacts or {}),
                _json_col(param_diff or {}),
                now, created_by, approved_by, notes,
            ),
        )
    # region agent log
    _debug_log(
        "H3_execution_spec_stored_in_db",
        "db.py:create_experiment_manifest",
        "create_manifest_inserted",
        {
            "manifest_id": manifest_id,
            "adapter_type": adapter_type,
            "stored_missing_required": [
                k for k in ("family", "config_path", "recipe_path", "repo_root")
                if not str((execution_spec or {}).get(k) or "").strip()
            ],
        },
    )
    # endregion
    update_search_case(
        case_id,
        force_transition=force_stage_transition,
        latest_manifest_id=manifest_id,
        stage="manifest_ready",
        status="active",
    )


def get_experiment_manifest(manifest_id: str) -> Optional[dict]:
    with get_conn() as conn:
        row = conn.execute("SELECT * FROM experiment_manifests WHERE manifest_id=?", (manifest_id,)).fetchone()
        return dict(row) if row else None


def list_experiment_manifests(
    *,
    case_id: str | None = None,
    status: str | None = None,
    adapter_type: str | None = None,
    execution_status: str | None = None,
    derived_from_verdict_id: str | None = None,
) -> list[dict]:
    where: list[str] = []
    params: list[Any] = []
    if case_id:
        where.append("case_id=?")
        params.append(case_id)
    if status:
        where.append("status=?")
        params.append(status)
    if adapter_type:
        where.append("adapter_type=?")
        params.append(adapter_type)
    if execution_status:
        where.append("execution_status=?")
        params.append(execution_status)
    if derived_from_verdict_id:
        where.append("derived_from_verdict_id=?")
        params.append(derived_from_verdict_id)
    sql = "SELECT * FROM experiment_manifests"
    if where:
        sql += " WHERE " + " AND ".join(where)
    sql += " ORDER BY created_at DESC"
    with get_conn() as conn:
        return [dict(r) for r in conn.execute(sql, params).fetchall()]


def update_experiment_manifest_status(manifest_id: str, status: str) -> None:
    with get_conn() as conn:
        conn.execute(
            "UPDATE experiment_manifests SET status=? WHERE manifest_id=?",
            (status, manifest_id),
        )


def claim_manifest(worker_id: str) -> Optional[dict]:
    now = utc_now()
    with get_conn() as conn:
        conn.execute("BEGIN IMMEDIATE")
        row = conn.execute(
            """
            SELECT *
            FROM experiment_manifests
            WHERE execution_status='ready'
            ORDER BY created_at ASC
            LIMIT 1
            """
        ).fetchone()
        if not row:
            return None
        manifest_id = str(row["manifest_id"])
        conn.execute(
            """
            UPDATE experiment_manifests
            SET execution_status='claimed', claimed_by=?, claimed_at=?, attempt_count=attempt_count+1
            WHERE manifest_id=? AND execution_status='ready'
            """,
            (worker_id, now, manifest_id),
        )
        claimed = conn.execute(
            "SELECT * FROM experiment_manifests WHERE manifest_id=?",
            (manifest_id,),
        ).fetchone()
        if not claimed or claimed["execution_status"] != "claimed" or claimed["claimed_by"] != worker_id:
            return None
        return dict(claimed)


def set_manifest_execution_state(
    manifest_id: str,
    execution_status: str,
    *,
    claimed_by: str | None = None,
    last_run_id: str | None = None,
    last_error: str | None = None,
) -> None:
    _ensure_allowed(execution_status, _VALID_MANIFEST_EXECUTION_STATUSES, "experiment_manifests.execution_status")
    with get_conn() as conn:
        conn.execute(
            """
            UPDATE experiment_manifests
            SET execution_status=?,
                claimed_by=COALESCE(?, claimed_by),
                claimed_at=CASE WHEN ?='claimed' THEN COALESCE(claimed_at, ?) ELSE claimed_at END,
                last_run_id=COALESCE(?, last_run_id),
                last_error=?
            WHERE manifest_id=?
            """,
            (
                execution_status,
                claimed_by,
                execution_status,
                utc_now(),
                last_run_id,
                last_error,
                manifest_id,
            ),
        )


def set_manifest_failed_with_retry_policy(
    manifest_id: str,
    *,
    last_error: str | None = None,
    max_retries: int = 3,
) -> tuple[str, int]:
    """Set failed/dead based on attempt_count and retry policy."""
    max_retries = max(1, int(max_retries))
    with get_conn() as conn:
        row = conn.execute(
            "SELECT attempt_count FROM experiment_manifests WHERE manifest_id=?",
            (manifest_id,),
        ).fetchone()
        if not row:
            raise KeyError(f"Manifest not found: {manifest_id}")
        attempt_count = int(row["attempt_count"] or 0)
        next_status = "dead" if attempt_count >= max_retries else "failed"
        conn.execute(
            """
            UPDATE experiment_manifests
            SET execution_status=?, last_error=?
            WHERE manifest_id=?
            """,
            (next_status, last_error, manifest_id),
        )
    return next_status, attempt_count


def list_ready_manifests(limit: int = 50) -> list[dict]:
    with get_conn() as conn:
        rows = conn.execute(
            """
            SELECT * FROM experiment_manifests
            WHERE execution_status='ready'
            ORDER BY created_at ASC
            LIMIT ?
            """,
            (max(1, int(limit)),),
        ).fetchall()
        return [dict(r) for r in rows]


def count_manifests_by_execution_status(
    execution_statuses: list[str] | tuple[str, ...],
    *,
    family: str | None = None,
    validation_level: str | None = None,
    derived_from_verdict_id: str | None = None,
) -> int:
    statuses = [str(item).strip() for item in execution_statuses if str(item).strip()]
    if not statuses:
        return 0
    placeholders = ", ".join("?" for _ in statuses)
    sql = f"SELECT COUNT(1) AS n FROM experiment_manifests WHERE execution_status IN ({placeholders})"
    params: list[Any] = list(statuses)
    if family:
        sql += " AND json_extract(strategy_identity_json, '$.family')=?"
        params.append(family)
    if validation_level:
        sql += " AND COALESCE(json_extract(execution_spec_json, '$.validation_level'), 'cheap')=?"
        params.append(validation_level)
    if derived_from_verdict_id:
        sql += " AND derived_from_verdict_id=?"
        params.append(derived_from_verdict_id)
    with get_conn() as conn:
        row = conn.execute(sql, params).fetchone()
        return int(row["n"] if row else 0)


def count_pending_manifests(
    *,
    family: str | None = None,
    validation_level: str | None = None,
    derived_from_verdict_id: str | None = None,
) -> int:
    return count_manifests_by_execution_status(
        ("ready", "claimed", "running"),
        family=family,
        validation_level=validation_level,
        derived_from_verdict_id=derived_from_verdict_id,
    )


def count_active_manifest_workers(*, stale_after_minutes: int = 180) -> int:
    cutoff_ts = datetime.now(timezone.utc).timestamp() - max(1, int(stale_after_minutes)) * 60
    with get_conn() as conn:
        rows = conn.execute(
            """
            SELECT DISTINCT claimed_by, claimed_at
            FROM experiment_manifests
            WHERE execution_status IN ('claimed', 'running')
              AND claimed_by IS NOT NULL
              AND claimed_by != ''
            """
        ).fetchall()
    active: set[str] = set()
    for row in rows:
        claimed_by = str(row["claimed_by"] or "").strip()
        claimed_at = str(row["claimed_at"] or "").strip()
        if not claimed_by:
            continue
        if not claimed_at:
            active.add(claimed_by)
            continue
        try:
            ts = datetime.fromisoformat(claimed_at.replace("Z", "+00:00")).timestamp()
        except ValueError:
            active.add(claimed_by)
            continue
        if ts >= cutoff_ts:
            active.add(claimed_by)
    return len(active)


def manifest_config_fingerprint_exists(
    config_fingerprint: str,
    *,
    include_completed_verdicts: bool = True,
    include_pending_manifests: bool = True,
) -> bool:
    fp = str(config_fingerprint or "").strip()
    if not fp:
        return False
    with get_conn() as conn:
        if include_completed_verdicts:
            verdict_row = conn.execute(
                """
                SELECT verdict_id
                FROM edge_verdicts
                WHERE config_fingerprint=?
                LIMIT 1
                """,
                (fp,),
            ).fetchone()
            if verdict_row:
                return True
        if include_pending_manifests:
            manifest_row = conn.execute(
                """
                SELECT manifest_id
                FROM experiment_manifests
                WHERE json_extract(planner_hints_json, '$.config_fingerprint')=?
                LIMIT 1
                """,
                (fp,),
            ).fetchone()
            if manifest_row:
                return True
    return False


def count_manifests_created_since(
    since_ts: str,
    *,
    case_id: str | None = None,
    family: str | None = None,
    validation_level: str | None = None,
) -> int:
    sql = "SELECT COUNT(1) AS n FROM experiment_manifests WHERE created_at>=?"
    params: list[Any] = [since_ts]
    if case_id:
        sql += " AND case_id=?"
        params.append(case_id)
    if family:
        sql += " AND json_extract(strategy_identity_json, '$.family')=?"
        params.append(family)
    if validation_level:
        sql += " AND COALESCE(json_extract(execution_spec_json, '$.validation_level'), 'cheap')=?"
        params.append(validation_level)
    with get_conn() as conn:
        row = conn.execute(sql, params).fetchone()
        return int(row["n"] if row else 0)


def create_edge_verdict(
    *,
    verdict_id: str,
    case_id: str,
    manifest_id: str,
    verdict_type: str,
    status: str,
    decision: str,
    decision_reason: str,
    metrics_snapshot: dict[str, Any],
    gate_results: dict[str, Any],
    run_id: str | None = None,
    confidence: float | None = None,
    verdict_score: float | None = None,
    experiment_score: float | None = None,
    near_miss_score: float | None = None,
    validation_level: str | None = None,
    batch_size: int | None = None,
    config_fingerprint: str | None = None,
    artifacts_root: str | None = None,
    dominant_failure_mode: str | None = None,
    policy_selected: str | None = None,
    mutation_recommendation: dict[str, Any] | None = None,
    promotion_state: dict[str, Any] | None = None,
    next_action: str | None = None,
    next_action_payload: dict[str, Any] | None = None,
    postmortem_summary: dict[str, Any] | None = None,
    review_mode: str | None = None,
    reviewed_by: str | None = None,
    approved_by: str | None = None,
    force_transition: bool = False,
) -> None:
    _ensure_allowed(decision, _VALID_VERDICT_DECISIONS, "edge_verdicts.decision")
    case = get_search_case(case_id)
    if not case:
        raise KeyError(f"Search case not found: {case_id}")
    current_stage = str(case.get("stage") or "")
    current_status = str(case.get("status") or "")
    if current_status in _TERMINAL_CASE_STATUSES and not force_transition:
        raise ValueError(f"Cannot add verdict to terminal case status={current_status}")
    now = utc_now()
    with get_conn() as conn:
        if status == "final":
            conn.execute(
                "UPDATE edge_verdicts SET status='superseded' WHERE case_id=? AND status='final'",
                (case_id,),
            )
        conn.execute(
            """
            INSERT INTO edge_verdicts (
                verdict_id, case_id, manifest_id, run_id,
                verdict_type, status, decision, decision_reason, confidence, verdict_score,
                experiment_score, near_miss_score, validation_level, batch_size, config_fingerprint,
                metrics_snapshot_json, gate_results_json, artifacts_root,
                dominant_failure_mode, policy_selected,
                mutation_recommendation_json, promotion_state_json,
                next_action, next_action_payload_json,
                postmortem_summary_json,
                review_mode, reviewed_by, approved_by, created_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                verdict_id, case_id, manifest_id, run_id,
                verdict_type, status, decision, decision_reason, confidence, verdict_score,
                experiment_score, near_miss_score, validation_level, batch_size, config_fingerprint,
                _json_col(metrics_snapshot),
                _json_col(gate_results),
                artifacts_root,
                dominant_failure_mode, policy_selected,
                _json_col(mutation_recommendation or {}),
                _json_col(promotion_state or {}),
                next_action,
                _json_col(next_action_payload or {}),
                _json_col(postmortem_summary or {}),
                review_mode, reviewed_by, approved_by, now,
            ),
        )
    stage_updates: dict[str, Any] = {"latest_verdict_id": verdict_id}
    if decision in ("MUTATE_WITH_POLICY", "RETEST_OOS", "RUN_BIGGER_SAMPLE"):
        stage_updates["stage"] = "manifest_ready"
        stage_updates["status"] = "active"
    elif decision == "PROMOTE_TO_PAPER":
        stage_updates["stage"] = "paper_candidate"
        stage_updates["status"] = "active"
    elif decision == "HOLD_FOR_MORE_DATA":
        stage_updates["stage"] = "awaiting_verdict"
        stage_updates["status"] = "on_hold"
    elif decision == "ASK_PREMIUM_REVIEW":
        stage_updates["stage"] = "promotion_review"
    elif decision in ("REJECT_EDGE", "ARCHIVE_CASE"):
        stage_updates.update(stage="closed", status="done", final_outcome=decision, closed_at=now)
    next_stage = str(stage_updates.get("stage", current_stage))
    _ensure_case_transition_allowed(current_stage, next_stage, force_transition=force_transition)
    update_search_case(case_id, force_transition=force_transition, **stage_updates)


def get_edge_verdict(verdict_id: str) -> Optional[dict]:
    with get_conn() as conn:
        row = conn.execute("SELECT * FROM edge_verdicts WHERE verdict_id=?", (verdict_id,)).fetchone()
        return dict(row) if row else None


def list_edge_verdicts(
    *,
    case_id: str | None = None,
    manifest_id: str | None = None,
    decision: str | None = None,
) -> list[dict]:
    where: list[str] = []
    params: list[Any] = []
    if case_id:
        where.append("case_id=?")
        params.append(case_id)
    if manifest_id:
        where.append("manifest_id=?")
        params.append(manifest_id)
    if decision:
        where.append("decision=?")
        params.append(decision)
    sql = "SELECT * FROM edge_verdicts"
    if where:
        sql += " WHERE " + " AND ".join(where)
    sql += " ORDER BY created_at DESC"
    with get_conn() as conn:
        return [dict(r) for r in conn.execute(sql, params).fetchall()]


def get_family_budget_state(family_id: str) -> Optional[dict]:
    with get_conn() as conn:
        row = conn.execute(
            "SELECT * FROM family_budget_state WHERE family_id=?",
            (family_id,),
        ).fetchone()
        return dict(row) if row else None


def list_family_budget_states() -> list[dict]:
    with get_conn() as conn:
        rows = conn.execute(
            "SELECT * FROM family_budget_state ORDER BY COALESCE(family_score, -1.0) DESC, family_id ASC"
        ).fetchall()
        return [dict(r) for r in rows]


def upsert_family_budget_state(
    *,
    family_id: str,
    status: str,
    priority: int,
    maturity: str,
    family_score: float | None,
    near_miss_rate: float | None,
    mutation_improvement_rate: float | None,
    robustness_survival_rate: float | None,
    dead_manifest_penalty: float | None,
    active_cases_count: int,
    total_cases_count: int,
    ready_manifest_count: int,
    running_manifest_count: int,
    completed_manifest_count: int,
    dead_manifest_count: int,
    latest_near_miss_score: float | None,
    recommended_action: str | None,
    budget_state: dict[str, Any] | None = None,
    motifs: dict[str, Any] | None = None,
) -> None:
    now = utc_now()
    with get_conn() as conn:
        conn.execute(
            """
            INSERT INTO family_budget_state (
                family_id, status, priority, maturity, family_score,
                near_miss_rate, mutation_improvement_rate, robustness_survival_rate, dead_manifest_penalty,
                active_cases_count, total_cases_count,
                ready_manifest_count, running_manifest_count, completed_manifest_count, dead_manifest_count,
                latest_near_miss_score, recommended_action,
                budget_state_json, motifs_json, updated_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(family_id) DO UPDATE SET
                status=excluded.status,
                priority=excluded.priority,
                maturity=excluded.maturity,
                family_score=excluded.family_score,
                near_miss_rate=excluded.near_miss_rate,
                mutation_improvement_rate=excluded.mutation_improvement_rate,
                robustness_survival_rate=excluded.robustness_survival_rate,
                dead_manifest_penalty=excluded.dead_manifest_penalty,
                active_cases_count=excluded.active_cases_count,
                total_cases_count=excluded.total_cases_count,
                ready_manifest_count=excluded.ready_manifest_count,
                running_manifest_count=excluded.running_manifest_count,
                completed_manifest_count=excluded.completed_manifest_count,
                dead_manifest_count=excluded.dead_manifest_count,
                latest_near_miss_score=excluded.latest_near_miss_score,
                recommended_action=excluded.recommended_action,
                budget_state_json=excluded.budget_state_json,
                motifs_json=excluded.motifs_json,
                updated_at=excluded.updated_at
            """,
            (
                family_id,
                status,
                priority,
                maturity,
                family_score,
                near_miss_rate,
                mutation_improvement_rate,
                robustness_survival_rate,
                dead_manifest_penalty,
                active_cases_count,
                total_cases_count,
                ready_manifest_count,
                running_manifest_count,
                completed_manifest_count,
                dead_manifest_count,
                latest_near_miss_score,
                recommended_action,
                _json_col(budget_state or {}),
                _json_col(motifs or {}),
                now,
            ),
        )


def upsert_family_registry_entry(
    *,
    family_id: str,
    generator_type: str,
    strategy_id: str,
    setup_name: str | None,
    data_requirements: list[str] | None,
    allowed_validation_levels: list[str] | None,
    batch_defaults: dict[str, Any] | None,
    status: str,
    priority: int,
    maturity: str,
    repo: str = "crypto-bot",
    notes: str = "",
) -> None:
    now = utc_now()
    with get_conn() as conn:
        conn.execute(
            """
            INSERT INTO family_registry (
                family_id, generator_type, strategy_id, setup_name,
                data_requirements_json, allowed_validation_levels_json, batch_defaults_json,
                status, priority, maturity, repo, notes, updated_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(family_id) DO UPDATE SET
                generator_type=excluded.generator_type,
                strategy_id=excluded.strategy_id,
                setup_name=excluded.setup_name,
                data_requirements_json=excluded.data_requirements_json,
                allowed_validation_levels_json=excluded.allowed_validation_levels_json,
                batch_defaults_json=excluded.batch_defaults_json,
                status=excluded.status,
                priority=excluded.priority,
                maturity=excluded.maturity,
                repo=excluded.repo,
                notes=excluded.notes,
                updated_at=excluded.updated_at
            """,
            (
                family_id,
                generator_type,
                strategy_id,
                setup_name,
                _json_col(data_requirements or []),
                _json_col(allowed_validation_levels or []),
                _json_col(batch_defaults or {}),
                status,
                priority,
                maturity,
                repo,
                notes,
                now,
            ),
        )


def list_family_registry(status: str | None = None) -> list[dict]:
    sql = "SELECT * FROM family_registry"
    params: list[Any] = []
    if status:
        sql += " WHERE status=?"
        params.append(status)
    sql += " ORDER BY priority DESC, family_id ASC"
    with get_conn() as conn:
        rows = conn.execute(sql, params).fetchall()
        return [dict(r) for r in rows]


def create_case_event(
    *,
    case_id: str,
    event_type: str,
    payload: dict[str, Any] | None = None,
    manifest_id: str | None = None,
    verdict_id: str | None = None,
) -> None:
    with get_conn() as conn:
        conn.execute(
            """
            INSERT INTO case_events (case_id, manifest_id, verdict_id, event_type, payload_json, created_at)
            VALUES (?, ?, ?, ?, ?, ?)
            """,
            (case_id, manifest_id, verdict_id, event_type, _json_col(payload or {}), utc_now()),
        )


def list_case_events(case_id: str) -> list[dict]:
    with get_conn() as conn:
        rows = conn.execute(
            "SELECT * FROM case_events WHERE case_id=? ORDER BY event_id ASC",
            (case_id,),
        ).fetchall()
        return [dict(r) for r in rows]


def case_event_exists(
    *,
    case_id: str,
    event_type: str,
    verdict_id: str | None = None,
    manifest_id: str | None = None,
) -> bool:
    where = ["case_id=?", "event_type=?"]
    params: list[Any] = [case_id, event_type]
    if verdict_id is not None:
        where.append("verdict_id=?")
        params.append(verdict_id)
    if manifest_id is not None:
        where.append("manifest_id=?")
        params.append(manifest_id)
    sql = "SELECT event_id FROM case_events WHERE " + " AND ".join(where) + " LIMIT 1"
    with get_conn() as conn:
        row = conn.execute(sql, params).fetchone()
        return row is not None


def create_telegram_decision(
    *,
    approval_id: str,
    case_id: str,
    action: str,
    actor: str,
    decision_scope: str = "research_case",
    manifest_id: str | None = None,
    run_id: str | None = None,
    message_id: str | None = None,
    payload: dict[str, Any] | None = None,
) -> None:
    with get_conn() as conn:
        conn.execute(
            """
            INSERT INTO telegram_decisions (
                approval_id, case_id, manifest_id, run_id,
                decision_scope, action, actor, message_id, payload_json, created_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                approval_id, case_id, manifest_id, run_id,
                decision_scope, action, actor, message_id,
                _json_col(payload or {}),
                utc_now(),
            ),
        )


def get_telegram_decision(approval_id: str) -> Optional[dict]:
    with get_conn() as conn:
        row = conn.execute(
            "SELECT * FROM telegram_decisions WHERE approval_id=?",
            (approval_id,),
        ).fetchone()
        return dict(row) if row else None


def list_telegram_decisions(case_id: str) -> list[dict]:
    with get_conn() as conn:
        rows = conn.execute(
            "SELECT * FROM telegram_decisions WHERE case_id=? ORDER BY created_at ASC",
            (case_id,),
        ).fetchall()
        return [dict(r) for r in rows]
