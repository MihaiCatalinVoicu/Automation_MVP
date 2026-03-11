from __future__ import annotations

import os
import socket
import time
from datetime import datetime, timezone
from pathlib import Path

from dotenv import load_dotenv

load_dotenv(Path(__file__).resolve().parent / ".env")

from db import claim_run, init_db, insert_event, list_ready_manifests

from runner import run_pipeline
from schedule_registry import materialize_due_runs, upsert_default_schedules
from shadow_recommendations import build_shadow_board
from strategy_lifecycle import run_due_reviews
from manifest_worker import process_one_manifest
from research_governance_scheduler import send_pending_research_governance_messages

WORKER_POLL_INTERVAL_SECONDS = int(os.getenv("WORKER_POLL_INTERVAL_SECONDS", "2"))
STRATEGY_REVIEW_INTERVAL_SECONDS = int(os.getenv("STRATEGY_REVIEW_INTERVAL_SECONDS", "0"))
RESEARCH_SCHEDULE_INTERVAL_SECONDS = int(os.getenv("RESEARCH_SCHEDULE_INTERVAL_SECONDS", "0"))
SHADOW_BOARD_INTERVAL_SECONDS = int(os.getenv("SHADOW_BOARD_INTERVAL_SECONDS", "0"))
RESEARCH_GOVERNANCE_RETRY_INTERVAL_SECONDS = int(os.getenv("RESEARCH_GOVERNANCE_RETRY_INTERVAL_SECONDS", "0"))
VERBOSE = os.getenv("WORKER_VERBOSE", "").lower() in ("1", "true", "yes")
HEARTBEAT_SECONDS = int(os.getenv("WORKER_HEARTBEAT_SECONDS", "60"))


def _ts() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")


def _log(msg: str) -> None:
    print(f"[{_ts()}] INFO {msg}", flush=True)


def main() -> None:
    init_db()
    upsert_default_schedules()
    worker_id = f"worker-{socket.gethostname()}-{os.getpid()}"
    _log(f"worker started worker_id={worker_id}")
    _log(
        "startup config "
        f"poll={WORKER_POLL_INTERVAL_SECONDS}s heartbeat={HEARTBEAT_SECONDS}s "
        f"schedule={RESEARCH_SCHEDULE_INTERVAL_SECONDS}s strategy_review={STRATEGY_REVIEW_INTERVAL_SECONDS}s "
        f"shadow={SHADOW_BOARD_INTERVAL_SECONDS}s governance_retry={RESEARCH_GOVERNANCE_RETRY_INTERVAL_SECONDS}s "
        f"db_path={os.getenv('DB_PATH', './data/orchestrator.db')} "
        f"telegram_enabled={'yes' if bool(os.getenv('TELEGRAM_BOT_TOKEN')) else 'no'} "
        f"verbose={'yes' if VERBOSE else 'no'}"
    )
    last_strategy_review_ts = 0.0
    last_schedule_check_ts = 0.0
    last_shadow_board_ts = 0.0
    last_research_governance_retry_ts = 0.0
    last_heartbeat_ts = 0.0

    while True:
        claimed = claim_run(worker_id)
        if not claimed:
            now_ts = time.time()
            if process_one_manifest(worker_id):
                if VERBOSE:
                    _log("processed one manifest from ready queue")
                continue
            if RESEARCH_SCHEDULE_INTERVAL_SECONDS > 0 and (time.time() - last_schedule_check_ts) >= RESEARCH_SCHEDULE_INTERVAL_SECONDS:
                try:
                    created = materialize_due_runs()
                    if created:
                        _log(f"materialized research runs count={len(created)}")
                except Exception as exc:
                    _log(f"research schedule materialization failed error={exc}")
                finally:
                    last_schedule_check_ts = time.time()
            if STRATEGY_REVIEW_INTERVAL_SECONDS > 0 and (time.time() - last_strategy_review_ts) >= STRATEGY_REVIEW_INTERVAL_SECONDS:
                try:
                    out_dir = Path("data") / "strategy_reviews" / "scheduled"
                    summary = run_due_reviews(output_dir=out_dir, review_kind="scheduled")
                    _log(f"scheduled strategy review reviewed={summary['review_count']}")
                except Exception as exc:
                    _log(f"scheduled strategy review failed error={exc}")
                finally:
                    last_strategy_review_ts = time.time()
            if SHADOW_BOARD_INTERVAL_SECONDS > 0 and (time.time() - last_shadow_board_ts) >= SHADOW_BOARD_INTERVAL_SECONDS:
                try:
                    out_dir = Path("data") / "shadow_recommendations"
                    board = build_shadow_board(out_dir)
                    _log(f"shadow board updated daily={len(board['daily_shadow_recommendations'])}")
                except Exception as exc:
                    _log(f"shadow board update failed error={exc}")
                finally:
                    last_shadow_board_ts = time.time()
            if (
                RESEARCH_GOVERNANCE_RETRY_INTERVAL_SECONDS > 0
                and (time.time() - last_research_governance_retry_ts) >= RESEARCH_GOVERNANCE_RETRY_INTERVAL_SECONDS
            ):
                try:
                    sent = send_pending_research_governance_messages(limit=20)
                    if sent:
                        _log(f"research governance retries sent={sent}")
                except Exception as exc:
                    _log(f"research governance retry failed error={exc}")
                finally:
                    last_research_governance_retry_ts = time.time()
            if HEARTBEAT_SECONDS > 0 and (now_ts - last_heartbeat_ts) >= HEARTBEAT_SECONDS:
                ready = len(list_ready_manifests(limit=100))
                _log(f"worker heartbeat idle ready_manifests={ready}")
                last_heartbeat_ts = now_ts
            time.sleep(WORKER_POLL_INTERVAL_SECONDS)
            continue

        run_id = claimed["id"]
        _log(
            "claimed run "
            f"run_id={run_id} repo={claimed.get('repo', '-')} task_type={claimed.get('task_type', '-')}"
        )
        insert_event(run_id, "worker_claimed_run", {"worker_id": worker_id})
        started = time.time()
        run_pipeline(run_id, worker_id)
        _log(f"completed run run_id={run_id} elapsed_s={time.time() - started:.1f}")


if __name__ == "__main__":
    main()
