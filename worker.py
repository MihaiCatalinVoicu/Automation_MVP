from __future__ import annotations

import os
import socket
import time
from pathlib import Path

from dotenv import load_dotenv

load_dotenv(Path(__file__).resolve().parent / ".env")

from db import claim_run, init_db, insert_event

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


def main() -> None:
    init_db()
    upsert_default_schedules()
    worker_id = f"worker-{socket.gethostname()}-{os.getpid()}"
    print(f"[worker] started: {worker_id}")
    last_strategy_review_ts = 0.0
    last_schedule_check_ts = 0.0
    last_shadow_board_ts = 0.0
    last_research_governance_retry_ts = 0.0

    while True:
        claimed = claim_run(worker_id)
        if not claimed:
            if process_one_manifest(worker_id):
                continue
            if RESEARCH_SCHEDULE_INTERVAL_SECONDS > 0 and (time.time() - last_schedule_check_ts) >= RESEARCH_SCHEDULE_INTERVAL_SECONDS:
                try:
                    created = materialize_due_runs()
                    if created:
                        print(f"[worker] materialized research runs: {len(created)}")
                except Exception as exc:
                    print(f"[worker] research schedule materialization failed: {exc}")
                finally:
                    last_schedule_check_ts = time.time()
            if STRATEGY_REVIEW_INTERVAL_SECONDS > 0 and (time.time() - last_strategy_review_ts) >= STRATEGY_REVIEW_INTERVAL_SECONDS:
                try:
                    out_dir = Path("data") / "strategy_reviews" / "scheduled"
                    summary = run_due_reviews(output_dir=out_dir, review_kind="scheduled")
                    print(f"[worker] scheduled strategy review: {summary['review_count']} reviewed")
                except Exception as exc:
                    print(f"[worker] scheduled strategy review failed: {exc}")
                finally:
                    last_strategy_review_ts = time.time()
            if SHADOW_BOARD_INTERVAL_SECONDS > 0 and (time.time() - last_shadow_board_ts) >= SHADOW_BOARD_INTERVAL_SECONDS:
                try:
                    out_dir = Path("data") / "shadow_recommendations"
                    board = build_shadow_board(out_dir)
                    print(f"[worker] shadow board updated: daily={len(board['daily_shadow_recommendations'])}")
                except Exception as exc:
                    print(f"[worker] shadow board update failed: {exc}")
                finally:
                    last_shadow_board_ts = time.time()
            if (
                RESEARCH_GOVERNANCE_RETRY_INTERVAL_SECONDS > 0
                and (time.time() - last_research_governance_retry_ts) >= RESEARCH_GOVERNANCE_RETRY_INTERVAL_SECONDS
            ):
                try:
                    sent = send_pending_research_governance_messages(limit=20)
                    if sent:
                        print(f"[worker] research governance retries sent: {sent}")
                except Exception as exc:
                    print(f"[worker] research governance retry failed: {exc}")
                finally:
                    last_research_governance_retry_ts = time.time()
            time.sleep(WORKER_POLL_INTERVAL_SECONDS)
            continue

        run_id = claimed["id"]
        insert_event(run_id, "worker_claimed_run", {"worker_id": worker_id})
        run_pipeline(run_id, worker_id)


if __name__ == "__main__":
    main()
