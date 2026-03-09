from __future__ import annotations

import os
import socket
import time

from dotenv import load_dotenv

from db import claim_run, init_db, insert_event
load_dotenv()

from runner import run_pipeline

WORKER_POLL_INTERVAL_SECONDS = int(os.getenv("WORKER_POLL_INTERVAL_SECONDS", "2"))


def main() -> None:
    init_db()
    worker_id = f"worker-{socket.gethostname()}-{os.getpid()}"
    print(f"[worker] started: {worker_id}")

    while True:
        claimed = claim_run(worker_id)
        if not claimed:
            time.sleep(WORKER_POLL_INTERVAL_SECONDS)
            continue

        run_id = claimed["id"]
        insert_event(run_id, "worker_claimed_run", {"worker_id": worker_id})
        run_pipeline(run_id, worker_id)


if __name__ == "__main__":
    main()
