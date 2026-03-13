from __future__ import annotations

import os
import socket
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from adapters import ADAPTERS
from db import (
    claim_manifest,
    create_case_event,
    get_experiment_manifest,
    init_db,
    list_ready_manifests,
    set_manifest_failed_with_retry_policy,
    set_manifest_execution_state,
)
from edge_verdict_writer import write_edge_verdict_for_manifest
from research_guardrails import evaluate_manifest_guardrails

AUTOMATION_ROOT = Path(__file__).resolve().parent
MANIFEST_MAX_RETRIES = max(1, int(os.getenv("MANIFEST_MAX_RETRIES", "3")))
MANIFEST_WORKER_POLL_INTERVAL_SECONDS = max(1, int(os.getenv("MANIFEST_WORKER_POLL_INTERVAL_SECONDS", "5")))
MANIFEST_WORKER_HEARTBEAT_SECONDS = max(0, int(os.getenv("MANIFEST_WORKER_HEARTBEAT_SECONDS", "60")))


def _ts() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")


def _log(message: str) -> None:
    print(f"[{_ts()}] INFO {message}", flush=True)


def _run_adapter(manifest: dict[str, Any]) -> dict[str, Any]:
    adapter_type = str(manifest.get("adapter_type") or "")
    handler = ADAPTERS.get(adapter_type)
    if not handler:
        return {
            "ok": False,
            "manifest_id": manifest["manifest_id"],
            "case_id": manifest["case_id"],
            "adapter_type": adapter_type,
            "warnings": [],
            "errors": [f"Unsupported adapter_type: {adapter_type}"],
        }
    return handler(manifest, AUTOMATION_ROOT)


def process_one_manifest(worker_id: str) -> bool:
    claimed = claim_manifest(worker_id)
    if not claimed:
        return False
    manifest_id = str(claimed["manifest_id"])
    case_id = str(claimed["case_id"])
    set_manifest_execution_state(manifest_id, "running", claimed_by=worker_id)
    create_case_event(case_id=case_id, manifest_id=manifest_id, event_type="manifest_started", payload={"worker_id": worker_id})
    manifest = get_experiment_manifest(manifest_id)
    if not manifest:
        set_manifest_execution_state(manifest_id, "failed", last_error="Manifest disappeared after claim")
        create_case_event(case_id=case_id, manifest_id=manifest_id, event_type="manifest_failed", payload={"error": "manifest_missing"})
        return True
    allowed, reason = evaluate_manifest_guardrails(manifest)
    if not allowed:
        set_manifest_execution_state(manifest_id, "cancelled", last_error=reason)
        create_case_event(
            case_id=case_id,
            manifest_id=manifest_id,
            event_type="manifest_skipped_guardrail",
            payload={"reason": reason},
        )
        return True
    result = _run_adapter(manifest)
    if not result.get("ok"):
        err = "; ".join(result.get("errors") or ["adapter_failed"])
        next_status, attempt_count = set_manifest_failed_with_retry_policy(
            manifest_id,
            last_error=err,
            max_retries=MANIFEST_MAX_RETRIES,
        )
        create_case_event(
            case_id=case_id,
            manifest_id=manifest_id,
            event_type="manifest_failed",
            payload={
                "adapter_type": manifest.get("adapter_type"),
                "errors": result.get("errors", []),
                "execution_status": next_status,
                "attempt_count": attempt_count,
                "max_retries": MANIFEST_MAX_RETRIES,
            },
        )
        if next_status == "dead":
            create_case_event(
                case_id=case_id,
                manifest_id=manifest_id,
                event_type="manifest_marked_dead",
                payload={"attempt_count": attempt_count, "max_retries": MANIFEST_MAX_RETRIES, "last_error": err},
            )
        return True
    set_manifest_execution_state(
        manifest_id,
        "completed",
        last_run_id=str(result.get("run_id") or ""),
        last_error=None,
    )
    create_case_event(
        case_id=case_id,
        manifest_id=manifest_id,
        event_type="manifest_completed",
        payload={"run_id": result.get("run_id"), "adapter_type": result.get("adapter_type")},
    )
    verdict = write_edge_verdict_for_manifest(manifest_id, result)
    create_case_event(
        case_id=case_id,
        manifest_id=manifest_id,
        event_type="manifest_verdict_attached",
        payload=verdict,
    )
    return True


def main() -> None:
    init_db()
    worker_id = f"manifest-worker-{socket.gethostname()}-{os.getpid()}"
    _log(
        f"manifest worker started worker_id={worker_id} "
        f"poll={MANIFEST_WORKER_POLL_INTERVAL_SECONDS}s "
        f"heartbeat={MANIFEST_WORKER_HEARTBEAT_SECONDS}s"
    )
    last_heartbeat_ts = 0.0
    while True:
        processed = process_one_manifest(worker_id)
        now_ts = time.time()
        if not processed:
            if MANIFEST_WORKER_HEARTBEAT_SECONDS > 0 and (now_ts - last_heartbeat_ts) >= MANIFEST_WORKER_HEARTBEAT_SECONDS:
                ready = len(list_ready_manifests(limit=100))
                _log(f"idle heartbeat ready_manifests={ready}")
                last_heartbeat_ts = now_ts
            time.sleep(MANIFEST_WORKER_POLL_INTERVAL_SECONDS)


if __name__ == "__main__":
    main()

