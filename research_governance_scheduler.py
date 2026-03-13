from __future__ import annotations

import json
from typing import Any

from db import (
    case_event_exists,
    create_case_event,
    create_telegram_decision,
    get_experiment_manifest,
    get_search_case,
    get_telegram_decision,
    list_edge_verdicts,
    list_search_cases,
)
from telegram_bot import send_research_governance_message


_NOTIFY_DECISIONS = {
    "MUTATE_WITH_POLICY",
    "RETEST_OOS",
    "RUN_BIGGER_SAMPLE",
    "PROMOTE_TO_PAPER",
    "HOLD_FOR_MORE_DATA",
    "ASK_PREMIUM_REVIEW",
    "REJECT_EDGE",
}


def _latest_final_verdict(case_id: str) -> dict[str, Any] | None:
    verdicts = list_edge_verdicts(case_id=case_id)
    for row in verdicts:
        if str(row.get("status") or "") == "final":
            return row
    return None


def _send_for_case(case: dict[str, Any]) -> bool:
    case_id = str(case["case_id"])
    verdict = _latest_final_verdict(case_id)
    if not verdict:
        return False
    verdict_id = str(verdict["verdict_id"])
    decision = str(verdict.get("decision") or "")
    if decision not in _NOTIFY_DECISIONS:
        return False
    if case_event_exists(case_id=case_id, event_type="research_governance_message_sent", verdict_id=verdict_id):
        return False
    if not case_event_exists(case_id=case_id, event_type="research_governance_message_failed", verdict_id=verdict_id):
        return False
    if str(case.get("latest_verdict_id") or "") != verdict_id:
        return False
    if str(case.get("status") or "") in {"done", "killed", "archived"}:
        return False

    manifest = get_experiment_manifest(str(verdict.get("manifest_id") or ""))
    if not manifest:
        create_case_event(
            case_id=case_id,
            verdict_id=verdict_id,
            event_type="research_governance_retry_skipped",
            payload={"reason": "manifest_missing"},
        )
        return False

    metrics = json.loads(verdict.get("metrics_snapshot_json") or "{}")
    try:
        tg = send_research_governance_message(
            case_id=case_id,
            family=str(case.get("family") or ""),
            strategy_id=case.get("strategy_id"),
            stage=str(case.get("stage") or ""),
            proposed_decision=decision,
            verdict_id=verdict_id,
            manifest_id=str(manifest["manifest_id"]),
            metrics=metrics,
            decision_reason=str(verdict.get("decision_reason") or ""),
            dominant_failure_mode=str(verdict.get("dominant_failure_mode") or verdict.get("decision_reason") or ""),
            verdict_score=verdict.get("verdict_score"),
            artifacts_root=verdict.get("artifacts_root"),
        )
    except Exception as exc:
        create_case_event(
            case_id=case_id,
            manifest_id=str(manifest["manifest_id"]),
            verdict_id=verdict_id,
            event_type="research_governance_message_retry_failed",
            payload={"error": str(exc)},
        )
        return False

    message_id = str((tg.get("result") or {}).get("message_id") or "")
    create_case_event(
        case_id=case_id,
        manifest_id=str(manifest["manifest_id"]),
        verdict_id=verdict_id,
        event_type="research_governance_message_sent",
        payload={"message_id": message_id, "decision": decision, "source": "scheduler_retry"},
    )
    td_key = f"td_{verdict_id}"
    if not get_telegram_decision(td_key):
        create_telegram_decision(
            approval_id=td_key,
            case_id=case_id,
            manifest_id=str(manifest["manifest_id"]),
            run_id=str(verdict.get("run_id") or ""),
            decision_scope="research_case",
            action=decision,
            actor="system",
            message_id=message_id,
            payload={"source": "research_governance_scheduler_retry"},
        )
    return True


def send_pending_research_governance_messages(limit: int = 20) -> int:
    sent = 0
    cases = list_search_cases()
    for case in cases:
        if sent >= max(1, int(limit)):
            break
        if _send_for_case(case):
            sent += 1
    return sent

