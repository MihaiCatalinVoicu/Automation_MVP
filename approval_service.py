from __future__ import annotations

import uuid

from db import (
    get_approval,
    get_pending_approval_for_run,
    get_run,
    insert_approval,
    insert_event,
    resolve_approval,
    update_run_routing,
    update_run_status,
)
from policies import decision_to_action
from telegram_bot import send_approval_message, send_pre_execution_message


def create_approval(run_id: str, summary: dict) -> str:
    existing = get_pending_approval_for_run(run_id)
    if existing:
        return existing["id"]

    approval_id = uuid.uuid4().hex[:10]
    try:
        message = send_approval_message(
            run_id=run_id,
            approval_id=approval_id,
            reason=summary["reason"],
            failed_command=summary.get("failed_command", ""),
            repeat_count=summary.get("repeat_count", 0),
            last_error=summary.get("last_error", ""),
            executor_agent=summary.get("executor_agent", "composer"),
            plan_b_hint=summary.get("plan_b_hint", "Try alternate path or request premium planner"),
        )
    except Exception as exc:
        insert_event(run_id, "approval_telegram_error", {"error": str(exc)})
        message = {"result": {"message_id": None}}

    telegram_message_id = None
    try:
        telegram_message_id = str(message["result"]["message_id"])
    except Exception:
        telegram_message_id = None

    insert_approval(
        approval_id=approval_id,
        run_id=run_id,
        reason=summary["reason"],
        summary=summary,
        status="PENDING",
        telegram_message_id=telegram_message_id,
    )
    update_run_status(run_id, "NEEDS_APPROVAL")
    insert_event(run_id, "approval_requested", summary)
    return approval_id


def create_pre_execution_approval(run_id: str, goal: str, reason: str) -> str:
    """Create approval for pre-execution (needs_approval_for_code). Run starts as NEEDS_APPROVAL."""
    existing = get_pending_approval_for_run(run_id)
    if existing:
        return existing["id"]

    approval_id = uuid.uuid4().hex[:10]
    try:
        message = send_pre_execution_message(
            run_id=run_id,
            approval_id=approval_id,
            goal=goal,
            reason=reason,
        )
    except Exception as exc:
        insert_event(run_id, "approval_telegram_error", {"error": str(exc)})
        message = {"result": {"message_id": None}}

    telegram_message_id = None
    try:
        telegram_message_id = str(message["result"]["message_id"])
    except Exception:
        telegram_message_id = None

    insert_approval(
        approval_id=approval_id,
        run_id=run_id,
        reason="pre_execution_code",
        summary={"goal": goal, "reason": reason},
        status="PENDING",
        telegram_message_id=telegram_message_id,
    )
    # Run is already NEEDS_APPROVAL from app.py
    insert_event(run_id, "approval_requested", {"goal": goal, "reason": reason, "type": "pre_execution"})
    return approval_id


def apply_decision(approval_id: str, decision: str, details: str = "") -> dict:
    approval = get_approval(approval_id)
    if not approval:
        raise ValueError(f"Approval {approval_id} not found")
    if approval["status"] != "PENDING":
        return {
            "ok": True,
            "run_id": approval["run_id"],
            "status": "IGNORED",
            "already_resolved": True,
            "reason": "already_resolved",
        }

    run_id = approval["run_id"]
    run = get_run(run_id)
    if not run:
        raise ValueError(f"Run {run_id} not found")

    action = decision_to_action(decision)
    resolve_approval(approval_id, decision=decision, decision_details=details)

    routing = __import__("json").loads(run["routing_json"])

    if action == "abort":
        update_run_status(run_id, "ABORTED")
        insert_event(run_id, "approval_decision", {"decision": decision, "action": action, "details": details})
        return {"ok": True, "run_id": run_id, "status": "ABORTED", "already_resolved": False}

    if action == "allow_execution":
        update_run_status(run_id, "QUEUED")
        insert_event(
            run_id,
            "approval_decision",
            {"decision": decision, "action": action, "details": details or "Pre-execution approved"},
        )
        return {"ok": True, "run_id": run_id, "status": "QUEUED", "already_resolved": False}

    if action == "reroute_plan_b":
        insert_event(
            run_id,
            "approval_decision",
            {"decision": decision, "action": action, "details": details or "Use alternate execution path"},
        )
        update_run_status(run_id, "RETRY_PENDING")
        return {"ok": True, "run_id": run_id, "status": "RETRY_PENDING", "action": action, "already_resolved": False}

    if action == "reroute_premium":
        routing["planner_agent"] = "premium"
        routing["reviewer_agent"] = "premium"
        update_run_routing(run_id, routing)
        insert_event(
            run_id,
            "approval_decision",
            {"decision": decision, "action": action, "details": details or "Escalate to premium planner"},
        )
        update_run_status(run_id, "RETRY_PENDING")
        return {"ok": True, "run_id": run_id, "status": "RETRY_PENDING", "action": action, "already_resolved": False}

    insert_event(
        run_id,
        "approval_decision",
        {"decision": decision, "action": action, "details": details or "Retry same path"},
    )
    update_run_status(run_id, "RETRY_PENDING")
    return {"ok": True, "run_id": run_id, "status": "RETRY_PENDING", "action": action, "already_resolved": False}
