from __future__ import annotations

import json
import uuid

from db import (
    create_case_event,
    create_experiment_manifest,
    create_telegram_decision,
    get_search_case,
    get_telegram_decision,
    get_experiment_manifest,
    get_edge_verdict,
    list_edge_verdicts,
    list_experiment_manifests,
    update_search_case,
    utc_now,
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


def apply_research_decision(
    *,
    case_id: str,
    action: str,
    actor: str,
    details: str = "",
    verdict_id: str | None = None,
    manifest_id: str | None = None,
    decision_key: str | None = None,
    message_id: str | None = None,
    source: str = "manual",
) -> dict:
    allowed_by_stage = {
        "manifest_ready": {"MUTATE_WITH_POLICY", "RETEST_OOS", "RUN_BIGGER_SAMPLE", "HOLD_FOR_MORE_DATA", "KILL_CASE"},
        "awaiting_verdict": {"MUTATE_WITH_POLICY", "RETEST_OOS", "RUN_BIGGER_SAMPLE", "HOLD_FOR_MORE_DATA", "KILL_CASE"},
        "promotion_review": {"PROMOTE_TO_PAPER", "ASK_PREMIUM_REVIEW", "RUN_BIGGER_SAMPLE", "KILL_CASE"},
        "paper_candidate": {"ASK_PREMIUM_REVIEW", "RUN_BIGGER_SAMPLE", "KILL_CASE"},
        "on_hold": {"RUN_BIGGER_SAMPLE", "RETEST_OOS", "ASK_PREMIUM_REVIEW", "KILL_CASE"},
    }
    terminal_statuses = {"done", "killed", "archived"}
    case = get_search_case(case_id)
    if not case:
        raise ValueError(f"Search case not found: {case_id}")
    if str(case.get("status") or "") in terminal_statuses:
        raise ValueError(f"Case is terminal: {case.get('status')}")
    manifests = list_experiment_manifests(case_id=case_id)
    if not manifests:
        raise ValueError(f"No manifests for case: {case_id}")
    manifest_lookup = {str(m["manifest_id"]): m for m in manifests}
    latest_manifest = manifests[0]
    target_manifest = manifest_lookup.get(manifest_id) if manifest_id else latest_manifest
    if not target_manifest:
        raise ValueError(f"Manifest not found on case: {manifest_id}")
    verdicts = list_edge_verdicts(case_id=case_id)
    final_verdict = next((v for v in verdicts if str(v.get("status")) == "final"), None)
    latest_verdict_id = str(final_verdict["verdict_id"]) if final_verdict else None
    if verdict_id and latest_verdict_id and verdict_id != latest_verdict_id:
        create_case_event(
            case_id=case_id,
            manifest_id=target_manifest["manifest_id"],
            verdict_id=latest_verdict_id,
            event_type="stale_decision_rejected",
            payload={
                "action": action,
                "actor": actor,
                "source": source,
                "latest_verdict_id": latest_verdict_id,
                "requested_verdict_id": verdict_id,
            },
        )
        return {
            "ok": False,
            "case_id": case_id,
            "status": "STALE_REJECTED",
            "latest_verdict_id": latest_verdict_id,
            "requested_verdict_id": verdict_id,
        }

    action_up = action.strip().upper()
    stage = str(case.get("stage") or "")
    if action_up not in allowed_by_stage.get(stage, set()) and action_up != "KILL_CASE":
        raise ValueError(f"Action {action_up} not allowed for stage {stage}")

    canonical_decision_key = (
        decision_key
        or f"td:{source}:{case_id}:{verdict_id or latest_verdict_id or 'none'}:{action_up}:{message_id or 'nomsg'}"
    )
    existing_decision = get_telegram_decision(canonical_decision_key)
    if existing_decision:
        return {
            "ok": True,
            "case_id": case_id,
            "action": action_up,
            "status": "IGNORED_DUPLICATE",
            "decision_key": canonical_decision_key,
        }
    create_case_event(
        case_id=case_id,
        manifest_id=target_manifest["manifest_id"],
        verdict_id=latest_verdict_id,
        event_type="research_decision_requested",
        payload={
            "action": action_up,
            "actor": actor,
            "details": details,
            "source": source,
            "message_id": message_id,
            "decision_key": canonical_decision_key,
        },
    )
    create_telegram_decision(
        approval_id=canonical_decision_key,
        case_id=case_id,
        manifest_id=target_manifest["manifest_id"],
        run_id=target_manifest.get("last_run_id"),
        decision_scope="research_case",
        action=action_up,
        actor=actor,
        message_id=message_id,
        payload={"details": details, "source": source, "verdict_id": verdict_id or latest_verdict_id},
    )

    if action_up in {"MUTATE_WITH_POLICY", "RETEST_OOS", "RUN_BIGGER_SAMPLE"}:
        parent_version = int(target_manifest.get("manifest_version", 1))
        next_version = parent_version + 1
        new_manifest_id = f"{target_manifest['manifest_id']}_{action_up.lower()}"
        dataset_spec = json.loads(target_manifest["dataset_spec_json"])
        if action_up == "RUN_BIGGER_SAMPLE":
            dataset_spec["extended_sample"] = True
        elif action_up == "RETEST_OOS":
            dataset_spec["oos_retest"] = True
        create_experiment_manifest(
            manifest_id=new_manifest_id,
            case_id=case_id,
            idempotency_key=f"idem_{new_manifest_id}",
            manifest_version=next_version,
            status="ready",
            parent_manifest_id=target_manifest["manifest_id"],
            derived_from_verdict_id=latest_verdict_id,
            derivation_reason=action_up.lower(),
            repo=target_manifest["repo"],
            adapter_type=target_manifest["adapter_type"],
            entrypoint=target_manifest["entrypoint"],
            strategy_identity=json.loads(target_manifest["strategy_identity_json"]),
            run_context_template=json.loads(target_manifest["run_context_template_json"]),
            dataset_spec=dataset_spec,
            execution_spec=json.loads(target_manifest["execution_spec_json"]),
            cost_model=json.loads(target_manifest["cost_model_json"]),
            gates=json.loads(target_manifest["gates_json"]),
            planner_hints=json.loads(target_manifest["planner_hints_json"]),
            artifacts=json.loads(target_manifest["artifacts_json"]),
            param_diff={"action": action_up, "details": details, "decision_source": source, "decision_actor": actor},
            created_by=actor,
            approved_by=actor,
            notes=f"Derived from {target_manifest['manifest_id']} via {action_up}",
            force_stage_transition=True,
        )
        create_case_event(
            case_id=case_id,
            manifest_id=new_manifest_id,
            verdict_id=latest_verdict_id,
            event_type="research_governance_manifest_created",
            payload={"action": action_up, "actor": actor, "source": source, "decision_key": canonical_decision_key},
        )
        create_case_event(
            case_id=case_id,
            manifest_id=new_manifest_id,
            verdict_id=latest_verdict_id,
            event_type="research_decision_applied",
            payload={"action": action_up, "actor": actor, "decision_key": canonical_decision_key},
        )
        return {"ok": True, "case_id": case_id, "action": action_up, "new_manifest_id": new_manifest_id, "decision_key": canonical_decision_key}

    if action_up in {"PROMOTE_TO_PAPER", "HOLD_FOR_MORE_DATA", "KILL_CASE", "ASK_PREMIUM_REVIEW"}:
        if action_up == "PROMOTE_TO_PAPER":
            if not latest_verdict_id:
                raise ValueError("Cannot promote without current final verdict")
            latest_verdict = get_edge_verdict(latest_verdict_id)
            if not latest_verdict:
                raise ValueError("Latest final verdict not found")
            gates = json.loads(latest_verdict.get("gate_results_json") or "{}")
            critical = ["min_trades_pass", "cost_adjusted_edge_pass", "walkforward_pass", "leakage_check_pass"]
            failed = [k for k in critical if not bool(gates.get(k))]
            if failed:
                raise ValueError(f"Promotion blocked, failed critical gates: {', '.join(failed)}")
        if action_up == "KILL_CASE":
            update_search_case(
                case_id,
                force_transition=True,
                stage="closed",
                status="killed",
                final_outcome="KILL_CASE",
                closed_at=utc_now(),
            )
        elif action_up == "HOLD_FOR_MORE_DATA":
            update_search_case(case_id, force_transition=True, status="on_hold", stage="awaiting_verdict")
        elif action_up == "PROMOTE_TO_PAPER":
            update_search_case(case_id, force_transition=True, status="active", stage="paper_candidate")
        elif action_up == "ASK_PREMIUM_REVIEW":
            update_search_case(case_id, force_transition=True, stage="promotion_review")
        create_case_event(
            case_id=case_id,
            manifest_id=target_manifest["manifest_id"],
            verdict_id=latest_verdict_id,
            event_type="research_governance_action",
            payload={"action": action_up, "actor": actor, "details": details, "source": source, "decision_key": canonical_decision_key},
        )
        create_case_event(
            case_id=case_id,
            manifest_id=target_manifest["manifest_id"],
            verdict_id=latest_verdict_id,
            event_type="research_decision_applied",
            payload={"action": action_up, "actor": actor, "decision_key": canonical_decision_key},
        )
        return {"ok": True, "case_id": case_id, "action": action_up, "decision_key": canonical_decision_key}

    raise ValueError(f"Unsupported research action: {action}")
