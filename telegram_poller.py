from __future__ import annotations

import hashlib
import os
import time
from datetime import datetime, timezone
from pathlib import Path

from dotenv import load_dotenv

load_dotenv(Path(__file__).resolve().parent / ".env")

from approval_service import apply_decision, apply_research_decision
from db import insert_event
from telegram_bot import answer_callback, get_updates, is_authorized_chat, is_authorized_user

POLL_INTERVAL_SECONDS = int(os.getenv("TELEGRAM_POLL_INTERVAL_SECONDS", "2"))
VERBOSE = os.getenv("TELEGRAM_POLLER_VERBOSE", "").lower() in ("1", "true", "yes")
HEARTBEAT_SECONDS = int(os.getenv("TELEGRAM_POLLER_HEARTBEAT_SECONDS", "60"))


def _ts() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")


def _log(msg: str) -> None:
    print(f"[{_ts()}] INFO {msg}", flush=True)


def main() -> None:
    _log(
        "telegram polling started "
        f"poll={POLL_INTERVAL_SECONDS}s heartbeat={HEARTBEAT_SECONDS}s "
        f"authorized_chat={'yes' if bool(os.getenv('TELEGRAM_CHAT_ID')) else 'no'} "
        f"authorized_user={'yes' if bool(os.getenv('TELEGRAM_ALLOWED_USER_ID')) else 'no'} "
        f"verbose={'yes' if VERBOSE else 'no'}"
    )
    offset = None
    last_heartbeat_ts = 0.0

    while True:
        try:
            data = get_updates(offset=offset, timeout_seconds=20)
            for item in data.get("result", []):
                offset = item["update_id"] + 1
                if VERBOSE:
                    cb = item.get("callback_query")
                    if cb:
                        _log(f"received callback data={cb.get('data', '')[:80]}")
                handle_update(item)
            now_ts = time.time()
            if HEARTBEAT_SECONDS > 0 and (now_ts - last_heartbeat_ts) >= HEARTBEAT_SECONDS:
                _log("telegram heartbeat idle waiting_updates")
                last_heartbeat_ts = now_ts
        except Exception as exc:
            _log(f"telegram polling error error={exc}")
            time.sleep(POLL_INTERVAL_SECONDS)


def handle_update(update: dict) -> None:
    callback = update.get("callback_query")
    if not callback:
        return

    callback_id = callback["id"]
    from_user = callback.get("from", {})
    message = callback.get("message", {})
    chat = message.get("chat", {})

    if not is_authorized_chat(chat.get("id", "")) or not is_authorized_user(from_user.get("id", "")):
        try:
            answer_callback(callback_id, text="Unauthorized")
        except Exception as exc:
            _log(f"callback ack warning error={exc}")
        return

    data = str(callback.get("data", "") or "")
    if data.startswith("scope=research_case|"):
        result = _handle_research_callback(callback, data)
        status = result.get("status") or ("OK" if result.get("ok") else "FAILED")
        if VERBOSE:
            parsed = _parse_research_callback(data)
            _log(f"research callback case={parsed.get('case_id')} action={parsed.get('action')} status={status}")
        try:
            answer_callback(callback_id, text=f"research => {status}")
        except Exception as exc:
            _log(f"callback ack warning error={exc}")
        return

    try:
        approval_id, decision, _run_id = data.split("|", 2)
    except ValueError:
        try:
            answer_callback(callback_id, text="Bad callback data")
        except Exception as exc:
            _log(f"callback ack warning error={exc}")
        return

    result = apply_decision(approval_id=approval_id, decision=decision)
    if VERBOSE:
        _log(f"approval callback approval_id={approval_id} decision={decision} status={result.get('status', '')}")
    try:
        answer_callback(callback_id, text=f"{decision} => {result['status']}")
    except Exception as exc:
        insert_event(
            _run_id,
            "telegram_callback_ack_warning",
            {
                "approval_id": approval_id,
                "decision": decision,
                "error": str(exc),
            },
        )
        _log(f"callback ack warning run_id={_run_id} error={exc}")


def _parse_research_callback(data: str) -> dict[str, str]:
    parts = [p.strip() for p in data.split("|") if p.strip()]
    out: dict[str, str] = {}
    for part in parts:
        if "=" not in part:
            continue
        k, v = part.split("=", 1)
        out[k.strip()] = v.strip()
    return out


def _handle_research_callback(callback: dict, data: str) -> dict:
    parsed = _parse_research_callback(data)
    case_id = parsed.get("case_id")
    action = parsed.get("action")
    verdict_id = parsed.get("verdict_id")
    manifest_id = parsed.get("manifest_id")
    callback_id = str(callback.get("id") or "")
    message = callback.get("message", {})
    message_id = str(message.get("message_id") or "")
    user = callback.get("from", {})
    actor = str(user.get("username") or user.get("id") or "telegram_user")
    if not case_id or not action:
        return {"ok": False, "status": "BAD_CALLBACK", "reason": "Missing case_id or action"}
    # Stable dedupe key for repeated taps on same message/action/verdict.
    raw = f"{case_id}|{verdict_id or ''}|{action}|{message_id}"
    dedupe = hashlib.sha256(raw.encode("utf-8")).hexdigest()[:20]
    decision_key = f"td:research:{dedupe}"
    try:
        return apply_research_decision(
            case_id=case_id,
            action=action,
            actor=actor,
            details=f"telegram_callback_id={callback_id}",
            verdict_id=verdict_id,
            manifest_id=manifest_id,
            decision_key=decision_key,
            message_id=message_id,
            source="telegram",
        )
    except Exception as exc:
        return {"ok": False, "status": "FAILED", "reason": str(exc)}


if __name__ == "__main__":
    main()
