from __future__ import annotations

import os
import time

from dotenv import load_dotenv

from approval_service import apply_decision
from db import insert_event
from telegram_bot import answer_callback, get_updates, is_authorized_chat, is_authorized_user

load_dotenv()

POLL_INTERVAL_SECONDS = int(os.getenv("TELEGRAM_POLL_INTERVAL_SECONDS", "2"))


def main() -> None:
    print("[telegram] polling started")
    offset = None

    while True:
        try:
            data = get_updates(offset=offset, timeout_seconds=20)
            for item in data.get("result", []):
                offset = item["update_id"] + 1
                handle_update(item)
        except Exception as exc:
            print(f"[telegram] polling error: {exc}")
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
            print(f"[telegram] callback ack warning: {exc}")
        return

    data = callback.get("data", "")
    try:
        approval_id, decision, _run_id = data.split("|", 2)
    except ValueError:
        try:
            answer_callback(callback_id, text="Bad callback data")
        except Exception as exc:
            print(f"[telegram] callback ack warning: {exc}")
        return

    result = apply_decision(approval_id=approval_id, decision=decision)
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
        print(f"[telegram] callback ack warning for run {_run_id}: {exc}")


if __name__ == "__main__":
    main()
