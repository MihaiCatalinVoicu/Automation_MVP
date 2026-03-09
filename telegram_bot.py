from __future__ import annotations

import os
from typing import Optional

import requests

TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "")
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID", "")
TELEGRAM_ALLOWED_USER_ID = os.getenv("TELEGRAM_ALLOWED_USER_ID", "")


class TelegramError(RuntimeError):
    pass


def telegram_api_url(method: str) -> str:
    if not TELEGRAM_BOT_TOKEN:
        raise TelegramError("TELEGRAM_BOT_TOKEN is not configured")
    return f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/{method}"


def send_approval_message(
    run_id: str,
    approval_id: str,
    reason: str,
    failed_command: str,
    repeat_count: int,
    last_error: str,
    executor_agent: str,
    plan_b_hint: str = "Try alternate path or request premium planner",
) -> dict:
    text = (
        f"Run blocked\n\n"
        f"Run: {run_id}\n"
        f"Reason: {reason}\n"
        f"Executor: {executor_agent}\n"
        f"Command: {failed_command or '-'}\n"
        f"Repeat count: {repeat_count}\n"
        f"Last error: {(last_error or '-')[:300]}\n"
        f"Plan B hint: {plan_b_hint[:120]}"
    )

    payload = {
        "chat_id": TELEGRAM_CHAT_ID,
        "text": text,
        "reply_markup": {
            "inline_keyboard": [
                [
                    {"text": "Retry Safe", "callback_data": f"{approval_id}|RETRY_SAFE|{run_id}"},
                    {"text": "Plan B", "callback_data": f"{approval_id}|PLAN_B|{run_id}"},
                ],
                [
                    {"text": "Abort", "callback_data": f"{approval_id}|ABORT|{run_id}"},
                    {"text": "Ask Premium", "callback_data": f"{approval_id}|ASK_PREMIUM|{run_id}"},
                ],
            ]
        },
    }

    resp = requests.post(telegram_api_url("sendMessage"), json=payload, timeout=20)
    resp.raise_for_status()
    data = resp.json()
    if not data.get("ok"):
        raise TelegramError(f"sendMessage failed: {data}")
    return data


def send_pre_execution_message(run_id: str, approval_id: str, goal: str, reason: str) -> dict:
    text = (
        f"Pre-execution approval required\n\n"
        f"Run: {run_id}\n"
        f"Goal: {(goal or '-')[:200]}\n"
        f"Reason: {reason}"
    )
    payload = {
        "chat_id": TELEGRAM_CHAT_ID,
        "text": text,
        "reply_markup": {
            "inline_keyboard": [
                [
                    {"text": "Allow", "callback_data": f"{approval_id}|ALLOW_EXECUTION|{run_id}"},
                    {"text": "Abort", "callback_data": f"{approval_id}|ABORT|{run_id}"},
                ],
            ]
        },
    }
    resp = requests.post(telegram_api_url("sendMessage"), json=payload, timeout=20)
    resp.raise_for_status()
    data = resp.json()
    if not data.get("ok"):
        raise TelegramError(f"sendMessage failed: {data}")
    return data


def answer_callback(callback_query_id: str, text: str = "Decision received") -> None:
    payload = {
        "callback_query_id": callback_query_id,
        "text": text,
        "show_alert": False,
    }
    resp = requests.post(telegram_api_url("answerCallbackQuery"), json=payload, timeout=20)
    resp.raise_for_status()


def get_updates(offset: Optional[int] = None, timeout_seconds: int = 20) -> dict:
    payload = {"timeout": timeout_seconds}
    if offset is not None:
        payload["offset"] = offset
    resp = requests.get(telegram_api_url("getUpdates"), params=payload, timeout=timeout_seconds + 10)
    resp.raise_for_status()
    data = resp.json()
    if not data.get("ok"):
        raise TelegramError(f"getUpdates failed: {data}")
    return data


def is_authorized_chat(chat_id: str | int) -> bool:
    return str(chat_id) == str(TELEGRAM_CHAT_ID)


def is_authorized_user(user_id: str | int) -> bool:
    return str(user_id) == str(TELEGRAM_ALLOWED_USER_ID)
