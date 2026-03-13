#!/usr/bin/env python3
import paramiko
import sys
import os

os.environ["PYTHONIOENCODING"] = "utf-8"

HOST = "207.154.248.232"
USER = "root"
PASS = "XcG*7SBt?NRxher"

COMMANDS = [
    # Fix crypto-bot-git conflict
    "echo '== FIX: move conflicting untracked file in crypto-bot-git =='",
    "cd ~/crypto-bot-git && mv docs/CRYPTO_STRATEGY_MAP.md docs/CRYPTO_STRATEGY_MAP.md.bak 2>/dev/null; echo ok",
    "cd ~/crypto-bot-git && git pull --ff-only origin lang-ml 2>&1",
    "cd ~/crypto-bot-git && git log -1 --oneline",

    # Resume from STEP 10: Enable timers
    "echo '== STEP 10: Enable timers =='",
    "systemctl daemon-reload 2>&1; echo done",
    "systemctl enable --now runtime-events-import.timer 2>&1 | cat",
    "systemctl enable --now lifecycle-reconcile.timer 2>&1 | cat",
    "systemctl enable --now daily-lifecycle-report.timer 2>&1 | cat",

    # STEP 11: Manual import test
    "echo '== STEP 11: Manual import test =='",
    "cd ~/automation-mvp && .venv/bin/python runtime_events_import_job.py --repos crypto-bot --output /root/automation-mvp/data/runtime_events_import_latest.json 2>&1",

    # STEP 12: Manual reconcile test
    "echo '== STEP 12: Manual reconcile test =='",
    "cd ~/automation-mvp && .venv/bin/python lifecycle_reconcile_job.py 2>&1",

    # STEP 13: Manual daily report test
    "echo '== STEP 13: Manual daily report test =='",
    "cd ~/automation-mvp && .venv/bin/python daily_lifecycle_report.py --since-hours 720 2>&1",

    # STEP 14: Timer status
    "echo '== STEP 14: Timer status =='",
    "systemctl list-timers --all 2>&1 | grep -E 'runtime-events|lifecycle-reconcile|daily-lifecycle' | cat",

    # STEP 15: Show artifacts
    "echo '== STEP 15: Import artifact =='",
    "cat ~/automation-mvp/data/runtime_events_import_latest.json 2>/dev/null || echo 'not found'",
    "echo '== STEP 15b: Daily report =='",
    "head -30 ~/automation-mvp/data/reports/daily_lifecycle_report_latest.md 2>/dev/null || echo 'not found'",

    # STEP 16: Verify automation-mvp commit
    "echo '== STEP 16: Final state =='",
    "cd ~/automation-mvp && git log -1 --oneline",
    "cd ~/crypto-bot-git && git log -1 --oneline",
]


def main():
    ssh = paramiko.SSHClient()
    ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    ssh.connect(HOST, username=USER, password=PASS, timeout=15)
    print(f"Connected to {HOST}")
    print("=" * 60)

    for cmd in COMMANDS:
        stdin, stdout, stderr = ssh.exec_command(cmd, timeout=120)
        out = stdout.read().decode("utf-8", errors="replace").strip()
        err = stderr.read().decode("utf-8", errors="replace").strip()
        if out:
            print(out)
        if err:
            print("STDERR:", err)

    ssh.close()
    print("=" * 60)
    print("== Deploy phase 2 complete. SSH session closed. ==")


if __name__ == "__main__":
    main()
