#!/usr/bin/env python3
import paramiko
import sys

HOST = "207.154.248.232"
USER = "root"
PASS = "XcG*7SBt?NRxher"

COMMANDS = [
    "echo '== Enable timers =='",
    "systemctl daemon-reload",
    "systemctl enable --now runtime-events-import.timer 2>&1 | tr -d '\\342\\206\\222'",
    "systemctl enable --now lifecycle-reconcile.timer 2>&1 | tr -d '\\342\\206\\222'",
    "systemctl enable --now daily-lifecycle-report.timer 2>&1 | tr -d '\\342\\206\\222'",

    "echo '== Manual import test =='",
    "cd ~/automation-mvp && .venv/bin/python runtime_events_import_job.py --repos crypto-bot --output /root/automation-mvp/data/runtime_events_import_latest.json 2>&1",

    "echo '== Manual reconcile test =='",
    "cd ~/automation-mvp && .venv/bin/python lifecycle_reconcile_job.py 2>&1",

    "echo '== Manual daily report test =='",
    "cd ~/automation-mvp && .venv/bin/python daily_lifecycle_report.py --since-hours 720 2>&1",

    "echo '== Timer status =='",
    "systemctl list-timers --all 2>&1 | grep -E 'runtime-events|lifecycle-reconcile|daily-lifecycle' | tr -d '\\342\\206\\222'",

    "echo '== Import artifact =='",
    "cat ~/automation-mvp/data/runtime_events_import_latest.json 2>/dev/null || echo 'not found'",

    "echo '== Daily report =='",
    "head -30 ~/automation-mvp/data/reports/daily_lifecycle_report_latest.md 2>/dev/null || echo 'not found'",

    "echo '== Final state =='",
    "cd ~/automation-mvp && git log -1 --oneline",
    "cd ~/crypto-bot-git && git log -1 --oneline",
]


def main():
    ssh = paramiko.SSHClient()
    ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    ssh.connect(HOST, username=USER, password=PASS, timeout=15)
    print(f"Connected to {HOST}")

    for cmd in COMMANDS:
        stdin, stdout, stderr = ssh.exec_command(cmd, timeout=120)
        raw_out = stdout.read()
        raw_err = stderr.read()
        out = raw_out.decode("utf-8", errors="replace").strip()
        err = raw_err.decode("utf-8", errors="replace").strip()
        if out:
            sys.stdout.buffer.write(out.encode("utf-8", errors="replace"))
            sys.stdout.buffer.write(b"\n")
            sys.stdout.buffer.flush()
        if err:
            sys.stdout.buffer.write(b"STDERR: ")
            sys.stdout.buffer.write(err.encode("utf-8", errors="replace"))
            sys.stdout.buffer.write(b"\n")
            sys.stdout.buffer.flush()

    ssh.close()
    sys.stdout.buffer.write(b"== Deploy complete ==\n")
    sys.stdout.buffer.flush()


if __name__ == "__main__":
    main()
