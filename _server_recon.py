#!/usr/bin/env python3
import paramiko

HOST = "207.154.248.232"
USER = "root"
PASS = "XcG*7SBt?NRxher"

COMMANDS = [
    "echo '== STEP 1: repo status =='",
    "for d in automation-mvp crypto-bot crypto-bot-git stocks-bot-git; do echo \"--- $d ---\"; [ -d ~/$d/.git ] && git -C ~/$d status -sb || echo 'no git repo'; done",
    "echo '== STEP 2: runtime_events.jsonl locations =='",
    "ls -la ~/crypto-bot/data/runtime_events.jsonl ~/crypto-bot-git/data/runtime_events.jsonl 2>/dev/null; echo 'done'",
    "echo '== STEP 3: automation-mvp branch and latest commit =='",
    "cd ~/automation-mvp && git log -1 --oneline && git branch --show-current",
    "echo '== STEP 4: crypto-bot branch and latest commit =='",
    "cd ~/crypto-bot && git log -1 --oneline && git branch --show-current",
    "echo '== STEP 5: disk usage =='",
    "df -h / | tail -1",
    "echo '== STEP 6: python3 version =='",
    "python3 --version",
    "echo '== STEP 7: existing systemd timers =='",
    "systemctl list-timers --all 2>/dev/null | head -20; echo 'done'",
    "echo '== STEP 8: crypto-bot-git vs crypto-bot diff =='",
    "diff <(ls ~/crypto-bot/*.py 2>/dev/null | xargs -n1 basename | sort) <(ls ~/crypto-bot-git/*.py 2>/dev/null | xargs -n1 basename | sort) 2>/dev/null || echo 'diff done or one dir missing'",
]


def main():
    ssh = paramiko.SSHClient()
    ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    ssh.connect(HOST, username=USER, password=PASS, timeout=15)
    print("Connected to", HOST)

    for cmd in COMMANDS:
        stdin, stdout, stderr = ssh.exec_command(cmd, timeout=30)
        out = stdout.read().decode().strip()
        err = stderr.read().decode().strip()
        if out:
            print(out)
        if err:
            print("STDERR:", err)

    ssh.close()
    print("== SSH session closed ==")


if __name__ == "__main__":
    main()
