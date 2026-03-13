#!/usr/bin/env python3
import paramiko
import sys
import time

HOST = "207.154.248.232"
USER = "root"
PASS = "XcG*7SBt?NRxher"

DEPLOY_COMMANDS = [
    # STEP 1: Pull automation-mvp
    ("echo '== STEP 1: Pull automation-mvp =='", False),
    ("cd ~/automation-mvp && git pull --ff-only origin feature/validation-battery", False),
    ("cd ~/automation-mvp && git log -1 --oneline", False),

    # STEP 2: Pull crypto-bot-git
    ("echo '== STEP 2: Pull crypto-bot-git =='", False),
    ("cd ~/crypto-bot-git && git pull --ff-only origin lang-ml", False),
    ("cd ~/crypto-bot-git && git log -1 --oneline", False),

    # STEP 3: Venv for automation-mvp
    ("echo '== STEP 3: Venv setup =='", False),
    ("cd ~/automation-mvp && python3 -m venv .venv 2>&1 | tail -3", False),
    ("cd ~/automation-mvp && .venv/bin/pip install -r requirements.txt -q 2>&1 | tail -5", False),

    # STEP 4: Init DB (creates new tables if missing)
    ("echo '== STEP 4: Init DB =='", False),
    ("cd ~/automation-mvp && .venv/bin/python -c 'from db import init_db; init_db(); print(\"DB initialized\")'", False),

    # STEP 5: Patch repos.json for server paths
    # crypto-bot path must match where crypto-bot systemd runs (e.g. /opt/crypto-bot/current)
    ("echo '== STEP 5: Patch repos.json =='", False),
    ("""cd ~/automation-mvp && python3 -c "
import json
from pathlib import Path
p = Path('repos.json')
data = json.loads(p.read_text())
data['automation-mvp']['path'] = '/root/automation-mvp'
data['crypto-bot']['path'] = '/opt/crypto-bot/current'
p.write_text(json.dumps(data, indent=2))
print('repos.json updated')
" """, False),

    # STEP 6: Install systemd unit files
    ("echo '== STEP 6: Install systemd units =='", False),
    ("cd ~/automation-mvp && install -m 0644 ops/systemd/runtime-events-import.service /etc/systemd/system/", False),
    ("cd ~/automation-mvp && install -m 0644 ops/systemd/runtime-events-import.timer /etc/systemd/system/", False),
    ("cd ~/automation-mvp && install -m 0644 ops/systemd/lifecycle-reconcile.service /etc/systemd/system/", False),
    ("cd ~/automation-mvp && install -m 0644 ops/systemd/lifecycle-reconcile.timer /etc/systemd/system/", False),
    ("cd ~/automation-mvp && install -m 0644 ops/systemd/daily-lifecycle-report.service /etc/systemd/system/", False),
    ("cd ~/automation-mvp && install -m 0644 ops/systemd/daily-lifecycle-report.timer /etc/systemd/system/", False),

    # STEP 7: Patch unit files for server paths
    ("echo '== STEP 7: Patch unit file paths =='", False),
    ("sed -i 's#/srv/automation-mvp#/root/automation-mvp#g' /etc/systemd/system/runtime-events-import.service", False),
    ("sed -i 's#/srv/automation-mvp#/root/automation-mvp#g' /etc/systemd/system/lifecycle-reconcile.service", False),
    ("sed -i 's#/srv/automation-mvp#/root/automation-mvp#g' /etc/systemd/system/daily-lifecycle-report.service", False),

    # STEP 8: Phase 1 = crypto-bot only
    ("echo '== STEP 8: Phase 1 crypto-bot only =='", False),
    ("sed -i 's/--repos crypto-bot,stocks-bot/--repos crypto-bot/' /etc/systemd/system/runtime-events-import.service", False),

    # STEP 9: Verify unit file content
    ("echo '== STEP 9: Verify unit files =='", False),
    ("cat /etc/systemd/system/runtime-events-import.service", False),
    ("cat /etc/systemd/system/lifecycle-reconcile.service", False),
    ("cat /etc/systemd/system/daily-lifecycle-report.service", False),

    # STEP 10: Enable timers
    ("echo '== STEP 10: Enable timers =='", False),
    ("systemctl daemon-reload", False),
    ("systemctl enable --now runtime-events-import.timer 2>&1", False),
    ("systemctl enable --now lifecycle-reconcile.timer 2>&1", False),
    ("systemctl enable --now daily-lifecycle-report.timer 2>&1", False),

    # STEP 11: Run import job manually once to verify
    ("echo '== STEP 11: Manual import test =='", False),
    ("cd ~/automation-mvp && .venv/bin/python runtime_events_import_job.py --repos crypto-bot --output /root/automation-mvp/data/runtime_events_import_latest.json 2>&1", False),

    # STEP 12: Run reconcile job manually once
    ("echo '== STEP 12: Manual reconcile test =='", False),
    ("cd ~/automation-mvp && .venv/bin/python lifecycle_reconcile_job.py 2>&1", False),

    # STEP 13: Run daily report manually once
    ("echo '== STEP 13: Manual daily report test =='", False),
    ("cd ~/automation-mvp && .venv/bin/python daily_lifecycle_report.py --since-hours 720 2>&1", False),

    # STEP 14: Check timer status
    ("echo '== STEP 14: Timer status =='", False),
    ("systemctl list-timers --all | grep -E 'runtime-events-import|lifecycle-reconcile|daily-lifecycle-report'", False),

    # STEP 15: Show generated artifacts
    ("echo '== STEP 15: Artifacts =='", False),
    ("cat ~/automation-mvp/data/runtime_events_import_latest.json 2>/dev/null || echo 'not found'", False),
    ("head -30 ~/automation-mvp/data/reports/daily_lifecycle_report_latest.md 2>/dev/null || echo 'not found'", False),
]


def main():
    ssh = paramiko.SSHClient()
    ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    ssh.connect(HOST, username=USER, password=PASS, timeout=15)
    print(f"Connected to {HOST}")
    print("=" * 60)

    for cmd, _ in DEPLOY_COMMANDS:
        stdin, stdout, stderr = ssh.exec_command(cmd, timeout=120)
        out = stdout.read().decode().strip()
        err = stderr.read().decode().strip()
        if out:
            print(out)
        if err:
            print("STDERR:", err)

    ssh.close()
    print("=" * 60)
    print("== Deploy complete. SSH session closed. ==")


if __name__ == "__main__":
    main()
