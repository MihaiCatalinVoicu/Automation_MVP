# Server Phase 1 Checklist

Scope for the first operational server rollout:

- deploy `automation-mvp`
- keep `automation-mvp` as control plane only
- connect it to `crypto-bot` runtime event producer path
- do **not** require `stocks-bot` as an always-on server process yet

## 1. Git First

Before any deploy:

- commit `automation-mvp`
- commit `crypto-bot`
- commit `stocks-bot` if runtime emitter changes are part of the milestone
- create a simple milestone tag, for example `lifecycle-mvp-v1`

## 2. Server Scope

Deploy now:

- `automation-mvp`
- `runtime-events-import.timer`
- `lifecycle-reconcile.timer`
- `daily-lifecycle-report.timer`

Do not deploy now:

- `stocks-bot` as always-on runtime
- live trading execution inside `automation-mvp`
- extra alerting or dashboards

## 3. Install Units

Copy the template units from `ops/systemd/` to `/etc/systemd/system/` and replace:

- `/srv/automation-mvp`
- `/srv/automation-mvp/.venv/bin/python`

Then:

```bash
sudo systemctl daemon-reload
sudo systemctl enable --now runtime-events-import.timer
sudo systemctl enable --now lifecycle-reconcile.timer
sudo systemctl enable --now daily-lifecycle-report.timer
```

## 4. Verify Two Real Cycles

Run:

```bash
systemctl list-timers --all | rg "runtime-events-import|lifecycle-reconcile|daily-lifecycle-report"
systemctl status runtime-events-import.service
systemctl status lifecycle-reconcile.service
systemctl status daily-lifecycle-report.service
```

Check generated artifacts:

- `data/runtime_events_import_latest.json`
- `data/reports/lifecycle_reconcile_latest.md`
- `data/reports/daily_lifecycle_report_latest.md`

## 5. Update Progress Ledger

After first stable cycles:

```powershell
python progress_ledger.py update --id wi_runtime_timers --status in_progress --progress 50 --reason "Server timers enabled and first cycles validated"
```

After the second clean cycle:

```powershell
python progress_ledger.py update --id wi_runtime_timers --status done --progress 100 --reason "Server timer layer stable for two cycles"
```
