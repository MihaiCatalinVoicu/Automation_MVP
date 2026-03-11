# Lifecycle Timers

Template units for the minimal lifecycle automation layer:

- `runtime-events-import.service` / `.timer`
- `lifecycle-reconcile.service` / `.timer`
- `daily-lifecycle-report.service` / `.timer`

## Before enabling

1. Copy the files to your server, for example under `/etc/systemd/system/`.
2. Replace:
   - `/srv/automation-mvp`
   - `/srv/automation-mvp/.venv/bin/python`
3. Ensure `repos.json` points to valid runtime producer paths on that machine.

## Enable sequence

```bash
sudo systemctl daemon-reload
sudo systemctl enable --now runtime-events-import.timer
sudo systemctl enable --now lifecycle-reconcile.timer
sudo systemctl enable --now daily-lifecycle-report.timer
```

## Check status

```bash
systemctl list-timers --all | rg "runtime-events-import|lifecycle-reconcile|daily-lifecycle-report"
systemctl status runtime-events-import.service
systemctl status lifecycle-reconcile.service
systemctl status daily-lifecycle-report.service
```
