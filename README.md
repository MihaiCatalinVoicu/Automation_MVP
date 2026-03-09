# automation-mvp

Minimal local orchestrator for:
- Composer as default executor
- premium as planner/reviewer escalation
- Telegram approvals
- SQLite audit trail
- separate worker process for durability

## What this version fixes from the demo
- no background `threading.Thread` inside FastAPI
- worker is a separate long-running process
- atomic run claiming to avoid double execution
- approval decisions map differently:
  - `RETRY_SAFE` -> rerun same path
  - `PLAN_B` -> reroute plan B path
  - `ASK_PREMIUM` -> reroute to premium planner/reviewer
  - `ABORT` -> abort run
- Telegram polling for local MVP
- authorization checks on `chat_id` and `user_id`
- SQLite configured with WAL + busy timeout
- repo registry with explicit local path and allowed check prefixes

## Start
```bash
python -m venv .venv
source .venv/bin/activate  # Linux/macOS
pip install -r requirements.txt
cp .env.example .env
# fill .env and repos.json
```

## Run services
Terminal 1:
```bash
uvicorn app:app --host 0.0.0.0 --port 8000
```

Terminal 2:
```bash
python worker.py
```

Terminal 3:
```bash
python telegram_poller.py
```

## Create a run
```bash
curl -X POST "http://127.0.0.1:8000/runs" \
  -H "Content-Type: application/json" \
  -d '{
    "repo": "crypto-bot",
    "goal": "Fix paper decision logging without changing strategy logic",
    "branch": "auto/fix-paper-logging",
    "task_type": "bugfix",
    "constraints": ["no strategy changes", "no live config changes"],
    "checks": ["pytest -q tests/test_overlay.py"],
    "preferred_executor": "composer"
  }'
```

In `EXECUTION_MODE=simulate`:
- the check fails 3 times
- Telegram sends an approval request
- you click one of the buttons
- worker picks up the run again
- the check passes according to the decision path

## Real execution integration
`runner.py -> invoke_executor()` is now wired to a Cursor headless CLI wrapper.

### How it routes
- `composer` stays the default executor
- `premium` is used only when `planner_agent` or `reviewer_agent` is set
- `ASK_PREMIUM` updates routing and the next retry runs with premium planning/review
- worker ownership is preserved during the whole run

### Required local setup
Install Cursor Agent CLI and authenticate it:

```bash
# macOS/Linux/WSL
curl https://cursor.com/install -fsS | bash

# Windows PowerShell
irm 'https://cursor.com/install?win32=true' | iex

agent status
```

If the binary is not on `PATH`, set `CURSOR_AGENT_BIN` in `.env` to the full executable path.

### Relevant `.env` settings
- `CURSOR_AGENT_BIN=agent`
- `CURSOR_COMPOSER_MODEL=` optional explicit model for composer runs
- `CURSOR_PREMIUM_MODEL=` optional explicit model for premium runs
- `CURSOR_FORCE_EXECUTOR=true` to allow file modifications
- `CURSOR_TRUST_WORKSPACE=true` for headless trusted workspace mode
- `CURSOR_TIMEOUT_SECONDS=1800`
- `CURSOR_SANDBOX=` optionally `enabled` or `disabled`
- `CURSOR_CLOUD_MODE=false`

### What the wrapper does
1. writes a task packet JSON to a temp file
2. optionally runs a premium planner step in `--mode plan`
3. runs the executor step with Cursor headless CLI in agent mode
4. optionally runs a premium reviewer step in `--mode ask`
5. records command/output metadata in `executor_result`

### Current limit
This is a real CLI integration, but it still assumes the local Cursor CLI is installed and authenticated. There is no broker/queue or remote execution layer yet.

## Operational policy and safe tasks

See `docs/OPERATIONAL_POLICY.md` for:
- validated flow summary
- safe task classes per repo profile
- allowed vs forbidden task types

Use `templates/safe_repo_task.json` for low-risk tasks (docs, read-only tooling, preflight).

## Validation battery (crypto Phase B)

Task type `validation_battery` runs a recipe of commands (replay, cost sensitivity, concentration),
extracts metrics, evaluates rules, outputs summary.json and verdict (PROMOTE/WARN/REJECT).

### Dry-run (standalone)
From automation-mvp root:
```bash
python recipe_runner.py recipes/crypto_phaseb_riskoff.json /path/to/crypto-bot/data/batch/run_top50_xxx
```
Or: `python -m recipe_runner recipes/crypto_phaseb_riskoff.json /path/to/run`
Output: `data/validation_artifacts/<run_name>/` with `command_logs/`, `summary.json`, `verdict.txt`.

### Via API
```bash
curl -X POST "http://127.0.0.1:8000/runs" \
  -H "Content-Type: application/json" \
  -d '{
    "repo": "crypto-bot",
    "goal": "Validate Phase B TREND_STRONG,RISK_OFF",
    "task_type": "validation_battery",
    "recipe": "recipes/crypto_phaseb_riskoff.json",
    "run_context": {"run_dir": "/root/crypto-bot-git/data/batch/run_top50_20260308_1551"}
  }'
```
Worker executes the recipe in the crypto-bot repo; artifacts go to `data/validation_artifacts/<run_id>/`.

## Limits of this MVP
- no queue broker yet
- no webhook deployment yet
- no OpenClaw integration yet
- no true Cursor wrapper yet, only a safe stub
