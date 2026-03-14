# Server Edge Search

This setup turns the existing edge-discovery pieces into a bounded server cycle:

- one always-on `manifest_worker`
- one timer-driven `mutation_cycle`
- one timer-driven `meta_search_report`
- one daily retention job for cheap and medium artifacts

## Control Plane Split

- `automation-mvp` remains the control plane for manifests, verdicts, mutation, reporting, and guardrails.
- `crypto-bot` remains the evaluator through `run_cohort_research.py` and the existing research loop adapter.

## Conservative Bootstrap

Use the existing Tier-1 families only:

- `trend_volatility_expansion`
- `pullback_in_trend`
- `relative_strength_rotation`

Suggested starting allocation on a small server:

- `trend_volatility_expansion`: `50%`
- `pullback_in_trend`: `30%`
- `relative_strength_rotation`: `20%`

Keep the current search surface small first:

- timeframe: `4h`
- universe: top `30`
- target throughput: `40-80` experiments/day

## Freeze Window

During the current execution freeze:

- no new families
- no new orchestrator fronts
- no portfolio-aware expansion
- no promotion automation shortcuts

Only work that proves convergence, runtime truth, baseline validity, cost reality, or forced verdicts should move.

## Required Services

- `edge-search-manifest-worker.service`
- `edge-search-mutation-cycle.timer`
- `edge-search-meta-report.timer`
- `edge-search-retention.timer`

The example units live in [D:/automation-mvp/ops/systemd](D:/automation-mvp/ops/systemd).

## Suggested Environment File

Copy [D:/automation-mvp/ops/systemd/edge-search.env.example](D:/automation-mvp/ops/systemd/edge-search.env.example) to something like:

```bash
/srv/automation-mvp/ops/systemd/edge-search.env
```

Important defaults:

- `RESEARCH_MAX_PENDING_MANIFESTS=200`
- `RESEARCH_TARGET_QUEUE_DEPTH_PER_WORKER=20`
- `RESEARCH_MIN_NEAR_MISS_SCORE_FOR_MUTATION=0.60`
- `RESEARCH_MIN_TRADES_FOR_MUTATION=80`
- `MUTATION_MAX_MANIFESTS_PER_CYCLE=10`
- `RESEARCH_MAX_MUTATION_BATCH_SIZE=6`
- `RESEARCH_MUTATION_RADIUS=0.10`
- `RESEARCH_ELITE_COUNT=1`
- `EDGE_SEARCH_TRIGGER_A_MIN_EXPERIMENTS=200`
- `EDGE_SEARCH_TRIGGER_B_MIN_EXPERIMENTS=1000`
- `EDGE_SEARCH_REQUIRE_MANUAL_PROMOTION=1`
- `EDGE_SEARCH_SHADOW_ONLY_DAYS=30`
- `EDGE_SEARCH_PROMOTION_MIN_DETAILS_CHARS=24`

## Repo Paths (repos.server.json)

`runtime_events_import_job.py` and `morning_review.sh` need server paths. Create:

```bash
cp repos.server.json.example repos.server.json
```

Edit `repos.server.json` if your paths differ. Crypto-bot must point to `/opt/crypto-bot/current` (the live deployment).

## Path Setup

Service units use `/srv/automation-mvp`. If the project lives elsewhere (e.g. `~/automation-mvp`), create a symlink:

```bash
sudo ln -s /root/automation-mvp /srv/automation-mvp
```

Verify:

```bash
test -x /srv/automation-mvp/.venv/bin/python && echo "OK" || echo "FAIL"
test -f /srv/automation-mvp/manifest_worker.py && echo "OK" || echo "FAIL"
```

## Install Sequence

```bash
cd ~/automation-mvp
# If project is in ~/automation-mvp: sudo ln -s /root/automation-mvp /srv/automation-mvp
cp ops/systemd/edge-search.env.example ops/systemd/edge-search.env

sudo cp ops/systemd/edge-search-manifest-worker.service /etc/systemd/system/
sudo cp ops/systemd/edge-search-mutation-cycle.service /etc/systemd/system/
sudo cp ops/systemd/edge-search-mutation-cycle.timer /etc/systemd/system/
sudo cp ops/systemd/edge-search-meta-report.service /etc/systemd/system/
sudo cp ops/systemd/edge-search-meta-report.timer /etc/systemd/system/
sudo cp ops/systemd/edge-search-retention.service /etc/systemd/system/
sudo cp ops/systemd/edge-search-retention.timer /etc/systemd/system/

sudo systemctl daemon-reload
sudo systemctl enable --now edge-search-manifest-worker.service
sudo systemctl enable --now edge-search-mutation-cycle.timer
sudo systemctl enable --now edge-search-meta-report.timer
sudo systemctl enable --now edge-search-retention.timer
```

## Manual Smoke Test

Run these once before letting timers run unattended:

```bash
cd /srv/automation-mvp
. .venv/bin/activate

python manifest_worker.py
python mutation_cycle.py --since-hours 72 --limit 10 --dry-run
python meta_search_report.py --loops-root data/research_loops --since-days 30
python research_retention.py --cheap-days 3 --medium-days 14 --dry-run
```

## First 48 Hours

The first goal is not alpha. It is stability.

Watch these metrics:

- queue depth
- manifests created vs completed
- duplicates skipped by `mutation_cycle`
- near misses selected
- family scores
- dominant failure motifs
- average runtime by validation level
- disk growth under `data/research_loops`

Use:

- `data/reports/mutation_cycle_latest.json`
- `data/reports/meta_search_report_latest.json`
- `data/reports/meta_search_report_latest.md`
- `data/reports/live_edge_search_review_latest.json`
- `data/reports/research_retention_latest.json`
- `journalctl -u edge-search-manifest-worker.service`

## Live Review Semantics

The bounded server loop now keeps an explicit runtime mode derived from the report and backlog state:

- `EXPLORE`: bootstrap / bounded fresh search
- `REVIEW`: enough evidence exists to inspect convergence, but not enough to expand
- `REFINE`: Trigger A passed, so compute should bias toward stronger families and near misses
- `SAFE_IDLE`: queue is saturated, so mutation pauses without changing trading/runtime state
- `FROZEN`: duplicate waste, backlog pressure, or missing convergence means proposal expansion must stop

The current mode and Trigger A-E readiness are exported in:

- `data/reports/meta_search_report_latest.json` under `live_edge_search`
- `data/reports/live_edge_search_review_latest.json`
- `data/reports/daily_ops_review_latest.json`
- `data/reports/weekly_evidence_pack_latest.json`

## Shadow-Only Promotion Policy

For month 1, recommendations remain shadow-only:

- `PROMOTE_TO_PAPER` must come from a manual source
- promotion requires written rationale
- promotion is blocked inside the configured shadow window

This is enforced through `approval_service.py` and the environment flags above.

## Stop Conditions

Pause unattended mutation if any of these appear repeatedly:

- pending backlog keeps rising and stays near the hard cap
- one family consumes most cheap-tier capacity without converting into stronger near misses
- duplicate fingerprints remain high despite dedup
- mutation cycle produces manifests faster than the worker drains them
- cheap-tier artifacts grow faster than retention can control

## Expected Early Behavior

Healthy early behavior looks like this:

- queue remains bounded
- expensive validations stay rare
- one or two families dominate useful near misses
- repeated fingerprints become visible in the report instead of silently wasting compute
- backlog gating skips proposal cycles when worker capacity is saturated
- Trigger A stays locked until the system shows real family structure instead of noise

## Deploy Runbook (local → server)

**Local (commit & push):**

```bash
cd automation-mvp
git add -A
git status
git commit -m "fix: DB migration near_miss_score, manifest-worker autostart, path docs"
git push
```

**Server (pull & restart):**

```bash
cd ~/automation-mvp
git pull

# Symlink if project is in ~ not /srv
sudo ln -sf /root/automation-mvp /srv/automation-mvp

# Copy updated units (if changed)
sudo cp ops/systemd/edge-search-manifest-worker.service /etc/systemd/system/
sudo systemctl daemon-reload

# Restart worker
sudo systemctl restart edge-search-manifest-worker.service

# Re-enable autostart after adding [Install]
sudo systemctl enable edge-search-manifest-worker.service
```

**Status & logs:**

```bash
systemctl status edge-search-manifest-worker.service
systemctl list-timers | grep edge-search
journalctl -u edge-search-manifest-worker.service -n 50 -f
journalctl -u edge-search-mutation-cycle.service -n 30 --no-pager
```

**Manual one-shot tests:**

```bash
cd ~/automation-mvp && . .venv/bin/activate
python mutation_cycle.py --since-hours 72 --limit 5 --dry-run
python daily_ops_review.py \
  --automation-report data/reports/meta_search_report_latest.json \
  --crypto-runtime-truth /root/crypto-bot-git/data/reports/runtime_truth_report_latest.json \
  --crypto-baseline /root/crypto-bot-git/data/reports/btc_structural_baseline_latest.json \
  --crypto-cost-gate /root/crypto-bot-git/data/reports/cost_sensitivity_latest.json \
  --stocks-verdict /root/stocks-bot-git/data/reports/winner_verdict_scorecard_latest.json
python weekly_evidence_pack.py \
  --automation-report data/reports/meta_search_report_latest.json \
  --crypto-runtime-truth /root/crypto-bot-git/data/reports/runtime_truth_report_latest.json \
  --crypto-baseline /root/crypto-bot-git/data/reports/btc_structural_baseline_latest.json \
  --crypto-cost-gate /root/crypto-bot-git/data/reports/cost_sensitivity_latest.json \
  --stocks-verdict /root/stocks-bot-git/data/reports/winner_verdict_scorecard_latest.json
sudo systemctl start edge-search-mutation-cycle.service
```

## Telegram & decision flow

The edge-search services **do not include** the Telegram poller. Flow with buttons works only if `telegram_poller.py` runs as a separate process:

- **manifest_worker** → runs manifests → **edge_verdict_writer** → when verdict needs human decision (e.g. `PROMOTE_TO_PAPER`, `ASK_PREMIUM_REVIEW`), sends message to Telegram via `send_research_governance_message`
- **telegram_poller** → polls for button clicks → **approval_service.apply_research_decision** → updates case and optionally creates new manifest

To keep Telegram flow active:

```bash
# Option A: tmux/screen
python telegram_poller.py

# Option B: systemd (add a unit for telegram_poller)
```

The logic (approval_service, telegram_decisions, policies) is unchanged. Only the process that *receives* the clicks must be running.

## Notes

- `mutation_cycle.py` prefers `next_batch_config.json` from the prior loop when available.
- config dedup is now canonicalized, so numeric formatting differences do not create fake novelty.
- manual follow-up manifests and automatic ones now both prefer the derived next-batch config when present.
