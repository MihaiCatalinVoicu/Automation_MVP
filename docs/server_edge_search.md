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

## Install Sequence

```bash
cd /srv/automation-mvp
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
- `data/reports/research_retention_latest.json`
- `journalctl -u edge-search-manifest-worker.service`

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

## Notes

- `mutation_cycle.py` prefers `next_batch_config.json` from the prior loop when available.
- config dedup is now canonicalized, so numeric formatting differences do not create fake novelty.
- manual follow-up manifests and automatic ones now both prefer the derived next-batch config when present.
