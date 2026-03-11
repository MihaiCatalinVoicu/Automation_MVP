# Live Research Factory v1

This system runs as an always-on research/search orchestrator for edge discovery.
It is NOT a live trading executor.

## Scope

- Run disciplined research manifests continuously on server.
- Keep human governance in loop via Telegram.
- Promote only to `paper_candidate` (never direct live trading).

## Mandatory Guardrails

- Family whitelist via `RESEARCH_ALLOWED_FAMILIES`.
- Daily manifest budget via `RESEARCH_MAX_MANIFESTS_PER_DAY`.
- Per-case daily budget via `RESEARCH_MAX_MANIFESTS_PER_CASE_PER_DAY`.
- Promotion requires critical gates (`min_trades`, costs, walkforward, leakage).

## Recommended MVP Settings

Set environment variables:

```bash
RESEARCH_ALLOWED_FAMILIES=btc_structural_daily,breakout_momentum,oi_cascade,cross_sectional_momentum
RESEARCH_MAX_MANIFESTS_PER_CASE_PER_DAY=3
RESEARCH_MAX_MANIFESTS_PER_DAY=20
RESEARCH_GOVERNANCE_RETRY_INTERVAL_SECONDS=60
WORKER_POLL_INTERVAL_SECONDS=2
```

Start processes:

```bash
python worker.py
python telegram_poller.py
```

Optional retry runner (manual):

```bash
python scripts_send_pending_research_governance.py
```

## What is live

- Manifest scheduler and worker
- Adapters (`research_loop`, `policy_benchmark`)
- Verdict writer and case memory
- Telegram research governance sender + callback handling

## What is NOT live

- Live order execution
- Auto-promotion to live trading
- Unlimited strategy generation without budget and family constraints

