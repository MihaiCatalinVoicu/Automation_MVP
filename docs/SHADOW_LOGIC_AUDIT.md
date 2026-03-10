# Shadow Logic Audit

Generated from central strategy registry in `D:\automation-mvp`.

## observability

### `legacy_decision_logging`

- Name: Legacy decision logging overlap
- Repo: crypto-bot
- Category: observability
- Purpose: Track overlapping CSV and JSONL decision logs that may no longer be consistent.
- Hypothesis: Logging duplication creates drift and hides runtime truth.
- Status: shadow (25%)
- Operational status: shadow
- Verdict: REMOVE
- Owner: mihai
- Last reviewed: 2026-03-10T10:41:52.291624+00:00
- Tags: logging, decision_log, shadow

Files:
- `D:\crypto-bot\trade_logger.py` [implementation]  (shadow)
- `D:\crypto-bot\core\decision_log.py` [implementation]  (shadow)
- `D:\crypto-bot\ml_pipeline.py` [implementation]  (shadow)

Metrics / thresholds:
- `consistency` target `single runtime truth` rule `audit`

Watchlist:
- `shadow_logging` trigger `duplicate decision logs persist` cadence `weekly` -> `REMOVE`

Latest version:
- `v1`: Seed import from current repo state (REMOVE)

## setup

### `pullback_setup`

- Name: Pullback setup
- Repo: crypto-bot
- Category: setup
- Purpose: Enter retracements inside trend context.
- Hypothesis: Pullback entries improve fill quality but require cleaner context control.
- Status: partial (50%)
- Operational status: shadow
- Verdict: WATCH
- Owner: mihai
- Last reviewed: 2026-03-10T10:41:52.246308+00:00
- Tags: pullback, entry, setup
- Notes: Exists in multiple forms (`Pullback`, `PullbackV2`) and needs consolidation.

Files:
- `D:\crypto-bot\main.py` [runtime]  (shadow)
- `D:\crypto-bot\core\ideas.py` [implementation]  (shadow)

Metrics / thresholds:
- `trades_executed` target `>=20` rule `sample-size`
- `max_drawdown_pct` target `>=-15` rule `safety`

Watchlist:
- `shadow_logic` trigger `duplicate pullback logic persists` cadence `weekly` -> `AUDIT`

Latest version:
- `v1`: Seed import from current repo state (WATCH)

## validation_tooling

### `legacy_sim_eval_stack`

- Name: Legacy ideas sim eval stack
- Repo: stocks-bot
- Category: validation_tooling
- Purpose: Older generic ideas -> sim -> eval pipeline kept as baseline/legacy path.
- Hypothesis: Legacy stack may remain useful as baseline but is no longer the canonical operating layer.
- Status: shadow (25%)
- Operational status: shadow
- Verdict: FREEZE
- Owner: mihai
- Last reviewed: 2026-03-10T10:41:52.338237+00:00
- Tags: legacy, ideas, sim, eval, shadow

Files:
- `D:\stocks-bot\src\sim\sim_job.py` [implementation]  (shadow)
- `D:\stocks-bot\src\eval\eval_job.py` [implementation]  (shadow)
- `D:\stocks-bot\src\ideas\idea_job.py` [implementation]  (shadow)

Metrics / thresholds:
- `relevance` target `baseline only` rule `audit`

Watchlist:
- `shadow_logic` trigger `legacy stack still drives decisions` cadence `monthly` -> `REMOVE`

Latest version:
- `v1`: Seed import from current repo state (FREEZE)
