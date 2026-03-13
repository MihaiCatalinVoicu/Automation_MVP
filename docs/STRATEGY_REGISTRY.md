# Strategy Registry

Generated from central strategy registry in `D:\automation-mvp`.

## execution_realism

### `execution_cost_realism`

- Name: Execution cost realism
- Repo: crypto-bot
- Category: execution_realism
- Purpose: Ensure edge survives fees and slippage.
- Hypothesis: Strategies without healthy breakeven buffer are fantasy.
- Status: complete (100%)
- Operational status: active
- Verdict: KEEP
- Owner: mihai
- Last reviewed: 2026-03-10T14:48:23.059921+00:00
- Tags: cost, slippage, fees, breakeven

Files:
- `D:\crypto-bot\core\cost_model.py` [implementation]
- `D:\crypto-bot\scripts\cost_sensitivity.py` [validation]

Metrics / thresholds:
- `breakeven_bps` target `>=80` rule `promotion`

Watchlist:
- `breakeven_bps` trigger `breakeven_bps < 80` cadence `6h` -> `FREEZE`

Latest version:
- `v1`: Seed import from current repo state (KEEP)

## ml_overlay

### `ml_risk_overlay`

- Name: ML risk overlay
- Repo: crypto-bot
- Category: ml_overlay
- Purpose: Use ML as risk gate / size modifier rather than directional oracle.
- Hypothesis: ML can improve risk control if it reduces drawdown without emptying the flow.
- Status: partial (50%)
- Operational status: supporting
- Verdict: WATCH
- Owner: mihai
- Last reviewed: 2026-03-10T14:48:23.074839+00:00
- Tags: ml, overlay, risk_gate, p_highrisk
- Notes: Current state has overlapping ML paths and should be audited before expansion.

Files:
- `D:\crypto-bot\core\ml_risk.py` [implementation]  (shadow)
- `D:\crypto-bot\ml_gate.py` [shadow]  (shadow)
- `D:\crypto-bot\ml_risk_gate.py` [shadow]  (shadow)

Metrics / thresholds:
- `max_drawdown_pct` target `improve vs baseline` rule `promotion`
- `rows_after_filter` target `not collapse` rule `promotion`

Watchlist:
- `duplicate_ml_logic` trigger `legacy ml gates still active` cadence `weekly` -> `AUDIT`

Latest version:
- `v1`: Seed import from current repo state (WATCH)

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
- Last reviewed: 2026-03-10T14:48:23.078048+00:00
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

## quality_filter

### `atr_quality_filter`

- Name: ATR quality filter
- Repo: crypto-bot
- Category: quality_filter
- Purpose: Reject excessive volatility contexts via ATR caps.
- Hypothesis: ATR filtering can improve signal quality if it preserves enough density.
- Status: functional (75%)
- Operational status: active
- Verdict: WATCH
- Owner: mihai
- Last reviewed: 2026-03-10T14:48:23.046925+00:00
- Tags: atr, volatility, quality, max_atr

Files:
- `D:\crypto-bot\scripts\portfolio_replay.py` [validation]
- `D:\crypto-bot\scripts\micro_autoresearch.py` [research]

Metrics / thresholds:
- `rows_after_filter` target `>=40` rule `search-gate`
- `top3_share_pct` target `<=70` rule `robustness`

Watchlist:
- `rows_after_filter` trigger `rows_after_filter < 40` cadence `weekly` -> `FREEZE`

Latest version:
- `v1`: Seed import from current repo state (WATCH)

### `event_candidate_stamp`

- Name: Event candidate stamp
- Repo: stocks-bot
- Category: quality_filter
- Purpose: Tag upstream events as strategy candidates using config-driven filters.
- Hypothesis: Pre-stamping candidates reduces downstream noise while preserving cohort signal.
- Status: functional (75%)
- Operational status: active
- Verdict: KEEP
- Owner: mihai
- Last reviewed: 2026-03-10T14:48:23.084308+00:00
- Tags: candidate, omb, roles, strict_cluster

Files:
- `D:\stocks-bot\src\ingest\build_events_job.py` [implementation]
- `D:\stocks-bot\src\config.py` [configuration]

Metrics / thresholds:
- `candidate_count` target `stable` rule `operational`

Watchlist:
- `candidate_count` trigger `candidate count collapses` cadence `weekly` -> `WATCH`

Latest version:
- `v1`: Seed import from current repo state (KEEP)

### `generic_risk_gate`

- Name: Generic risk gate
- Repo: stocks-bot
- Category: quality_filter
- Purpose: Apply rule-based risk rejection on liquidity, spread, price, and event features.
- Hypothesis: A generic risk gate provides a defensible baseline but may now be secondary to winner cohorts.
- Status: functional (75%)
- Operational status: supporting
- Verdict: WATCH
- Owner: mihai
- Last reviewed: 2026-03-10T14:48:23.087413+00:00
- Tags: risk_gate, liquidity, spread, baseline

Files:
- `D:\stocks-bot\src\scoring\risk_gate.py` [implementation]

Metrics / thresholds:
- `pass_rate` target `explainable` rule `governance`

Watchlist:
- `relevance` trigger `winner path bypasses gate` cadence `monthly` -> `AUDIT`

Latest version:
- `v1`: Seed import from current repo state (WATCH)

## ranking

### `generic_ranker`

- Name: Generic ranker
- Repo: stocks-bot
- Category: ranking
- Purpose: Rank generic insider ideas via rule-based scoring.
- Hypothesis: Rule-based ranking is useful as a baseline but must remain explainable and auditable.
- Status: functional (75%)
- Operational status: supporting
- Verdict: WATCH
- Owner: mihai
- Last reviewed: 2026-03-10T14:48:23.090756+00:00
- Tags: ranker, scoring, ideas, baseline

Files:
- `D:\stocks-bot\src\scoring\ranker.py` [implementation]
- `D:\stocks-bot\src\scoring\explain.py` [implementation]

Metrics / thresholds:
- `precision` target `improve vs baseline` rule `validation`

Watchlist:
- `explainability` trigger `scoring cannot be explained` cadence `monthly` -> `FREEZE`

Latest version:
- `v1`: Seed import from current repo state (WATCH)

## regime_filter

### `btc_regime_gate`

- Name: BTC regime gate
- Repo: crypto-bot
- Category: regime_filter
- Purpose: Restrict scanning/execution to favorable BTC structural regimes.
- Hypothesis: Macro context filter should reduce poor-context entries and drawdown.
- Status: functional (75%)
- Operational status: active
- Verdict: KEEP
- Owner: mihai
- Last reviewed: 2026-03-10T14:48:23.040584+00:00
- Tags: btc, regime, trend_strong, filter

Files:
- `D:\crypto-bot\core\regime_gate.py` [implementation]
- `D:\crypto-bot\main.py` [runtime]
- `D:\crypto-bot\scripts\regime_opportunity_report.py` [validation]

Metrics / thresholds:
- `max_drawdown_pct` target `>=-15` rule `safety`
- `rows_after_filter` target `>=40` rule `density`

Watchlist:
- `rows_after_filter` trigger `rows_after_filter < 40` cadence `weekly` -> `WATCH`

Latest version:
- `v1`: Seed import from current repo state (KEEP)

### `btc_risk_off_filter`

- Name: BTC risk-off filter
- Repo: crypto-bot
- Category: regime_filter
- Purpose: Use BTC daily EMA200 risk-off logic to filter or constrain entries.
- Hypothesis: BTC risk-off state improves replay quality but may overfilter and concentrate edge.
- Status: functional (75%)
- Operational status: active
- Verdict: WATCH
- Owner: mihai
- Last reviewed: 2026-03-10T14:48:23.043786+00:00
- Tags: btc, risk_off, ema200, filter

Files:
- `D:\crypto-bot\main.py` [runtime]
- `D:\crypto-bot\scripts\portfolio_replay.py` [validation]
- `D:\crypto-bot\scripts\phaseb_incremental.sh` [validation]

Metrics / thresholds:
- `profit_factor` target `>=1.3` rule `promotion`
- `top3_share_pct` target `<=80` rule `concentration`

Watchlist:
- `top3_share_pct` trigger `top3_share_pct > 80` cadence `6h` -> `FREEZE`
- `profit_factor` trigger `PF < 1.3` cadence `6h` -> `TUNE`

Latest version:
- `v1`: Seed import from current repo state (WATCH)

## research_family

### `breakout_momentum`

- Name: Breakout Momentum family
- Repo: crypto-bot
- Category: research_family
- Purpose: Month-1 research family for continuation breakouts on 4h Top-50 data.
- Hypothesis: Simple breakout continuation on 4h may survive costs if signal density and concentration are acceptable.
- Status: functional (75%)
- Operational status: research_active
- Verdict: WATCH
- Owner: mihai
- Last reviewed: 2026-03-10T14:48:23.125468+00:00
- Tags: month1, research, breakout, momentum, 4h

Files:
- `core/ideas.py` [implementation]
- `scripts/run_cohort_research.py` [orchestration]
- `configs/discovery_profiles_breakout_momentum.json` [configuration]
- `scripts/profile_discovery.py` [validation]

Metrics / thresholds:
- `candidate_count` target `>=1` rule `shadow-screen`
- `window_passes` target `>=1` rule `shadow-screen`
- `top3_share_pct` target `<=70` rule `robustness`

Watchlist:
- `candidate_count` trigger `candidate_count < 1` cadence `daily` -> `REJECT`
- `window_passes` trigger `window_passes < 1` cadence `daily` -> `WATCH`

Latest version:
- `v1`: Seed import from current repo state (WATCH)

### `cross_sectional_momentum`

- Name: Cross-sectional Momentum family
- Repo: crypto-bot
- Category: research_family
- Purpose: Month-1 research family for ranking Top-50 coins by relative strength and replaying the strongest candidates.
- Hypothesis: Relative-strength leadership on 4h may persist long enough to produce repeatable edge when ranked cross-sectionally.
- Status: functional (75%)
- Operational status: research_active
- Verdict: WATCH
- Owner: mihai
- Last reviewed: 2026-03-10T14:48:23.132055+00:00
- Tags: month1, research, cross_sectional, momentum, top50

Files:
- `scripts/generate_cross_sectional_batch.py` [implementation]
- `scripts/run_cohort_research.py` [orchestration]
- `configs/discovery_profiles_cross_sectional_momentum.json` [configuration]
- `scripts/profile_discovery.py` [validation]

Metrics / thresholds:
- `candidate_count` target `>=1` rule `shadow-screen`
- `window_passes` target `>=1` rule `shadow-screen`
- `top3_share_pct` target `<=70` rule `robustness`

Watchlist:
- `candidate_count` trigger `candidate_count < 1` cadence `daily` -> `REJECT`
- `window_passes` trigger `window_passes < 1` cadence `daily` -> `WATCH`

Latest version:
- `v1`: Seed import from current repo state (WATCH)

### `spike_mean_reversion`

- Name: Spike Mean Reversion family
- Repo: crypto-bot
- Category: research_family
- Purpose: Month-1 research family for long mean reversion after downside 4h liquidation-like flushes.
- Hypothesis: Oversold high-volume downside spikes may mean-revert over the next 1-3 bars on 4h data.
- Status: functional (75%)
- Operational status: research_active
- Verdict: WATCH
- Owner: mihai
- Last reviewed: 2026-03-10T14:48:23.128890+00:00
- Tags: month1, research, mean_reversion, spike, 4h

Files:
- `core/ideas.py` [implementation]
- `scripts/run_cohort_research.py` [orchestration]
- `configs/discovery_profiles_spike_mean_reversion.json` [configuration]
- `scripts/profile_discovery.py` [validation]

Metrics / thresholds:
- `candidate_count` target `>=1` rule `shadow-screen`
- `window_passes` target `>=1` rule `shadow-screen`
- `max_drawdown_pct` target `>=-25` rule `early-screen`

Watchlist:
- `candidate_count` trigger `candidate_count < 1` cadence `daily` -> `REJECT`
- `window_passes` trigger `window_passes < 1` cadence `daily` -> `WATCH`

Latest version:
- `v1`: Seed import from current repo state (WATCH)

## risk_sizing

### `cooldown_logic`

- Name: Cooldown logic
- Repo: crypto-bot
- Category: risk_sizing
- Purpose: Reduce clustering and low-quality immediate re-entries.
- Hypothesis: Cooldown reduces repeated poor entries and concentration of losses.
- Status: partial (50%)
- Operational status: active
- Verdict: WATCH
- Owner: mihai
- Last reviewed: 2026-03-10T14:48:23.053357+00:00
- Tags: cooldown, reentry, clustering

Files:
- `D:\crypto-bot\main.py` [runtime]
- `D:\crypto-bot\scripts\portfolio_replay.py` [validation]

Metrics / thresholds:
- `max_losing_streak` target `<=7` rule `safety`

Watchlist:
- `max_losing_streak` trigger `max_losing_streak > 7` cadence `weekly` -> `TUNE`

Latest version:
- `v1`: Seed import from current repo state (WATCH)

### `max_positions_cap`

- Name: Max positions cap
- Repo: crypto-bot
- Category: risk_sizing
- Purpose: Control portfolio overlap and diversify edge.
- Hypothesis: Max positions changes the distribution of captured opportunities and concentration.
- Status: partial (50%)
- Operational status: active
- Verdict: WATCH
- Owner: mihai
- Last reviewed: 2026-03-10T14:48:23.056494+00:00
- Tags: max_positions, portfolio, overlap

Files:
- `D:\crypto-bot\main.py` [runtime]
- `D:\crypto-bot\scripts\portfolio_replay.py` [validation]

Metrics / thresholds:
- `top3_share_pct` target `<=70` rule `robustness`

Watchlist:
- `top3_share_pct` trigger `top3_share_pct > 70` cadence `weekly` -> `WATCH`

Latest version:
- `v1`: Seed import from current repo state (WATCH)

### `risk_per_trade_sizing`

- Name: Risk-per-trade sizing
- Repo: crypto-bot
- Category: risk_sizing
- Purpose: Scale exposure per trade without changing signal logic.
- Hypothesis: Sizing should amplify valid edge, not create it.
- Status: complete (100%)
- Operational status: active
- Verdict: KEEP
- Owner: mihai
- Last reviewed: 2026-03-10T14:48:23.050141+00:00
- Tags: sizing, risk_per_trade, exposure

Files:
- `D:\crypto-bot\risk_manager.py` [implementation]
- `D:\crypto-bot\scripts\portfolio_replay.py` [validation]

Metrics / thresholds:
- `max_drawdown_pct` target `>=-15` rule `safety`

Watchlist:
- `max_drawdown_pct` trigger `max_drawdown_pct < -15` cadence `weekly` -> `TUNE`

Latest version:
- `v1`: Seed import from current repo state (KEEP)

## runtime_filter

### `strategy_v1_runtime_filter`

- Name: Strategy V1 runtime filter
- Repo: stocks-bot
- Category: runtime_filter
- Purpose: Operational paper-trading filter used by paper_job and related commands.
- Hypothesis: A simple runtime filter can support paper accounting even if research moves toward winner cohorts.
- Status: functional (75%)
- Operational status: active
- Verdict: WATCH
- Owner: mihai
- Last reviewed: 2026-03-10T14:48:23.094097+00:00
- Tags: strategy_v1, paper, runtime

Files:
- `D:\stocks-bot\src\strategy\strategy_v1.py` [implementation]
- `D:\stocks-bot\src\strategy\paper_job.py` [runtime]

Metrics / thresholds:
- `candidate_count` target `stable` rule `operational`

Watchlist:
- `alignment` trigger `runtime filter diverges from canonical cohorts` cadence `weekly` -> `AUDIT`

Latest version:
- `v1`: Seed import from current repo state (WATCH)

## setup

### `breakout_setup`

- Name: Breakout setup
- Repo: crypto-bot
- Category: setup
- Purpose: Capture strong continuation moves in favorable regime.
- Hypothesis: Breakout signals produce tradable edge when paired with regime/risk filters.
- Status: functional (75%)
- Operational status: active
- Verdict: WATCH
- Owner: mihai
- Last reviewed: 2026-03-10T14:48:23.029326+00:00
- Tags: breakout, entry, setup, trend

Files:
- `D:\crypto-bot\core\ideas.py` [implementation]
- `D:\crypto-bot\main.py` [runtime]
- `D:\crypto-bot\paper_engine\runner.py` [paper-runtime]

Metrics / thresholds:
- `profit_factor` target `>=1.3` rule `promotion`
- `top3_share_pct` target `<=70` rule `robustness`

Watchlist:
- `profit_factor` trigger `PF < 1.3` cadence `weekly` -> `TUNE`
- `top3_share_pct` trigger `top3_share_pct > 70` cadence `weekly` -> `FREEZE`

Latest version:
- `v1`: Seed import from current repo state (WATCH)

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
- Last reviewed: 2026-03-10T14:48:23.034128+00:00
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

### `pullback_v2_setup`

- Name: Pullback V2 setup
- Repo: crypto-bot
- Category: setup
- Purpose: Refined pullback logic used by batch/paper paths.
- Hypothesis: A more explicit pullback variant may behave differently from legacy runtime pullback.
- Status: partial (50%)
- Operational status: supporting
- Verdict: WATCH
- Owner: mihai
- Last reviewed: 2026-03-10T14:48:23.037455+00:00
- Tags: pullbackv2, pullback, paper-engine

Files:
- `D:\crypto-bot\core\ideas.py` [implementation]
- `D:\crypto-bot\paper_engine\config.py` [configuration]

Metrics / thresholds:
- `rows_after_filter` target `>=40` rule `search-gate`

Watchlist:
- `alignment` trigger `runtime and paper disagree` cadence `weekly` -> `AUDIT`

Latest version:
- `v1`: Seed import from current repo state (WATCH)

## signal_source

### `sec_form4_ingest`

- Name: SEC Form 4 ingest
- Repo: stocks-bot
- Category: signal_source
- Purpose: Build event-level insider transaction signals from Form 4 filings.
- Hypothesis: Reliable insider-event ingest is the base signal source for all downstream strategy layers.
- Status: functional (75%)
- Operational status: active
- Verdict: KEEP
- Owner: mihai
- Last reviewed: 2026-03-10T14:48:23.081277+00:00
- Tags: sec, form4, ingest, events

Files:
- `D:\stocks-bot\src\ingest\build_events_job.py` [implementation]
- `D:\stocks-bot\src\features\event_aggregates.py` [implementation]

Metrics / thresholds:
- `coverage` target `maintain` rule `operational`
- `freshness` target `maintain` rule `operational`

Watchlist:
- `coverage` trigger `coverage degrades materially` cadence `weekly` -> `WATCH`

Latest version:
- `v1`: Seed import from current repo state (KEEP)

## validation_tooling

### `c10_candidate_audit`

- Name: C10 candidate audit
- Repo: stocks-bot
- Category: validation_tooling
- Purpose: Audit operational candidate set for the current winner cohort.
- Hypothesis: Forward candidate audit is necessary before trusting paper/live promotion.
- Status: functional (75%)
- Operational status: active
- Verdict: KEEP
- Owner: mihai
- Last reviewed: 2026-03-10T14:48:23.115081+00:00
- Tags: c10, candidate_audit, paper_eval

Files:
- `D:\stocks-bot\src\backtest\c10_candidate_audit.py` [implementation]

Metrics / thresholds:
- `candidate_quality` target `stable` rule `validation`

Watchlist:
- `candidate_quality` trigger `audit quality weakens` cadence `weekly` -> `WATCH`

Latest version:
- `v1`: Seed import from current repo state (KEEP)

### `cohort_baseline_validation`

- Name: Cohort baseline validation
- Repo: stocks-bot
- Category: validation_tooling
- Purpose: Validate winner cohorts under constrained equal-weight baselines.
- Hypothesis: Winner cohorts must outperform simple baselines to be meaningful.
- Status: functional (75%)
- Operational status: active
- Verdict: KEEP
- Owner: mihai
- Last reviewed: 2026-03-10T14:48:23.107594+00:00
- Tags: cohort_baseline, validation, equal_weight

Files:
- `D:\stocks-bot\src\backtest\cohort_baseline.py` [implementation]

Metrics / thresholds:
- `20d_excess` target `> baseline` rule `validation`

Watchlist:
- `20d_excess` trigger `baseline edge disappears` cadence `weekly` -> `FREEZE`

Latest version:
- `v1`: Seed import from current repo state (KEEP)

### `concentration_check`

- Name: Concentration check
- Repo: crypto-bot
- Category: validation_tooling
- Purpose: Reject fragile edge dominated by a few symbols or outlier trades.
- Hypothesis: Healthy edge should not be monopolized by a tiny subset of symbols/trades.
- Status: complete (100%)
- Operational status: active
- Verdict: KEEP
- Owner: mihai
- Last reviewed: 2026-03-10T14:48:23.063344+00:00
- Tags: concentration, top3, outliers, robustness

Files:
- `D:\crypto-bot\scripts\concentration_check.py` [implementation]

Metrics / thresholds:
- `top3_share_pct` target `<=70` rule `promotion`
- `top5_trades_pct` target `<=80` rule `watch`

Watchlist:
- `top3_share_pct` trigger `top3_share_pct > 70` cadence `6h` -> `FREEZE`

Latest version:
- `v1`: Seed import from current repo state (KEEP)

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
- Last reviewed: 2026-03-10T14:48:23.121222+00:00
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

### `micro_autoresearch`

- Name: Micro autoresearch
- Repo: crypto-bot
- Category: validation_tooling
- Purpose: Run tiny offline sweeps to invalidate fragile parameter candidates quickly.
- Hypothesis: A microscopic search tool can expose false-positive parameter winners without touching core logic.
- Status: functional (75%)
- Operational status: active
- Verdict: WATCH
- Owner: mihai
- Last reviewed: 2026-03-10T14:48:23.071121+00:00
- Tags: micro_autoresearch, search, offline, sweep

Files:
- `D:\crypto-bot\scripts\micro_autoresearch.py` [implementation]
- `D:\crypto-bot\configs\search_space.yaml` [configuration]

Metrics / thresholds:
- `trades_executed` target `>=20` rule `gate`
- `rows_after_filter` target `>=40` rule `gate`
- `top3_share_pct` target `<=70` rule `gate`

Watchlist:
- `score` trigger `no valid configs across runs` cadence `weekly` -> `FREEZE`

Latest version:
- `v1`: Seed import from current repo state (WATCH)

### `paper_eval_accounting`

- Name: Paper eval accounting
- Repo: stocks-bot
- Category: validation_tooling
- Purpose: Operational paper-style accounting engine for forward evaluation.
- Hypothesis: Accounting correctness is required before any forward result can be trusted.
- Status: functional (75%)
- Operational status: active
- Verdict: KEEP
- Owner: mihai
- Last reviewed: 2026-03-10T14:48:23.118262+00:00
- Tags: paper_eval, accounting, forward

Files:
- `D:\stocks-bot\src\strategy\paper_job.py` [implementation]
- `D:\stocks-bot\tests\test_paper_eval_accounting.py` [test]

Metrics / thresholds:
- `accounting_integrity` target `pass` rule `validation`

Watchlist:
- `accounting_integrity` trigger `paper accounting drift/failures` cadence `weekly` -> `FREEZE`

Latest version:
- `v1`: Seed import from current repo state (KEEP)

### `validation_battery`

- Name: Validation battery
- Repo: shared
- Category: validation_tooling
- Purpose: Run replay + cost + concentration batteries and emit structured verdicts.
- Hypothesis: Repeated, structured validation beats ad-hoc terminal interpretation.
- Status: functional (75%)
- Operational status: active
- Verdict: KEEP
- Owner: mihai
- Last reviewed: 2026-03-10T14:48:23.066415+00:00
- Tags: validation_battery, recipe_runner, verdict

Files:
- `D:\automation-mvp\recipe_runner.py` [implementation]
- `D:\automation-mvp\recipes\crypto_phaseb_riskoff.json` [recipe]
- `D:\automation-mvp\recipes\crypto_phaseb_rangechop.json` [recipe]

Metrics / thresholds:
- `verdict` target `PROMOTE/WARN/REJECT` rule `output`

Watchlist:
- `recipe_health` trigger `recipe failures or stale metrics` cadence `daily` -> `WATCH`

Latest version:
- `v1`: Seed import from current repo state (KEEP)

### `winner_sensitivity_validation`

- Name: Winner sensitivity validation
- Repo: stocks-bot
- Category: validation_tooling
- Purpose: Stress-test winner cohorts under slots/cost/candidate-cap variations.
- Hypothesis: Robust winner cohorts should survive realistic operational perturbations.
- Status: functional (75%)
- Operational status: active
- Verdict: KEEP
- Owner: mihai
- Last reviewed: 2026-03-10T14:48:23.111083+00:00
- Tags: winner_sensitivity, validation, costs

Files:
- `D:\stocks-bot\src\backtest\winner_sensitivity.py` [implementation]

Metrics / thresholds:
- `cost_survival` target `positive` rule `validation`

Watchlist:
- `cost_survival` trigger `cost sensitivity degrades` cadence `weekly` -> `TUNE`

Latest version:
- `v1`: Seed import from current repo state (KEEP)

## winner_cohort

### `a20_reference`

- Name: A20 premium reference
- Repo: stocks-bot
- Category: winner_cohort
- Purpose: Premium/reference cohort used as upper benchmark, not default operational target.
- Hypothesis: A20 serves as reference rather than the immediate practical profile.
- Status: functional (75%)
- Operational status: supporting
- Verdict: WATCH
- Owner: mihai
- Last reviewed: 2026-03-10T14:48:23.104380+00:00
- Tags: a20, premium, benchmark

Files:
- `D:\stocks-bot\configs\canonical_profiles.yaml` [configuration]
- `D:\stocks-bot\docs\WINNER_VERDICT.md` [decision-record]

Metrics / thresholds:
- `benchmark_gap` target `track vs c10` rule `validation`

Watchlist:
- `benchmark_gap` trigger `A20 no longer informative` cadence `monthly` -> `FREEZE`

Latest version:
- `v1`: Seed import from current repo state (WATCH)

### `b20_cohort`

- Name: B20 winner cohort
- Repo: stocks-bot
- Category: winner_cohort
- Purpose: Backup/reference winner cohort compared against C10.
- Hypothesis: B20 may remain a valid benchmark even if not the primary operating profile.
- Status: functional (75%)
- Operational status: supporting
- Verdict: WATCH
- Owner: mihai
- Last reviewed: 2026-03-10T14:48:23.100939+00:00
- Tags: b20, winner, cohort, benchmark

Files:
- `D:\stocks-bot\src\backtest\winner_sensitivity.py` [validation]
- `D:\stocks-bot\configs\canonical_profiles.yaml` [configuration]

Metrics / thresholds:
- `baseline_quality` target `benchmark` rule `validation`

Watchlist:
- `benchmark_value` trigger `no longer useful as benchmark` cadence `monthly` -> `FREEZE`

Latest version:
- `v1`: Seed import from current repo state (WATCH)

### `c10_cohort`

- Name: C10 winner cohort
- Repo: stocks-bot
- Category: winner_cohort
- Purpose: Primary operational candidate cohort for current paper validation.
- Hypothesis: Officer/director cohort with low OMB threshold provides better operational candidate quality.
- Status: functional (75%)
- Operational status: active
- Verdict: WATCH
- Owner: mihai
- Last reviewed: 2026-03-10T14:48:23.097790+00:00
- Tags: c10, winner, cohort, paper

Files:
- `D:\stocks-bot\src\backtest\cohort_baseline.py` [validation]
- `D:\stocks-bot\src\backtest\c10_candidate_audit.py` [validation]
- `D:\stocks-bot\configs\canonical_profiles.yaml` [configuration]
- `D:\stocks-bot\docs\WINNER_VERDICT.md` [decision-record]

Metrics / thresholds:
- `paper_eval_quality` target `maintain` rule `promotion`
- `candidate_count` target `maintain` rule `operational`

Watchlist:
- `paper_eval_quality` trigger `paper candidate quality degrades` cadence `weekly` -> `TUNE`

Latest version:
- `v1`: Seed import from current repo state (WATCH)

## Runtime Audit Findings

### `crypto-bot`

- Unmapped live logic: 69
- Shadow/duplicate logic: 11
- Dead registry links: 0
- Unmapped: `core/__init__.py`, `core/exit_engine.py`, `core/features.py`, `core/heartbeat.py`, `core/model.py`
- Shadow/Duplicate: `core/decision_log.py`, `core/decision_log.py`, `core/ideas.py`, `core/ml_risk.py`, `core/ml_risk.py`

### `stocks-bot`

- Unmapped live logic: 73
- Shadow/duplicate logic: 3
- Dead registry links: 0
- Unmapped: `src/ingest/__init__.py`, `src/ingest/contracts.py`, `src/ingest/form4_downloader.py`, `src/ingest/form4_parser.py`, `src/ingest/health_check.py`
- Shadow/Duplicate: `src/eval/eval_job.py`, `src/ideas/idea_job.py`, `src/sim/sim_job.py`
