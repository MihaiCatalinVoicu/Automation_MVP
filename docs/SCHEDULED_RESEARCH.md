# Scheduled Research

`automation-mvp` can act as the thin server-side control plane for the month-1 edge research loop.

## Canonical model

- local machine: development, ad hoc debugging, smoke validation
- server: scheduled runs, artifact ownership, daily scoreboards, lifecycle evidence

## Built-in daily research schedules

- `breakout_momentum_daily`
- `spike_mean_reversion_daily`
- `cross_sectional_momentum_daily`

These schedules materialize `validation_battery` runs that execute family-specific
research recipes in the `crypto-bot` repo.

## Worker environment knobs

- `RESEARCH_SCHEDULE_INTERVAL_SECONDS`
- `STRATEGY_REVIEW_INTERVAL_SECONDS`
- `SHADOW_BOARD_INTERVAL_SECONDS`

If `RESEARCH_SCHEDULE_INTERVAL_SECONDS > 0`, the worker periodically materializes
due research runs into the normal `runs` table.

## API reporting

- `GET /research/schedules`
- `GET /research/artifacts`
- `GET /research/shadow-board`

## Artifact layout

- `data/validation_artifacts/<run_id>/`
- `data/registry_audits/`
- `data/strategy_reviews/`
- `data/shadow_recommendations/`

For month 1, recommendations remain `shadow-only`. No schedule should auto-promote
directly into paper or live execution.
