# Assistant Cross-Reference Workflow

This document defines the mandatory workflow for any non-trivial operational or code-change request.

## Scope

Apply this workflow before:
- code edits
- new scripts/tools
- config changes
- validation experiments
- operational/promotional decisions

Do not apply it to trivial factual Q&A.

## Required fields

Every non-trivial task should resolve to:
- `repo`
- `goal`
- `change_kind`
- `strategy_id` or `new_strategy_proposal`
- optional `category_id`

## Workflow

1. Identify the affected repo and summarize the requested change.
2. Run the preflight gate:
   ```bash
   python preflight_crossref.py --repo <repo> --goal "<goal>" --change-kind <change_kind> [--strategy-id <id>] [--category-id <category>] [--new-strategy-proposal "<name>"]
   ```
3. Interpret the decision:
   - `ALLOW`: proceed
   - `ALLOW_WITH_REGISTRY_UPDATE`: proceed, but log/update registry metadata
   - `REQUIRES_NEW_STRATEGY_ENTRY`: create a new strategy proposal first
   - `BLOCK_DUPLICATE`: do not create parallel logic; link to the existing strategy
   - `BLOCK_UNSCOPED_CHANGE`: stop until the task is linked to a strategy/category
4. When the change affects behavior, ensure it creates or updates:
   - `change_log`
   - optionally `strategy_version`
5. After execution or validation, record outcomes against the same strategy.

## Anti-drift rules

- No new script without a strategy/category link.
- No behavioral change without a `change_log` entry.
- If existing runtime logic has no registry entry, mark it `shadow` and audit it.
- Mirror docs in `crypto-bot` and `stocks-bot` are generated outputs, not canonical sources.
