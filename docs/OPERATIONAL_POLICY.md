# Operational Policy

## Enforcement (policy engine)

Profiles are enforced server-side, not only advisory:

- **safe_docs**: Blocks tasks whose goal mentions forbidden scope (scoring, strategy, ingest, src/, etc.). Allows only docs/README/templates.
- **safe_readonly**: Same forbidden-scope check; checks must be read-only (py_compile, status/preflight scripts).
- **needs_approval_for_code**: Non-docs tasks require pre-execution approval (Telegram Allow/Abort) before executor runs.
- **premium_on_repeat_failures**: After repeat-failure threshold, auto-escalates to premium (no approval UI); applies ASK_PREMIUM logic directly.

Policy events: `policy_validation_passed`, `policy_escalation_required`, `policy_auto_escalation`.

## Strategy cross-reference policy

For non-trivial operational or code-change tasks, the task must be linked to the central strategy registry in `automation-mvp`.

Required metadata:
- `strategy_id` or `new_strategy_proposal`
- optional `category_id`
- `change_kind`

Cross-reference gate outcomes:
- `ALLOW`
- `ALLOW_WITH_REGISTRY_UPDATE`
- `REQUIRES_NEW_STRATEGY_ENTRY`
- `BLOCK_DUPLICATE`
- `BLOCK_UNSCOPED_CHANGE`

Use `python preflight_crossref.py ...` to resolve the task before submit when needed.

## Test matrix

To prove enforcement (not just "looks correct"):

```bash
python tests/test_policy_enforcement.py         # unit tests (policy engine)
python tests/test_policy_enforcement.py --api   # + API submit + events
```

Output: PASS/FAIL per rule. Covers: safe_docs rejects, safe_readonly checks, needs_approval_for_code, premium_on_repeat_failures, risk_level, and API events (`policy_validation_passed`, `run_created`).

## Validated flow (as of 2026-03)

- `RETRY_SAFE` end-to-end via Telegram
- `ASK_PREMIUM` end-to-end via Telegram
- Reroute premium real (planner/reviewer invoked on retry)
- Idempotent approvals
- Telegram callback ack best-effort (non-blocking on 400)

## Safe task classes (by repo profile)

For repos with `safe_readonly` or `safe_docs` profiles, only these task types are allowed:

| Class             | Allowed                                 | Forbidden                          |
|-------------------|-----------------------------------------|------------------------------------|
| docs              | README, docs/*, operational notes       | —                                  |
| read-only tooling | status scripts, preflight, validation   | —                                  |
| preflight/validation | preflight, doctor, summary scripts  | —                                  |
| operational summaries | reports, summaries, helper output  | —                                  |

**Explicitly forbidden** (do not submit to safe repos):

- scoring
- pipeline outcome logic
- ingest
- filters
- strategy logic
- anything that changes C10/B20/A20 verdicts or paper eval results

## Repo profiles

Profiles are defined per repo in `repos.json` and influence task guidance:

| Profile                   | Meaning                                                          |
|---------------------------|------------------------------------------------------------------|
| `safe_readonly`           | Only read-only utilities, no strategy/config changes             |
| `safe_docs`               | Docs and operational notes only                                 |
| `needs_approval_for_code` | Code changes require explicit approval (future enforcement)      |
| `premium_on_repeat_failures` | Suggest ASK_PREMIUM on repeated check failures (already default) |

## Template

Use `templates/safe_repo_task.json` for low-risk tasks. Adapt `repo`, `goal`, `branch`, and `checks` as needed.
