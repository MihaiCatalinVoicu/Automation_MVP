# Operational Policy

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
