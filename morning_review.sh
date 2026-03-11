#!/usr/bin/env bash
set -Eeuo pipefail

ROOT="/root/automation-mvp"
VENV="$ROOT/.venv"
ENV_FILE="$ROOT/.env"

if [[ -f "$ENV_FILE" ]]; then
  # shellcheck disable=SC1090
  source "$ENV_FILE"
fi

DB_PATH_VALUE="${DB_PATH:-./data/orchestrator.db}"
if [[ "$DB_PATH_VALUE" = /* ]]; then
  DB="$DB_PATH_VALUE"
else
  DB="$ROOT/$DB_PATH_VALUE"
fi

cd "$ROOT"

if [[ ! -d "$VENV" ]]; then
  echo "[ERROR] Virtualenv not found: $VENV"
  exit 1
fi

source "$VENV/bin/activate"

if ! command -v sqlite3 >/dev/null 2>&1; then
  echo "[ERROR] sqlite3 is not installed"
  exit 1
fi

if [[ ! -f "$DB" ]]; then
  echo "[ERROR] DB not found: $DB"
  exit 1
fi

ts() {
  date +"%Y-%m-%d %H:%M:%S"
}

section() {
  echo
  echo "================================================================"
  echo "[$(ts)] $1"
  echo "================================================================"
}

run_python_job() {
  local job="$1"
  local script="${job%% *}"
  if [[ -f "$script" ]]; then
    echo "[$(ts)] Running: python $job"
    python $job
  else
    echo "[$(ts)] SKIP: $script not found"
  fi
}

section "Morning review started"
echo "[$(ts)] ROOT=$ROOT"
echo "[$(ts)] DB=$DB"
echo "[$(ts)] Python=$(python --version 2>&1)"

section "Refreshing lifecycle artifacts"
run_python_job "runtime_events_import_job.py --repos crypto-bot"
run_python_job "lifecycle_reconcile_job.py"
run_python_job "daily_lifecycle_report.py"

section "Daily lifecycle report"
REPORT_MD="$ROOT/data/reports/daily_lifecycle_report_latest.md"
if [[ -f "$REPORT_MD" ]]; then
  cat "$REPORT_MD"
else
  echo "[$(ts)] No daily report found at: $REPORT_MD"
fi

section "1) PROMOTE_TO_PAPER candidates"
sqlite3 -header -column "$DB" "
SELECT
  sc.case_id,
  sc.family,
  sc.strategy_id,
  sc.stage,
  ev.verdict_id,
  ev.manifest_id,
  ev.decision,
  ev.verdict_score,
  ev.created_at
FROM search_cases sc
JOIN edge_verdicts ev
  ON ev.verdict_id = sc.latest_verdict_id
WHERE ev.status = 'final'
  AND ev.decision = 'PROMOTE_TO_PAPER'
  AND sc.status NOT IN ('done', 'killed', 'archived')
  AND COALESCE(json_extract(ev.gate_results_json, '$.min_trades_pass'), 0) = 1
  AND COALESCE(json_extract(ev.gate_results_json, '$.cost_adjusted_edge_pass'), 0) = 1
  AND COALESCE(json_extract(ev.gate_results_json, '$.walkforward_pass'), 0) = 1
  AND COALESCE(json_extract(ev.gate_results_json, '$.leakage_check_pass'), 0) = 1
ORDER BY ev.created_at DESC;
"

section "2) Repeated / blocked manifests"
sqlite3 -header -column "$DB" "
SELECT
  manifest_id,
  case_id,
  adapter_type,
  execution_status,
  attempt_count,
  claimed_by,
  claimed_at,
  last_error
FROM experiment_manifests
WHERE execution_status IN ('failed', 'claimed', 'running')
ORDER BY attempt_count DESC, claimed_at DESC;
"

section "3) Governance waiting"
sqlite3 -header -column "$DB" "
WITH sent AS (
  SELECT case_id, MAX(created_at) AS sent_at
  FROM case_events
  WHERE event_type = 'research_governance_message_sent'
  GROUP BY case_id
),
applied AS (
  SELECT case_id, MAX(created_at) AS applied_at
  FROM case_events
  WHERE event_type = 'research_decision_applied'
  GROUP BY case_id
)
SELECT
  sc.case_id,
  sc.family,
  sc.stage,
  sent.sent_at,
  applied.applied_at,
  ROUND((julianday('now') - julianday(sent.sent_at)) * 24, 1) AS hours_waiting
FROM sent
JOIN search_cases sc
  ON sc.case_id = sent.case_id
LEFT JOIN applied
  ON applied.case_id = sent.case_id
WHERE sc.status NOT IN ('done', 'killed', 'archived')
  AND (applied.applied_at IS NULL OR applied.applied_at < sent.sent_at)
ORDER BY sent.sent_at DESC;
"

section "4) Recent final verdicts / suspicious metrics scan"
sqlite3 -header -column "$DB" "
SELECT
  ev.created_at,
  sc.case_id,
  sc.family,
  ev.verdict_id,
  ev.manifest_id,
  ev.decision,
  ev.dominant_failure_mode,
  json_extract(ev.metrics_snapshot_json, '$.trades') AS trades,
  json_extract(ev.metrics_snapshot_json, '$.profit_factor') AS pf,
  json_extract(ev.metrics_snapshot_json, '$.oos_profit_factor') AS oos_pf,
  json_extract(ev.metrics_snapshot_json, '$.max_drawdown_pct') AS dd_pct,
  ev.verdict_score
FROM edge_verdicts ev
JOIN search_cases sc
  ON sc.case_id = ev.case_id
WHERE ev.status = 'final'
ORDER BY ev.created_at DESC
LIMIT 30;
"

section "Open risk-debt work items"
python progress_ledger.py list --format markdown || true

section "Morning review finished"
