#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
from collections import Counter
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

from db import (
    get_last_maintenance_job_run,
    get_conn,
    init_db,
    record_maintenance_job_run,
    upsert_family_budget_state,
)
from edge_search_state import persist_live_edge_search_review
from family_registry import as_dict, list_family_definitions, sync_family_registry_db
from policy_benchmark import build_benchmark


def _safe_float(value: Any) -> float:
    try:
        return float(value)
    except (TypeError, ValueError):
        return 0.0


def _safe_int(value: Any) -> int:
    try:
        return int(value)
    except (TypeError, ValueError):
        return 0


def _safe_rate(numerator: int, denominator: int) -> float:
    if denominator <= 0:
        return 0.0
    value = numerator / denominator
    if value < 0.0:
        value = 0.0
    if value > 1.0:
        value = 1.0
    return round(value, 4)


def _load_json(text: str | None) -> dict[str, Any]:
    if not text:
        return {}
    try:
        return json.loads(text)
    except json.JSONDecodeError:
        return {}


def _load_recent_verdict_rows(since_days: int) -> list[dict[str, Any]]:
    cutoff = (datetime.now(timezone.utc) - timedelta(days=since_days)).isoformat()
    with get_conn() as conn:
        rows = conn.execute(
            """
            SELECT
                ev.*,
                sc.title,
                sc.priority,
                sc.status AS case_status,
                sc.stage AS case_stage,
                sc.family AS case_family
            FROM edge_verdicts ev
            JOIN search_cases sc ON sc.case_id = ev.case_id
            WHERE ev.created_at >= ?
            ORDER BY ev.created_at DESC
            """,
            (cutoff,),
        ).fetchall()
    return [dict(r) for r in rows]


def _load_search_case_rows() -> list[dict[str, Any]]:
    with get_conn() as conn:
        rows = conn.execute("SELECT * FROM search_cases ORDER BY opened_at DESC").fetchall()
    return [dict(r) for r in rows]


def _load_manifest_rows() -> list[dict[str, Any]]:
    with get_conn() as conn:
        rows = conn.execute("SELECT * FROM experiment_manifests ORDER BY created_at DESC").fetchall()
    return [dict(r) for r in rows]


def _fingerprint_stats(verdict_rows: list[dict[str, Any]]) -> dict[str, Any]:
    counts = Counter()
    for row in verdict_rows:
        fingerprint = str(row.get("config_fingerprint") or "").strip()
        if fingerprint:
            counts[fingerprint] += 1
    repeated = sorted(((fp, n) for fp, n in counts.items() if n > 1), key=lambda item: (-item[1], item[0]))
    return {
        "unique_fingerprints": len(counts),
        "repeated_fingerprints": len(repeated),
        "top_repeated_fingerprints": [{"config_fingerprint": fp, "count": n} for fp, n in repeated[:5]],
    }


def _collect_motifs(verdict_rows: list[dict[str, Any]]) -> dict[str, int]:
    counts = Counter()
    for row in verdict_rows:
        reason = str(row.get("decision_reason") or "").strip()
        if reason:
            counts[reason] += 1
        postmortem = _load_json(row.get("postmortem_summary_json"))
        regime_failure = str(postmortem.get("regime_failure_mode") or "").strip()
        if regime_failure:
            counts[regime_failure] += 1
    return dict(counts)


def _recommended_action(score: float) -> str:
    if score < 0.20:
        return "FREEZE"
    if score < 0.50:
        return "CHEAP_ONLY"
    if score < 0.70:
        return "CHEAP_MEDIUM"
    return "EXPAND_CHEAP"


def _family_score(near_miss_rate: float, mutation_improvement_rate: float, robustness_survival_rate: float, dead_manifest_penalty: float) -> float:
    return round(
        (0.35 * near_miss_rate)
        + (0.25 * mutation_improvement_rate)
        + (0.20 * robustness_survival_rate)
        + (0.20 * dead_manifest_penalty),
        4,
    )


def build_meta_payload(*, loops_root: Path, since_days: int = 30) -> dict[str, Any]:
    init_db()
    sync_family_registry_db()

    registry_rows = list_family_definitions()
    benchmark = build_benchmark(loops_root, policy_version="any")
    benchmark_families = benchmark.get("families") or {}
    case_rows = _load_search_case_rows()
    manifest_rows = _load_manifest_rows()
    verdict_rows = _load_recent_verdict_rows(since_days)

    family_ids = {row.family_id for row in registry_rows}
    family_ids.update(str(row.get("family") or "") for row in case_rows if row.get("family"))

    payload_families: list[dict[str, Any]] = []
    near_miss_cases: list[dict[str, Any]] = []
    waste_cases: list[dict[str, Any]] = []
    actions: list[dict[str, Any]] = []
    queue_health = {
        "pending_total": sum(
            1 for row in manifest_rows if str(row.get("execution_status") or "") in {"ready", "claimed", "running"}
        ),
        "ready_total": sum(1 for row in manifest_rows if str(row.get("execution_status") or "") == "ready"),
        "running_total": sum(1 for row in manifest_rows if str(row.get("execution_status") or "") == "running"),
        "dead_total": sum(1 for row in manifest_rows if str(row.get("execution_status") or "") == "dead"),
        "completed_total": sum(1 for row in manifest_rows if str(row.get("execution_status") or "") == "completed"),
    }
    latest_mutation_cycle = get_last_maintenance_job_run("mutation_cycle")
    latest_meta_report = get_last_maintenance_job_run("meta_search_report")

    for family_id in sorted(family_ids):
        registry = next((item for item in registry_rows if item.family_id == family_id), None)
        case_bucket = [row for row in case_rows if str(row.get("family") or "") == family_id]
        manifest_bucket = [
            row
            for row in manifest_rows
            if str(_load_json(row.get("strategy_identity_json")).get("family") or "") == family_id
        ]
        verdict_bucket = [row for row in verdict_rows if str(row.get("case_family") or "") == family_id]
        final_verdicts = [row for row in verdict_bucket if str(row.get("status") or "") == "final"]
        benchmark_bucket = benchmark_families.get(family_id) or {}

        near_miss_candidates = [row for row in final_verdicts if _safe_float(row.get("near_miss_score")) >= 0.60]
        near_miss_rate = _safe_rate(len(near_miss_candidates), len(final_verdicts))
        mutation_improvement_rate = _safe_float(benchmark_bucket.get("mutation_improvement_rate"))
        robustness_survival_rate = _safe_float(benchmark_bucket.get("robustness_survival_rate"))
        dead_manifest_count = sum(1 for row in manifest_bucket if str(row.get("execution_status") or "") == "dead")
        dead_rate = dead_manifest_count / max(1, len(manifest_bucket))
        dead_manifest_penalty = round(max(0.0, 1.0 - min(1.0, dead_rate)), 4)
        latest_near_miss_score = max((_safe_float(row.get("near_miss_score")) for row in final_verdicts), default=0.0)
        score = _family_score(
            near_miss_rate=near_miss_rate,
            mutation_improvement_rate=mutation_improvement_rate,
            robustness_survival_rate=robustness_survival_rate,
            dead_manifest_penalty=dead_manifest_penalty,
        )
        recommended_action = _recommended_action(score)
        motif_counts = _collect_motifs(final_verdicts)
        budget_state = {
            "validation_caps": {
                "cheap": 12 if score >= 0.70 else 8 if score >= 0.50 else 4 if score >= 0.20 else 0,
                "medium": 6 if score >= 0.50 else 2 if score >= 0.20 else 0,
                "expensive": 1 if score >= 0.70 else 0,
            },
            "latest_near_miss_score": round(latest_near_miss_score, 4),
        }
        upsert_family_budget_state(
            family_id=family_id,
            status="frozen" if recommended_action == "FREEZE" else "active",
            priority=int(registry.priority if registry else 50),
            maturity=str(registry.maturity if registry else "experimental"),
            family_score=score,
            near_miss_rate=near_miss_rate,
            mutation_improvement_rate=mutation_improvement_rate,
            robustness_survival_rate=robustness_survival_rate,
            dead_manifest_penalty=dead_manifest_penalty,
            active_cases_count=sum(1 for row in case_bucket if str(row.get("status") or "") in {"active", "on_hold"}),
            total_cases_count=len(case_bucket),
            ready_manifest_count=sum(1 for row in manifest_bucket if str(row.get("execution_status") or "") == "ready"),
            running_manifest_count=sum(1 for row in manifest_bucket if str(row.get("execution_status") or "") == "running"),
            completed_manifest_count=sum(1 for row in manifest_bucket if str(row.get("execution_status") or "") == "completed"),
            dead_manifest_count=dead_manifest_count,
            latest_near_miss_score=latest_near_miss_score,
            recommended_action=recommended_action,
            budget_state=budget_state,
            motifs=motif_counts,
        )

        fingerprint_stats = _fingerprint_stats(final_verdicts)
        family_row = {
            "family_id": family_id,
            "registry": as_dict(registry) if registry else None,
            "family_score": score,
            "final_verdict_count": len(final_verdicts),
            "near_miss_count": len(near_miss_candidates),
            "near_miss_rate": near_miss_rate,
            "mutation_improvement_rate": mutation_improvement_rate,
            "robustness_survival_rate": robustness_survival_rate,
            "dead_manifest_penalty": dead_manifest_penalty,
            "latest_near_miss_score": round(latest_near_miss_score, 4),
            "recommended_action": recommended_action,
            "case_counts": {
                "total": len(case_bucket),
                "active": sum(1 for row in case_bucket if str(row.get("status") or "") in {"active", "on_hold"}),
            },
            "manifest_counts": {
                "total": len(manifest_bucket),
                "ready": sum(1 for row in manifest_bucket if str(row.get("execution_status") or "") == "ready"),
                "running": sum(1 for row in manifest_bucket if str(row.get("execution_status") or "") == "running"),
                "completed": sum(1 for row in manifest_bucket if str(row.get("execution_status") or "") == "completed"),
                "dead": dead_manifest_count,
            },
            "motifs": motif_counts,
            "fingerprints": fingerprint_stats,
        }
        payload_families.append(family_row)
        actions.append(
            {
                "family_id": family_id,
                "action": recommended_action,
                "reason": f"score={score:.3f} near_miss_rate={near_miss_rate:.3f}",
            }
        )

        for row in near_miss_candidates[:3]:
            near_miss_cases.append(
                {
                    "case_id": row.get("case_id"),
                    "title": row.get("title"),
                    "family_id": family_id,
                    "near_miss_score": round(_safe_float(row.get("near_miss_score")), 4),
                    "decision_reason": row.get("decision_reason"),
                    "validation_level": row.get("validation_level"),
                }
            )

        if recommended_action == "FREEZE":
            for row in final_verdicts[:2]:
                waste_cases.append(
                    {
                        "case_id": row.get("case_id"),
                        "title": row.get("title"),
                        "family_id": family_id,
                        "decision_reason": row.get("decision_reason"),
                        "experiment_score": round(_safe_float(row.get("experiment_score")), 4),
                    }
                )

    payload_families.sort(key=lambda item: (-_safe_float(item.get("family_score")), item.get("family_id")))
    near_miss_cases.sort(key=lambda item: (-_safe_float(item.get("near_miss_score")), item.get("family_id")))

    payload = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "since_days": since_days,
        "loops_root": str(loops_root),
        "queue_health": queue_health,
        "maintenance": {
            "latest_mutation_cycle": latest_mutation_cycle,
            "latest_meta_search_report": latest_meta_report,
        },
        "family_ranking": payload_families,
        "near_miss_cases": near_miss_cases[:20],
        "waste_cases": waste_cases[:20],
        "actions": actions,
    }
    payload["live_edge_search"] = persist_live_edge_search_review(payload)
    record_maintenance_job_run("meta_search_report", "ok", payload)
    return payload


def render_markdown(payload: dict[str, Any]) -> str:
    lines: list[str] = []
    lines.append("# Meta Search Report")
    lines.append("")
    lines.append(f"- Generated at: `{payload.get('generated_at')}`")
    lines.append(f"- Window: last `{payload.get('since_days')}` days")
    lines.append("")
    queue_health = payload.get("queue_health") or {}
    lines.append("## Queue Health")
    lines.append(
        f"- pending=`{queue_health.get('pending_total', 0)}` ready=`{queue_health.get('ready_total', 0)}` "
        f"running=`{queue_health.get('running_total', 0)}` completed=`{queue_health.get('completed_total', 0)}` "
        f"dead=`{queue_health.get('dead_total', 0)}`"
    )
    latest_mutation_cycle = ((payload.get("maintenance") or {}).get("latest_mutation_cycle") or {})
    if latest_mutation_cycle:
        summary = _load_json(latest_mutation_cycle.get("summary_json"))
        lines.append(
            f"- latest_mutation_cycle status=`{latest_mutation_cycle.get('status')}` "
            f"created=`{summary.get('created_count', 0)}` candidates=`{summary.get('candidate_count', 0)}`"
        )
    live_state = payload.get("live_edge_search") or {}
    if live_state:
        metrics = live_state.get("metrics") or {}
        lines.append(
            f"- live_edge_search mode=`{live_state.get('mode')}` status=`{live_state.get('status')}` "
            f"evaluated=`{metrics.get('evaluated_total', 0)}` near_miss=`{metrics.get('near_miss_total', 0)}` "
            f"duplicate_ratio=`{metrics.get('duplicate_ratio', 0)}`"
        )
    lines.append("")
    lines.append("## Family Ranking")
    lines.append("| Family | Score | Near Miss | Mutation Improve | Robustness | Dead Penalty | Action |")
    lines.append("|---|---:|---:|---:|---:|---:|---|")
    for item in payload.get("family_ranking") or []:
        lines.append(
            f"| {item.get('family_id')} | {item.get('family_score')} | {item.get('near_miss_rate')} | "
            f"{item.get('mutation_improvement_rate')} | {item.get('robustness_survival_rate')} | "
            f"{item.get('dead_manifest_penalty')} | {item.get('recommended_action')} |"
        )
    lines.append("")
    lines.append("## Near-Miss Cases")
    for item in payload.get("near_miss_cases") or []:
        lines.append(
            f"- `{item.get('case_id')}` `{item.get('family_id')}` near_miss=`{item.get('near_miss_score')}` "
            f"reason=`{item.get('decision_reason')}`"
        )
    if not (payload.get("near_miss_cases") or []):
        lines.append("- none")
    lines.append("")
    lines.append("## Waste Cases")
    for item in payload.get("waste_cases") or []:
        lines.append(
            f"- `{item.get('case_id')}` `{item.get('family_id')}` score=`{item.get('experiment_score')}` "
            f"reason=`{item.get('decision_reason')}`"
        )
    if not (payload.get("waste_cases") or []):
        lines.append("- none")
    lines.append("")
    lines.append("## Recommended Actions")
    for item in payload.get("actions") or []:
        lines.append(f"- `{item.get('family_id')}` -> `{item.get('action')}` ({item.get('reason')})")
    return "\n".join(lines)


def _main() -> int:
    ap = argparse.ArgumentParser(description="Generate family-level meta search report")
    ap.add_argument("--loops-root", default="data/research_loops")
    ap.add_argument("--since-days", type=int, default=30)
    ap.add_argument("--output-md", default="data/reports/meta_search_report_latest.md")
    ap.add_argument("--output-json", default="data/reports/meta_search_report_latest.json")
    ap.add_argument("--output-live-review-json", default="data/reports/live_edge_search_review_latest.json")
    args = ap.parse_args()

    payload = build_meta_payload(
        loops_root=Path(args.loops_root).expanduser().resolve(),
        since_days=max(1, args.since_days),
    )
    md_path = Path(args.output_md).expanduser().resolve()
    json_path = Path(args.output_json).expanduser().resolve()
    live_review_json_path = Path(args.output_live_review_json).expanduser().resolve()
    md_path.parent.mkdir(parents=True, exist_ok=True)
    json_path.parent.mkdir(parents=True, exist_ok=True)
    live_review_json_path.parent.mkdir(parents=True, exist_ok=True)
    md_path.write_text(render_markdown(payload), encoding="utf-8")
    json_path.write_text(json.dumps(payload, indent=2), encoding="utf-8")
    live_review_json_path.write_text(json.dumps(payload.get("live_edge_search") or {}, indent=2), encoding="utf-8")
    print(f"Wrote: {md_path}")
    print(f"Wrote: {json_path}")
    print(f"Wrote: {live_review_json_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(_main())
