from __future__ import annotations

import json
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

from artifact_store import list_artifacts
from db import get_conn, utc_now

ROOT = Path(__file__).resolve().parent


def _latest_reviews() -> dict[str, dict[str, Any]]:
    with get_conn() as conn:
        rows = conn.execute(
            """
            SELECT strategy_id, recommended_verdict, recommended_operational_status, status, created_at, artifact_path
            FROM strategy_reviews
            ORDER BY created_at DESC
            """
        ).fetchall()
    latest: dict[str, dict[str, Any]] = {}
    for row in rows:
        item = dict(row)
        latest.setdefault(item["strategy_id"], item)
    return latest


def build_shadow_board(output_dir: Path, lookback_days: int = 7) -> dict[str, Any]:
    output_dir.mkdir(parents=True, exist_ok=True)
    cutoff = datetime.now(timezone.utc) - timedelta(days=lookback_days)
    latest_reviews = _latest_reviews()
    daily_rows = []
    weekly_rows = []

    for artifact in list_artifacts(limit=500):
        if artifact["artifact_kind"] != "validation_summary":
            continue
        created_at = datetime.fromisoformat(artifact["created_at"])
        summary = artifact.get("summary", {})
        research_summary = summary.get("research_summary", {}) if isinstance(summary, dict) else {}
        family_summary = research_summary.get("family_summary", {}) if isinstance(research_summary, dict) else {}
        strategy_id = artifact.get("strategy_id")
        review = latest_reviews.get(strategy_id or "", {})
        row = {
            "created_at": artifact["created_at"],
            "family_name": artifact.get("family_name"),
            "strategy_id": strategy_id,
            "validation_verdict": summary.get("verdict"),
            "research_verdict": family_summary.get("research_verdict"),
            "candidate_count": family_summary.get("candidate_count"),
            "best_variant_name": family_summary.get("best_variant_name"),
            "lifecycle_recommended_verdict": review.get("recommended_verdict"),
            "lifecycle_status": review.get("status"),
            "artifact_path": artifact.get("artifact_path"),
        }
        daily_rows.append(row)
        if created_at >= cutoff:
            weekly_rows.append(row)

    daily_rows.sort(key=lambda x: x["created_at"], reverse=True)
    weekly_rows.sort(key=lambda x: x["created_at"], reverse=True)
    board = {
        "created_at": utc_now(),
        "daily_shadow_recommendations": daily_rows[:20],
        "weekly_shadow_recommendations": weekly_rows,
    }
    (output_dir / "shadow_recommendations_daily.json").write_text(json.dumps(board, indent=2), encoding="utf-8")

    md_lines = ["# Shadow Recommendations", "", f"Generated at `{board['created_at']}`.", ""]
    for row in weekly_rows[:20]:
        md_lines.append(
            f"- `{row['family_name']}` strategy=`{row['strategy_id']}` validation=`{row['validation_verdict']}` "
            f"research=`{row['research_verdict']}` candidates=`{row['candidate_count']}` "
            f"best_variant=`{row['best_variant_name']}` lifecycle=`{row['lifecycle_recommended_verdict']}`"
        )
    (output_dir / "shadow_recommendations_weekly.md").write_text("\n".join(md_lines).rstrip() + "\n", encoding="utf-8")
    return board


def main() -> int:
    import argparse

    ap = argparse.ArgumentParser(description="Build daily/weekly shadow recommendation boards")
    ap.add_argument("--output-dir", default=str(ROOT / "data" / "shadow_recommendations"))
    ap.add_argument("--lookback-days", type=int, default=7)
    args = ap.parse_args()

    board = build_shadow_board(Path(args.output_dir), lookback_days=args.lookback_days)
    print(json.dumps({"daily_count": len(board["daily_shadow_recommendations"]), "weekly_count": len(board["weekly_shadow_recommendations"])}, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
