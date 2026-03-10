from __future__ import annotations

import json
import uuid
from pathlib import Path
from typing import Any

from db import get_conn, utc_now


def _json(obj: Any) -> str:
    return json.dumps(obj, ensure_ascii=True, sort_keys=True)


def register_artifact(
    *,
    run_id: str,
    repo: str,
    artifact_kind: str,
    artifact_path: str,
    summary: dict | None = None,
    schedule_id: str | None = None,
    strategy_id: str | None = None,
    family_name: str | None = None,
) -> str:
    artifact_id = uuid.uuid4().hex[:12]
    with get_conn() as conn:
        conn.execute(
            """
            INSERT INTO artifact_manifests (
                id, schedule_id, run_id, repo, strategy_id, family_name, artifact_kind,
                artifact_path, summary_json, created_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                artifact_id,
                schedule_id,
                run_id,
                repo,
                strategy_id,
                family_name,
                artifact_kind,
                artifact_path,
                _json(summary or {}),
                utc_now(),
            ),
        )
    return artifact_id


def register_validation_artifacts(
    *,
    run_id: str,
    repo: str,
    output_dir: Path,
    summary: dict,
    schedule_id: str | None = None,
    strategy_id: str | None = None,
    family_name: str | None = None,
) -> list[str]:
    manifests = []
    manifests.append(
        register_artifact(
            run_id=run_id,
            repo=repo,
            artifact_kind="validation_summary",
            artifact_path=str((output_dir / "summary.json").resolve()),
            summary=summary,
            schedule_id=schedule_id,
            strategy_id=strategy_id,
            family_name=family_name,
        )
    )
    verdict_path = output_dir / "verdict.txt"
    if verdict_path.exists():
        manifests.append(
            register_artifact(
                run_id=run_id,
                repo=repo,
                artifact_kind="validation_verdict",
                artifact_path=str(verdict_path.resolve()),
                summary={"verdict": summary.get("verdict")},
                schedule_id=schedule_id,
                strategy_id=strategy_id,
                family_name=family_name,
            )
        )
    return manifests


def list_artifacts(*, family_name: str | None = None, limit: int = 100) -> list[dict]:
    query = "SELECT * FROM artifact_manifests"
    params: list[Any] = []
    if family_name:
        query += " WHERE family_name=?"
        params.append(family_name)
    query += " ORDER BY created_at DESC LIMIT ?"
    params.append(limit)
    with get_conn() as conn:
        rows = conn.execute(query, params).fetchall()
    out = []
    for row in rows:
        item = dict(row)
        item["summary"] = json.loads(item.get("summary_json") or "{}")
        out.append(item)
    return out
