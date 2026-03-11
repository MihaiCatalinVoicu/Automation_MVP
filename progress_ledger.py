#!/usr/bin/env python3
from __future__ import annotations

import argparse
import hashlib
import json
from pathlib import Path
from typing import Any

from db import (
    create_work_item,
    create_work_item_event,
    get_work_item,
    get_work_item_by_source,
    init_db,
    list_work_item_events,
    list_work_items,
    update_work_item,
)


def _default_progress(status: str) -> int:
    mapping = {
        "proposed": 0,
        "approved": 0,
        "pending": 0,
        "in_progress": 50,
        "blocked": 0,
        "deferred": 0,
        "done": 100,
        "completed": 100,
        "killed": 0,
    }
    return mapping.get(status, 0)


def _infer_repo(text: str, source_doc: str) -> str:
    text_only = (text or "").lower()
    source_only = (source_doc or "").lower()
    mentions = {
        "crypto-bot": "crypto-bot" in text_only,
        "stocks-bot": "stocks-bot" in text_only,
        "automation-mvp": "automation-mvp" in text_only,
    }
    mentioned = [repo for repo, present in mentions.items() if present]
    if len(mentioned) == 1:
        return mentioned[0]
    if len(mentioned) > 1:
        return "shared"
    if "automation-mvp" in source_only:
        return "automation-mvp"
    if "stocks-bot" in source_only:
        return "stocks-bot"
    if "crypto-bot" in source_only:
        return "crypto-bot"
    return "shared"


def _infer_scope_type(text: str) -> str:
    lowered = text.lower()
    if "integration" in lowered or "contract" in lowered or "cross-repo" in lowered:
        return "integration"
    if "strategy" in lowered:
        return "strategy"
    if "experiment" in lowered or "research" in lowered:
        return "experiment"
    if "ops" in lowered or "timer" in lowered or "scheduler" in lowered:
        return "ops"
    return "module"


def _priority_rank(priority: str) -> tuple[int, str]:
    order = {"critical": 0, "high": 1, "medium": 2, "low": 3}
    return (order.get(priority, 9), priority)


def _render_markdown(items: list[dict[str, Any]]) -> str:
    rows = sorted(items, key=lambda x: (_priority_rank(str(x.get("priority") or "medium")), str(x.get("updated_at") or "")), reverse=False)
    lines = [
        "# Progress Ledger",
        "",
        "| ID | Repo | Status | Progress | Priority | Title |",
        "|---|---|---|---:|---|---|",
    ]
    for item in rows:
        lines.append(
            f"| {item.get('id')} | {item.get('repo')} | {item.get('status')} | "
            f"{item.get('progress_pct')} | {item.get('priority')} | {item.get('title')} |"
        )
    lines.append("")
    return "\n".join(lines)


def _slug_id(parts: list[str]) -> str:
    raw = "::".join(parts)
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()[:16]


def _parse_frontmatter_todos(path: Path) -> tuple[str, list[dict[str, str]]]:
    text = path.read_text(encoding="utf-8")
    if not text.startswith("---"):
        return ("", [])
    try:
        _, fm, _rest = text.split("---", 2)
    except ValueError:
        return ("", [])
    overview = ""
    todos: list[dict[str, str]] = []
    current: dict[str, str] | None = None
    for raw_line in fm.splitlines():
        line = raw_line.rstrip()
        stripped = line.strip()
        if stripped.startswith("overview:"):
            overview = stripped.split(":", 1)[1].strip()
            continue
        if stripped.startswith("- id:"):
            if current:
                todos.append(current)
            current = {"id": stripped.split(":", 1)[1].strip()}
            continue
        if current and stripped.startswith("content:"):
            current["content"] = stripped.split(":", 1)[1].strip().strip('"')
            continue
        if current and stripped.startswith("status:"):
            current["status"] = stripped.split(":", 1)[1].strip()
            continue
    if current:
        todos.append(current)
    return overview, todos


def _import_plan(path: Path, phase: str | None = None) -> dict[str, int]:
    source_doc = str(path.resolve())
    overview, todos = _parse_frontmatter_todos(path)
    stats = {"created": 0, "updated": 0, "skipped": 0}
    for todo in todos:
        todo_id = todo.get("id") or "unknown"
        content = todo.get("content") or todo_id
        status = todo.get("status") or "pending"
        progress = _default_progress(status)
        repo = _infer_repo(content, source_doc)
        scope_type = _infer_scope_type(content)
        existing = get_work_item_by_source(source_doc, todo_id)
        if existing:
            update_work_item(
                existing["id"],
                title=content,
                repo=repo,
                scope_type=scope_type,
                status=status,
                progress_pct=progress,
                phase=phase or existing.get("phase"),
                notes=content,
                reason="import_plan_refresh",
                event_type="progress_update",
            )
            stats["updated"] += 1
            continue
        work_item_id = f"wi_{_slug_id([source_doc, todo_id])}"
        create_work_item(
            work_item_id=work_item_id,
            title=content,
            repo=repo,
            strategy_id=None,
            scope_type=scope_type,
            status=status,
            progress_pct=progress,
            priority="medium",
            phase=phase,
            source_doc=source_doc,
            source_item_id=todo_id,
            notes=overview,
        )
        create_work_item_event(
            work_item_id=work_item_id,
            event_type="created",
            reason="import_plan_seed",
            new_status=status,
            new_progress_pct=progress,
            new_payload={"title": content, "source_doc": source_doc, "source_item_id": todo_id},
        )
        stats["created"] += 1
    return stats


def _cmd_create(args: argparse.Namespace) -> None:
    progress = args.progress if args.progress is not None else _default_progress(args.status)
    create_work_item(
        work_item_id=args.id,
        title=args.title,
        repo=args.repo,
        strategy_id=args.strategy_id,
        scope_type=args.scope_type,
        status=args.status,
        progress_pct=progress,
        priority=args.priority,
        phase=args.phase,
        owner=args.owner,
        blocked_by=args.blocked_by,
        deferred_reason=args.deferred_reason,
        decision_ref=args.decision_ref,
        source_doc=args.source_doc,
        source_item_id=args.source_item_id,
        acceptance_criteria=args.acceptance_criteria,
        notes=args.notes or "",
        target_date=args.target_date,
    )
    create_work_item_event(
        work_item_id=args.id,
        event_type="created",
        reason=args.reason or "manual_create",
        new_status=args.status,
        new_progress_pct=progress,
        new_payload={"title": args.title},
    )
    print(f"Created: {args.id}")


def _cmd_update(args: argparse.Namespace) -> None:
    updates = {
        key: value
        for key, value in {
            "title": args.title,
            "repo": args.repo,
            "strategy_id": args.strategy_id,
            "scope_type": args.scope_type,
            "status": args.status,
            "progress_pct": args.progress,
            "priority": args.priority,
            "phase": args.phase,
            "owner": args.owner,
            "blocked_by": args.blocked_by,
            "deferred_reason": args.deferred_reason,
            "decision_ref": args.decision_ref,
            "acceptance_criteria": args.acceptance_criteria,
            "notes": args.notes,
            "target_date": args.target_date,
        }.items()
        if value is not None
    }
    update_work_item(args.id, reason=args.reason or "manual_update", event_type=args.event_type, **updates)
    print(f"Updated: {args.id}")


def _cmd_list(args: argparse.Namespace) -> None:
    items = list_work_items(repo=args.repo, strategy_id=args.strategy_id, status=args.status, scope_type=args.scope_type)
    if args.format == "json":
        print(json.dumps(items, indent=2))
    else:
        print(_render_markdown(items))


def _cmd_history(args: argparse.Namespace) -> None:
    events = list_work_item_events(args.id)
    print(json.dumps(events, indent=2))


def _cmd_import_plan(args: argparse.Namespace) -> None:
    totals = {"created": 0, "updated": 0, "skipped": 0}
    for raw in args.paths:
        stats = _import_plan(Path(raw).expanduser().resolve(), phase=args.phase)
        for key in totals:
            totals[key] += stats[key]
        print(f"{raw}: created={stats['created']} updated={stats['updated']} skipped={stats['skipped']}")
    print(f"Totals: created={totals['created']} updated={totals['updated']} skipped={totals['skipped']}")


def build_parser() -> argparse.ArgumentParser:
    ap = argparse.ArgumentParser(description="Strategy Progress / Deferred Decisions Ledger")
    sub = ap.add_subparsers(dest="cmd", required=True)

    p_create = sub.add_parser("create")
    p_create.add_argument("--id", required=True)
    p_create.add_argument("--title", required=True)
    p_create.add_argument("--repo", required=True)
    p_create.add_argument("--strategy-id", default=None)
    p_create.add_argument("--scope-type", default="module")
    p_create.add_argument("--status", default="proposed")
    p_create.add_argument("--progress", type=int, default=None)
    p_create.add_argument("--priority", default="medium")
    p_create.add_argument("--phase", default=None)
    p_create.add_argument("--owner", default=None)
    p_create.add_argument("--blocked-by", default=None)
    p_create.add_argument("--deferred-reason", default=None)
    p_create.add_argument("--decision-ref", default=None)
    p_create.add_argument("--source-doc", default=None)
    p_create.add_argument("--source-item-id", default=None)
    p_create.add_argument("--acceptance-criteria", default=None)
    p_create.add_argument("--notes", default="")
    p_create.add_argument("--target-date", default=None)
    p_create.add_argument("--reason", default="manual_create")
    p_create.set_defaults(func=_cmd_create)

    p_update = sub.add_parser("update")
    p_update.add_argument("--id", required=True)
    p_update.add_argument("--title", default=None)
    p_update.add_argument("--repo", default=None)
    p_update.add_argument("--strategy-id", default=None)
    p_update.add_argument("--scope-type", default=None)
    p_update.add_argument("--status", default=None)
    p_update.add_argument("--progress", type=int, default=None)
    p_update.add_argument("--priority", default=None)
    p_update.add_argument("--phase", default=None)
    p_update.add_argument("--owner", default=None)
    p_update.add_argument("--blocked-by", default=None)
    p_update.add_argument("--deferred-reason", default=None)
    p_update.add_argument("--decision-ref", default=None)
    p_update.add_argument("--acceptance-criteria", default=None)
    p_update.add_argument("--notes", default=None)
    p_update.add_argument("--target-date", default=None)
    p_update.add_argument("--reason", default="manual_update")
    p_update.add_argument("--event-type", default="progress_update")
    p_update.set_defaults(func=_cmd_update)

    p_list = sub.add_parser("list")
    p_list.add_argument("--repo", default=None)
    p_list.add_argument("--strategy-id", default=None)
    p_list.add_argument("--status", default=None)
    p_list.add_argument("--scope-type", default=None)
    p_list.add_argument("--format", choices=["markdown", "json"], default="markdown")
    p_list.set_defaults(func=_cmd_list)

    p_hist = sub.add_parser("history")
    p_hist.add_argument("--id", required=True)
    p_hist.set_defaults(func=_cmd_history)

    p_import = sub.add_parser("import-plan")
    p_import.add_argument("paths", nargs="+")
    p_import.add_argument("--phase", default=None)
    p_import.set_defaults(func=_cmd_import_plan)

    return ap


def _main() -> int:
    init_db()
    parser = build_parser()
    args = parser.parse_args()
    args.func(args)
    return 0


if __name__ == "__main__":
    raise SystemExit(_main())
