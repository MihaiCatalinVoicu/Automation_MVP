from __future__ import annotations

import argparse
from pathlib import Path

from db import init_db
from repo_registry import RepoRegistry
from strategy_registry import get_strategy_children, list_strategies

ROOT = Path(__file__).resolve().parent


def _group_by_repo() -> dict[str, list[dict]]:
    groups: dict[str, list[dict]] = {}
    for item in list_strategies():
        groups.setdefault(item["repo"], []).append(item)
    return groups


def _render_entry(item: dict) -> str:
    children = get_strategy_children(item["id"])
    lines = [
        f"### `{item['id']}`",
        "",
        f"- Name: {item['name']}",
        f"- Repo: {item['repo']}",
        f"- Category: {item['category']}",
        f"- Purpose: {item['purpose']}",
        f"- Hypothesis: {item['business_hypothesis']}",
        f"- Status: {item['status_state']} ({item['status_pct']}%)",
        f"- Operational status: {item['operational_status']}",
        f"- Verdict: {item['current_verdict']}",
        f"- Owner: {item['owner']}",
        f"- Last reviewed: {item['last_reviewed_at']}",
    ]
    if item.get("tags"):
        lines.append(f"- Tags: {', '.join(item['tags'])}")
    if item.get("notes"):
        lines.append(f"- Notes: {item['notes']}")

    if children["files"]:
        lines.extend(["", "Files:"])
        for fl in children["files"]:
            flag = " (shadow)" if fl.get("is_shadow") else ""
            lines.append(f"- `{fl['file_path']}` [{fl['role']}] {flag}".rstrip())

    if children["metrics"]:
        lines.extend(["", "Metrics / thresholds:"])
        for mt in children["metrics"]:
            lines.append(f"- `{mt['metric_name']}` target `{mt['target_value']}` rule `{mt['threshold_rule']}`")

    if children["watchlist"]:
        lines.extend(["", "Watchlist:"])
        for wt in children["watchlist"]:
            lines.append(
                f"- `{wt['metric_name']}` trigger `{wt['trigger_rule']}` cadence `{wt['reevaluation_cadence']}` -> `{wt['trigger_action']}`"
            )

    if children["versions"]:
        latest = children["versions"][-1]
        lines.extend(["", "Latest version:"])
        lines.append(f"- `{latest['version']}`: {latest['summary']} ({latest['decision']})")

    lines.append("")
    return "\n".join(lines)


def _render_registry(title: str, rows: list[dict]) -> str:
    lines = [f"# {title}", "", f"Generated from central strategy registry in `{ROOT}`.", ""]
    grouped: dict[str, list[dict]] = {}
    for row in rows:
        grouped.setdefault(row["category"], []).append(row)
    for category in sorted(grouped):
        lines.extend([f"## {category}", ""])
        for row in sorted(grouped[category], key=lambda x: x["id"]):
            lines.append(_render_entry(row))
    return "\n".join(lines).rstrip() + "\n"


def _shadow_rows(rows: list[dict]) -> list[dict]:
    return [r for r in rows if r["status_state"] == "shadow" or "shadow" in (r.get("operational_status") or "")]


def main() -> int:
    ap = argparse.ArgumentParser(description="Generate mirror docs from central strategy registry")
    args = ap.parse_args()

    init_db()
    registry = RepoRegistry()
    all_rows = list_strategies()
    auto_docs = ROOT / "docs"
    auto_docs.mkdir(parents=True, exist_ok=True)

    (auto_docs / "STRATEGY_REGISTRY.md").write_text(
        _render_registry("Strategy Registry", all_rows),
        encoding="utf-8",
    )
    (auto_docs / "SHADOW_LOGIC_AUDIT.md").write_text(
        _render_registry("Shadow Logic Audit", _shadow_rows(all_rows)),
        encoding="utf-8",
    )

    repo_docs = {
        "crypto-bot": ("CRYPTO_STRATEGY_MAP.md", "Crypto Strategy Map"),
        "stocks-bot": ("STOCKS_STRATEGY_MAP.md", "Stocks Strategy Map"),
    }
    for repo_name, (filename, title) in repo_docs.items():
        repo_cfg = registry.get(repo_name)
        docs_dir = Path(repo_cfg["path"]) / "docs"
        docs_dir.mkdir(parents=True, exist_ok=True)
        repo_rows = [r for r in all_rows if r["repo"] in {repo_name, "shared"}]
        (docs_dir / filename).write_text(_render_registry(title, repo_rows), encoding="utf-8")

    print("Generated registry docs.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
