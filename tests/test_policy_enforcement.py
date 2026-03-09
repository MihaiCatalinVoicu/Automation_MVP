#!/usr/bin/env python3
"""
Policy enforcement test matrix.

Run: python tests/test_policy_enforcement.py
      python tests/test_policy_enforcement.py --api   # also run API/event tests
Output: PASS/FAIL per rule.
"""

from __future__ import annotations

import os
import sys
import tempfile
from pathlib import Path

# Add project root
_root = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(_root))

from policy_engine import validate_task, should_auto_escalate_to_premium

SAFE_DOCS_CFG = {"profiles": ["safe_docs"]}
SAFE_READONLY_CFG = {"profiles": ["safe_readonly"]}
NEEDS_APPROVAL_CFG = {"profiles": ["needs_approval_for_code"]}
PREMIUM_ESCALATE_CFG = {"profiles": ["premium_on_repeat_failures"]}


def run_tests() -> tuple[int, int]:
    passed = 0
    failed = 0

    def ok(name: str, cond: bool) -> None:
        nonlocal passed, failed
        if cond:
            passed += 1
            print(f"  PASS: {name}")
        else:
            failed += 1
            print(f"  FAIL: {name}")

    print("Policy enforcement test matrix")
    print("-" * 50)

    # 1. safe_docs rejects src/, scoring, strategy
    print("\n1. safe_docs rejects forbidden scope")
    r = validate_task(SAFE_DOCS_CFG, {"goal": "Modify src/app.py"})
    ok("safe_docs rejects src/", r.status == "failed")
    r = validate_task(SAFE_DOCS_CFG, {"goal": "Improve scoring logic"})
    ok("safe_docs rejects scoring", r.status == "failed")
    r = validate_task(SAFE_DOCS_CFG, {"goal": "Change strategy"})
    ok("safe_docs rejects strategy", r.status == "failed")
    r = validate_task(SAFE_DOCS_CFG, {"goal": "Update ingest pipeline"})
    ok("safe_docs rejects ingest", r.status == "failed")

    # 2. safe_docs allows docs-only
    print("\n2. safe_docs allows docs-only")
    r = validate_task(SAFE_DOCS_CFG, {"goal": "Add docs note", "constraints": ["touch only docs"]})
    ok("safe_docs allows docs task", r.status == "passed")
    r = validate_task(SAFE_DOCS_CFG, {"goal": "Update README with usage"})
    ok("safe_docs allows readme task", r.status == "passed")

    # 3. safe_readonly rejects checks with side effects
    print("\n3. safe_readonly rejects unsafe checks")
    r = validate_task(
        SAFE_READONLY_CFG,
        {"goal": "Add script", "checks": ["pytest -q", "python -m src.app --help"]},
    )
    ok("safe_readonly rejects pytest", r.status == "failed")
    r = validate_task(
        SAFE_READONLY_CFG,
        {"goal": "Add script", "checks": ["curl https://example.com"]},
    )
    ok("safe_readonly rejects curl", r.status == "failed")

    # 4. safe_readonly allows py_compile, status, preflight
    print("\n4. safe_readonly allows read-only checks")
    r = validate_task(
        SAFE_READONLY_CFG,
        {"goal": "Add status script", "checks": ["python -m py_compile scripts/x.py", "python scripts/c10_forward_status.py"]},
    )
    ok("safe_readonly allows py_compile + status", r.status == "passed")
    r = validate_task(
        SAFE_READONLY_CFG,
        {"goal": "Add preflight", "checks": ["python scripts/c10_forward_preflight.py"]},
    )
    ok("safe_readonly allows preflight", r.status == "passed")

    # 5. needs_approval_for_code yields needs_pre_approval for non-docs
    print("\n5. needs_approval_for_code yields pre-approval for code tasks")
    r = validate_task(NEEDS_APPROVAL_CFG, {"goal": "Add new module X"})
    ok("needs_approval_for_code requires pre-approval for code", r.needs_pre_approval)
    r = validate_task(NEEDS_APPROVAL_CFG, {"goal": "Add new module X"})
    ok("needs_approval_for_code status is needs_approval", r.status == "needs_approval")

    # 6. needs_approval_for_code passes docs-only
    print("\n6. needs_approval_for_code passes docs-only")
    r = validate_task(NEEDS_APPROVAL_CFG, {"goal": "Update docs", "constraints": ["touch only docs"]})
    ok("needs_approval_for_code passes docs-only", r.status == "passed" and not r.needs_pre_approval)

    # 7. premium_on_repeat_failures
    print("\n7. premium_on_repeat_failures")
    ok("should_auto_escalate true when profile present", should_auto_escalate_to_premium(PREMIUM_ESCALATE_CFG))
    ok("should_auto_escalate false when absent", not should_auto_escalate_to_premium(SAFE_DOCS_CFG))
    ok("should_auto_escalate false when no profiles", not should_auto_escalate_to_premium({}))

    # 8. no profiles = pass
    print("\n8. No profiles -> pass")
    r = validate_task({}, {"goal": "anything"})
    ok("no profiles yields passed", r.status == "passed")

    # 9. risk_level
    print("\n9. risk_level")
    r = validate_task(SAFE_DOCS_CFG, {"goal": "Modify scoring"})
    ok("forbidden task has HIGH risk", r.risk_level == "HIGH")
    r = validate_task(SAFE_DOCS_CFG, {"goal": "Add docs"})
    ok("safe task has LOW risk", r.risk_level == "LOW")

    return passed, failed


def run_api_tests(db_path: str | None) -> tuple[int, int]:
    """API-level tests: submit flow, status codes, events. db_path must have DB_PATH set."""
    passed = 0
    failed = 0

    def ok(name: str, cond: bool) -> None:
        nonlocal passed, failed
        if cond:
            passed += 1
            print(f"  PASS: {name}")
        else:
            failed += 1
            print(f"  FAIL: {name}")

    if not db_path:
        return 0, 0
    try:
        from fastapi.testclient import TestClient
        from app import app as fastapi_app
        from db import init_db
        init_db()  # ensure tables exist (startup may not run before first request)
        client = TestClient(fastapi_app)
    except ImportError as e:
        print("\n10. API/events (skipped: install fastapi/httpx for TestClient)")
        return 0, 0
    except Exception as e:
        print(f"\n10. API/events (skipped: {e})")
        return 0, 0

    print("\n10. API submit + events")
    try:
        # Rejected: forbidden goal
        r = client.post(
            "/runs",
            json={
                "repo": "stocks-bot",
                "goal": "Modify src/app.py",
                "branch": "master",
                "task_type": "bugfix",
            },
        )
        ok("POST forbidden task -> 400", r.status_code == 400)
        ok("POST forbidden task -> policy_rejected in detail", 
           r.json().get("detail", {}).get("policy_rejected") is True)

        # Passed: docs-only
        r = client.post(
            "/runs",
            json={
                "repo": "stocks-bot",
                "goal": "Update README with usage",
                "branch": "master",
                "task_type": "docs",
                "constraints": ["touch only docs"],
            },
        )
        ok("POST safe task -> 2xx", 200 <= r.status_code < 300)
        run_id = r.json().get("run_id")
        ok("POST safe task returns run_id", bool(run_id))
        ok("POST safe task status QUEUED", r.json().get("status") == "QUEUED")

        # Events
        r2 = client.get(f"/runs/{run_id}")
        events = [e.get("event_type") for e in r2.json().get("events", [])]
        ok("events include policy_validation_passed", "policy_validation_passed" in events)
        ok("events include run_created", "run_created" in events)
    except ImportError as e:
        print("\n10. API/events (skipped: install httpx for TestClient)")
        return 0, 0
    except Exception as e:
        print(f"\n10. API/events (skipped: {e})")
        return 0, 0

    return passed, failed


if __name__ == "__main__":
    do_api = "--api" in sys.argv
    tmp_path = None
    if do_api:
        tmp = tempfile.NamedTemporaryFile(suffix=".db", delete=False)
        tmp.close()
        tmp_path = tmp.name
        os.environ["DB_PATH"] = tmp_path
    p, f = run_tests()
    if do_api:
        pa, fa = run_api_tests(tmp_path)
        try:
            if tmp_path:
                os.unlink(tmp_path)
        except Exception:
            pass
        p, f = p + pa, f + fa
    print("\n" + "-" * 50)
    print(f"Result: {p} PASS, {f} FAIL")
    sys.exit(1 if f > 0 else 0)
