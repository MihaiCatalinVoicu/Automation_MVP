from __future__ import annotations

from db import init_db
from research_governance_scheduler import send_pending_research_governance_messages


def main() -> int:
    init_db()
    sent = send_pending_research_governance_messages(limit=50)
    print(f"research governance messages sent: {sent}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

