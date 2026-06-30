"""Package entry point with an offline selfcheck.

The selfcheck imports modules, validates fixtures, confirms config loads, and
performs a Redis readiness probe. It does NOT run the agent loop, call candidate
stubs, or require a provider key.
"""
from __future__ import annotations

import asyncio
import json
import sys
from pathlib import Path

from .config import CONFIG
from .streaming import get_redis

FIXTURES_DIR = Path(__file__).resolve().parent.parent / "fixtures"


def _validate_fixtures() -> None:
    requests_path = FIXTURES_DIR / "tickets.json"
    data = json.loads(requests_path.read_text())
    assert isinstance(data, list) and data, "tickets.json must be a non-empty list"
    for t in data:
        assert "ticket_id" in t and "issue" in t, "ticket missing required fields"
    traces = (FIXTURES_DIR / "sample_trace.jsonl").read_text().strip().splitlines()
    assert traces, "sample_trace.jsonl must not be empty"
    for line in traces:
        json.loads(line)


async def _redis_probe() -> None:
    client = get_redis()
    try:
        key = "_selfcheck_probe"
        msg_id = await client.xadd(key, {"k": "v"})
        await client.xlen(key)
        await client.delete(key)
        assert msg_id is not None
    finally:
        await client.aclose()


def selfcheck() -> int:
    print("[selfcheck] importing modules...")
    import agent_orchestrator.orchestrator  # noqa: F401
    import agent_orchestrator.monitor  # noqa: F401
    import agent_orchestrator.streaming  # noqa: F401
    import agent_orchestrator.llm_client  # noqa: F401

    print("[selfcheck] validating fixtures...")
    _validate_fixtures()

    print(f"[selfcheck] config loaded (test_mode={CONFIG.test_mode}, model={CONFIG.model})")

    print("[selfcheck] probing Redis...")
    asyncio.run(_redis_probe())

    print("[selfcheck] OK")
    return 0


if __name__ == "__main__":
    if "--selfcheck" in sys.argv:
        sys.exit(selfcheck())
    print("usage: python -m agent_orchestrator --selfcheck")
    sys.exit(2)
