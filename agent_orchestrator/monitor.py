"""Separate consumer process that watches runs and stops runaway ones.

The monitor consumes the fleet-wide Redis Stream written by the orchestrator and
requests cooperative cancellation for runs that exceed the monitor's runaway
policy. The policy is deliberately simple and defensible:

* more than ``MONITOR_RUNAWAY_STEPS`` started steps without terminal status; or
* more than ``MONITOR_RUNAWAY_SECONDS`` elapsed seconds without terminal status.

The orchestrator also has its own hard ``MAX_STEPS`` bound, so the monitor is an
operator visibility/control layer rather than the only safety mechanism.
"""
from __future__ import annotations

import asyncio
import os
import time
from dataclasses import dataclass, field
from typing import Any

from .streaming import PROGRESS_STREAM, get_redis, request_cancel

MONITOR_RUNAWAY_STEPS = int(os.getenv("MONITOR_RUNAWAY_STEPS", "5"))
MONITOR_RUNAWAY_SECONDS = float(os.getenv("MONITOR_RUNAWAY_SECONDS", "120"))
MONITOR_BLOCK_MILLISECONDS = int(os.getenv("MONITOR_BLOCK_MILLISECONDS", "500"))
MONITOR_BATCH_COUNT = int(os.getenv("MONITOR_BATCH_COUNT", "100"))
TERMINAL_STATUSES = {"completed", "cancelled", "failed", "terminated"}


@dataclass
class RunObservation:
    first_ts: float
    last_ts: float
    steps_started: set[int] = field(default_factory=set)
    terminal: bool = False
    cancel_requested: bool = False

    @property
    def step_count(self) -> int:
        return len(self.steps_started)


def _event_ts(fields: dict[str, Any]) -> float:
    try:
        return float(fields.get("ts") or time.time())
    except (TypeError, ValueError):
        return time.time()


def _event_step(fields: dict[str, Any]) -> int | None:
    raw = fields.get("step")
    if raw in (None, ""):
        return None
    try:
        return int(raw)
    except (TypeError, ValueError):
        return None


def _is_terminal_event(fields: dict[str, Any]) -> bool:
    event_type = str(fields.get("event_type") or "")
    status = str(fields.get("status") or "")
    return event_type == "run_finished" or status in TERMINAL_STATUSES


async def _handle_event(client, states: dict[str, RunObservation], fields: dict[str, Any]) -> None:
    run_id = str(fields.get("run_id") or "")
    if not run_id:
        return

    ts = _event_ts(fields)
    state = states.get(run_id)
    if state is None:
        state = RunObservation(first_ts=ts, last_ts=ts)
        states[run_id] = state
    state.last_ts = max(state.last_ts, ts)

    if _is_terminal_event(fields):
        state.terminal = True
        return

    if state.terminal:
        return

    event_type = str(fields.get("event_type") or "")
    if event_type == "step_started":
        step = _event_step(fields)
        if step is not None:
            state.steps_started.add(step)

    age = max(0.0, time.time() - state.first_ts)
    runaway_by_steps = state.step_count > MONITOR_RUNAWAY_STEPS
    runaway_by_time = age > MONITOR_RUNAWAY_SECONDS

    if (runaway_by_steps or runaway_by_time) and not state.cancel_requested:
        await request_cancel(client, run_id)
        state.cancel_requested = True


async def monitor_once(stop_after_idle_polls: int = 50) -> None:
    """Observe run progress and stop any run that has gone runaway.

    This function is suitable for tests and one-shot operation. It starts at the
    beginning of the stream by default so it can catch recent runs; set
    ``MONITOR_START_ID=$`` for only new events in a long-running deployment.
    It exits after ``stop_after_idle_polls`` consecutive empty reads.
    """
    client = get_redis()
    last_id = os.getenv("MONITOR_START_ID", "0-0")
    idle_polls = 0
    states: dict[str, RunObservation] = {}

    try:
        while idle_polls < stop_after_idle_polls:
            response = await client.xread(
                {PROGRESS_STREAM: last_id},
                count=MONITOR_BATCH_COUNT,
                block=MONITOR_BLOCK_MILLISECONDS,
            )
            if not response:
                idle_polls += 1
                continue

            idle_polls = 0
            for _stream_name, entries in response:
                for message_id, fields in entries:
                    last_id = message_id
                    await _handle_event(client, states, fields)
    finally:
        await client.aclose()


def main() -> None:
    asyncio.run(monitor_once())


if __name__ == "__main__":
    main()
