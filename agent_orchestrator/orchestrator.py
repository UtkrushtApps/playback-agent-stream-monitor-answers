"""Observable, cancellable plan-execute loop for the playback support agent.

The orchestrator uses bounded autonomy and cooperative cancellation:

* bounded autonomy: a run can execute at most ``MAX_STEPS`` plan/tool cycles;
* observability: every step, model boundary, tool decision/boundary, and
  terminal status is emitted to Redis Streams;
* cancellation: a Redis cancellation key is checked only at safe points, before
  starting work or after model/tool calls complete, so terminal state is written
  consistently and local state is never partially mutated.
"""
from __future__ import annotations

import asyncio
import json
import os
import time
import uuid
from typing import Any

import redis.asyncio as redis

from .llm_client import StepDecision, decide_next_step
from .streaming import is_cancel_requested, publish_progress
from .tools import run_tool

# Operational policy. Environment overrides make the policy tunable without
# changing code; defaults are intentionally conservative for support automation.
MAX_STEPS = int(os.getenv("AGENT_MAX_STEPS", "8"))
MODEL_CALL_TIMEOUT_SECONDS = float(os.getenv("AGENT_MODEL_TIMEOUT_SECONDS", "30"))
TOOL_CALL_TIMEOUT_SECONDS = float(os.getenv("AGENT_TOOL_TIMEOUT_SECONDS", "10"))
RESULT_TTL_SECONDS = int(os.getenv("AGENT_RESULT_TTL_SECONDS", str(24 * 60 * 60)))
RESULT_KEY_PREFIX = "agent:run:"
RESULT_KEY_SUFFIX = ":result"


def new_run_id() -> str:
    return f"run-{uuid.uuid4().hex[:12]}"


def result_key(run_id: str) -> str:
    return f"{RESULT_KEY_PREFIX}{run_id}{RESULT_KEY_SUFFIX}"


def _initial_history(request: dict[str, Any]) -> list[dict[str, Any]]:
    # Keep the raw ticket inside process memory for the model/tool loop only. It
    # is never included in progress events. The fixture LLM looks for _runaway on
    # the first history item, so preserve that flag for offline tests.
    return [
        {
            "role": "user",
            "content": str(request.get("issue", "Playback support request")),
            "_runaway": bool(request.get("_runaway")),
        }
    ]


def _decision_details(decision: StepDecision) -> dict[str, Any]:
    # Publish argument key names, not values, to avoid PII such as user_id.
    return {
        "action": decision.action,
        "tool": decision.tool,
        "tool_arg_keys": sorted(str(k) for k in (decision.tool_args or {}).keys()),
    }


def _tool_result_summary(tool_name: str | None, result: dict[str, Any]) -> str:
    if "error" in result:
        return "tool returned error"
    if tool_name == "lookup_subscription":
        return f"subscription {result.get('status', 'checked')}"
    if tool_name == "check_device_capability":
        return "device capability checked"
    if tool_name == "check_outage":
        return "active outage found" if result.get("active") else "no active outage"
    return "tool completed"


async def _call_model(history: list[dict[str, Any]]) -> StepDecision:
    # ``decide_next_step`` is synchronous and may call a real provider. Run it in
    # a worker thread and bound the await so the orchestration coroutine cannot
    # hang forever on a slow provider call.
    return await asyncio.wait_for(
        asyncio.to_thread(decide_next_step, history),
        timeout=MODEL_CALL_TIMEOUT_SECONDS,
    )


async def _call_tool(name: str, args: dict[str, Any]) -> dict[str, Any]:
    return await asyncio.wait_for(
        asyncio.to_thread(run_tool, name, args),
        timeout=TOOL_CALL_TIMEOUT_SECONDS,
    )


async def _record_terminal(
    client: redis.Redis,
    run_id: str,
    *,
    status: str,
    reason: str,
    steps: int,
    started_at: float,
    final_answer: str | None = None,
) -> dict[str, Any]:
    terminal = {
        "run_id": run_id,
        "status": status,
        "reason": reason,
        "steps": steps,
        "duration_seconds": round(time.time() - started_at, 3),
        "final_answer": final_answer,
        "cancelled": status == "cancelled",
        "terminal": True,
    }
    # Store terminal state before publishing run_finished so readers that react
    # to the terminal event can immediately fetch a consistent result.
    await client.set(result_key(run_id), json.dumps(terminal, sort_keys=True), ex=RESULT_TTL_SECONDS)
    await publish_progress(
        client,
        run_id,
        {
            "event_type": "run_finished",
            "phase": "terminal",
            "status": status,
            "step": steps,
            "summary": reason,
            "details": {"duration_seconds": terminal["duration_seconds"]},
        },
    )
    return terminal


async def run_agent(
    client: redis.Redis,
    run_id: str,
    request: dict[str, Any],
) -> dict[str, Any]:
    """Drive a single ticket to resolution as an observable, cancellable run."""
    started_at = time.time()
    history = _initial_history(request)
    completed_steps = 0

    await publish_progress(
        client,
        run_id,
        {
            "event_type": "run_started",
            "phase": "lifecycle",
            "status": "running",
            "summary": "playback ticket received",
            "details": {
                "ticket_id_present": "ticket_id" in request,
                "bounded_max_steps": MAX_STEPS,
            },
        },
    )

    try:
        for step in range(1, MAX_STEPS + 1):
            if await is_cancel_requested(client, run_id):
                return await _record_terminal(
                    client,
                    run_id,
                    status="cancelled",
                    reason="run stopped by cancellation request",
                    steps=completed_steps,
                    started_at=started_at,
                )

            await publish_progress(
                client,
                run_id,
                {
                    "event_type": "step_started",
                    "phase": "step",
                    "status": "running",
                    "step": step,
                    "summary": "starting plan step",
                },
            )

            await publish_progress(
                client,
                run_id,
                {
                    "event_type": "model_call_started",
                    "phase": "model",
                    "status": "running",
                    "step": step,
                    "summary": "requesting next action",
                },
            )
            decision = await _call_model(history)
            await publish_progress(
                client,
                run_id,
                {
                    "event_type": "model_call_finished",
                    "phase": "model",
                    "status": "ok",
                    "step": step,
                    "summary": decision.summary or "model decision received",
                    "details": _decision_details(decision),
                },
            )

            if await is_cancel_requested(client, run_id):
                return await _record_terminal(
                    client,
                    run_id,
                    status="cancelled",
                    reason="run stopped after model boundary",
                    steps=completed_steps,
                    started_at=started_at,
                )

            if decision.action == "final":
                completed_steps = step
                await publish_progress(
                    client,
                    run_id,
                    {
                        "event_type": "step_finished",
                        "phase": "step",
                        "status": "completed",
                        "step": step,
                        "summary": decision.summary or "resolution provided",
                    },
                )
                return await _record_terminal(
                    client,
                    run_id,
                    status="completed",
                    reason="resolution provided",
                    steps=completed_steps,
                    started_at=started_at,
                    final_answer=decision.final_answer,
                )

            if decision.action != "call_tool" or not decision.tool:
                completed_steps = step
                await publish_progress(
                    client,
                    run_id,
                    {
                        "event_type": "step_finished",
                        "phase": "step",
                        "status": "failed",
                        "step": step,
                        "summary": "invalid model action",
                        "details": _decision_details(decision),
                    },
                )
                return await _record_terminal(
                    client,
                    run_id,
                    status="failed",
                    reason="invalid model action",
                    steps=completed_steps,
                    started_at=started_at,
                )

            await publish_progress(
                client,
                run_id,
                {
                    "event_type": "tool_decision",
                    "phase": "tool_decision",
                    "status": "selected",
                    "step": step,
                    "summary": decision.summary or "tool selected",
                    "details": _decision_details(decision),
                },
            )

            if await is_cancel_requested(client, run_id):
                return await _record_terminal(
                    client,
                    run_id,
                    status="cancelled",
                    reason="run stopped before tool execution",
                    steps=completed_steps,
                    started_at=started_at,
                )

            await publish_progress(
                client,
                run_id,
                {
                    "event_type": "tool_call_started",
                    "phase": "tool",
                    "status": "running",
                    "step": step,
                    "summary": "calling support tool",
                    "details": {
                        "tool": decision.tool,
                        "tool_arg_keys": sorted(str(k) for k in decision.tool_args.keys()),
                    },
                },
            )
            tool_result = await _call_tool(decision.tool, decision.tool_args)
            history.append({"role": "tool", "tool": decision.tool, "result": tool_result})
            completed_steps = step

            await publish_progress(
                client,
                run_id,
                {
                    "event_type": "tool_call_finished",
                    "phase": "tool",
                    "status": "ok" if "error" not in tool_result else "error",
                    "step": step,
                    "summary": _tool_result_summary(decision.tool, tool_result),
                    "details": {"tool": decision.tool, "result_keys": sorted(tool_result.keys())},
                },
            )
            await publish_progress(
                client,
                run_id,
                {
                    "event_type": "step_finished",
                    "phase": "step",
                    "status": "completed",
                    "step": step,
                    "summary": "step completed",
                },
            )

            if await is_cancel_requested(client, run_id):
                return await _record_terminal(
                    client,
                    run_id,
                    status="cancelled",
                    reason="run stopped after tool boundary",
                    steps=completed_steps,
                    started_at=started_at,
                )

        return await _record_terminal(
            client,
            run_id,
            status="terminated",
            reason="terminated after maximum autonomous steps",
            steps=completed_steps,
            started_at=started_at,
        )

    except asyncio.TimeoutError:
        return await _record_terminal(
            client,
            run_id,
            status="failed",
            reason="terminated after operation timeout",
            steps=completed_steps,
            started_at=started_at,
        )
    except Exception as exc:
        await publish_progress(
            client,
            run_id,
            {
                "event_type": "run_error",
                "phase": "error",
                "status": "failed",
                "step": completed_steps,
                "summary": "run failed safely",
                "details": {"error_type": type(exc).__name__},
            },
        )
        return await _record_terminal(
            client,
            run_id,
            status="failed",
            reason="run failed safely",
            steps=completed_steps,
            started_at=started_at,
        )
