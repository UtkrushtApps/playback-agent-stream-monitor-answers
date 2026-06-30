"""Complete, production-safe LLM wrapper for the orchestration loop.

Live path uses litellm against the candidate's provider key. When
AGENT_TEST_MODE=1, returns deterministic fixture responses so readiness and
offline invariant tests run without a key. This module is COMPLETE — it is not
part of the candidate's work.

The loop asks the model, per step, for a JSON decision describing the next
action. The returned summary is intentionally a short operational phrase, never
full hidden reasoning.
"""
from __future__ import annotations

import json
from dataclasses import dataclass
from typing import Any

from .config import CONFIG


@dataclass
class StepDecision:
    action: str  # "call_tool" or "final"
    tool: str | None
    tool_args: dict[str, Any]
    summary: str  # short, audit-safe operational summary of the step
    final_answer: str | None


_SYSTEM_PROMPT = (
    "You are a media-streaming playback support agent. You resolve a customer's "
    "playback issue step by step. Available tools: lookup_subscription(user_id), "
    "check_device_capability(device_model), check_outage(region). "
    "At each step respond ONLY with a compact JSON object with keys: "
    "action ('call_tool' or 'final'), tool (tool name or null), "
    "tool_args (object), summary (a SHORT operational phrase describing the step, "
    "no internal reasoning), final_answer (string or null). "
    "Do not include chain-of-thought. Keep summary under 12 words."
)


def _fixture_decision(history: list[dict[str, Any]]) -> StepDecision:
    """Deterministic offline behavior for AGENT_TEST_MODE.

    Models a normal short resolution. A request flagged as runaway in its
    metadata keeps choosing to call a tool, never terminating on its own, so
    tests can exercise runaway detection and cancellation.
    """
    runaway = bool(history and history[0].get("_runaway"))
    tool_steps = sum(1 for h in history if h.get("role") == "tool")
    if runaway:
        return StepDecision(
            action="call_tool",
            tool="check_outage",
            tool_args={"region": "us-east"},
            summary="re-checking outage status",
            final_answer=None,
        )
    if tool_steps == 0:
        return StepDecision(
            action="call_tool",
            tool="lookup_subscription",
            tool_args={"user_id": "u-100"},
            summary="checking subscription entitlement",
            final_answer=None,
        )
    return StepDecision(
        action="final",
        tool=None,
        tool_args={},
        summary="providing resolution",
        final_answer="Your subscription is active; please update the app to the latest version.",
    )


def decide_next_step(history: list[dict[str, Any]]) -> StepDecision:
    """Ask the model (or fixture) for the next plan-execute decision."""
    if CONFIG.test_mode:
        return _fixture_decision(history)

    import litellm

    messages = [{"role": "system", "content": _SYSTEM_PROMPT}]
    for h in history:
        if h.get("role") == "tool":
            messages.append(
                {
                    "role": "user",
                    "content": f"Tool {h.get('tool')} returned: {json.dumps(h.get('result'))}",
                }
            )
        elif h.get("role") == "user":
            messages.append({"role": "user", "content": str(h.get("content", ""))})

    resp = litellm.completion(
        model=CONFIG.model,
        messages=messages,
        temperature=0,
        response_format={"type": "json_object"},
    )
    raw = resp["choices"][0]["message"]["content"]
    data = json.loads(raw)
    return StepDecision(
        action=data.get("action", "final"),
        tool=data.get("tool"),
        tool_args=data.get("tool_args") or {},
        summary=str(data.get("summary", ""))[:120],
        final_answer=data.get("final_answer"),
    )
