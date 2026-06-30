"""Redis Stream progress and cooperative-cancellation helpers.

Event design
============
Progress is written to two Redis Streams:

* ``agent:progress`` is the fleet-wide stream consumed by the monitor.
* ``agent:run:{run_id}:events`` is a per-run audit stream that makes a single
  run easy to inspect and ensures each run leaves externally observable state.

Events are intentionally concise and audit-safe. The schema records only
operational boundaries/status, short summaries, and non-sensitive metadata such
as action/tool names and argument key names. It does not publish raw customer
requests, prompts, secrets, model messages, or chain-of-thought.

Cancellation uses a separate Redis string key, ``agent:cancel:{run_id}``, with a
TTL. The orchestrator checks this key at safe points between model/tool calls
and before mutating terminal state.
"""
from __future__ import annotations

import json
import time
from typing import Any

import redis.asyncio as redis

from .config import CONFIG

PROGRESS_STREAM = "agent:progress"
RUN_STREAM_PREFIX = "agent:run:"
RUN_STREAM_SUFFIX = ":events"
CANCEL_KEY_PREFIX = "agent:cancel:"
STREAM_MAXLEN = 10_000
CANCEL_TTL_SECONDS = 24 * 60 * 60
SCHEMA_VERSION = "1"

_SENSITIVE_KEY_PARTS = (
    "api_key",
    "apikey",
    "secret",
    "token",
    "password",
    "authorization",
    "cookie",
    "prompt",
    "chain_of_thought",
    "chain-of-thought",
)


def get_redis() -> redis.Redis:
    return redis.from_url(CONFIG.redis_url, decode_responses=True)


def run_stream_key(run_id: str) -> str:
    return f"{RUN_STREAM_PREFIX}{run_id}{RUN_STREAM_SUFFIX}"


def cancel_key(run_id: str) -> str:
    return f"{CANCEL_KEY_PREFIX}{run_id}"


def _safe_summary(value: Any, default: str = "progress update") -> str:
    text = str(value or default).replace("\n", " ").replace("\r", " ").strip()
    return text[:160]


def _redact(value: Any) -> Any:
    """Return a JSON-serializable, size-limited, sensitive-key-redacted value."""
    if isinstance(value, dict):
        out: dict[str, Any] = {}
        for k, v in value.items():
            key = str(k)
            lowered = key.lower()
            if any(part in lowered for part in _SENSITIVE_KEY_PARTS):
                out[key] = "[redacted]"
            else:
                out[key] = _redact(v)
        return out
    if isinstance(value, list):
        return [_redact(v) for v in value[:20]]
    if isinstance(value, tuple):
        return [_redact(v) for v in list(value)[:20]]
    if isinstance(value, (str, int, float, bool)) or value is None:
        if isinstance(value, str):
            return value[:240]
        return value
    return str(value)[:240]


def _event_fields(run_id: str, event: dict[str, Any]) -> dict[str, str]:
    event_type = str(event.get("event_type") or event.get("phase") or "progress")
    phase = str(event.get("phase") or event_type)
    status = str(event.get("status") or "")
    step = event.get("step", "")

    details = event.get("details") or {}
    if not isinstance(details, dict):
        details = {"value": details}

    fields = {
        "schema_version": SCHEMA_VERSION,
        "ts": f"{time.time():.6f}",
        "run_id": run_id,
        "event_type": event_type,
        "phase": phase,
        "step": str(step),
        "status": status,
        "summary": _safe_summary(event.get("summary"), default=event_type),
        "details": json.dumps(_redact(details), sort_keys=True, separators=(",", ":")),
    }
    return fields


async def publish_progress(client: redis.Redis, run_id: str, event: dict[str, Any]) -> None:
    """Surface one unit of run progress so a separate process can observe it.

    The same normalized event is appended to the global monitor stream and the
    per-run stream. ``MAXLEN`` bounds Redis memory while leaving recent audit
    history available.
    """
    fields = _event_fields(run_id, event)
    await client.xadd(PROGRESS_STREAM, fields, maxlen=STREAM_MAXLEN, approximate=True)
    await client.xadd(run_stream_key(run_id), fields, maxlen=STREAM_MAXLEN, approximate=True)


async def request_cancel(client: redis.Redis, run_id: str) -> None:
    """Signal that a given run should stop cooperatively.

    Cancellation is idempotent: repeated requests refresh the TTL and publish a
    concise audit event. The orchestrator remains responsible for stopping only
    at safe points and writing terminal state once.
    """
    payload = json.dumps({"requested_at": time.time(), "run_id": run_id})
    await client.set(cancel_key(run_id), payload, ex=CANCEL_TTL_SECONDS)
    await publish_progress(
        client,
        run_id,
        {
            "event_type": "cancellation_requested",
            "phase": "control",
            "status": "cancel_requested",
            "summary": "cancellation requested",
        },
    )


async def is_cancel_requested(client: redis.Redis, run_id: str) -> bool:
    """Report whether a stop has been requested for a given run."""
    return bool(await client.exists(cancel_key(run_id)))
