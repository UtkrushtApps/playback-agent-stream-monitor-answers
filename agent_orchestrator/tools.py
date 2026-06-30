"""Support tools available to the playback agent. Complete scaffold.

These are deterministic, side-effect-free lookups against static fixtures so the
focus stays on orchestration, observability, and cancellation.
"""
from __future__ import annotations

from typing import Any

_SUBSCRIPTIONS = {
    "u-100": {"status": "active", "tier": "premium"},
    "u-200": {"status": "expired", "tier": "basic"},
}

_DEVICES = {
    "roku-ultra": {"max_resolution": "4k", "hdr": True},
    "old-tv-2015": {"max_resolution": "1080p", "hdr": False},
}

_OUTAGES = {
    "us-east": {"active": False},
    "eu-west": {"active": True, "eta_minutes": 30},
}


def lookup_subscription(user_id: str) -> dict[str, Any]:
    return _SUBSCRIPTIONS.get(user_id, {"status": "unknown"})


def check_device_capability(device_model: str) -> dict[str, Any]:
    return _DEVICES.get(device_model, {"max_resolution": "unknown"})


def check_outage(region: str) -> dict[str, Any]:
    return _OUTAGES.get(region, {"active": False})


TOOLS = {
    "lookup_subscription": lookup_subscription,
    "check_device_capability": check_device_capability,
    "check_outage": check_outage,
}


def run_tool(name: str, args: dict[str, Any]) -> dict[str, Any]:
    fn = TOOLS.get(name)
    if fn is None:
        return {"error": f"unknown tool: {name}"}
    return fn(**args)
