"""Environment and connection configuration for the orchestration service.

Deliberately does not hard-code any operational policy values (such as what
counts as a runaway run, timeouts, or status vocabulary). Those are design
decisions left to the orchestration logic.
"""
from __future__ import annotations

import os
from dataclasses import dataclass

from dotenv import load_dotenv

load_dotenv()


@dataclass(frozen=True)
class Config:
    redis_url: str
    model: str
    test_mode: bool

    @staticmethod
    def load() -> "Config":
        return Config(
            redis_url=os.getenv("REDIS_URL", "redis://localhost:6379/0"),
            model=os.getenv("AGENT_MODEL", "gpt-4o-mini"),
            test_mode=os.getenv("AGENT_TEST_MODE", "0") == "1",
        )


CONFIG = Config.load()
