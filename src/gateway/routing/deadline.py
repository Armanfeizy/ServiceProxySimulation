"""One deadline per caller request, created at ingress and shared by every attempt."""

from __future__ import annotations

import time
from collections.abc import Callable


class Deadline:
    __slots__ = ("_clock", "_expires_at", "budget_s")

    def __init__(
        self,
        budget_s: float,
        *,
        started_at: float | None = None,
        clock: Callable[[], float] = time.monotonic,
    ) -> None:
        """`started_at` (same clock) lets the budget start at ingress, not at the handler."""
        self._clock = clock
        self.budget_s = budget_s
        self._expires_at = (clock() if started_at is None else started_at) + budget_s

    def remaining(self) -> float:
        return max(0.0, self._expires_at - self._clock())

    def expired(self) -> bool:
        return self.remaining() <= 0.0

    def attempt_timeout(self, provider_timeout_s: float) -> float:
        """Each attempt gets min(provider ceiling, what the caller has left)."""
        return min(provider_timeout_s, self.remaining())
