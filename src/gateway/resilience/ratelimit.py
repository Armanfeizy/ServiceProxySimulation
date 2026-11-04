"""Local token bucket and AIMD policy.

The shared, cross-replica versions run in Redis (`lua/token_bucket.lua`, `lua/aimd.lua`).
These local versions back the degrade-open mode and define the semantics under test.
"""

from __future__ import annotations

import math
from dataclasses import dataclass


class LocalTokenBucket:
    __slots__ = ("_burst", "_last_ms", "_rate", "_tokens")

    def __init__(self, rate: float, burst: float, now_ms: float) -> None:
        self._rate = rate
        self._burst = burst
        self._tokens = burst
        self._last_ms = now_ms

    def set_rate(self, rate: float) -> None:
        self._rate = max(rate, 1e-6)

    def acquire(self, now_ms: float, cost: float = 1.0) -> tuple[bool, int]:
        """Returns (allowed, retry_after_ms)."""
        elapsed = now_ms - self._last_ms
        if elapsed > 0:
            self._tokens = min(self._burst, self._tokens + elapsed * self._rate / 1000.0)
            self._last_ms = now_ms
        if self._tokens >= cost:
            self._tokens -= cost
            return True, 0
        return False, math.ceil((cost - self._tokens) * 1000.0 / self._rate)


@dataclass(slots=True)
class AimdState:
    """Additive-increase / multiplicative-decrease of a provider's effective rate.

    Discovers a provider's real limit instead of trusting a configured guess:
    halve on 429 (at most once per window, so a burst of 429s is one signal),
    creep back up by `step` after every window with no 429.
    """

    max_rps: float
    min_rps: float
    factor: float
    step: float
    window_ms: float
    effective_rps: float = 0.0
    last_decrease_ms: float = -math.inf
    last_increase_ms: float = -math.inf

    def __post_init__(self) -> None:
        if self.effective_rps <= 0:
            self.effective_rps = self.max_rps

    def on_rate_limited(self, now_ms: float) -> None:
        if now_ms - self.last_decrease_ms >= self.window_ms:
            self.effective_rps = max(self.min_rps, self.effective_rps * self.factor)
            self.last_decrease_ms = now_ms
            self.last_increase_ms = now_ms

    def tick(self, now_ms: float) -> None:
        clean = now_ms - self.last_decrease_ms >= self.window_ms
        if clean and now_ms - self.last_increase_ms >= self.window_ms:
            self.effective_rps = min(self.max_rps, self.effective_rps + self.step)
            self.last_increase_ms = now_ms
