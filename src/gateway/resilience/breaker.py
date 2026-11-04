"""Circuit breaker policy.

The authoritative state lives in Redis (`lua/breaker.lua`) so every replica agrees.
This module holds the same policy in pure Python, used for:
  * per-replica counting between flushes to Redis,
  * the degrade-open fallback when Redis is unreachable,
  * unit tests that pin the semantics the Lua script must match.
"""

from __future__ import annotations

from dataclasses import dataclass
from enum import IntEnum


class BreakerState(IntEnum):
    # Int values double as the `breaker_state` gauge (0/1/2).
    CLOSED = 0
    HALF_OPEN = 1
    OPEN = 2

    @classmethod
    def parse(cls, value: str | bytes) -> BreakerState:
        text = value.decode() if isinstance(value, bytes) else value
        return cls[text.upper()]


@dataclass(frozen=True, slots=True)
class BreakerPolicy:
    window_s: int = 10
    failure_ratio: float = 0.5
    min_volume: int = 20
    cooldown_ms: int = 5_000

    def should_trip(self, total: int, failures: int) -> bool:
        # The minimum-volume gate stops one failure in a quiet window from tripping it.
        return total >= self.min_volume and failures / total > self.failure_ratio


class RollingWindow:
    """Per-second (total, failures) buckets over the last `window_s` seconds."""

    __slots__ = ("_buckets", "_window_s")

    def __init__(self, window_s: int) -> None:
        self._window_s = window_s
        self._buckets: dict[int, list[int]] = {}

    def add(self, second: int, failed: bool) -> None:
        bucket = self._buckets.get(second)
        if bucket is None:
            bucket = self._buckets[second] = [0, 0]
        bucket[0] += 1
        if failed:
            bucket[1] += 1

    def totals(self, now_s: int) -> tuple[int, int]:
        horizon = now_s - self._window_s
        for second in [s for s in self._buckets if s <= horizon]:
            del self._buckets[second]
        total = sum(b[0] for b in self._buckets.values())
        failures = sum(b[1] for b in self._buckets.values())
        return total, failures

    def drain(self) -> dict[int, list[int]]:
        """Hand over the accumulated buckets (used to flush deltas to Redis)."""
        buckets, self._buckets = self._buckets, {}
        return buckets

    def clear(self) -> None:
        self._buckets.clear()


class LocalBreaker:
    """Single-replica breaker: same transitions as breaker.lua, state kept in memory."""

    __slots__ = ("_opened_at_ms", "_policy", "_probe_inflight", "_window", "state")

    def __init__(self, policy: BreakerPolicy) -> None:
        self._policy = policy
        self._window = RollingWindow(policy.window_s)
        self._opened_at_ms = 0.0
        self._probe_inflight = False
        self.state = BreakerState.CLOSED

    def record(self, now_ms: float, failed: bool) -> None:
        self._window.add(int(now_ms // 1000), failed)

    def counts(self, now_ms: float) -> tuple[int, int]:
        """(total, failures) in the rolling window."""
        return self._window.totals(int(now_ms // 1000))

    def tick(self, now_ms: float) -> BreakerState:
        if self.state is BreakerState.CLOSED:
            total, failures = self._window.totals(int(now_ms // 1000))
            if self._policy.should_trip(total, failures):
                self._open(now_ms)
        elif self.state is BreakerState.OPEN:
            if now_ms - self._opened_at_ms >= self._policy.cooldown_ms:
                self.state = BreakerState.HALF_OPEN
                self._probe_inflight = False
        return self.state

    def try_acquire_probe(self) -> bool:
        if self.state is not BreakerState.HALF_OPEN or self._probe_inflight:
            return False
        self._probe_inflight = True
        return True

    def probe_result(self, now_ms: float, success: bool) -> None:
        self._probe_inflight = False
        if success:
            self.state = BreakerState.CLOSED
            self._window.clear()
        else:
            self._open(now_ms)

    def _open(self, now_ms: float) -> None:
        self.state = BreakerState.OPEN
        self._opened_at_ms = now_ms
        self._window.clear()
